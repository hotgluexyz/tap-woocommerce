"""REST client handling, including WooCommerceStream base class."""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional, cast, Callable

import backoff
import requests
from urllib3.exceptions import ProtocolError
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem, Popularity
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from http.client import RemoteDisconnected
logging.getLogger("backoff").setLevel(logging.CRITICAL)


class WooCommerceStream(RESTStream):
    """WooCommerce stream class."""

    error_counter = 0

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        site_url = self.config["site_url"]
        return f"{site_url}/wp-json/wc/v3/"

    def get_wc_version(self):
        if self.config.get("use_old_version"):
            return False
        status_url = f"{self.url_base}system_status"
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers or {})
        try:
            result = self.requests_session.get(url=status_url, headers=headers)
            result_dict = result.json()
        except:
            return True
        if not result_dict.get("environment"):
            return True
        wc_version = result_dict["environment"].get("version")
        wc_version = ".".join(wc_version.split(".")[:-1])
        wc_version = float(wc_version)
        if wc_version >= 5.6:
            return True
        return False

    records_jsonpath = "$[*]"
    software_names = [SoftwareName.FIREFOX.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.MAC.value]
    popularity = [Popularity.POPULAR.value]
    user_agents = UserAgent(software_names=software_names, operating_systems=operating_systems, popularity = popularity, limit=100)
    new_version = None

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("consumer_key"),
            password=self.config.get("consumer_secret"),
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # Get the total pages header
        total_pages = response.headers.get("X-WP-TotalPages")
        if response.status_code >= 400:
            if self.error_counter>20:
                return None
            previous_token = previous_token or 1
            total_pages = previous_token + 1
        else:
            self.error_counter = 0

        if total_pages is None:
            return None

        if previous_token is None:
            return 2

        if int(total_pages) > previous_token:
            return previous_token + 1

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""

        if self.new_version == None:
            self.new_version = self.get_wc_version()

        params: dict = {}
        params["per_page"] = 100
        params["order"] = "asc"
        params["consumer_key"] = self.config.get("consumer_key"),
        params["consumer_secret"] = self.config.get("consumer_secret"),
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            self.start_date = self.get_starting_timestamp(context).replace(tzinfo=None)
            if self.new_version:
                params["modified_after"] = self.start_date.isoformat()
            else:
                lookup_days = self.config.get("check_modify_date", 60)
                params["after"] = (self.start_date - timedelta(days=lookup_days)).isoformat()
        return params

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:

        # Refresh the User-Agent on every request.
        if not self.config.get("user_agent"):
            prepared_request.headers["User-Agent"] = self.user_agents.get_random_user_agent()
        else:
            prepared_request.headers["User-Agent"] = self.config.get("user_agent")
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = prepared_request.path_url
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        self.validate_response(response)
        logging.debug("Response received successfully.")
        return response

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if response.status_code>=400 and self.config.get("ignore_server_errors"):
            return []
        if self.replication_key and not self.new_version:
            for record in extract_jsonpath(
                self.records_jsonpath, input=response.json()
            ):
                record_mod_date = datetime.strptime(
                    record[self.replication_key], "%Y-%m-%dT%H:%M:%S"
                )
                if record_mod_date > self.start_date:
                    yield record
        else:
            yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = self._http_headers
        result["Content-Type"] = "application/json"
        result["User-Agent"] = self.user_agents.get_random_user_agent().strip()
        return result

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code == 401:
            raise FatalAPIError(
                f"Unauthorized: {response.status_code} {response.reason} at {self.path}"
            )
        if response.status_code >= 400 and self.config.get("ignore_server_errors"):
            self.error_counter += 1
            # NOTE: We return because there's no need for further validation
            return
        elif 500 <= response.status_code < 600 or response.status_code in [429, 403, 104]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)
        try:
            response.json()
        except:
            raise RetriableAPIError(f"Invalid JSON: {response.text}")

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                ProtocolError,
                RemoteDisconnected
            ),
            max_tries=10,
            factor=4,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                if child_context:
                    child_stream.sync(context=child_context)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if row.get(self.replication_key) is None:
            if row.get("date_created"):
                row[self.replication_key] = row["date_created"]
            else:
                row[self.replication_key] = datetime(1970,1,1)
        return row
    
    @property
    def timeout(self) -> int:
        """Return the request timeout limit in seconds.

        The default timeout is 300 seconds, or as defined by DEFAULT_REQUEST_TIMEOUT.

        Returns:
            The request timeout limit as number of seconds.
        """
        return 500
    
    def backoff_handler(self, details) -> None:
        """Adds additional behaviour prior to retry.

        By default will log out backoff details, developers can override
        to extend or change this behaviour.

        Args:
            details: backoff invocation details
                https://github.com/litl/backoff#event-handlers
        """
        logging.info(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )
    
    
