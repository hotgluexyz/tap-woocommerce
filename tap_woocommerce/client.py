"""REST client handling, including WooCommerceStream base class."""

import requests
import copy
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from woocommerce import API


class WooCommerceStream(RESTStream):
    """WooCommerce stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        site_url = self.config["site_url"]
        return f"{site_url}/wp-json/wc/v3"

    records_jsonpath = "$[*]"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # Get the total pages header
        total_pages = response.headers.get("X-WP-TotalPages")
        if total_pages is None:
            return None

        total_pages = int(total_pages)
        # Only increment the next token if there is another page
        if previous_token is not None and total_pages > previous_token:
            return previous_token + 1

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["per_page"] = 100
        params["order"] = "asc"
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            start_date = self.get_starting_timestamp(context).replace(tzinfo=None)
            params["modified_after"] = start_date.isoformat()
            params["after"] = start_date.isoformat()
            params["date_query_column"] = "post_modified"
            
        return params

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.
        """
        wcapi = API(
            url=self.config["site_url"],
            consumer_key=self.config["consumer_key"],
            consumer_secret=self.config["consumer_secret"],
            timeout=900
        )
        # Start at page 1
        next_page_token = 1
        finished = False
        while not finished:
            params = self.get_url_params(context, next_page_token=next_page_token)
            resp = wcapi.get(self.path, params=params)
            for row in self.parse_response(resp):
                # Convert empty string to None
                for (key,value) in row.items():
                    if value=="":
                        row[key] = None
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

