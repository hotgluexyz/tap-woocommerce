"""Tests standard target features using the built-in SDK tests library."""

import typing as t

import pytest
from singer_sdk.exceptions import AbortedSyncFailedException, AbortedSyncPausedException
from singer_sdk.testing import _get_tap_catalog

from tap_woocommerce.tap import TapWooCommerce
from tap_woocommerce.tests.utils import compare_dicts, load_json_from_s3

# Load configurations from S3
S3_PREFIX = 'tap-woocommerce/default'

SAMPLE_CONFIG = load_json_from_s3(S3_PREFIX, 'config.json')
SAMPLE_STATE = load_json_from_s3(S3_PREFIX, 'state.json')
SAMPLE_CATALOG = load_json_from_s3(S3_PREFIX, 'catalog.json')


class TapWrapper:
    """
    Tap Wrapper to override functions for testing without overriding
    actual Tap behaviour
    """

    def __init__(self, instance):
        self.instance = instance

    def __getattr__(self, name):
        # Forward attribute access to the TapWooCommerce instance
        return getattr(self.instance, name)

    @t.final
    def run_connection_test(self, stream_names=None) -> bool:
        """Run connection test, aborting each stream after 1 record.

        Returns:
            True if the test succeeded.
        """
        streams = self.streams.values()
        # filter only streams in stream_names
        if stream_names:
            streams = [stream for stream in streams if stream.name in stream_names]

        return self.run_sync_dry_run(
            dry_run_record_limit=1,
            streams=streams,
        )


def discovery(config) -> None:
    catalog = _get_tap_catalog(TapWooCommerce, config or {}, select_all=False)
    return catalog


@pytest.fixture(scope="module")  # or "session" for sharing across the entire test run
def tap_instance():
    if SAMPLE_CONFIG and SAMPLE_STATE:
        print("Generating catalog...")
        catalog = discovery(SAMPLE_CONFIG)

        print("Instantiating Tap...")
        instance = TapWooCommerce(
            config=SAMPLE_CONFIG, parse_env_config=True, catalog=catalog, state=SAMPLE_STATE
        )
        yield TapWrapper(instance)
    else:
        raise ValueError("SAMPLE_CONFIG or SAMPLE_STATE is not set")


def test_catalog(tap_instance):
    if SAMPLE_CATALOG:
        catalog = tap_instance.instance.catalog_dict
        assert compare_dicts(catalog, SAMPLE_CATALOG)
    else:
        raise ValueError("SAMPLE_CATALOG is not set")


def test_cli_prints(tap_instance) -> None:
    tap1 = tap_instance
    # Test CLI prints
    tap1.print_version()
    tap1.print_about()
    tap1.print_about(output_format="json")


def test_stream_connections(tap_instance) -> None:
    # Initialize with basic config
    tap1 = tap_instance
    # pass the streams to be tested, if no stream_names passed run_connection_test will sync all streams
    stream_names = ["product_variance", "products"]  # child stream, normal stream
    tap1.run_connection_test(stream_names)


def test_pkeys_in_schema(tap_instance) -> None:
    """Verify that primary keys are actually in the stream's schema."""
    tap = tap_instance
    for name, stream in tap.streams.items():
        pkeys = stream.primary_keys or []
        schema_props = set(stream.schema["properties"].keys())
        for pkey in pkeys:
            error_message = (
                f"Coding error in stream '{name}': "
                f"primary_key '{pkey}' is missing in schema"
            )
            assert pkey in schema_props, error_message


def test_state_partitioning_keys_in_schema(tap_instance) -> None:
    """Verify that state partitioning keys are actually in the stream's schema."""
    tap = tap_instance
    for name, stream in tap.streams.items():
        sp_keys = stream.state_partitioning_keys or []
        schema_props = set(stream.schema["properties"].keys())
        for sp_key in sp_keys:
            assert sp_key in schema_props, (
                f"Coding error in stream '{name}': state_partitioning_key "
                f"'{sp_key}' is missing in schema"
            )


def test_replication_keys_in_schema(tap_instance) -> None:
    """Verify that the replication key is actually in the stream's schema."""
    tap = tap_instance
    for name, stream in tap.streams.items():
        rep_key = stream.replication_key
        if rep_key is None:
            continue
        schema_props = set(stream.schema["properties"].keys())
        assert rep_key in schema_props, (
            f"Coding error in stream '{name}': replication_key "
            f"'{rep_key}' is missing in schema"
        )


def test_pagination(tap_instance):
    """Run standard target tests from the SDK."""

    tap = tap_instance
    # change per_page to test pagination
    tap._config["per_page"] = 1

    stream = [stream for stream in tap.streams.values() if stream.name == "customers"][
        0
    ]
    stream.ABORT_AT_RECORD_COUNT = 2

    context = None
    record_count = 0
    context_list = [context] if context is not None else stream.partitions

    for current_context in context_list or [{}]:
        current_context = current_context or None
        stream._write_starting_replication_value(current_context)
        try:
            for _ in stream.get_records(current_context):
                record_count += 1
                stream._check_max_record_limit(record_count)
        except (AbortedSyncFailedException, AbortedSyncPausedException):
            assert record_count == 2


def test_rep_key(tap_instance):
    """Run standard target tests from the SDK."""

    # test a stream using config start_date, and a stream using state date
    stream_config = [
        stream for stream in tap_instance.streams.values() if stream.name == "coupons"
    ][0]
    stream_state = [
        stream for stream in tap_instance.streams.values() if stream.name == "products"
    ][0]

    streams = [stream_config, stream_state]

    for stream in streams:
        stream.ABORT_AT_RECORD_COUNT = 1

    for stream in streams:
        context = None
        record_count = 0
        context_list = [context] if context is not None else stream.partitions

        for current_context in context_list or [{}]:
            filter_date = stream.stream_state.get(
                "replication_key_value"
            ) or tap_instance.config.get("start_date")
            current_context = current_context or None
            stream._write_starting_replication_value(current_context)
            try:
                for record in stream.get_records(current_context):
                    assert record[stream.replication_key] > filter_date
                    record_count += 1
                    stream._check_max_record_limit(record_count)
            except (AbortedSyncFailedException, AbortedSyncPausedException):
                pass
