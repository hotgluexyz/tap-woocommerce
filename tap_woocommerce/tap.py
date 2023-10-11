"""WooCommerce tap class."""

from typing import List

from pip import main
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_woocommerce.streams import (
    WooCommerceStream, 
    ProductsStream, 
    OrdersStream, 
    CouponsStream,
    ProductVarianceStream,
    SubscriptionStream,
    CustomersStream,
    StoreSettingsStream,
    OrderNotesStream
)

STREAM_TYPES = [
    ProductsStream,
    OrdersStream,
    CouponsStream,
    ProductVarianceStream,
    SubscriptionStream,
    CustomersStream,
    StoreSettingsStream,
    OrderNotesStream
]


class TapWooCommerce(Tap):
    """WooCommerce tap class."""
    name = "tap-woocommerce"

    config_jsonschema = th.PropertiesList(
        th.Property("consumer_key", th.StringType, required=True),
        th.Property("consumer_secret", th.StringType, required=True),
        th.Property("site_url", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, default="2000-01-01T00:00:00.000Z")
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    TapWooCommerce.cli()