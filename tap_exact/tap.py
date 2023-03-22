from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_exact.streams import (
    ExactStream,
    ItemsStream,
    PurchaseOrdersStream,
    SalesOrderLinesStream,
    SalesOrderStream,
    SupplierProductsStream,
    WarehouseStream,
    PurchaseOrderLinesStream,
    SupplierStream,
    SalesInvoicesStream,
    SalesInvoiceLinesStream,
    SalesItemsPrices,
    StockPositionsStream,
    LogisticsStockPositionsStream,
    Deleted
)

STREAM_TYPES = [
    ItemsStream,
    SalesOrderStream,
    PurchaseOrdersStream,
    WarehouseStream,
    SupplierProductsStream,
    SalesOrderLinesStream,
    PurchaseOrderLinesStream,
    SupplierStream,
    SalesInvoicesStream,
    SalesInvoiceLinesStream,
    SalesItemsPrices,
    LogisticsStockPositionsStream,
    StockPositionsStream,
    Deleted
]


class TapExact(Tap):
    """Exact tap class."""

    name = "tap-exact"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self.config_file = config[0]

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=False),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("current_division", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapExact.cli()
