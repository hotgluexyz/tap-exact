from typing import List, Type, Dict

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.helpers._compat import final
from singer_sdk.streams import Stream

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
    Deleted,
    MeStream,
    DivisionsStream,
    GLAccountsStream,
    PurchaseInvoicesStream,
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
    Deleted,
    MeStream,
    DivisionsStream,
    GLAccountsStream,
    PurchaseInvoicesStream,
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
        th.Property("current_division", th.StringType, required=False),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type and not streams_by_type[stream_type][0].ignore_parent_stream:
                parent_stream_type = streams_by_type[stream_type][0].parent_stream_type if type(stream_type.parent_stream_type) == property else stream_type.parent_stream_type
                parents = streams_by_type[parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )

    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if not stream.ignore_parent_stream and stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()


if __name__ == "__main__":
    TapExact.cli()
