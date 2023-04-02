from typing import Any, Dict, Optional

from tap_exact.client import ExactStream
from singer_sdk.helpers._state import increment_state
import datetime
import pendulum

REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"

class ExactSyncStream(ExactStream):
    
    def get_starting_time(self, context):
        rep_key = self.get_starting_timestamp(context)
        return rep_key or 1

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        timestamp = self.get_starting_time(context)
        if self.select:
            params["$select"] = self.select
        if self.replication_key and timestamp:
            if timestamp == 1:
                date_filter = f"Timestamp gt {timestamp}"
            else:
                date_filter = f"Timestamp gt {timestamp}L"
            params["$filter"] = date_filter
        if next_page_token:
            params["$skiptoken"] = next_page_token
        return params
    
    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[datetime.datetime]:
        value = self.get_starting_replication_key_value(context)
        if not value:
            return None
        else:
            value = pendulum.parse(value)
        if isinstance(value, datetime.date):
            return None
        return value
    
    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                increment_state(
                    state_dict,
                    replication_key="Timestamp",
                    latest_record=latest_record,
                    is_sorted=treat_as_sorted,
                )