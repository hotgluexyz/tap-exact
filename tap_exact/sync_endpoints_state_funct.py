from typing import Optional
from singer_sdk.helpers._state import (
    reset_state_progress_markers
)

REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"
PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"

def finalize_state_progress_markers(stream_or_partition_state: dict) -> Optional[dict]:
    # function for sync endpoints, only change signpost > than timestamp has been removed as timestamp is an integer that can´t be converted to a date
    stream_or_partition_state.pop(STARTING_MARKER, None)
    if PROGRESS_MARKERS in stream_or_partition_state:
        if "replication_key" in stream_or_partition_state[PROGRESS_MARKERS]:
            # Replication keys valid (only) after sync is complete
            progress_markers = stream_or_partition_state[PROGRESS_MARKERS]
            stream_or_partition_state["replication_key"] = progress_markers.pop(
                "replication_key"
            )
            new_rk_value = progress_markers.pop("replication_key_value")
            stream_or_partition_state["replication_key_value"] = new_rk_value
    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(stream_or_partition_state)