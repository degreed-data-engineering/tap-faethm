"""MySourceName_sample tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_datadog.streams import (
    event_logs,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    event_logs,
]


class TapDatadog(Tap):
    """MySourceName_sample tap class."""
    name = "tap-datadog"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,

            description="Data dog api key"
        ),
        th.Property(
            "app_key",
            th.StringType,
            required=True,

            description="Data dog application key"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "base_url",
            th.StringType,
            default="https://api.datadoghq.com",
            description="The url for the API service"
        ),
        th.Property(
            "query",
            th.StringType,
            default="source:degreed.api env:production",
            description="The url for the API service"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapDatadog.cli()
