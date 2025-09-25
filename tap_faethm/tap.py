"""faethm tap class."""


from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_faethm.streams import (
    IndustriesStream,
    IndustrySkillsStream,
    SkillsCatalogStream,
    OccupationsStream,
    OccupationSkillsStream,
    OccupationDetailsStream
)

PLUGIN_NAME = "tap-faethm"

STREAM_TYPES = [ 
    IndustriesStream,
    IndustrySkillsStream,
    SkillsCatalogStream,
    OccupationsStream,
    OccupationSkillsStream,
    OccupationDetailsStream
]

class TapFaethm(Tap):
    """Faethm tap class for extracting data from Faethm API."""

    name = "tap-faethm"
    config_jsonschema = th.PropertiesList(
        th.Property("api_base_url", th.StringType, required=False, description="Url base for the source endpoint"),
        th.Property("api_key", th.StringType, required=False, description="API key"),
        th.Property("country_code", th.StringType, required=False, description="coutry code for the data"),
        th.Property("page_size", th.IntegerType, required=False, description="Page size for pagination (default 50)")
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapFaethm.cli