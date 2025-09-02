"""faethm tap class."""


from typing import List, Dict, Type
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_faethm.streams import (
    IndustriesStream,
    EmergingSkillsStream,
    TrendingSkillsStream,
    DecliningSkillsStream,
    SkillsCatalogStream
)

PLUGIN_NAME = "tap-faethm"

STREAM_GROUPS: Dict[str, List[Type[Stream]]] = {
    "industry_skills": [
        IndustriesStream,
        EmergingSkillsStream,
        TrendingSkillsStream,
        DecliningSkillsStream,
    ],
    "skill_list": [
        SkillsCatalogStream,
    ],
}

class TapFaethm(Tap):
    """Faethm tap class for extracting data from Faethm API."""

    name = "tap-faethm"
    config_jsonschema = th.PropertiesList(
        th.Property("api_base_url", th.StringType, required=False, description="Url base for the source endpoint"),
        th.Property("api_key", th.StringType, required=False, description="API key"),
        th.Property("country_code", th.StringType, required=False, description="coutry code for the data"),
        th.Property("page_size", th.IntegerType, required=False, description="Page size for pagination (default 50)"),
        th.Property(
            "stream_group",
            th.StringType,
            required=False,
            description="Which streams to run: industry_skills | skill_list | all",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        group = (self.config.get("stream_group") or "all").lower()
        if group == "all":
            stream_classes: List[Type[Stream]] = sum(STREAM_GROUPS.values(), [])
        else:
            stream_classes = STREAM_GROUPS.get(group, [])
        return [stream_class(tap=self) for stream_class in stream_classes]


# CLI Execution:
cli = TapFaethm.cli