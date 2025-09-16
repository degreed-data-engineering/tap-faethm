"""Stream class for tap-faethm."""

from typing import Dict, Any, List, Optional
import time
import logging

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

logging.basicConfig(level=logging.INFO)


class TapFaethmStream(RESTStream):
    """Base stream class for Faethm API endpoints."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    # Make all streams opt-in; select explicitly via Meltano rules.
    selected_by_default = False

    REQUEST_TIMEOUT = 300  # 5 minute timeout
    RATE_LIMIT_DELAY = 1   # 1 second between requests

    @property
    def request_decorator(self):
        """Return a decorator that adds a delay between requests."""
        def sleep_on_request(func):
            def wrapper(*args, **kwargs):
                time.sleep(self.RATE_LIMIT_DELAY)  # Add delay before each request
                return func(*args, **kwargs)
            return wrapper
        return sleep_on_request

    @property
    def url_base(self) -> str:
        """Return the base URL for API requests."""
        try:
            return self.config["api_base_url"]
        except Exception:
            logging.exception("Error retrieving base URL from configuration")
            raise

    @property
    def http_headers(self) -> Dict[str, str]:
        """Return the HTTP headers needed for API requests."""
        try:
            api_key = self.config.get("api_key")
            if not api_key:
                raise ValueError("api_key is required in config")
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}",
            }
            return headers
        except Exception:
            logging.exception("Error generating HTTP headers")
            raise

    def get_url_params(
        self,
        context: Optional[dict] = None,
        next_page_token: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Get URL parameters for the API request.
        (Validate required config; most endpoints don't take query params here.)
        """
        try:
            # Validate required config
            country_code = self.config.get("country_code")
            if not country_code:
                raise ValueError("country_code is required in config")
            # No default params used by base class
            return {}
        except Exception:
            logging.exception("Error generating URL parameters")
            raise


class IndustriesStream(TapFaethmStream):
    """Stream for handling Faethm Industries responses."""

    # Stream configuration
    name: str = "industries"
    path: str = "/industries"
    primary_keys: List[str] = ["id"]
    replication_key: Optional[str] = None

    # JSON response parsing
    records_jsonpath: str = "$[*]"

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def get_child_context(
        self,
        record: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate context for child streams from a parent record.
        """
        try:
            industry_id = record.get("id")
            if not industry_id:
                raise KeyError("Industry Id is missing")

            country_code = self.config.get("country_code")
            if not country_code:
                raise ValueError("country_code is required in config")

            return {
                "industry_id": industry_id,
                "country_code": country_code,
            }
        except Exception:
            logging.exception("Error generating child context")
            raise

    def post_process(self, row, context):
        """Enrich each industry record with country_code from config."""
        country_code = self.config.get("country_code")
        if country_code:
            row["country_code"] = country_code
        return row


class EmergingSkillsStream(TapFaethmStream):
    """
    Emerging Skills stream class, child of Industries.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_emerging_extraction_counters = {}

    # Stream configuration
    name: str = "emerging_skills"
    path: str = "/industries/{industry_id}/skills/emerging"
    primary_keys: List[str] = ["id", "industry_id", "category"]
    records_jsonpath: str = "$[*]"

    # Parent stream settings
    parent_stream_type = IndustriesStream
    parent_streams = ["industries"]
    ignore_parent_replication_keys: bool = True

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("rank", th.IntegerType),
        th.Property("category", th.StringType),
        th.Property("industry_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def post_process(self, row, context):
        """Enrich skill rows with parent context, category, rank, and country_code."""
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "emerging"

            key = f"{industry_id}"
            if key not in self._skills_emerging_extraction_counters:
                self._skills_emerging_extraction_counters[key] = 0
            self._skills_emerging_extraction_counters[key] += 1
            row["rank"] = self._skills_emerging_extraction_counters[key]

        if context and "country_code" in context:
            row["country_code"] = context["country_code"]

        return row


class TrendingSkillsStream(TapFaethmStream):
    """
    Trending Skills stream class, child of Industries.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_trending_extraction_counters = {}

    # Stream configuration
    name: str = "trending_skills"
    path: str = "/industries/{industry_id}/skills/trending"
    primary_keys: List[str] = ["id", "industry_id", "category"]
    records_jsonpath: str = "$[*]"

    # Parent stream settings
    parent_stream_type = IndustriesStream
    parent_streams = ["industries"]
    ignore_parent_replication_keys: bool = True

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("rank", th.IntegerType),
        th.Property("category", th.StringType),
        th.Property("industry_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def post_process(self, row, context):
        """Enrich skill rows with parent context, category, rank, and country_code."""
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "trending"

            key = f"{industry_id}"
            if key not in self._skills_trending_extraction_counters:
                self._skills_trending_extraction_counters[key] = 0
            self._skills_trending_extraction_counters[key] += 1
            row["rank"] = self._skills_trending_extraction_counters[key]

        if context and "country_code" in context:
            row["country_code"] = context["country_code"]

        return row


class DecliningSkillsStream(TapFaethmStream):
    """
    Declining Skills stream class, child of Industries.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_declining_extraction_counters = {}

    # Stream configuration
    name: str = "declining_skills"
    path: str = "/industries/{industry_id}/skills/declining"
    primary_keys: List[str] = ["id", "industry_id", "category"]
    records_jsonpath: str = "$[*]"

    # Parent stream settings
    parent_stream_type = IndustriesStream
    parent_streams = ["industries"]
    ignore_parent_replication_keys: bool = True

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("rank", th.IntegerType),
        th.Property("category", th.StringType),
        th.Property("industry_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def post_process(self, row, context):
        """Enrich skill rows with parent context, category, rank, and country_code."""
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "declining"

            key = f"{industry_id}"
            if key not in self._skills_declining_extraction_counters:
                self._skills_declining_extraction_counters[key] = 0
            self._skills_declining_extraction_counters[key] += 1
            row["rank"] = self._skills_declining_extraction_counters[key]

        if context and "country_code" in context:
            row["country_code"] = context["country_code"]

        return row


class SkillsCatalogStream(TapFaethmStream):
    """Standalone skills catalog endpoint."""

    name: str = "skills_list"
    path: str = "/skills"
    primary_keys: List[str] = ["id"]
    records_jsonpath: str = "$.skills[*]"

    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: Optional[dict] = None,
        next_page_token: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Build URL parameters for skills list with cursor-based pagination.
        Adds `limit` from config (default 50) and `cursor_key` when paginating.
        """
        params: Dict[str, Any] = {}
        page_size = self.config.get("page_size") or 50
        params["limit"] = page_size

        if next_page_token:
            params["cursor_key"] = next_page_token

        return params

    def get_next_page_token(
        self, response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """
        Determine the next cursor_key from the response.

        Assumes the endpoint returns an object with a `skills` array and
        that the `cursor_key` corresponds to the last returned `id`.
        When fewer than `limit` records are returned, pagination stops.
        """
        try:
            data = response.json()
            if isinstance(data, dict):
                skills = data.get("skills") or []
            elif isinstance(data, list):
                # Fallback for legacy behavior
                skills = data
            else:
                skills = []

            page_size = int(self.config.get("page_size") or 50)

            if not isinstance(skills, list) or len(skills) == 0:
                return None
            if len(skills) < page_size:
                return None

            last_item = skills[-1]
            next_cursor = last_item.get("id") if isinstance(last_item, dict) else None
            return next_cursor
        except Exception:
            return None
