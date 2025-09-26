"""Stream class for tap-faethm."""

from typing import Dict, Any, List, Optional
import time
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
import logging
logging.basicConfig(level=logging.INFO)


class TapFaethmStream(RESTStream):
    """Base industry stream class for Faethm API endpoints."""

    _LOG_REQUEST_METRIC_URLS: bool = True
    
    # Make all streams opt-in; select explicitly via Meltano rules.
    selected_by_default = False

    REQUEST_TIMEOUT = 300  # 5 minute timeout
    RATE_LIMIT_DELAY = 1  # 1 seconds between requests

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
        """
        Return the base URL for API requests.
        
        Returns:
            str: The base URL.
            
        Raises:
            Exception: If there's an error accessing configuration
        """
        try:
            return self.config["api_base_url"]
        except Exception as e:
            logging.exception("Error retrieving base URL from configuration")
            raise

    @property
    def http_headers(self) -> Dict[str, str]:
        """
        Return the HTTP headers needed for API requests.
        
        Returns:
            Dictionary containing required HTTP headers
            
        Raises:
            Exception: If there's an error accessing configuration
        """

        try:
            api_key = self.config.get("api_key")
            if not api_key:
                raise ValueError("api_key is required in config")
                
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            return headers
        
        except Exception as e:
            logging.exception("Error generating HTTP headers")
            raise


    def get_url_params(
            self,
            context: Optional[dict] = None,
            next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Get URL parameters for the API request.
        
        Args:
            context: The stream context.
            next_page_token: The token for the next page.
        
        Returns:
            Dictionary of URL parameters

        Raises:
            ValueError: If country_code is not provided in config
        """
        try:
            params = {}

            country_code = self.config.get("country_code")
            if not country_code:
                raise ValueError("country_code is required in config")
            return params

        except Exception as e:
            logging.exception("Error generating URL parameters")
            raise
    

class IndustriesStream(TapFaethmStream):
    """Stream for handling faethm Industries responses."""
    
    # Stream configuration
    name: str = "industries"
    path: str = "/industries"
    primary_keys: List[str] = ["id"]
    replication_key: Optional[str] = None
    
    # JSON response parsing
    records_jsonpath: str = "$[*]"

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        # Response metadata
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

        Args:
            record: The parent stream record containing industry id information
            context: Optional context dictionary passed from parent streams

        Returns:
            Dict containing Industry ID and country_code for child streams

        Raises:
            KeyError: If required profile data is missing from the record
        """
        try:
            if not (industry_id := record.get("id")):
                raise KeyError("Industry Id is missing")

            country_code = self.config.get("country_code")
            if not country_code:
                raise ValueError("country_code is required in config")

            return {
                "industry_id": industry_id,
                "country_code": country_code
            }
        except Exception as e:
            logging.exception("Error generating child context")
            raise
        
    def post_process(self, row, context):
        """
        Process each row of data after extraction from the API response.
        
        This method enriches industry records with additional context:
        1. Adds the country_code from configuration to each industry record
        
        Args:
            row (dict): The raw industry record from the API
            context (dict): Contextual information (not used in this stream)
            
        Returns:
            dict: The processed industry record with additional fields
        """
        
        country_code = self.config.get("country_code")
        if country_code:
            row["country_code"] = country_code
        
        return row


class IndustrySkillsStream(TapFaethmStream):
    """
    Industry Skills stream class, child of Indusrties.
    
    This stream handles emerging, trending and  declining skills data associated with respective industries.,
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_extraction_counters = {} 
     
    # Stream configuration
    name: str = "industry_skills"
    path: str = "/industries/{industry_id}/skills/{category}"
    primary_keys: List[str] = ["rank", "industry_id", "category"] 
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


    categories = ["emerging", "trending", "declining"]

    def get_records(self, context):
        industry_id = context["industry_id"]
        # if you want only one category, read from config (e.g. config['skills_category'])
        chosen = self.config.get("skills_category")
        categories = [chosen] if chosen else self.categories

        for category in categories:
            url = self.path.format(industry_id=industry_id, category=category)
            # use RESTStream helper to fetch and parse records for that url
            for record in super().get_records(context={"industry_id": industry_id, "category": category}):
                record["category"] = category
                record["industry_id"] = industry_id

                # Add rank within category
                category_key = f"{industry_id}_{category}"
                if category_key not in self._skills_extraction_counters:
                    self._skills_extraction_counters[category_key] = 0
                self._skills_extraction_counters[category_key] += 1
                record["rank"] = self._skills_extraction_counters[category_key]

                yield record
    
    
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
            next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Build URL parameters for skills list with cursor-based pagination.

        Adds `limit` from config (default 50) and `cursor_key` when paginating.
        """
        params: Dict[str, Any] = {}
        # Page size from config, default to 50
        page_size = self.config.get("page_size") or 50
        params["limit"] = page_size

        # Cursor key for next page
        if next_page_token:
            params["cursor_key"] = next_page_token

        return params

    def get_next_page_token(self, response, previous_token: Optional[Any]) -> Optional[Any]:
        """
        Determine the next cursor_key from the response.

        Assumes the endpoint returns an object with a `skills` array and
        that the `cursor_key` corresponds to the last returned `id`.
        When fewer than `limit` records are returned, pagination stops.
        """
        try:
            data = response.json()
            skills = []
            if isinstance(data, dict):
                skills = data.get("skills") or []
            elif isinstance(data, list):
                # Fallback for legacy behavior
                skills = data

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


class OccupationsStream(TapFaethmStream):
    """Stream for handling Occupations responses."""
    
    # Stream configuration
    name: str = "occupations"
    path: str = "/occupations"
    primary_keys: List[str] = ["id"]
    replication_key: Optional[str] = None
    
    # JSON response parsing
    records_jsonpath: str = "$.occupations[*]"

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def get_url_params(
            self,
            context: Optional[dict] = None,
            next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Build URL parameters for occupations list with cursor-based pagination.
        """
        params = super().get_url_params(context, next_page_token)
        
        # Page size from config, default to 50
        page_size = self.config.get("page_size") or 50
        params["limit"] = page_size

        # Cursor key for next page
        if next_page_token:
            params["cursor_key"] = next_page_token

        return params

    def get_child_context(
        self,
        record: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None, 
    ) -> Dict[str, Any]:
        """Generate context for child streams from a parent record."""
        try:
            if not (occupation_id := record.get("id")):
                raise KeyError("Occupation Id is missing")

            country_code = self.config.get("country_code")
            if not country_code:
                raise ValueError("country_code is required in config")

            return {
                "occupation_id": occupation_id,
                "country_code": country_code
            }
        except Exception as e:
            logging.exception("Error generating child context")
            raise

    def post_process(self, row, context):
        """Process each row of data after extraction."""
        country_code = self.config.get("country_code")
        if country_code:
            row["country_code"] = country_code
        return row

    def get_next_page_token(self, response, previous_token: Optional[Any]) -> Optional[Any]:
        """Get the next page token from response."""
        try:
            data = response.json()
            records = data if isinstance(data, list) else []
            
            page_size = int(self.config.get("page_size") or 50)

            if len(records) < page_size:
                return None

            last_item = records[-1]
            next_cursor = last_item.get("id")
            return next_cursor
        except Exception:
            return None


class OccupationSkillsStream(TapFaethmStream):
    """
    Occupation Skills stream class, child of Occupations.
    
    This stream handles emerging, trending and declining skills data 
    associated with respective occupations.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_extraction_counters = {}
     
    # Stream configuration
    name: str = "occupation_skills"
    path: str = "/occupations/{occupation_id}/skills/{category}"
    primary_keys: List[str] = ["rank", "occupation_id", "category"]
    records_jsonpath: str = "$[*]"
    
    # Parent stream settings
    parent_stream_type = OccupationsStream
    parent_streams = ["occupations"]
    ignore_parent_replication_keys: bool = True

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("rank", th.IntegerType),
        th.Property("category", th.StringType),
        th.Property("occupation_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()

    categories = ["emerging", "trending", "declining"]

    def get_url_params(
            self,
            context: Optional[dict] = None,
            next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Add country_code parameter to requests."""
        params = super().get_url_params(context, next_page_token)
        country_code = self.config.get("country_code", "US")
        params["country_code"] = country_code
        return params

    def get_records(self, context):
        """Get records for all skill categories."""
        occupation_id = context["occupation_id"]
        # if you want only one category, read from config
        chosen = self.config.get("skills_category")
        categories = [chosen] if chosen else self.categories

        for category in categories:
            url = self.path.format(occupation_id=occupation_id, category=category)
            # use RESTStream helper to fetch and parse records for that url
            for record in super().get_records(context={"occupation_id": occupation_id, "category": category}):
                record["category"] = category
                record["occupation_id"] = occupation_id

                # Add rank within category
                category_key = f"{occupation_id}_{category}"
                if category_key not in self._skills_extraction_counters:
                    self._skills_extraction_counters[category_key] = 0
                self._skills_extraction_counters[category_key] += 1
                record["rank"] = self._skills_extraction_counters[category_key]

                yield record


class OccupationDetailsStream(TapFaethmStream):
    """
    Occupation Details stream class, child of Occupations.
    
    This stream handles detailed information for each occupation.
    """
     
    # Stream configuration
    name: str = "occupation_details"
    path: str = "/occupations/{occupation_id}"
    primary_keys: List[str] = ["id"]
    records_jsonpath: str = "$"
    
    # Parent stream settings
    parent_stream_type = OccupationsStream
    parent_streams = ["occupations"]
    ignore_parent_replication_keys: bool = True

    # Stream schema definition
    schema: Dict[str, Any] = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("category_name", th.StringType),
        th.Property("stream", th.StringType),
        th.Property("common_role_names", th.ArrayType(th.StringType)),
        th.Property("level", th.StringType),
        th.Property("top_skills_mentions", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("description", th.StringType),
            th.Property("mentions", th.IntegerType),
            th.Property("skill_percentage", th.NumberType)
        ))),
        th.Property("top_skills_specific", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("description", th.StringType),
            th.Property("skill_percentage", th.NumberType)
        ))),
        th.Property("top_skills_note", th.StringType),

        th.Property("top_tasks", th.ArrayType(th.ObjectType(
            th.Property("id", th.IntegerType),
            th.Property("description", th.StringType),
            th.Property("time_percent", th.IntegerType)
        ))),
        th.Property("country_code", th.StringType),
    ).to_dict()

    def validate_response(self, response):
        """Handle 403 Forbidden responses by logging and continuing."""
        if response.status_code == 403:
            occupation_id = response.url.split('/')[-1]
            logging.warning(f"Received 403 Forbidden for occupation_id: {occupation_id}. Skipping...")
            # Raise a custom exception to skip this record
            raise RuntimeError("Skip 403")
        # For all other status codes, use the parent class validation
        return super().validate_response(response)

    def get_url_params(
            self,
            context: Optional[dict] = None,
            next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Add country_code parameter to requests."""
        params = super().get_url_params(context, next_page_token)
        country_code = self.config.get("country_code", "US")
        params["country_code"] = country_code
        return params

    def get_records(self, context):
        """Get detailed record for a single occupation."""
        occupation_id = context["occupation_id"]
        
        try:
            for record in super().get_records(context={"occupation_id": occupation_id}):
                record["country_code"] = self.config.get("country_code", "US")
                yield record
        except RuntimeError as e:
            if str(e) == "Skip 403":
                # Silently skip this record
                return
            raise
        except Exception as e:
            # Log other errors and continue
            logging.warning(f"Error fetching details for occupation_id {occupation_id}: {str(e)}")
            return