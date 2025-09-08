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


class EmergingSkillsStream(TapFaethmStream):
    """
    Emerging Skills stream class, child of Indusrties.
    
    This stream handles Emerging Skills data associated with respective industries.,
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_emerging_extraction_counters = {} 
     
    # Stream configuration
    name: str = "industry_skills"
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
        th.Property("industry_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()


    def post_process(self, row, context):
        """
        Process each row of data after extraction from the API response.
        
        This method enriches skill records with additional context:
        1. Adds the parent industry_id to each skill record
        2. Sets the category to "emerging" for all records in this stream
        3. Assigns an incremental rank based on extraction order within each industry
        4. Adds the country_code from parent context
        
        Args:
            row (dict): The raw skill record from the API
            context (dict): Contextual information from parent stream, containing industry_id and country_code
            
        Returns:
            dict: The processed skill record with additional fields
        """
        
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "emerging"

            # Initialize counter for this industry if not exists
            industry_category = f"{industry_id}"
            if industry_category not in self._skills_emerging_extraction_counters:
                self._skills_emerging_extraction_counters[industry_category] = 0

            self._skills_emerging_extraction_counters[industry_category] += 1
            row["rank"] = self._skills_emerging_extraction_counters[industry_category]
        
        # Add country_code from context
        if context and "country_code" in context:
            row["country_code"] = context["country_code"]
        
        return row
    

class TrendingSkillsStream(TapFaethmStream):

    """
    Trending Skills stream class, child of Indusrties.
    
    This stream handles Trending Skills data associated with respective industries.,
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_trending_extraction_counters = {} 
     
    
    # Stream configuration
    name: str = "industry_skills"
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
        """
        Process each row of data after extraction from the API response.
        
        This method enriches skill records with additional context:
        1. Adds the parent industry_id to each skill record
        2. Sets the category to "trending" for all records in this stream
        3. Assigns an incremental rank based on extraction order within each industry
        4. Adds the country_code from parent context
        
        Args:
            row (dict): The raw skill record from the API
            context (dict): Contextual information from parent stream, containing industry_id and country_code
            
        Returns:
            dict: The processed skill record with additional fields
        """
        
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "trending"

            # Initialize counter for this industry if not exists
            industry_category = f"{industry_id}"
            if industry_category not in self._skills_trending_extraction_counters:
                self._skills_trending_extraction_counters[industry_category] = 0

            self._skills_trending_extraction_counters[industry_category] += 1
            row["rank"] = self._skills_trending_extraction_counters[industry_category]
        
        # Add country_code from context
        if context and "country_code" in context:
            row["country_code"] = context["country_code"]
        
        return row
    

class DecliningSkillsStream(TapFaethmStream):

    """
    Declining Skills stream class, child of Indusrties.
    
    This stream handles Declining Skills data associated with respective industries.,
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_declining_extraction_counters = {} 
     
    
    # Stream configuration
    name: str = "industry_skills"
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
        """
        Process each row of data after extraction from the API response.
        
        This method enriches skill records with additional context:
        1. Adds the parent industry_id to each skill record
        2. Sets the category to "declining" for all records in this stream
        3. Assigns an incremental rank based on extraction order within each industry
        4. Adds the country_code from parent context
        
        Args:
            row (dict): The raw skill record from the API
            context (dict): Contextual information from parent stream, containing industry_id and country_code
            
        Returns:
            dict: The processed skill record with additional fields
        """
        
        if context and "industry_id" in context:
            industry_id = context["industry_id"]
            row["industry_id"] = industry_id
            row["category"] = "declining"

            # Initialize counter for this industry if not exists
            industry_category = f"{industry_id}"
            if industry_category not in self._skills_declining_extraction_counters:
                self._skills_declining_extraction_counters[industry_category] = 0

            self._skills_declining_extraction_counters[industry_category] += 1
            row["rank"] = self._skills_declining_extraction_counters[industry_category]
        
        # Add country_code from context
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