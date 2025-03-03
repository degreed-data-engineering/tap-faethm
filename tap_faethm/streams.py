"""Stream class for tap-template."""

from typing import Dict, Any, Iterable, Union, List, Optional

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseHATEOASPaginator
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
        import time
        
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
        context: Optional[Dict[str, Any]],
        next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """
        Get URL parameters for the API request.

        Args:
            context: The stream context dictionary
            next_page_token: Token for pagination

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
    ).to_dict()
    

    def get_child_context(
    self,
    record: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate context for child streams from a parent record.

        Args:
            record: The parent stream record containing industry id information
            context: Optional parent stream context

        Returns:
            Dict containing profile ID and profile data for child streams

        Raises:
            KeyError: If required profile data is missing from the record
        """
        try:
            if not (industry_id := record.get("id")):
                raise KeyError("Industry Id is missing")

            return {
                "industry_id": industry_id
            }
        except Exception as e:
            logging.exception("Error generating child context")
            raise
        

class EmergingSkillsStream(TapFaethmStream):
    """
    Emerging Skills stream class, child of Indusrties.
    
    This stream handles Emerging Skills data associated with respective industries.,
    
    """
    
    # Stream configuration
    name: str = "emerging_skills"
    path: str = "/industries/{industry_id}/skills/emerging"
    primary_keys: List[str] = ["id"]
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
        th.Property("industry_id", th.StringType),
    ).to_dict()


    def post_process(self, row, context):
        
        if context and "industry_id" in context:
            row["industry_id"] = context["industry_id"]
        
        return row
    

class TrendingSkillsStream(TapFaethmStream):
    """
    Trending Skills stream class, child of Indusrties.
    
    This stream handles Trending Skills data associated with respective industries.,
    
    """
    
    # Stream configuration
    name: str = "trending_skills"
    path: str = "/industries/{industry_id}/skills/trending"
    primary_keys: List[str] = ["id"]
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
        th.Property("industry_id", th.StringType),
    ).to_dict()


    def post_process(self, row, context):
        
        if context and "industry_id" in context:
            row["industry_id"] = context["industry_id"]
        
        return row
    

class DecliningSkillsStream(TapFaethmStream):
    """
    Declining Skills stream class, child of Indusrties.
    
    This stream handles Declining Skills data associated with respective industries.,
    
    """
    
    # Stream configuration
    name: str = "declining_skills"
    path: str = "/industries/{industry_id}/skills/declining"
    primary_keys: List[str] = ["id"]
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
        th.Property("industry_id", th.StringType),
    ).to_dict()


    def post_process(self, row, context):
        
        if context and "industry_id" in context:
            row["industry_id"] = context["industry_id"]
        
        return row