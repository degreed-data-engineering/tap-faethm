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
    ).to_dict()
    

    # def get_child_context(self, record: dict, context: Optional[dict] = None) -> dict:
    #     """
    #     Return a context dictionary for child streams.
        
    #     Args:
    #         record: The current record being processed
    #         context: Optional parent stream's context
            
    #     Returns:
    #         dict with industry_id for child streams
    #     """
    #     return {
    #         "industry_id": record["id"]
    #     }

        
    # def generate_child_contexts(
    #     self,
    #     record: Dict[str, Any],
    #     context: Optional[Dict[str, Any]] = None, 
    # ) -> Dict[str, Any]:
    #     """
    #     Generate context for child streams from a parent record.

    #     Args:
    #         record: The parent stream record containing industry id information
    #         context: Optional context dictionary passed from parent streams

    #     Returns:
    #         Dict containing Industry ID for child streams

    #     Raises:
    #         KeyError: If required profile data is missing from the record
    #     """
    #     try:
    #         if not (industry_id := record.get("id")):
    #             raise KeyError("Industry Id is missing")

    #         return {
    #             "industry_id": industry_id
    #         }
    #     except Exception as e:
    #         logging.exception("Error generating child context")
    #         raise

    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return { "industry_id": record["id"]}
        

# class EmergingSkillsStream(TapFaethmStream):
#     """
#     Emerging Skills stream class, child of Indusrties.
    
#     This stream handles Emerging Skills data associated with respective industries.,
    
#     """

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._skills_emerging_extraction_counters = {} 
     
#     # Stream configuration
#     name: str = "skills_emerging"
#     path: str = "/industries/{industry_id}/skills/emerging"
#     primary_keys: List[str] = ["id"]
#     records_jsonpath: str = "$[*]"
    
#     # Parent stream settings
#     parent_stream_type = IndustriesStream
#     parent_streams = ["industries"] 
#     ignore_parent_replication_keys: bool = True

#     # Stream schema definition
#     schema: Dict[str, Any] = th.PropertiesList(
#         th.Property("id", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("description", th.StringType),
#         th.Property("industry_id", th.StringType),
#         th.Property("category", th.StringType),
#         th.Property("rank", th.IntegerType),
#     ).to_dict()


#     def post_process(self, row, context):
#         """
#         Process each row of data after extraction from the API response.
        
#         This method enriches skill records with additional context:
#         1. Adds the parent industry_id to each skill record
#         2. Sets the category to "emerging" for all records in this stream
#         3. Assigns an incremental rank based on extraction order within each industry
        
#         Args:
#             row (dict): The raw skill record from the API
#             context (dict): Contextual information from parent stream, containing industry_id
            
#         Returns:
#             dict: The processed skill record with additional fields
#         """
        
#         if context and "industry_id" in context:
#             industry_id = context["industry_id"]
#             row["industry_id"] = industry_id
#             row["category"] = "emerging"

#             # Initialize counter for this industry if not exists
#             industry_category = f"{industry_id}_emerging"
#             if industry_category not in self._skills_emerging_extraction_counters:
#                 self._skills_emerging_extraction_counters[industry_category] = 0

#             self._skills_emerging_extraction_counters[industry_category] += 1
#             row["rank"] = self._skills_emerging_extraction_counters[industry_category]
        
#         return row
    

# class TrendingSkillsStream(TapFaethmStream):

#     """
#     Trending Skills stream class, child of Indusrties.
    
#     This stream handles Trending Skills data associated with respective industries.,
    
#     """

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._skills_trending_extraction_counters = {} 
     
    
#     # Stream configuration
#     name: str = "skills_trending"
#     path: str = "/industries/{industry_id}/skills/trending"
#     primary_keys: List[str] = ["id"]
#     records_jsonpath: str = "$[*]"
    
#     # Parent stream settings
#     parent_stream_type = IndustriesStream
#     parent_streams = ["industries"] 
#     ignore_parent_replication_keys: bool = True


#     # Stream schema definition
#     schema: Dict[str, Any] = th.PropertiesList(
#         th.Property("id", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("description", th.StringType),
#         th.Property("industry_id", th.StringType),
#         th.Property("category", th.StringType),
#         th.Property("rank", th.IntegerType),
#     ).to_dict()


#     def post_process(self, row, context):
#         """
#         Process each row of data after extraction from the API response.
        
#         This method enriches skill records with additional context:
#         1. Adds the parent industry_id to each skill record
#         2. Sets the category to "trending" for all records in this stream
#         3. Assigns an incremental rank based on extraction order within each industry
        
#         Args:
#             row (dict): The raw skill record from the API
#             context (dict): Contextual information from parent stream, containing industry_id
            
#         Returns:
#             dict: The processed skill record with additional fields
#         """
        
#         if context and "industry_id" in context:
#             industry_id = context["industry_id"]
#             row["industry_id"] = industry_id
#             row["category"] = "trending"

#             # Initialize counter for this industry if not exists
#             industry_category = f"{industry_id}_trending"
#             if industry_category not in self._skills_trending_extraction_counters:
#                 self._skills_trending_extraction_counters[industry_category] = 0

#             self._skills_trending_extraction_counters[industry_category] += 1
#             row["rank"] = self._skills_trending_extraction_counters[industry_category]
        
#         return row
    

# class DecliningSkillsStream(TapFaethmStream):

#     """
#     Declining Skills stream class, child of Indusrties.
    
#     This stream handles Declining Skills data associated with respective industries.,
    
#     """

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._skills_declining_extraction_counters = {} 
     
    
#     # Stream configuration
#     name: str = "skills_declining"
#     path: str = "/industries/{industry_id}/skills/declining"
#     primary_keys: List[str] = ["id"]
#     records_jsonpath: str = "$[*]"
    
#     # Parent stream settings
#     parent_stream_type = IndustriesStream
#     parent_streams = ["industries"] 
#     ignore_parent_replication_keys: bool = True


#     # Stream schema definition
#     schema: Dict[str, Any] = th.PropertiesList(
#         th.Property("id", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("description", th.StringType),
#         th.Property("industry_id", th.StringType),
#         th.Property("category", th.StringType),
#         th.Property("rank", th.IntegerType),
#     ).to_dict()


#     def post_process(self, row, context):
#         """
#         Process each row of data after extraction from the API response.
        
#         This method enriches skill records with additional context:
#         1. Adds the parent industry_id to each skill record
#         2. Sets the category to "declining" for all records in this stream
#         3. Assigns an incremental rank based on extraction order within each industry
        
#         Args:
#             row (dict): The raw skill record from the API
#             context (dict): Contextual information from parent stream, containing industry_id
            
#         Returns:
#             dict: The processed skill record with additional fields
#         """
        
#         if context and "industry_id" in context:
#             industry_id = context["industry_id"]
#             row["industry_id"] = industry_id
#             row["category"] = "declining"

#             # Initialize counter for this industry if not exists
#             industry_category = f"{industry_id}_declining"
#             if industry_category not in self._skills_declining_extraction_counters:
#                 self._skills_declining_extraction_counters[industry_category] = 0

#             self._skills_declining_extraction_counters[industry_category] += 1
#             row["rank"] = self._skills_declining_extraction_counters[industry_category]
        
#         return row


class DecliningSkillsStream(TapFaethmStream):

    """
    Declining Skills stream class, child of Indusrties.
    
    This stream handles Declining Skills data associated with respective industries.,
    
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._skills_declining_extraction_counters = {} 
     
    
    # Stream configuration
    name: str = "skills_declining"
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
      #  th.Property("category", th.StringType),
      #  th.Property("rank", th.IntegerType),
    ).to_dict()


    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["industry_id"] = context["industry_id"]
        return row
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            if "404 Client Error" in str(e):
                self.logger.warning(
                    f"Jobs do not exist in the project {context.get('industry_id')}. Skipping."
                )
                return []
            else:
                raise

    # def post_process(self, row, context):
    #     """
    #     Process each row of data after extraction from the API response.
        
    #     This method enriches skill records with additional context:
    #     1. Adds the parent industry_id to each skill record
    #     2. Sets the category to "declining" for all records in this stream
    #     3. Assigns an incremental rank based on extraction order within each industry
        
    #     Args:
    #         row (dict): The raw skill record from the API
    #         context (dict): Contextual information from parent stream, containing industry_id
            
    #     Returns:
    #         dict: The processed skill record with additional fields
    #     """
        
    #     if context and "industry_id" in context:
    #         industry_id = context["industry_id"]
    #         row["industry_id"] = industry_id
    #         row["category"] = "declining"

    #         # Initialize counter for this industry if not exists
    #         industry_category = f"{industry_id}_declining"
    #         if industry_category not in self._skills_declining_extraction_counters:
    #             self._skills_declining_extraction_counters[industry_category] = 0

    #         self._skills_declining_extraction_counters[industry_category] += 1
    #         row["rank"] = self._skills_declining_extraction_counters[industry_category]
        
    #     return row