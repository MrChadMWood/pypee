from google.ads.googleads.client import GoogleAdsClient
from .res.update_refresh_token import update_refresh_token
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
import proto
import json
import time

class Google_Ads():
    def __init__(self, credentials):
        ref_exp = json.loads(credentials.notes)
        
        # Handles updating refresh token if expired
        if ref_exp['refresh_token_expires'] < time.time():
            print('Google Ads Refresh token has expired.')
            credentials = update_refresh_token()
        
        # Initializes client
        self.client = GoogleAdsClient.load_from_dict(
            json.loads(credentials.password)
        )
    
    def get_data(self, request: dict):            
        # The service object, not sure what this is honestly.
        ga_service = self.client.get_service("GoogleAdsService")
        
        # Issues a search request using streaming.
        stream = ga_service.search_stream(
            customer_id=request['customer_id'], 
            query=request['query']
        )
        
        # Converts stram to list of dicts
        data = list()
        for batch in stream:
            for row in batch.results:
                newrow = proto.Message.to_json(
                    row, 
                    preserving_proto_field_name=True, 
                    use_integers_for_enums=False,
                    including_default_value_fields=False
                )
                data.append(json.loads(newrow))

        return data


class Google_Analytics4():  
    def __init__(self, credentials):
        credentials = json.loads(credentials.password)
        keys = service_account.Credentials.from_service_account_info(credentials)
        self.client = BetaAnalyticsDataClient(credentials=keys)

    # Runs the provided request
    def __run_request__(self, request_obj):
        return self.client.run_report(request_obj)

    # Converts a Google Report Response to list_of_dicts or dataframe
    def __format_response__(self, response):
        dim_len = len(response.dimension_headers)
        metric_len = len(response.metric_headers)

        formatted = []
        # Formats each rows data as dict
        for row in response.rows:
            row_data = {}

            # Collects dimension headers as key & dimension vales as value
            for i in range(0, dim_len):
                row_data.update({response.dimension_headers[i].name: row.dimension_values[i].value})
            # Collects metric headers as key & metric vales as value
            for i in range(0, metric_len):
                row_data.update({response.metric_headers[i].name: row.metric_values[i].value})

            formatted.append(row_data)           
            
        return formatted
    
    # Collects and formats data 
    def get_data(self, request_kwargs: dict):            
        # Creates request object
        request_obj = RunReportRequest(**request_kwargs)
        
        # Performs request
        response    = self.__run_request__(request_obj)
        
        # Formats response
        formatted   = self.__format_response__(response)

        return formatted
