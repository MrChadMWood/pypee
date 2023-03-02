from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi


class Facebook_Ads():
    def __init__(self, credentials):
        self.ad_account_id  = credentials.username

        # Initializes client
        self.client = FacebookAdsApi.init(
            access_token=credentials.password
        )
            

    def get_data(self, request_kwargs: dict):      
        response = AdAccount(self.ad_account_id).get_insights(
            **request_kwargs
        )
        ad_data = [dict(row) for row in response]
        self.last_result = 'pass'

        return ad_data
