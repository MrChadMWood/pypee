import os
import json
import time
from pykeepass import PyKeePass
from .generate_app_credentials import get_refresh_token

# Retrieves credentials
def get_keepass():
    # Get environment variables
    keyfile = os.getenv('keepass_keyfile')
    key = str(os.getenv('keepass_key'))
    database = os.getenv('keepass_directory')

    # Returns DB instance
    kp = PyKeePass(database, password=key, keyfile=keyfile)

    return kp

def update_refresh_token():   
    kp = get_keepass()
    
    # Collects/parses credentials for refresh token request
    refresher = kp.find_entries(
        title='Ads Refresh Token', first=True
    ).password
    
    refresher = json.loads(refresher)
    
    # Creates link for oauth process, returns token when finished
    refresh_token = get_refresh_token(refresher)

    # Time to expiration (unix)
    exp_time = time.time()+(60*60*24*7)
    
    # Locates Google Ads Entry
    ads_entry = kp.find_entries(title='Ads', first=True)

    # Parses credentials
    cred = ads_entry.password
    cred = json.loads(cred)

    # Updates value in parsed credentials
    cred['refresh_token'] = refresh_token

    # Loads expiration time
    exp = json.loads(ads_entry.notes)
    exp['refresh_token_expires'] = exp_time
    
    # Places parsed credentials/exp back into KP
    ads_entry.password = json.dumps(cred)
    ads_entry.notes    = json.dumps(exp)

    kp.save()
    
    return ads_entry