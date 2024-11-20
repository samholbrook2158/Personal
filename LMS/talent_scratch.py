import json

import pandas as pd
import requests
import base64
from requests.auth import HTTPBasicAuth

api_key = "uc4jikXXiYcIAAauBgT2BWwSTc8xUD"
base_url = "https://nyprta-rightsandrecovery.talentlms.com/api/v1/users/page_size:10"
# one way to call the url .. url = 'https://{}:@{}.talentlms.com/api/v1/users'.format(
          #  "uc4jikXXiYcIAAauBgT2BWwSTc8xUD", "nyprta-rightsandrecovery")
headers = {"Authorization": f"Basic {api_key}"}
#print(base_url, headers)
#response = requests.post(base_url,  auth=HTTPBasicAuth(api_key, ''))
#print(response.status_code)
#return_data = response.json()
#if len(return_data) == 10:
#print(json.dumps(response.json(), indent=4))

page_size = 500
page_number = 1
base_url = "https://nyprta-rightsandrecovery.talentlms.com/api/v1/users/page_size:10"
df = None
while True:
    base_url = f"https://nyprta-rightsandrecovery.talentlms.com/api/v1/users/page_size:{page_size},page_number:{page_number}"
    headers = {"Authorization": f"Basic {api_key}"}
    print(base_url, headers)
    response = requests.post(base_url, auth=HTTPBasicAuth(api_key, ''))
    print(response.status_code)
    if response.status_code != 200:
        break
    return_data = response.json()
    if page_number == 1:
        df = pd.DataFrame(return_data)
    else:
        df = pd.concat([df, pd.DataFrame(return_data)], ignore_index=True)
    if len(return_data) < page_size:
        print(f"return_size {len(return_data)}")
        print("No more pages")
        break
    page_number = page_number +1
    print(f"get_page_number{page_number}")
print(df)
print(len(df.index))

