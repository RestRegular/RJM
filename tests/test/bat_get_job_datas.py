import pprint

import requests

from test import *

result = requests.get(f"{BASE_URL}/job/bat_get", params={
    'page': 5,
    'page_size': 2
})

pprint.pprint(result.json())