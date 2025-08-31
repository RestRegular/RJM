import pprint

import requests

from test import *

result = requests.get(f"{BASE_URL}/resume/search_resume", params={
    'skills': ['Java'],
    'page': 2
})

pprint.pprint(result.json())
