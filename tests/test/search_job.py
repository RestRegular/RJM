import pprint

import requests

from test import *

result = requests.get(f"{BASE_URL}/job/search_job", params={
    'salary_max': '40'
})

pprint.pprint(result.json())
