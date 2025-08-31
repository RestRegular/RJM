import pprint

import requests

from test.test_function import *

result = requests.get(f"{BASE_URL}/job/search_job", params={
    'salary_max': '40'
})

pprint.pprint(result.json())
