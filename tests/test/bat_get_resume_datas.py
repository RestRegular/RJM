import pprint

import requests

result = requests.get("http://localhost:8000/api/resume/bat_get", params={
    'page': 5,
    'page_size': 2
})

pprint.pprint(result.json())