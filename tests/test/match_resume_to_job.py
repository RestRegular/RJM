import pprint

import requests

from test import *

result = requests.post(f"{BASE_URL}/matching/resume_to_job", json={
    "ids": [1, 2, 3],
    "num": 10,
    "threshold": 10
})

pprint.pprint(result.json())
