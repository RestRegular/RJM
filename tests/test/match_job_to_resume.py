from pprint import pprint

import requests

from test import *

result = requests.post(f"{BASE_URL}/matching/job_to_resume", json={
    "ids": [],
    "num": 10,
    "threshold": 10
})

pprint(result.json())
