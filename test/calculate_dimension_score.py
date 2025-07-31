from pprint import pprint

import requests

from test import *

result = requests.get(f"{BASE_URL}/calculating/match_dimension_score", data={
    'resume_id': 3,
    'job_id': 1,
    'match_dimension': ['education_match']
})

pprint(result.json())