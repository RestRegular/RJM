from pprint import pprint

import requests

from test.test_function import *
from rjm.components.data_manager import JobDataBuilder

jobs = []

for i in range(10):
    jobs.append(JobDataBuilder.generate_random_data().build())

result = requests.post( f"{BASE_URL}/job/bat_upload", json=jobs)
pprint(result.json())
