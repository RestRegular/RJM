from pprint import pprint

from test import *
import requests
from src.components.data_manager import ResumeDataBuilder

resumes = []

for i in range(10):
    resumes.append(ResumeDataBuilder.generate_random_data().build())

result = requests.post( f"{BASE_URL}/resume/bat_upload", json=resumes)
pprint(result.json())
