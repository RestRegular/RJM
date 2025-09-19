import os
import json
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv
load_dotenv()

from data_simulation.utils.request_tools import join_url

BASE_URL = "https://www.mxnzp.com/api/address/list"

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")

def main():
    url = join_url(BASE_URL, app_id=APP_ID, app_secret=APP_SECRET)
    result = requests.get(url).json()["data"]
    with open("../resource/city_list.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)


def import_city_list() -> List[Dict[str, Any]]:
    with open("./resource/city_list.json", "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == '__main__':
    main()
