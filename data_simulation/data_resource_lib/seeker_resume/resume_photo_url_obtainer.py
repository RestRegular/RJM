import json
import pprint
import time
import re
from typing import List

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

RESUME_PHOTO_URLS_FILE_PATH = "D:\\repositories\\RJM\\data_simulation\\data_resource_lib\\resource\\resume_photo_urls.json"


def obtain_source() -> str:
    chrome_options = webdriver.ChromeOptions()
    driver_path = "C:\\Users\\32280\\.wdm\\drivers\\chromedriver\\win64\\140.0.7339.185\\chromedriver-win32\\chromedriver.exe"
    driver = webdriver.Chrome(
        service=Service(driver_path),
        options=chrome_options
    )

    driver.get("https://image.baidu.com/search/index?word=%E7%AE%80%E5%8E%86%E7%85%A7%E7%89%87")

    try:
        # 滚动到页面底部
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)  # 等待页面加载

        # 滚动回页面顶部
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)  # 等待页面稳定

        return driver.page_source
    finally:
        time.sleep(5)
        driver.quit()


def extract_photo_urls(source_content: str) -> List[str]:
    pattern = re.compile(r'<img src="(https://[^"]+)"[^>]+>', re.MULTILINE)
    return pattern.findall(source_content)


def import_photo_urls() -> List[str]:
    with open(RESUME_PHOTO_URLS_FILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == '__main__':
    source = obtain_source()
    urls = extract_photo_urls(source)
    with open(RESUME_PHOTO_URLS_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(urls, f, ensure_ascii=False)
