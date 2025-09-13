import re
import time
from typing import List, Tuple, Dict

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


def obtain_source() -> str:
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    print("准备打开网页，请关闭 VPN ...")

    time.sleep(5)

    driver.get("http://www.job.mohrss.gov.cn/cjobs/jobinfolist/listJobinfolist")

    try:
        job_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ca11"))
        )
        job_button.click()
        time.sleep(2)
        return driver.page_source
    finally:
        time.sleep(5)
        driver.quit()


def obtain_job_info(page_source: str = None) -> Dict[str, List[str]]:
    if not page_source:
        with open("../resource/web.html", "r", encoding="utf-8") as file:
            page_source = file.read()
    pattern = re.compile(r'<input id="selectItem_(\d+)" name="([^"]+)"', re.MULTILINE)
    result: List[Tuple[str, str]] = pattern.findall(page_source)
    jobs: Dict[str, List[str]] = {}
    job_id_map: Dict[str, str] = {}
    for jid, job in result:
        jid_pre = jid[:-4]
        if jid.endswith("0000"):
            jobs[job] = []
            job_id_map[jid_pre] = job
        else:
            if jid_pre not in job_id_map:
                raise ValueError(f"Invalid job id: '{jid}'({job})")
            jobs[job_id_map[jid_pre]].append(job)
    return jobs


if __name__ == '__main__':
    with open("../resource/web.html", "r", encoding="utf-8") as f:
        print(obtain_job_info(f.read()))
