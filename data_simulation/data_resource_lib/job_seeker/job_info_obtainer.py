import re
import time
import json
from typing import List, Tuple, Dict, Union

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from data_simulation.utils.request_tool import request_and_get_result


def obtain_source() -> str:
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

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


def obtain_industry_info() -> Dict[str, str]:
    result = request_and_get_result("http://www.job.mohrss.gov.cn/cjobs/jobinfolist/listJobinfolist")
    pattern = re.compile(r'<p class="selects" value="(\d{4})">([^<]+)</p>\s*', re.MULTILINE)
    result = pattern.findall(result)
    return {name: rid for rid, name in result}


def obtain_job_info(page_source: str = None) -> Dict[str, List[Dict[str, str]]]:
    if not page_source:
        with open("../resource/web.html", "r", encoding="utf-8") as file:
            page_source = file.read()
    pattern = re.compile(r'<input id="selectItem_(\d+)" name="([^"]+)"', re.MULTILINE)
    result: List[Tuple[str, str]] = pattern.findall(page_source)
    jobs: Dict[str, List[Dict[str, str]]] = {}
    job_id_map: Dict[str, str] = {}
    for jid, job in result:
        jid_pre = jid[:-4]
        if jid.endswith("0000"):
            jobs[job] = []
            job_id_map[jid_pre] = job
        else:
            if jid_pre not in job_id_map:
                raise ValueError(f"Invalid job id: '{jid}'({job})")
            jobs[job_id_map[jid_pre]].append({jid: job})
    return jobs


def import_job_info() -> Dict[str, Dict[str, List[Dict[str, Union[str, List[str]]]]]]:
    with open("./resource/job_details.json", "r", encoding="utf-8") as file:
        return json.load(file)


if __name__ == '__main__':
    with open("../resource/web.html", "r", encoding="utf-8") as f:
        jobs = obtain_job_info(f.read())

    with open("../resource/job_infos.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4)

    # with open("../resource/industry_info.json", "w", encoding="utf-8") as f:
    #     json.dump(obtain_industry_info(), f, ensure_ascii=False, indent=4)
