import json
import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def obtain_source() -> str:
    chrome_options = webdriver.ChromeOptions()
    path = ChromeDriverManager().install()
    driver = webdriver.Chrome(
        service=Service(path),
        options=chrome_options
    )

    driver.get("https://hanyu.baidu.com/sentence/search?from=aladdin&gssda_res=%7B%22sentence_type%22%3A%22%E7%AD%BE%E5%90%8D%22%7D&query=%E4%B8%AA%E6%80%A7%E7%AD%BE%E5%90%8D%E5%A4%A7%E5%85%A8&smpid=&srcid=51451&tab_type=%E5%85%A8%E9%83%A8&wd=%E4%B8%AA%E6%80%A7%E7%AD%BE%E5%90%8D%E5%A4%A7%E5%85%A8&ret_type=mood")

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


def obtain_signatures(source: str):
    pattern = re.compile(r'<div data-v-65785b50="" class="content">(.+?)</div>', re.MULTILINE)
    results = pattern.findall(source)
    pattern = re.compile(r'<span style="background-color:#fdf6bf;"></span>([^<])', re.MULTILINE)
    _signatures = []
    for result in results:
        s_words = pattern.findall(result)
        signature = "".join(s_words)
        _signatures.append(signature)
    return _signatures


def import_signatures():
    with open("./resource/signature.json", "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == '__main__':
    with open("../resource/signature.html", "w", encoding="utf-8") as f:
        f.write(obtain_source())
    with open("../resource/signature.html", "r", encoding="utf-8") as f:
        signatures = obtain_signatures(f.read())
    with open("../resource/signature.json", "w", encoding="utf-8") as f:
        json.dump(signatures, f, ensure_ascii=False)
