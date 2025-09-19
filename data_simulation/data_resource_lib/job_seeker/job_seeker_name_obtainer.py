import json
import re

from newrcc.c_console import process

from data_simulation.utils.request_tools import request_and_get_result, join_url


BASE_URL = "https://www.resgain.net"

def crawl_data_of_name_encyclopedia():
    first_name_page = request_and_get_result(join_url(BASE_URL, "xmdq.html"))
    first_name_url_pattern = re.compile(r'<a class="btn btn2" href="([^"]+?)"[^>]+?>(.+?)姓名字大全</a>', re.MULTILINE)
    first_name_urls = first_name_url_pattern.findall(first_name_page)
    names = {}
    with process("正在爬取姓名", "姓名爬取完成", total=len(first_name_urls), length=30, style=3) as progress:
        for first_name_url, first_name in progress(first_name_urls):
            first_name = first_name.strip(' \t')
            names[first_name] = {}
            name_pattern = re.compile(r'<div class="cname">([^<]+?)</div>', re.MULTILINE)
            first_name_page = request_and_get_result(join_url(BASE_URL, first_name_url, gender=1, wx1=None, wx2=None))
            names[first_name]["male"] = [name.strip(' \t') for name in name_pattern.findall(first_name_page)]
            first_name_page = request_and_get_result(join_url(BASE_URL, first_name_url, gender=2, wx1=None, wx2=None))
            names[first_name]["female"] = [name.strip(' \t') for name in name_pattern.findall(first_name_page)]
    with open("../resource/job_seeker_names.json", "w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False)


if __name__ == '__main__':
    crawl_data_of_name_encyclopedia()
