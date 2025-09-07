import requests

from test.test_api import *

def main():
    with open("./data_flow/domain/executors/result_executor.py", "r", encoding="utf-8") as f:
        files = {
            "file": ("result_executor.py", f)
        }
        try:
            response = requests.post(f"{BASE_URL}/executors/upload_executor/", files=files)
            print("状态码: ", response.status_code)
            print("响应内容: ", response.json())
        except Exception as e:
            print("请求失败: ", str(e))


if __name__ == '__main__':
    main()
