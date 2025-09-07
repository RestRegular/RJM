import requests

from test.test_api import *


def main():
    try:
        response = requests.get(f"{BASE_URL}/graphs/graph_info/")
        print("状态码: ", response.status_code)
        print("响应内容: ", response.json())
    except Exception as e:
        print("请求失败: ", str(e))



if __name__ == '__main__':
    main()
