import requests

from test.test_api import *


def main():
    try:
        graph_ids = input("请输入要查询的流转图 ID（多个 ID 用空格分隔）\n<<< ").strip(" ").split(" ")
        response = requests.post(f"{BASE_URL}/graphs/query_graph/", json=graph_ids)
        print("状态码: ", response.status_code)
        print("响应内容: ", response.json())
    except Exception as e:
        print("请求失败: ", str(e))



if __name__ == '__main__':
    main()
