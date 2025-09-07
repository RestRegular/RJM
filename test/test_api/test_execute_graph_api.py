import requests

from test.test_api import *


def main():
    try:
        data = {
            "executing_graph_ids": [
                "graph_c04f4fb5_9d1b_4a08_8282_83a511e375b5"
            ],
            "context_params": {
                "initial_data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            }
        }
        response = requests.post(f"{BASE_URL}/executions/execute/", json=data)
        print("状态码: ", response.status_code)
        print("响应内容: ", response.json())
        print("执行结果：", "\n".join([str(result["results"]["result_data"]) for gid, result in response.json()["result"].items()]))
    except Exception as e:
        print("请求失败: ", str(e))



if __name__ == '__main__':
    main()
