from data_simulation.utils.request_tools import request_and_get_result


if __name__ == "__main__":
    result = request_and_get_result("https://image.baidu.com/")
    print(result)
