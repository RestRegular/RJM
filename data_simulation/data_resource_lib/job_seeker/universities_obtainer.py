import os.path

import requests
import pandas as pd
from io import BytesIO
import pyexcel  # 用于读取 .xls 格式文件

UNIVERSITIES_DATA_FILE_PATH = "D:\\repositories\\RJM\\data_simulation\\data_resource_lib\\resource\\china_universities_2025.csv"


def get_universities_data(file_url):
    try:
        # 1. 模拟浏览器请求，下载 .xls 文件
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        response = requests.get(file_url, headers=headers, timeout=30)
        response.raise_for_status()  # 若请求失败（如404），直接抛出错误
        print("文件下载成功，开始解析...")

        # 2. 用 pyexcel 读取 .xls 文件（转换为列表格式）
        excel_data = BytesIO(response.content)
        sheets = pyexcel.get_sheet(file_type='xls', file_content=excel_data.getvalue())

        # 3. 转换为 pandas DataFrame（便于后续分析）
        # 提取表头（第一行）和数据（其余行）
        raws = [row for row in sheets.to_array() if '' not in row[:-1]]  # 第一行为表头
        df = pd.DataFrame(raws[1:], columns=raws[0])
        print(f"数据解析完成，共 {len(df)} 所高校")
        return df
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        return None


def import_universities() -> pd.DataFrame:
    """
    导入 University 数据
    """
    with open(UNIVERSITIES_DATA_FILE_PATH, "r", encoding="utf-8-sig") as file:
        data_frame = pd.read_csv(file)
        return data_frame


if __name__ == "__main__":
    if os.path.exists(UNIVERSITIES_DATA_FILE_PATH):
        df = pd.read_csv(UNIVERSITIES_DATA_FILE_PATH)

    else:
        # 教育部高校名单 .xls 文件 URL（确保链接有效）
        url = "http://www.moe.gov.cn/jyb_xxgk/s5743/s5744/A03/202506/W020250729615142156867.xls"
        # 获取并处理数据
        df = get_universities_data(url)
        df.to_csv(UNIVERSITIES_DATA_FILE_PATH, index=False, encoding="utf-8-sig")
        print(f"\n数据已保存为：{UNIVERSITIES_DATA_FILE_PATH}")

    if df is not None:
        # 示例1：查看数据结构（前5行 + 列名）
        print("\n数据列名：", df.columns.tolist())
        print("\n前5行数据：")
        print(df.head())

        # 示例2：统计各省份高校数量（需确保Excel表头为"所在地"，若不同需修改）
        if "所在地" in df.columns:
            province_count = df["所在地"].value_counts()
            print("\n各省份高校数量（前10）：")
            print(province_count.head(10))
