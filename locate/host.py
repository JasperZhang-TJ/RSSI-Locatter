import pandas as pd
import time
import json

def read_kth_row_from_sheets(excel_file, k):
    """
    从 Excel 文件的每个 sheet 读取第 k 行数据，并存储在字典中。

    输入:
    - excel_file: Excel 文件名
    - k: 要读取的行数 (从1开始)

    输出:
    - data_dict: 包含每个 sheet 第 k 行数据的字典
    """
    data_dict = {}
    xls = pd.ExcelFile(excel_file)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        if k <= len(df):
            data_dict[sheet_name] = df.iloc[k - 1]  # 读取第 k 行（从1开始）
        else:
            data_dict[sheet_name] = pd.Series(dtype=float)  # 如果没有第 k 行，返回空的 Series

    return data_dict

def generate_input_json(data_dict, k):
    """
    根据读取的第 k 行数据生成 input.json 文件。

    输入:
    - data_dict: 包含每个 sheet 第 k 行数据的字典
    - k: 当前读取的行数 (从1开始)

    输出:
    - 生成 input.json 文件
    """
    inputs = {}
    timestamp = None  # 用于存储时间戳

    for sheet_name, row in data_dict.items():
        transmitter_id = sheet_name.split('_')[1]  # 从 sheet 名字提取 transmitter_id

        if timestamp is None and 'RECEIVE_TIME' in row.index:
            timestamp = row['RECEIVE_TIME']  # 获取时间戳

        # 过滤出有数值的部分
        numeric_row = row.drop('RECEIVE_TIME').dropna()  # 去掉时间戳列，并去掉空值

        # 检查 numeric_row 是否为空或全为 NaN
        if not numeric_row.empty and numeric_row.notna().any():
            # 使用 max 函数找到最大值及其对应的接收器
            max_rssi_value = numeric_row.max()
            max_rssi_receiver = numeric_row[numeric_row == max_rssi_value].index[0]
            inputs[transmitter_id] = max_rssi_receiver

    # 如果没有获取到时间戳，使用当前时间作为时间戳
    if timestamp is None:
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp = pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    input_data = {
        "identifier": f"unique_id_{k}",
        "timestamp": timestamp,
        "inputs": inputs
    }

    with open('input.json', 'w') as f:
        json.dump(input_data, f, indent=4)

    print(f"Generated input.json for row {k}: {input_data}")

def main():
    excel_file = 'dataProcessed.xlsx'
    k = 1  # 初始化开始迭代的行数

    while True:
        data_dict = read_kth_row_from_sheets(excel_file, k)
        generate_input_json(data_dict, k)
        k += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
