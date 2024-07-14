import pandas as pd
import numpy as np

def generate_receiver_ids(start, end):
    start_num = int(start[1:])
    end_num = int(end[1:])
    return [f'D{str(i).zfill(2)}' for i in range(start_num, end_num + 1)]

def process_data(data, receiver_fill=False):
    # 将 RECEIVE_TIME 列转换为 datetime 类型
    data['RECEIVE_TIME'] = pd.to_datetime(data['RECEIVE_TIME'])

    # 创建 TRANSMITTER_ID 映射字典
    transmitter_id_map = {41: 1, 42: 2, 43: 3, 44: 4, 45: 5, 46: 6, 47: 7, 48: 8, 49: 9}

    # 应用映射字典替换 TRANSMITTER_ID
    data['TRANSMITTER_ID'] = data['TRANSMITTER_ID'].replace(transmitter_id_map)

    # 获取所有的 TRANSMITTER_ID
    transmitter_ids = data['TRANSMITTER_ID'].unique()

    # 获取所有的 RECEIVER_ID
    receiver_ids = data['RECEIVER_ID'].unique()
    if receiver_fill:
        min_receiver = min(receiver_ids)
        max_receiver = max(receiver_ids)
        receiver_ids = generate_receiver_ids(min_receiver, max_receiver)

    # 创建一个新的 Excel writer 对象
    with pd.ExcelWriter('dataProcessed.xlsx') as writer_processed:
        for transmitter_id in transmitter_ids:
            # 过滤当前 TRANSMITTER_ID 的数据
            filtered_data = data[data['TRANSMITTER_ID'] == transmitter_id]

            # 按 RECEIVER_ID 升序排序
            filtered_data = filtered_data.sort_values(by='RECEIVER_ID')

            # 重置索引
            filtered_data = filtered_data.reset_index(drop=True)

            # 构建新的 DataFrame，行是分钟，列是 RECEIVER_ID
            pivot_table = pd.pivot_table(filtered_data, index=filtered_data['RECEIVE_TIME'].dt.floor('min'),
                                         columns='RECEIVER_ID', values='positivated RSSI', aggfunc='mean')

            # 精确到两位小数
            pivot_table = pivot_table.round(2)

            if receiver_fill:
                for receiver_id in receiver_ids:
                    if receiver_id not in pivot_table.columns:
                        pivot_table[receiver_id] = np.nan

            pivot_table = pivot_table.sort_index(axis=1)

            # 只保留每行和最大的五个连续值
            def keep_top_five_window(series):
                max_sum = float('-inf')
                best_window = None
                for i in range(len(series) - 4):  # 确保窗口大小为5且是连续的
                    window = series.iloc[i:i + 5]
                    window_sum = window.sum()
                    if window_sum > max_sum:
                        max_sum = window_sum
                        best_window = window

                if best_window is not None:
                    mask = series.index.isin(best_window.index)
                    result_series = series.where(mask, other=np.nan)
                    result_series[mask & result_series.isna()] = 0
                    return result_series
                else:
                    return series.apply(lambda x: np.nan)

            #filtered_pivot_table = pivot_table.apply(keep_top_five_window, axis=1)

            # 将数据写入 dataProcessed.xlsx
            pivot_table.to_excel(writer_processed, sheet_name=f'Transmitter_{transmitter_id}')

    # 读取处理后的数据并进行归一化
    with pd.ExcelWriter('dataNormalized.xlsx') as writer_normalized:
        for transmitter_id in transmitter_ids:
            # 读取每个 sheet
            processed_data = pd.read_excel('dataProcessed.xlsx', sheet_name=f'Transmitter_{transmitter_id}', index_col=0)

            # 对每一行进行归一化处理
            def normalize_series(series):
                window_sum = series.sum()
                if window_sum > 0:
                    return series / window_sum
                else:
                    return series.apply(lambda x: np.nan)

            normalized_data = processed_data.apply(normalize_series, axis=1)

            if receiver_fill:
                for receiver_id in receiver_ids:
                    if receiver_id not in normalized_data.columns:
                        normalized_data[receiver_id] = np.nan

            normalized_data = normalized_data.sort_index(axis=1)

            # 将归一化的数据写入 dataNormalized.xlsx
            normalized_data.to_excel(writer_normalized, sheet_name=f'Normalized_{transmitter_id}')

    print("Two Excel files created successfully.")

def main():
    # 提问是否补全 receiver
    receiver_fill_input = input("数据预处理时是否补全 receiver 编码？ (y/n): ").strip().lower()
    receiver_fill = receiver_fill_input == 'y'

    # 读取表格数据
    data = pd.read_excel('dataOrigin.xlsx')

    # 处理数据
    process_data(data, receiver_fill)

if __name__ == "__main__":
    main()
