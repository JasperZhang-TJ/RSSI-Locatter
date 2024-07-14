import pandas as pd
import numpy as np

def calculate_new_metric(receiver_id, transmitter_id, rssi, combinations_data, k):
    """
    计算新的指标，根据 RSSI 和接收器位置得分。

    输入:
    - receiver_id: 当前接收器的ID (str)
    - transmitter_id: 当前发射器的ID (str)
    - rssi: 接收到的信号强度指示 (float)
    - combinations_data: 包含之前组合数据的 DataFrame
    - k: 当前的行数 (int)

    输出:
    - 计算得到的指标 (float)
    """
    # 定义位置得分
    score_map = {
        0: 6,
        1: 3,
        -1: 5,
        2: 1,
        -2: 2,
    }

    def get_score(current_position, previous_position, num_receivers):
        diff = -(current_position - previous_position)
        if diff in score_map:
            return score_map[diff]
        else:
            return 0

    # 获取当前接收器的位置索引
    num_receivers = len(combinations_data.columns)
    current_position = combinations_data.columns.get_loc(receiver_id)

    total_score = 0

    # 遍历之前的所有行数据
    for idx, (_, row) in enumerate(combinations_data.iterrows()):
        decay_factor = 0.5 ** (k - idx - 1)
        for prev_receiver_id, prev_transmitter_id in row.items():
            if prev_transmitter_id == transmitter_id:
                previous_position = combinations_data.columns.get_loc(prev_receiver_id)
                total_score += get_score(current_position, previous_position, num_receivers) * decay_factor

    # 最终的指标计算
    new_metric = rssi * total_score
    return new_metric

# 示例调用
receiver_id = "D07"
transmitter_id = "T01"
rssi = -65.5
combinations_data = pd.DataFrame({
    'Timestamp': ["2024-06-27 14:34", "2024-06-27 14:35", "2024-06-27 14:36"],
    'D00': [np.nan, "T01", np.nan],
    'D01': [np.nan, np.nan, np.nan],
    'D02': [np.nan, np.nan, np.nan ],
    'D03': [np.nan, np.nan, np.nan],
    'D04': ["T02", np.nan, np.nan],
    'D05': [np.nan, "T03", np.nan],
    'D06': [np.nan, np.nan, "T01"],
    'D07': [np.nan, np.nan, np.nan],
    'D08': [np.nan, np.nan, np.nan],
    'D09': [np.nan, np.nan, np.nan]
})
combinations_data.set_index('Timestamp', inplace=True)

metric = calculate_new_metric(receiver_id, transmitter_id, rssi, combinations_data)
print(metric)

