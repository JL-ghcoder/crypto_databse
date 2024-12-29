import pandas as pd
import os

def check_data_completeness(symbols_list, interval, data_dir="./binance_data", required_start_date=None, required_end_date=None):
    """
    检查指定列表中的标的是否下载完全
    :param symbols_list: 需要检查的交易对列表，例如 ["BTCUSDT", "ETHUSDT"]
    :param interval: 时间间隔，例如 "1h", "1d"
    :param data_dir: 保存的本地数据目录
    :param required_start_date: 数据要求的起始日期 (可选: yyyy-mm-dd)
    :return: 下载状态列表 (missing 和 incomplete 的标的列表)
    """
    missing_files = []  # 缺失的标的
    incomplete_files = []  # 数据不完整的标的

    # 将 required_start_date 转换为时间戳，如果未指定则为 None
    required_start_timestamp = None
    required_end_timestamp = None
    if required_start_date:
        required_start_timestamp = int(pd.to_datetime(required_start_date).timestamp() * 1000)  # 转为毫秒时间戳
    if required_end_date:
        required_end_timestamp = int(pd.to_datetime(required_end_date).timestamp() * 1000)  # 转为毫秒时间戳

    # 遍历所有标的
    for symbol in symbols_list:
        # 构造文件路径
        file_path = os.path.join(data_dir, f"{symbol}_{interval}.pkl")
        
        if not os.path.exists(file_path):
            # 如果文件不存在，则标记为缺失
            missing_files.append(symbol)
            print(f"[缺失] 文件不存在: {file_path}")
        else:
            try:
                # 尝试读取文件
                df = pd.read_pickle(file_path)
                if df.empty:  # 文件存在但没有数据
                    incomplete_files.append(symbol)
                    print(f"[不完整] 文件为空: {file_path}")
                else:
                    if required_start_timestamp:
                        # 检查数据的起始时间是否满足要求
                        file_start_timestamp = int(df["Open time"].iloc[0].timestamp() * 1000)
                        if file_start_timestamp > required_start_timestamp:
                            incomplete_files.append(symbol)
                            print(f"[不完整] 文件起始时间不足: {symbol}, 文件起始时间: {df['Open time'].iloc[0]}，要求起始时间: {required_start_date}")

                    if required_end_timestamp:
                        # 检查数据的结束时间是否满足要求
                        file_end_timestamp = int(df["Open time"].iloc[-1].timestamp() * 1000)
                        if file_end_timestamp < required_end_timestamp:
                            incomplete_files.append(symbol)
                            print(f"[不完整] 文件结束时间不足: {symbol}, 文件结束时间: {df['Open time'].iloc[-1]}，要求结束时间: {required_end_date}")
                    
            except Exception as e:
                # 如果文件损坏，标记为不完整
                incomplete_files.append(symbol)
                print(f"[错误] 无法读取文件: {file_path}, 错误: {e}")

    return {"missing": missing_files, "incomplete": incomplete_files}