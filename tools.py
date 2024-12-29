import os
import requests
import pandas as pd

def create_prices_dataframe(data_dir, start_date=None, end_date=None, fields=['Open', 'Close']):
    """
    从指定的目录中提取所有 pkl 文件并生成多索引大表。
    
    :param data_dir: 本地文件夹路径，包含以 pkl 格式存储的各标的数据。
    :param start_date: 数据的开始日期，格式为 "yyyy-mm-dd" (字符串类型或 None)。
    :param end_date: 数据的结束日期，格式为 "yyyy-mm-dd" (字符串类型或 None)。
    :param fields: 要提取的字段列表，默认 ['Open', 'Close']。
    :return: 格式化后的 Pandas DataFrame。
    """
    # 初始化一个空的字典，用于拼接数据
    data_dict = {}

    # 遍历目录中的所有 pkl 文件
    for file_name in os.listdir(data_dir):
        # 检查仅处理以 .pkl 结尾的文件
        if file_name.endswith(".pkl"):
            # 提取 symbol 名称
            symbol = file_name.split("_")[0]

            # 读取 pkl 文件
            file_path = os.path.join(data_dir, file_name)
            try:
                df = pd.read_pickle(file_path)
            except Exception as e:
                print(f"[错误] 无法读取文件 {file_path}: {e}")
                continue

            # 检查所需字段是否在文件中存在
            missing_fields = [field for field in fields if field not in df.columns]
            if missing_fields:
                print(f"[警告] 文件 {file_name} 缺失必要字段 {missing_fields}，跳过处理！")
                continue

            # 保留索引为时间列，并重命名为 trade_date
            if "Open time" not in df.columns:
                print(f"[警告] 文件 {file_name} 缺失 'Open time' 列，跳过处理！")
                continue

            df.rename(columns={"Open time": "trade_date"}, inplace=True)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df.set_index("trade_date", inplace=True)

            # 按时间范围过滤数据
            if start_date:
                start_date_ts = pd.to_datetime(start_date)
                df = df[df.index >= start_date_ts]
            if end_date:
                end_date_ts = pd.to_datetime(end_date)
                df = df[df.index <= end_date_ts]

            # 提取指定字段并加入字典
            data_dict[symbol] = df[fields]

    # 合并所有标的数据
    if data_dict:
        prices_df = pd.concat(data_dict, axis=1)  # 将字典按列（symbol）合并
        prices_df.sort_index(inplace=True)       # 按时间排序索引
        return prices_df
    else:
        print("[警告] 未发现有效数据文件！")
        return pd.DataFrame()  # 返回空 DataFrame

def get_binance_u_based_futures(base_asset="USDT"):
    """
    获取币安所有 U 本位合约交易对并返回为列表
    :param base_asset: Quote asset (默认: USDT, 也可以是 BUSD)
    :return: 包含所有符合条件合约名称的列表
    """
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # 筛选出所有符号 (symbol) 且 quoteAsset 为指定的 USDT
        futures_symbols = [
            symbol["symbol"] for symbol in data["symbols"]
            if symbol["quoteAsset"] == base_asset and symbol["contractType"] == "PERPETUAL"
        ]
        return futures_symbols
    except Exception as e:
        print(f"[错误] 无法获取数据: {e}")
        return []

def resample_to_higher_freq(df, target_freq='1D'):
    """
    将低级别 K线数据合并成高级别数据
    :param df: 低级别的K线数据，包含列 ["Open time", "Close time", "Open", "High", "Low", "Close", "Volume", ...]
    :param target_freq: 目标频率，例如 "1D" 表示合并成日级别数据
    :return: 合并后的 DataFrame
    """

    # 确保 Open time 为 datetime 类型
    df['Open time'] = pd.to_datetime(df['Open time'])

    # 设置 Open time 为索引，以便 resample 操作
    df = df.set_index('Open time')

    # 定义合并逻辑
    resampled = df.resample(target_freq).agg({
        'Open': 'first',              # 第一个 Open 值
        'High': 'max',                # 最高价
        'Low': 'min',                 # 最低价
        'Close': 'last',              # 最后一个 Close 值
        'Volume': 'sum',              # 成交量总和
        'Quote asset volume': 'sum',  # Quote asset volume 总和
        'Number of trades': 'sum',    # 成交笔数总和
        'Taker buy base asset volume': 'sum',  # 主动买入成交量总和
        'Taker buy quote asset volume': 'sum'  # 主动买入成交额总和
    })

    # 重新创建 Close time 列，表示每个周期的结束时间
    resampled['Close time'] = resampled.index + pd.to_timedelta(target_freq) - pd.Timedelta(milliseconds=1)

    # 将索引重置为普通列
    resampled = resampled.reset_index().rename(columns={"Open time": "Open time"})

    # 调整列顺序
    resampled = resampled[[
        'Open time', 'Close time', 'Open', 'High', 'Low', 'Close',
        'Volume', 'Quote asset volume', 'Number of trades',
        'Taker buy base asset volume', 'Taker buy quote asset volume'
    ]]

    return resampled