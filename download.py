from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
import time
import os

# 如果为现货标的
spot_kline_url = 'https://api.binance.com/api/v3/klines'

# 如果为合约标的
swap_kline_url = 'https://fapi.binance.com/fapi/v1/klines'

# 多线程版的下载K线数据方法
def download_historical_data_multi_threads(symbols, interval, start_date, end_date, output_dir="./binance_data", max_workers=10):
    """
    批量下载多个交易对的历史数据并保存为pkl文件（多线程版本）
    :param symbols: 要下载的交易对列表。例如 ["BTCUSDT", "ETHUSDT"]
    :param interval: K线周期
    :param start_date: 开始日期。例如 2023-01-01
    :param end_date: 结束日期。例如 2023-10-01
    :param output_dir: 数据保存的目录
    :param max_workers: 并发线程数
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 转换开始和结束日期为时间戳
    start_time = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    def download_symbol_data(symbol):
        """
        下载单个交易对的数据。
        """
        try:
            print(f"[信息] 开始下载 {symbol} 的数据...")
            listing_time = get_symbol_listing_time(symbol)
            if not listing_time:
                print(f"[跳过] 无法获取 {symbol} 的上市时间，跳过...")
                return symbol, False
            
            if listing_time > end_time:
                print(f"[跳过] {symbol} 上市时间晚于结束时间 {end_date}，跳过...")
                return symbol, False

            # 如果上市时间晚于开始时间，则调整为上市时间
            if listing_time > start_time:
                print(f"[提示] {symbol} 上市时间晚于起始时间 {start_date}，使用上市时间 {datetime.fromtimestamp(listing_time / 1000).strftime('%Y-%m-%d')} 作为开始时间。")
            current_start_time = max(start_time, listing_time)

            all_data = pd.DataFrame()

            while current_start_time < end_time:
                df = get_binance_kline_data(symbol, interval, current_start_time, end_time)
                if df.empty:
                    print(f"[提示] {symbol} 数据已下载完成")
                    break

                all_data = pd.concat([all_data, df], ignore_index=True)
                current_start_time = int(df["Open time"].iloc[-1].timestamp() * 1000) + 1
                # 日级别可以设置为0.2
                time.sleep(1)  # 避免触发API频率限制

            # 保存数据
            output_file = os.path.join(output_dir, f"{symbol}_{interval}.pkl")
            all_data.to_pickle(output_file)
            print(f"[完成] {symbol} 数据已保存到 {output_file}")
            return symbol, True

        except Exception as e:
            print(f"[错误] 下载 {symbol} 数据时出错: {e}")
            return symbol, False

    # 多线程下载
    successful_symbols = []
    failed_symbols = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_symbol_data, symbol): symbol for symbol in symbols}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result_symbol, success = future.result()
                if success:
                    successful_symbols.append(result_symbol)
                else:
                    failed_symbols.append(result_symbol)
            except Exception as e:
                print(f"[错误] 处理 {symbol} 结果时出错: {e}")
                failed_symbols.append(symbol)

    print(f"[总结] 成功下载 {len(successful_symbols)} 个交易对数据，失败 {len(failed_symbols)} 个。")
    if failed_symbols:
        print(f"[失败列表]: {failed_symbols}")

# 获取K线数据的函数
def get_binance_kline_data(symbol, interval, start_time, end_time):
    """
    从Binance获取历史K线数据
    :param symbol: 交易对。例如 BTCUSDT
    :param interval: K线周期。例如1m, 5m, 1h, 1d
    :param start_time: 开始时间，时间戳(毫秒)
    :param end_time: 结束时间，时间戳(毫秒)
    :return: 返回DataFrame格式的历史K线数据
    """
    url = f"https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000  # 每次最大返回1000条数据
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # 如果响应状态码不是200，会抛出HTTPError

        # 响应解析为JSON
        data = response.json()

        if len(data) == 0:
            # print(f"[警告] 获取不到数据，可能是时间范围过小")
            return pd.DataFrame()

        # 将数据格式化为DataFrame
        columns = [
            "Open time", "Open", "High", "Low", "Close", "Volume",
            "Close time", "Quote asset volume", "Number of trades",
            "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
        ]
        df = pd.DataFrame(data, columns=columns)

        # 转换时间戳为可读时间
        df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
        df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")

        # 转换数值列为浮点型
        numeric_columns = ["Open", "High", "Low", "Close", "Volume",
                           "Quote asset volume", "Taker buy base asset volume", "Taker buy quote asset volume"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 转换交易次数为整数类型
        df["Number of trades"] = pd.to_numeric(df["Number of trades"], errors='coerce').astype('Int64')


        # 保留所需的核心字段
        df = df[["Open time", "Close time", "Open", "High", "Low", "Close", "Volume", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume"]]

        return df
    except Exception as e:
        print(f"[错误] 请求失败: {e}")
        return pd.DataFrame()

# 批量获取历史数据的函数
def download_historical_data(symbols, interval, start_date, end_date, output_dir="./binance_data"):
    """
    批量下载多个交易对的历史数据并保存为pkl文件
    :param symbols: 要下载的交易对列表。例如 ["BTCUSDT", "ETHUSDT"]
    :param interval: K线周期
    :param start_date: 开始日期。例如 2023-01-01
    :param end_date: 结束日期。例如 2023-10-01
    :param output_dir: 数据保存的目录
    """
    import os
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 转换开始和结束日期为时间戳
    start_time = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    for symbol in symbols:
        print(f"正在下载交易对 {symbol} 的数据...")

        # 获取交易对的上市时间
        listing_time = get_symbol_listing_time(symbol)
        if listing_time is None:  # 若交易对不存在或无法获取时间
            print(f"[跳过] 无法获取交易对 {symbol} 的上市时间，跳过该交易对。")
            continue

        elif listing_time > end_time:  # 若上市时间晚于结束时间
            print(f"[跳过] 交易对 {symbol} 上市时间晚于结束时间 {end_date}，跳过该交易对。")
            continue

        # 如果上市时间晚于指定的 start_time，则调整为上市时间
        elif start_time < listing_time:
            current_start_time = listing_time
            print(f"[提示] 交易对 {symbol} 上市时间晚于起始时间 {start_date}，使用上市时间 {datetime.fromtimestamp(listing_time / 1000).strftime('%Y-%m-%d')} 作为开始时间。")

        else:
            current_start_time = start_time

        all_data = pd.DataFrame()
        #current_start_time = start_time

        while current_start_time < end_time:
            # 获取K线数据
            df = get_binance_kline_data(symbol, interval, current_start_time, end_time)

            if df.empty:
                print(f"[提示] {symbol} 数据已下载完成")
                break

            all_data = pd.concat([all_data, df], ignore_index=True) # 把多次下载的数据合并起来

            # 更新起始时间 (下次从最后的时间开始)
            current_start_time = int(df["Open time"].iloc[-1].timestamp() * 1000) + 1

            # 每次请求等一小会儿，避免触发币安限频
            time.sleep(0.2)

        # 保存为pkl文件
        output_file = os.path.join(output_dir, f"{symbol}_{interval}.pkl")
        all_data.to_pickle(output_file)  # 保存为pkl格式
        print(f"[完成] {symbol} 数据已保存到 {output_file}")

# 补充或下载数据的函数
def update_historical_data(symbol, interval, output_dir="/Users/zhoupeng/Desktop/tiger_quant/data", update_start_time=None):
    """
    补充下载指定交易对的数据，保存为pkl格式
    :param symbol: 交易对名称，例如 BTCUSDT
    :param interval: 时间间隔（K线周期）
    :param output_dir: 数据保存文件夹
    :param update_start_time: 从指定时间开始更新，以 "yyyy-mm-dd" 格式
    """
    # 数据保存路径
    output_file = os.path.join(output_dir, f"{symbol}_{interval}.pkl")

    # 如果文件存在，则加载现有数据
    if os.path.exists(output_file):
        print(f"[信息] 已发现数据文件 {output_file}")
        existing_data = pd.read_pickle(output_file)
        print(f"[信息] 当前数据从 {existing_data['Open time'].iloc[0]} 到 {existing_data['Open time'].iloc[-1]}")
        last_timestamp = existing_data['Open time'].iloc[-1]

        # 从最后时间点的前五天开始覆盖数据
        last_timestamp = int(last_timestamp.timestamp() * 1000)  # 转换为毫秒时间戳
        last_timestamp = last_timestamp - 5 * 24 * 60 * 60 * 1000  # 往前减去5天（毫秒单位）

    else:
        print(f"[信息] 未发现现有数据，准备从头下载 {symbol} 的数据")
        existing_data = pd.DataFrame()
        last_timestamp = None

    # 如果指定了补充开始时间，则覆盖默认的最后时间戳
    if update_start_time:
        print(f"[信息] 用户指定补充时间起点 {update_start_time}")
        last_timestamp = int(datetime.strptime(update_start_time, "%Y-%m-%d").timestamp() * 1000)

    # 获取当前时间作为结束时间
    end_time = int(datetime.now().timestamp() * 1000)

    # 累积新增数据
    all_new_data = pd.DataFrame()
    current_start_time = last_timestamp

    print(f"[信息] 开始从 {datetime.fromtimestamp(current_start_time / 1000)} 补充数据")
    while current_start_time < end_time:
        # 获取K线数据
        df = get_binance_kline_data(symbol, interval, current_start_time, end_time)

        if df.empty:
            print(f"[提示] 数据下载完成")
            break

        # 累积
        all_new_data = pd.concat([all_new_data, df], ignore_index=True)

        # 更新下一次请求的时间戳
        current_start_time = int(df["Open time"].iloc[-1].timestamp() * 1000) + 1

        time.sleep(0.2)  # 避免触发API频率限制

    # 合并新老数据
    all_data = pd.concat([existing_data, all_new_data], ignore_index=True)

    # 删除重复行（根据时间去重）
    all_data = all_data.drop_duplicates(subset=["Open time"], keep="last")
    all_data = all_data.sort_values(by="Open time")  # 按时间排序

    # 保存为pkl文件
    all_data.to_pickle(output_file)
    print(f"[完成] {symbol} 数据已更新并保存到 {output_file}")

# 多线程版更新数据
def update_historical_data_multi_threaded(symbols, interval, output_dir, update_start_time=None, max_workers=5):
    """
    使用多线程补充下载指定交易对的数据，保存为pkl格式。
    :param symbols: 交易对列表，例如 ["BTCUSDT", "ETHUSDT"]
    :param interval: K线周期，例如 "1h", "1d"
    :param output_dir: 数据保存文件夹
    :param update_start_time: 起始更新时间，例如 "yyyy-mm-dd"
    :param max_workers: 并发线程数
    """
    def update_single_symbol(symbol):
        """处理单个交易对的数据更新"""
        # 数据保存路径
        output_file = os.path.join(output_dir, f"{symbol}_{interval}.pkl")

        # 如果文件存在，则加载现有数据
        if os.path.exists(output_file):
            existing_data = pd.read_pickle(output_file)
            last_timestamp = existing_data['Open time'].iloc[-1]
            last_timestamp = int(last_timestamp.timestamp() * 1000)  # 转换为毫秒时间戳
            last_timestamp = last_timestamp - 5 * 24 * 60 * 60 * 1000  # 往前减去5天（毫秒单位）
        else:
            # 如果文件不存在，则获取交易对的上市时间
            listing_time = get_symbol_listing_time(symbol)
            if listing_time is None:
                print(f"[错误] 无法获取 {symbol} 的上市时间，跳过该交易对。")
                return

            print(f"[信息] 未发现现有数据，使用上市时间 {datetime.fromtimestamp(listing_time / 1000)} 作为开始时间进行补充")
            last_timestamp = listing_time
            existing_data = pd.DataFrame()

        # 如果指定了补充开始时间，则覆盖默认的最后时间戳
        if update_start_time:
            last_timestamp = int(datetime.strptime(update_start_time, "%Y-%m-%d").timestamp() * 1000)

        # 获取当前时间作为结束时间
        end_time = int(datetime.now().timestamp() * 1000)

        # 累积新增数据
        all_new_data = pd.DataFrame()
        current_start_time = last_timestamp

        if current_start_time is None:
            if update_start_time:
                current_start_time = int(datetime.strptime(update_start_time, "%Y-%m-%d").timestamp() * 1000)
            else:
                raise ValueError(f"无法确定{symbol}开始时间, 请提供update_start_time参数或确保数据文件存在。")


        print(f"[信息] {symbol}: 开始从 {datetime.fromtimestamp(current_start_time / 1000)} 补充数据")
        while current_start_time < end_time:
            df = get_binance_kline_data(symbol, interval, current_start_time, end_time)
            if df.empty:
                break
            all_new_data = pd.concat([all_new_data, df], ignore_index=True)
            current_start_time = int(df["Open time"].iloc[-1].timestamp() * 1000) + 1
            time.sleep(0.2)  # 避免API频率限制

        # 合并新老数据并去重
        all_data = pd.concat([existing_data, all_new_data], ignore_index=True)
        all_data = all_data.drop_duplicates(subset=["Open time"], keep="last").sort_values(by="Open time")

        # 保存数据
        all_data.to_pickle(output_file)
        print(f"[完成] {symbol} 数据已更新并保存到 {output_file}")

    # 创建线程池执行器
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(update_single_symbol, symbol) for symbol in symbols]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[错误] 数据下载时出错: {e}")

# 获取币安交易对上市时间的函数
def get_symbol_listing_time(symbol):
    """
    获取交易对的上市时间 (Binance Futures)
    :param symbol: 交易对名称 (如 BTCUSDT)
    :return: 上市时间的时间戳 (毫秒)，如果交易对不存在返回 None
    """
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"  # Binance Futures Exchange Info API
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for sym in data["symbols"]:
            if sym["symbol"] == symbol:
                # print(datetime.fromtimestamp(sym["onboardDate"] / 1000))
                return sym["onboardDate"]  # Binance 的上市时间是毫秒时间戳
        print(f"[警告] 未找到交易对 {symbol} 的上市时间！可能交易对不存在。")
        return None
    except Exception as e:
        print(f"[错误] 无法获取交易对 {symbol} 的上市时间: {e}")
        return None