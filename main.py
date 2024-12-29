import pandas as pd
from datetime import datetime

from download import download_historical_data_multi_threads, download_historical_data
from download import update_historical_data, update_historical_data_multi_threaded

from check import check_data_completeness

from tools import get_binance_u_based_futures

# 存储目录
daily_output_directory = "/Users/zhoupeng/Desktop/crypto_database/data/daily"
MIN15_output_directory = "/Users/zhoupeng/Desktop/crypto_database/data/15MINS"

usdt_futures_pairs = get_binance_u_based_futures(base_asset="USDT")
print("当前交易所上USDT合约交易对数量: ", len(usdt_futures_pairs))

# 配置文件 ----------
target_directory = MIN15_output_directory # 目标目录

black_list = [] # 黑名单
trading_pairs = [sym for sym in usdt_futures_pairs if sym not in black_list]  # trading_pairs
#trading_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

kline_interval = "15m"  # K线时间间隔

start_date = "2020-01-01"  # 起始日期
end_date = "2024-12-17"  # 截止日期

# ----------


if __name__ == "__main__":

    # 1. 下载数据
    # download_historical_data(trading_pairs, kline_interval, start_date, end_date, daily_output_directory)
    # download_historical_data_multi_threads(trading_pairs, kline_interval, start_date, end_date, target_directory, max_workers=5)

    # 2. 检查数据完整性
    # check = check_data_completeness(trading_pairs, kline_interval, target_directory, required_end_date='2024-12-17')
    # print("缺失文件: ", len(check['missing']))
    # print("不完整文件: ", len(check['incomplete']))

    # 3. 重新下载缺失数据 / 这个不需要开多线程，可以一个一个看看问题在什么
    # download_historical_data(check["missing"], kline_interval, start_date, end_date, target_directory)

    # 4. 更新数据
    update_historical_data_multi_threaded(trading_pairs, kline_interval, target_directory, max_workers=5)
