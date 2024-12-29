# Crypto Database

## 1. 概览
**数据中心主要提供这样的功能：**
1. 数据获取 (支持多线程)
2. 数据检查 (是否有缺失数据，是否有不完整的数据)
3. 增量数据更新 (把最新的数据增量更新到已有数据 / 新增标的会完整下载下来 / 同样支持多线程)

**这些功能主要通过这几个封装方法完成：**
1. **download_historical_data_multi_threads** 多线程的下载数据
2. **check_data_completeness** 输出指定symbol集与时间段里的缺失文件与不完整文件
3. **update_historical_data_multi_threaded** 多线程的更新数据

正常情况下我们只需要调用**download_historical_data_multi_threads**方法先下载一次数据，然后**check_data_completeness** + **download_historical_data**把第一次因为各种原因（主要是API速率限制）没有下载完全的数据再单独下一遍之后，往日只需要使用**update_historical_data_multi_threaded** + **check_data_completeness**下载并且检查增量数据就足够了。

这些数据都是从Binance API获取得到的，并且提供了三个比较重要的额外方法：
1. **get_binance_u_based_futures** 可以获取交易所当前所有U本位合约，包括已经下市的
2. **create_prices_dataframe** 可以把某个目录的所有文件整理成一张大表，格式符合回测框架要求 (我另外一个Athena项目)
3. **resample_to_higher_freq** 可以把低freq数据向高freq数据转化，也就是其实我们只需要维护一个5m或者15m的数据集就足够了

## 2. API Limit
下载数据需要特别关心API Limit的问题，尤其是在使用多线程的情况
- 如果是下载1d数据可以直接把max_workers拉到10，多线程方法里每一次kline API访问可以休眠0.2s
- 但是如果下载15m数据这个方案会导致API的暂时封禁，我的方案是（不是最低要求），max_workers拉到2，然后多线程方法里每一次kline API访问休眠1s
- 这个可以自己多试试，不赶时间可以就使用我这个默认方案

## 3. 参数配置
在**main.py**需要配置这几个参数来启动下载：

```python
# 存储目录
daily_output_directory = "/.../daily"
MIN15_output_directory = "/.../15MINS"
target_directory = MIN15_output_directory # 目标目录

black_list = [] # 黑名单
# 也可以直接指定trading_pairs

kline_interval = "15m"  # K线时间间隔

start_date = "2020-01-01"  # 起始日期
end_date = "2024-12-17"  # 截止日期
```