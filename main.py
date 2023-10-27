import setup
import common
import pandas as pd
import numpy as np
import akshare as ak
from matplotlib import pyplot as plt
from urllib.parse import urlencode
import sys
import datetime
import stock
import schedule
import time
import utils
import one_data
import ak_share
import multi_factor
import call_auction
import sequent_down
import big_increase
import volume_up

# stock.create_db()

# 交易日15点之后执行
def dailyTask():
  # todo: if 非交易日 then return
  # 更新股票列表
  stock.update_stock_basic_append()
  # 更新股票当日行情
  stock.update_stock_hist_today()
  # 更新股票当日估值信息
  stock.update_stock_valuation_today()
  # 更新股票指数当日行情
  stock.update_stock_index_hist_today()
  stock.update_stock_300()

def periodTask(start_date = None, end_date = None, start_code = None):
  if start_date is None:
    start_date = datetime.date.today().strftime("%Y%m%d")
  # 更新股票列表
  stock.update_stock_basic_append()
  # 更新股票指定日期行情
  stock.update_stock_hist_period(start_date=start_date, end_date=end_date, start_code=start_code)
  # 更新股票估值信息
  stock.update_stock_valuation_period(start_date=start_date, end_date=end_date)
  # 更新股票指数指定日期行情
  stock.update_stock_index_hist_period(start_date=start_date, end_date=end_date)

dailyTask()
# stock.update_stock_300()
# periodTask(start_date='2023-10-18', end_date='2023-10-19', start_code='000001')
# def test():
#   print(1111)

# schedule.every(5).seconds.do(test)
# while True:
#   schedule.run_pending()
#   time.sleep(1)

# multi_factor.test(test_date = '2022-09-02', count_bench = 30, future_days = 7)

# call_auction.test('2023-08-25')

# sequent_down.algo()
# big_increase.test('2023-08-08')
# big_increase.test_30_days('2023-05-29')

# [8.23-10.11: 7781]
# [7.12-8.22: -11832]
# [5.29-7.11: -7291]
# [4.13-5.29: -14421]

# volume_up.test('2023-10-13', exclude_688 = True, exclude_48 = True, exclude_3 = True, exclude_cannot_buy = True)
# volume_up.test_30_days('2023-07-11', exclude_688 = True, exclude_48 = True, exclude_3 = False, exclude_cannot_buy = True)
# volume_up.test_hist()

# volume_up.test_limit_up('2023-10-13')
###########################################
# stocks = volume_up.algo('2023-10-12', exclude_688 = True, exclude_48 = True, exclude_3 = True, exclude_cannot_buy = True, debug = True)
# stocks = volume_up.algo_today(exclude_688 = True, exclude_48 = True, exclude_3 = True, exclude_cannot_buy = True, debug = True)

common.close_db()
