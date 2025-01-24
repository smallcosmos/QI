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
import diaomao

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

# 开盘 9:30 选当日股
def xuangu(update_hist=True, filter_by_return=False):
  utils.set_xuangu_condition()
  # 更新股票当日行情, 获取开盘价
  if update_hist:
    stock.update_stock_hist_today()
  
  # 取前一个交易日
  select_date = utils.get_previous_trading_date(datetime.date.today())
  # diaomao策略
  df1 = diaomao.selectDiaoMaoStock1_1(select_date=select_date, filter_by_return=filter_by_return)
  df1_2 = diaomao.selectDiaoMaoStock1_2(select_date=select_date, filter_by_return=filter_by_return)
  df4 = diaomao.selectDiaoMaoStock4_1(select_date=select_date, filter_by_return=filter_by_return)
  df2_2 = diaomao.selectDiaoMaoStock2_2(select_date=select_date, filter_by_return=filter_by_return)
  df2_3 = diaomao.selectDiaoMaoStock2_3(select_date=select_date, filter_by_return=filter_by_return)
  utils.reset_xuangu_condition()
  print(df1, df1_2, df4, df2_2, df2_3)

def pre_xuangu(select_date: str, filter_by_return=True):
  df1 = diaomao.selectDiaoMaoStock1_1(select_date=select_date, pre_select=True, filter_by_return=filter_by_return)
  df4 = diaomao.selectDiaoMaoStock4_1(select_date=select_date, pre_select=True, filter_by_return=filter_by_return)
  df2 = diaomao.selectDiaoMaoStock2_2(select_date=select_date, pre_select=True, filter_by_return=filter_by_return)

  df = pd.concat([df1, df4, df2])
  print(df)
  # for index, row in df.iterrows():
  #   line_df = utils.get_yinxiaxian_next_n(select_date, row['code']);
  #   print(line_df)


# dailyTask()

# common.close_db()

diaomao.test_by_year('2025', 2, all_type=True, filter_by_return=True)


# xuangu(update_hist=True, filter_by_return=True)
# xuangu(update_hist=False, filter_by_return=False)
# pre_xuangu('2025-01-23', filter_by_return=False)

# diaomao.auto_run_ai()
