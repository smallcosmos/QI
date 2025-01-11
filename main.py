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
def xuangu():
  # 更新股票当日行情, 获取开盘价
  stock.update_stock_hist_today()
  
  # 取前一个交易日
  select_date = utils.get_previous_trading_date(datetime.date.today())
  # diaomao策略
  diaomao.selectDiaoMaoStock1(select_date=select_date)
  diaomao.selectDiaoMaoStock2(select_date=select_date)
  diaomao.selectDiaoMaoStock3(select_date=select_date)

def pre_xuanfu(select_date: str):
  diaomao.selectDiaoMaoStock1(select_date=select_date, pre_select=True)
  diaomao.selectDiaoMaoStock2(select_date=select_date, pre_select=True)
  diaomao.selectDiaoMaoStock3(select_date=select_date, pre_select=True)

# diaomao.selectDiaoMaoStock1('2025-01-08')
# dailyTask()

# common.close_db()

# diaomao.test_15_days('2025-01-02', 2, with_dynamic=True)
# diaomao.test_30_days('2025-01-07', 2, with_dynamic=True)
diaomao.test_by_year('2020', 2, with_dynamic=True, all_type=False)


# xuangu()
# pre_xuanfu('2025-01-09')
