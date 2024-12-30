# coding=utf-8
import numpy as np
import pandas as pd
import utils
# from gm.api import *
from pandas import DataFrame
import schedule
import datetime

# '''
# diaomao选股
# 收盘上涨3%～5%
# 三个模型【？？？】近一个月趋势相似度80%，且向上
# 板块数据【？？？】
# 过滤条件：市值不低于100亿
# 买入条件：第二天达到3%涨幅
# 持有天数：x
# '''

# date: 选股日
def algo(date: str, select_can_buy=True):
  # 买入日，选出符合当天达到3%涨幅的股票
#   buy_day = utils.get_previous_trading_date(date=date, include_this_day=true)
#   stockUpThan3 = utils.getStockUpThan3(buy_day)
  # 选股日， 筛选出符合涨幅在3% ～5%的股票, 且市值 > 100亿
  diaoMaoStock = utils.getDiaoMaoStock(date=date, select_can_buy=select_can_buy)

  exclude_3 = True
  exclude_688 = True
  if exclude_3:
    diaoMaoStock = diaoMaoStock[~diaoMaoStock['code'].str.startswith('3')]
  if exclude_688:
    diaoMaoStock = diaoMaoStock[~diaoMaoStock['code'].str.startswith('688')]
  
  return diaoMaoStock


def test(test_date: str, count_bench: int = 10):
  symbols_pool = algo(test_date)
  buy_date = utils.get_next_trading_date(test_date)
  # 每股买入
  money_per_code = 10000
  if count_bench < 1:
    count_bench = 1

  df = pd.DataFrame(columns=[f"{buy_date}买入", f"未来{count_bench}日涨跌详情", '盈亏'])
  for code in symbols_pool['code'].values:
    next_n_p_change = utils.get_stock_p_change_next_N(code, test_date, count_bench = count_bench + 2)
    # 忽略第一天、并默认第二天以3%预设买入
    total_p_change = next_n_p_change['p_change'].sum() - next_n_p_change['p_change'].iloc[0] - 3
    joined_p_change = ', '.join(next_n_p_change['p_change'].astype(str))
    df.loc[len(df)] = [code, joined_p_change, total_p_change * money_per_code / 100]
  
  if(len(df)):
    df.loc[len(df)] = ['平均', '', round(df['盈亏'].sum() / len(df), 1)]
    print(df)
  
  return df

def test_30_days(date: str, count_bench: int = 10):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=30)
  df = pd.DataFrame(columns=[f"日期", "买入", f"盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"30天盈亏: {df['盈亏'].sum()}")

def test_365_days(date: str, count_bench: int = 10):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=365)
  df = pd.DataFrame(columns=["日期", "买入", "盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"365天盈亏: {df['盈亏'].sum()}")


def selectDiaoMaoStock(date: str = datetime.date.today()):
  pre_day = utils.get_previous_trading_date((pd.to_datetime(date) - datetime.timedelta(days = 1)).strftime('%Y-%m-%d'))
  df = algo(pre_day, select_can_buy=False)
  if(len(df)):
    df['can_buy_price'] = df['close'] * 1.03
    print(df)