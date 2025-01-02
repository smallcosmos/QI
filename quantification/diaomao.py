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

# date: 即购买日
def algo(date: str, select_can_buy=True):
  diaoMaoStock = utils.getDiaoMaoStock(date=date, select_can_buy=select_can_buy)

  diaoMaoStock = filter(diaoMaoStock)
  return diaoMaoStock

def filter(df):
  exclude_3 = True
  exclude_688 = True
  exclude_48 = True
  if exclude_3:
    df = df[~df['code'].str.startswith('3')]
  if exclude_688:
    df = df[~df['code'].str.startswith('688')]
  if exclude_48:
    df = df[~df['code'].str.startswith('8')]
    df = df[~df['code'].str.startswith('4')]

  df = df.sort_values('free_market_cap', ascending=False)
  return df

def test(buy_date: str, count_bench: int = 10, with_dynamic: bool = False):
  buy_date = utils.get_next_trading_date(buy_date, include_this_day = True)
  test_date = utils.get_previous_trading_date(buy_date)
  symbols_pool = algo(test_date)
  # 每股买入
  money_per_code = 10000
  # 针对<100亿市值的case做优化。 过去一年的数据上测试，7的效果远远高于2-15的其他数字。 过去2-3年的数据上，不过滤效果更好
  earnings_days = 7
  if count_bench < 1:
    count_bench = 1

  df = pd.DataFrame(columns=[f"{buy_date}买入", "市值", f"近{earnings_days}天收益率", f"近{earnings_days}天沪深300指数收益率", "是否跑赢指数", f"未来{count_bench}日涨跌详情", '盈亏'])
  for index, row in symbols_pool.iterrows():
    code = row['code']
    free_market_cap = row['free_market_cap']
    next_n_p_change = utils.get_stock_p_change_next_N(code, test_date, count_bench = count_bench + 2)
    # 忽略第一天、并默认第二天以3%预设买入
    total_p_change = next_n_p_change['p_change'].sum() - next_n_p_change['p_change'].iloc[0] - 3
    joined_p_change = ', '.join(next_n_p_change['p_change'].astype(str))
    stock_return = '-'
    market_return = '-'
    runThanStock300 = 1
    # stock_return = utils.get_stock_earnings(symbol=code, date=test_date, count=earnings_days)
    # market_return = utils.get_stock_index_earnings(symbol='000300', date=test_date, count=earnings_days)
    # runThanStock300 = 1 if stock_return - market_return > 0 else 0
    if runThanStock300 == 1:
      df.loc[len(df)] = [code, free_market_cap, stock_return, market_return, runThanStock300, joined_p_change, total_p_change * money_per_code / 100]
  
  if(len(df)):
    earning = round(df['盈亏'].sum(), 1) if with_dynamic else round(df['盈亏'].sum() / len(df), 1)
    df.loc[len(df)] = ['平均', '-', '-', '-', '-', '-', earning]
    print(df)
  
  return df

def test_15_days(date: str, count_bench: int = 10, with_dynamic: bool = False):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=15)
  df = pd.DataFrame(columns=[f"日期", "买入", f"盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench, with_dynamic=with_dynamic)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"15天盈亏: {df['盈亏'].sum()}")

def test_30_days(date: str, count_bench: int = 10, with_dynamic: bool = False):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=30)
  df = pd.DataFrame(columns=[f"日期", "买入", f"盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench, with_dynamic=with_dynamic)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"30天盈亏: {df['盈亏'].sum()}")

def test_by_year(year: str, count_bench: int = 10, with_dynamic: bool = False):
  start_date = f"{year}-01-01";
  end_date = f"{year}-12-31";
  days = utils.get_count_trading_date(start_date, end_date)
  dates = utils.get_next_trading_date(start_date, include_this_day=True, days=days)
  df = pd.DataFrame(columns=["日期", "买入", "盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench, with_dynamic=with_dynamic)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"{year}全年盈亏: {df['盈亏'].sum()}")


def selectDiaoMaoStock(date: str = datetime.date.today()):
  pre_day = utils.get_previous_trading_date((pd.to_datetime(date) - datetime.timedelta(days = 1)).strftime('%Y-%m-%d'), include_this_day=True)
  df = algo(pre_day, select_can_buy=False)
  if(len(df)):
    df['can_buy_price'] = df['close'] * 1.03
  
  print(df)

# 收盘预选第二天
def preSelect(date: str):
  df = utils.getDiaoMaoStockPreSelect(date)
  df['can_buy_price'] = df['close'] * 1.03
  df = filter(df)
  print(df)


# >200亿 每日固定1万, 持有3天
#   2020年：2483.1
#   2021年：50.6
#   2022年: 7546.4
#   2023年: -1235.7
#   2024年: 9666.0

# >100亿 每日固定1万, 持有3天
#   2020年： -127.7
#   2021年: 5805.9
#   2022年: -600.5
#   2023年: -1016.7
#   2024年: 10960.6

# <100亿 每日固定1万, 持有3天
#   2020年: -4782.8
#   2021年: 15190.5
#   2022年: -5181.9
#   2023年: 10172.69
#   2024年: 4417.8

# >200亿 每日每只股票1万， 持有3天
#   2020年: 5052.0
#   2021年: 175.0
#   2022年: 8789.0
#   2023年: -3238.0
#   2024年: 10563.0

# >100亿 每日每只股票1万， 持有3天
#   2020年: 9316.0
#   2021年: -5259.0
#   2022年: 561.0
#   2023年: -4909.0
#   2024年: 24470.0

# <100亿 每日每只股票1万
#   2020年： -4853.0
#   2021年： 31935.0
#   2022年： 536.0
#   2023年： 7703.0
#   2024年： 118712.0