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
def algo(date: str, min_market_cap = 150, max_market_cap = -1):
  diaoMaoStock = utils.getDiaoMaoStock(date=date, pre_select=False, min_market_cap=min_market_cap, max_market_cap=max_market_cap)

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

def test(buy_date: str, count_bench: int = 10, with_dynamic: bool = False, all_type: bool = False):
  buy_date = utils.get_next_trading_date(buy_date, include_this_day = True)
  select_date = utils.get_previous_trading_date(buy_date)
  symbols_pool = pd.DataFrame([])
  if all_type:
    symbols_pool = algo(select_date, min_market_cap=150)
    small_30_symbols = algo(select_date, min_market_cap=30, max_market_cap=40)
    small_20_symbols =  algo(select_date, min_market_cap=20, max_market_cap=30)
    if len(small_30_symbols):
      symbols_pool= pd.concat([symbols_pool, small_30_symbols]) if len(symbols_pool) else small_30_symbols
    if len(small_20_symbols):
      symbols_pool= pd.concat([symbols_pool, small_20_symbols]) if len(symbols_pool) else small_20_symbols
  else: 
    symbols_pool = algo(select_date, min_market_cap=150)
  # 每股买入
  money_per_code = 10000
  # 针对<100亿市值的case做优化。 过去一年的数据上测试，7的效果远远高于2-15的其他数字。 过去2-3年的数据上，不过滤效果更好
  earnings_days = 3
  if count_bench < 1:
    count_bench = 1

  df = pd.DataFrame(columns=[f"{buy_date}买入", "当日开盘涨跌幅", "市值", "量比", f"近{earnings_days}天收益率", f"近{earnings_days}天沪深300指数收益率", "是否跑赢指数", f"未来{count_bench}日涨跌详情", '盈亏'])
  for index, row in symbols_pool.iterrows():
    code = row['code']
    free_market_cap = f"{round(row['free_market_cap'], 1)}亿"
    volume = f"{round(row['volume'], 1)}万"
    
    if len(df) < 3:
      open_p_change = round((row['next_open'] - row['close']) / row['close'] * 100, 2)
      next_n_p_change = utils.get_stock_p_change_next_N(code, select_date, count_bench = count_bench + 2)
      joined_p_change = ', '.join(next_n_p_change['p_change'].astype(str))
      # 忽略第一天、并默认第二天以3%预设买入
      total_p_change = next_n_p_change['p_change'].sum() - next_n_p_change['p_change'].iloc[0] - (open_p_change + 1)
      # stock_return = '-'
      # market_return = '-'
      # runThanStock300 = 1
      stock_return = utils.get_stock_earnings(symbol=code, date=select_date, count=earnings_days) * 100
      market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=earnings_days) * 100
      runThanStock300 = 1 if stock_return - market_return > 0 else 0

      # volumeChange = utils.getStockVolumeChange(code=code, date=select_date)
      # print(f"{code} {volumeChange['dates']} 成交量变化： {volumeChange['volume_change']}")

      volume_radio = utils.getStockVolumeRadio(code, date=select_date)
      # if volume_radio > 1 and (market_return > 0 and market_return < 1) and stock_return > 12 and stock_return < 100:
      # if volume_radio > 1 and ((stock_return > 4 and stock_return < 10) or (stock_return > 10 and market_return > 2)):
      df.loc[len(df)] = [code, open_p_change, free_market_cap, volume_radio, stock_return, market_return, runThanStock300, joined_p_change, total_p_change * money_per_code / 100]
  
  if(len(df)):
    # index_change = utils.getStockIndexChange(select_date)
    # print(f"{index_change['dates']} 大盘指数变化：")
    # print(f"上证指数： {index_change['shang_index']}")
    # print(f"深圳指数： {index_change['shen_index']}")

    earning = round(df['盈亏'].sum(), 1) if with_dynamic else round(df['盈亏'].sum() / len(df), 1)
    df.loc[len(df)] = ['平均', '-', '-', '-', '-', '-', '-', '-', earning]
    print(df)
    print('')
  
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

def test_by_year(year: str, count_bench: int = 10, with_dynamic: bool = False, all_type: bool = False):
  start_date = f"{year}-01-01";
  end_date = f"{year}-12-31";
  days = utils.get_count_trading_date(start_date, end_date)
  dates = utils.get_next_trading_date(start_date, include_this_day=True, days=days)
  df = pd.DataFrame(columns=["日期", "买入", "盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench, with_dynamic=with_dynamic, all_type=all_type)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]

  print(df)
  print(f"{year}全年盈亏: {df['盈亏'].sum()}")

def addExtraInfo(df, select_date: str):
  earnings_days = 3
  new_df = pd.DataFrame(columns=['code', 'close', 'free_market_cap', 'volume_radio', 'volume', 'stock_return', 'market_return', 'select_date', 'buy_date', 'next_open'])
  for index, row in df.iterrows():
    # [code, free_market_cap, volume_radio, stock_return, market_return, runThanStock300, joined_p_change, total_p_change * money_per_code / 100]
    code = row['code']
    free_market_cap = f"{round(row['free_market_cap'], 1)}亿"
    volume = f"{round(row['volume'], 1)}万"
    
    stock_return = utils.get_stock_earnings(symbol=code, date=select_date, count=earnings_days) * 100
    market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=earnings_days) * 100

    # volumeChange = utils.getStockVolumeChange(code=code, date=select_date)
    # print(f"{code} {volumeChange['dates']} 成交量变化： {volumeChange['volume_change']}")

    volume_radio = utils.getStockVolumeRadio(code, date=select_date)
  
    new_row = {
      'code': row['code'],
      'close': row['close'],
      'free_market_cap': free_market_cap,
      'volume_radio': volume_radio,
      'volume': volume,
      'stock_return': stock_return,
      'market_return': market_return,
      'select_date': row['select_date'],
      'buy_date': row['buy_date'],
      'next_open': row['next_open'],
    }
    new_df.loc[len(new_df)] = new_row
  return new_df

def selectDiaoMaoStock1(select_date: str, pre_select = False):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 150)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if(len(df)):
    df['can_buy_price'] = df['close'] * 1.03
  
  print(df)

def selectDiaoMaoStock2(select_date: str, pre_select = False):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 20, max_market_cap = 30)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if(len(df)):
    df['can_buy_price'] = df['close'] * 1.03
  
  print(df)

def selectDiaoMaoStock3(select_date: str, pre_select = False):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 30, max_market_cap = 40)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if(len(df)):
    df['can_buy_price'] = df['close'] * 1.03
  
  print(df)

