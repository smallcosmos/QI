# coding=utf-8
import numpy as np
import pandas as pd
import utils
import time
from pandas import DataFrame
import schedule
import datetime

def algo(date: str = None):
  big_increase = utils.get_big_increase_stock(date)
  small_stock = utils.get_small_stock(date)
  # big_increase = big_increase[big_increase['code'].isin(small_stock)]
  print(f"{date} big increase: ")
  print(big_increase)
  stock_pool_df = pd.DataFrame(columns=['code', 'pef'])
  for code in big_increase['code'].values:
    df = utils.get_stock_p_change_after_big_increase(code, end_date = date)
    pef = df['p_change'].mean()
    stock_pool_df.loc[len(stock_pool_df)] = [code, pef]
  
  stock_pool_df = stock_pool_df.sort_values('pef', ascending=False).reset_index(drop=True)

  print('stock_pool_df: ')
  print(stock_pool_df)
  return stock_pool_df

def test(date: str):
  trading_date = utils.get_previous_trading_date(date, include_this_day=True)
  next_date = utils.get_next_trading_date(date)
  stock_pool_df = algo(trading_date)
  stock_pool_df = stock_pool_df.head(6)

  df = pd.DataFrame(columns=['code', 'p_change'])
  for code in stock_pool_df['code'].values:
    p_change = utils.get_stock_close_p_change(code, next_date)
    df.loc[len(df)] = [code, p_change]
  
  diff = stock_pool_df.index.size * 10000 * df['p_change'].mean() / 100
  print(f"{trading_date}: 买入{stock_pool_df.index.size}支，共{stock_pool_df.index.size}万， 盈亏{diff}元, 详情：{df}")
  return [trading_date, ','.join(stock_pool_df['code'].values), diff]

def test_30_days(date: str):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=30)
  results = pd.DataFrame(columns=['trading_date', 'symbols', 'diff'])
  for trading_date in dates:
    result = test(trading_date)
    results.loc[len(results)] = result
  
  print(results)
  print('30天总盈亏：', results['diff'].sum())