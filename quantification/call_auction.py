# coding=utf-8
import numpy as np
import pandas as pd
import utils
# from gm.api import *
from pandas import DataFrame
import schedule
import datetime

# '''
# 集合竞价选股
# 本策略通过获取SHSE.000300沪深300的成份股数据并统计其30天内
# 开盘价大于前收盘价的天数,并在该天数大于阈值10的时候加入股票池
# 随后对不在股票池的股票平仓并等权配置股票池的标的,每次交易间隔1个月.
# 回测数据为:SHSE.000300在2015-01-15的成份股
# 回测时间为:2017-07-01 08:00:00到2017-10-01 16:00:00
# '''

def algo(date: str):
  context = {
    # 数据滑窗
    'count_bench': 30,
    'count_reach': 18
  }
  # 获取上一个交易日的日期
  last_day = utils.get_previous_trading_date(date=date)
  # 获取沪深300成份股
  stock300 = utils.get_stock_300()
  trade_symbols = []
  # todo 过滤指定期间无交易的股票
  ###
  for stock in stock300:
    recent_data = utils.get_period_stock_call_auction(symbol=stock, end_date=last_day, days=context['count_bench'])
    diff = recent_data['open'] - recent_data['pre_close']
    # 获取累计天数超过阙值的标的池.并剔除当天没有交易的股票
    if len(diff[diff > 0]) >= context['count_reach']:
      trade_symbols.append(stock)

  print('trade_symbols: ', trade_symbols)
  return trade_symbols


def test(test_date: str):
  symbols_pool = algo(test_date)
  
  stock_300 = utils.get_stock_300()
  next_date = utils.get_next_trading_date(test_date)
  stock_300_df = pd.DataFrame(columns=['symbol', 'open_p_change', 'close_p_change'])
  for symbol in stock_300:
    open_p_change = utils.get_stock_open_p_change(symbol=symbol, date=next_date)
    close_p_change = utils.get_stock_close_p_change(symbol=symbol, date=next_date)
    stock_300_df.loc[len(stock_300_df)] = [symbol, open_p_change, close_p_change]
  
  # 沪深300股票的下一日开盘涨跌幅
  stock_300_open_df = stock_300_df.sort_values('open_p_change', ascending=False).reset_index(drop=True)
  print('stock_300_open_df: ', stock_300_open_df)

  stock_300_close_df = stock_300_df.sort_values('close_p_change', ascending=False).reset_index(drop=True)
  print('stock_300_close_df: ', stock_300_close_df)

  stock_pool_df = stock_300_df[stock_300_df['symbol'].isin(symbols_pool)]
  stock_pool_df['symbol'] = pd.Categorical(stock_pool_df['symbol'], categories=symbols_pool, ordered=True)
  stock_pool_df = stock_pool_df.sort_values('symbol')
  print('stock_pool_df: ', stock_pool_df)

