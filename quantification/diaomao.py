# coding=utf-8
import numpy as np
import pandas as pd
import utils
# from gm.api import *
from pandas import DataFrame
import schedule
import datetime
import math
import threading

# '''
# diaomao选股
# 收盘上涨3%～5%
# 三个模型【？？？】近一个月趋势相似度80%，且向上
# 板块数据【？？？】
# 过滤条件：市值不低于100亿
# 买入条件：第二天达到3%涨幅
# 持有天数：x
# '''

__min_next_open__=1
__max_next_open__=2
__next_up_p_change__=1
__min_market_cap__=150
__max_market_cap__=-1
__earnings_days__=3
__max_earnings_gap__=5


# date: 即购买日
def algo(date: str, min_market_cap = 150, max_market_cap = -1, min_next_open=1, max_next_open=3, next_up_p_change = 1):
  diaoMaoStock = utils.getDiaoMaoStock(date=date, pre_select=False, min_market_cap=min_market_cap, max_market_cap=max_market_cap, min_next_open=min_next_open, max_next_open=max_next_open, next_up_p_change = next_up_p_change)

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

def clac_earning(buy_date, select_date, symbols_pool, count_bench=2, next_up_p_change=1):
  global __earnings_days__
  global __max_earnings_gap__
  # 每股买入
  money_per_code = 10000

  if count_bench < 1:
    count_bench = 1
  
  df = pd.DataFrame(columns=[f"{buy_date}买入", "当日开盘涨跌幅", "买入价涨跌幅", "市值", "量比", f"近{__earnings_days__}天收益率", f"近{__earnings_days__}天沪深300指数收益率", "是否跑赢指数", f"未来{count_bench}日涨跌详情", '盈亏'])
  for index, row in symbols_pool.iterrows():
    code = row['code']
    free_market_cap = row['free_market_cap']
    volume = row['volume']
    
    volume_radio = utils.getStockVolumeRadio(code, date=select_date)

    if len(df) < 3:
    # if len(df) < 3 and volume_radio > 0.8:
      open_p_change = round((row['next_open'] - row['close']) / row['close'] * 100, 2)
      next_n_p_change = utils.get_stock_p_change_next_N(code, select_date, count_bench = count_bench + 2)
      joined_p_change = ', '.join(next_n_p_change['p_change'].astype(str))
      buy_p_change = open_p_change + next_up_p_change
      # 持有策略1， 默认持有3天，第三天尾盘出
      total_3_days_p_change = round(next_n_p_change['p_change'].sum() - next_n_p_change['p_change'].iloc[0] - buy_p_change, 2)

      # 持有策略2，当日收盘价小于3， 下一日开盘出。 最大持有3天，第三天尾盘出 【验证不可行， 无差别】
      # next_n_open_p_change = utils.get_stock_open_p_change_next_N(code, select_date, count_bench = count_bench + 2)
      # joined_next_open_p_change = ', '.join(next_n_open_p_change['next_open_p_change'].astype(str))
      # total_p_change = 0
      # for jndex, p_change_row in next_n_p_change.iterrows():
      #   if jndex > 0:
      #     p_change = p_change_row['p_change']
      #     total_p_change += p_change
      #     if p_change < -3 and jndex < len(next_n_p_change) - 1:
      #       next_open_p_change = next_n_open_p_change[next_n_open_p_change['date'] == p_change_row['date']]['next_open_p_change'].values[0]
      #       total_p_change += next_open_p_change
      #       break
      # total_p_change = round(total_p_change - buy_p_change, 2)
      # if total_p_change != total_3_days_p_change:
      #   print(f"策略提前卖, 卖出策略总涨跌： {total_p_change}, 持有3天策略总涨跌 {total_3_days_p_change}")

      # 持有策略3， 

      # stock_return = '-'
      # market_return = '-'
      # runThanStock300 = 1
      stock_return = utils.get_stock_earnings(symbol=code, date=select_date, count=__earnings_days__) * 100
      market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=__earnings_days__) * 100
      runThanStock300 = 1 if stock_return - market_return > 0 else 0

      # recent_10_return = utils.get_stock_earnings(symbol=code, date=select_date, count=10) * 100
      # recent_10_market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=10) * 100
      # if (market_return < 0):
      #   print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', code, joined_p_change, '>>>>>>>>>>>>>>>', stock_return, market_return, total_3_days_p_change)
      #   continue
      # if (stock_return - market_return > __max_earnings_gap__):
      #   print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', code, joined_p_change, '>>>>>>>>>>>>>>>', stock_return, market_return, total_3_days_p_change)
      #   continue
      # if recent_10_return > 30 and recent_10_market_return < 6:
      #   print('>>>>>>>>>>>>>>>', code, recent_10_return, '>>>>>>>>>>>>>>', total_3_days_p_change, recent_10_market_return)
      #   continue

      # volumeChange = utils.getStockVolumeChange(code=code, date=select_date)
      # print(f"{code} {volumeChange['dates']} 成交量变化： {volumeChange['volume_change']}")

      # if volume_radio > 1 and (market_return > 0 and market_return < 1) and stock_return > 12 and stock_return < 100:
      # if volume_radio > 1 and ((stock_return > 4 and stock_return < 10) or (stock_return > 10 and market_return > 2)):
      df.loc[len(df)] = [code, open_p_change, buy_p_change, free_market_cap, volume_radio, stock_return, market_return, runThanStock300, joined_p_change, total_3_days_p_change * money_per_code / 100]
  
  return df

def test(buy_date: str, count_bench=2, all_type=False, filter_by_return=True):
  buy_date = utils.get_next_trading_date(buy_date, include_this_day = True)
  select_date = utils.get_previous_trading_date(buy_date)
  symbols_pool = pd.DataFrame([])
  df = pd.DataFrame([])
  if all_type:
    large_150_symbols_1 = selectDiaoMaoStock1_1(select_date, pre_select=False, filter_by_return=filter_by_return)
    large_150_symbols_2 = selectDiaoMaoStock1_2(select_date, pre_select=False, filter_by_return=filter_by_return)
    larder_100_symbos_1 = selectDiaoMaoStock4_1(select_date, pre_select=False, filter_by_return=filter_by_return)
    
    # 2019年数据滑铁卢， -15587， 不采用
    # small_20_40_symbols_1 = selectDiaoMaoStock2_1(select_date, pre_select=False)
    small_20_40_symbols_2 = selectDiaoMaoStock2_2(select_date, pre_select=False, filter_by_return=filter_by_return)
    small_20_40_symbols_3 = selectDiaoMaoStock2_3(select_date, pre_select=False, filter_by_return=filter_by_return)

    if len(large_150_symbols_1):
      result = clac_earning(buy_date, select_date, large_150_symbols_1, count_bench=count_bench, next_up_p_change=1)
      df = pd.concat([df, result]) if len(df) else result
    if len(large_150_symbols_2):
      result = clac_earning(buy_date, select_date, large_150_symbols_2, count_bench=count_bench, next_up_p_change=1.4)
      df = pd.concat([df, result]) if len(df) else result
    if len(larder_100_symbos_1):
      result = clac_earning(buy_date, select_date, larder_100_symbos_1, count_bench=count_bench, next_up_p_change=1.4)
      df = pd.concat([df, result]) if len(df) else result
    if len(small_20_40_symbols_2):
      result = clac_earning(buy_date, select_date, small_20_40_symbols_2, count_bench=count_bench, next_up_p_change=1.6)
      df = pd.concat([df, result]) if len(df) else result
    # if len(small_20_40_symbols_1):
    #   result = clac_earning(buy_date, select_date, small_20_40_symbols_1, count_bench=count_bench, next_up_p_change=1.8)
    #   df = pd.concat([df, result]) if len(df) else result
    if len(small_20_40_symbols_3):
      result = clac_earning(buy_date, select_date, small_20_40_symbols_3, count_bench=count_bench, next_up_p_change=1.0)
      df = pd.concat([df, result]) if len(df) else result
  else: 
    global __min_next_open__
    global __max_next_open__
    global __next_up_p_change__
    global __min_market_cap__
    global __max_market_cap__
    # symbols_pool = algo(select_date, min_market_cap=__min_market_cap__, max_market_cap=__max_market_cap__, min_next_open=__min_next_open__, max_next_open=__max_next_open__, next_up_p_change=__next_up_p_change__)
    symbols_pool = selectDiaoMaoStock2_3(select_date, pre_select=False)
    if len(symbols_pool):
      result = clac_earning(buy_date, select_date, symbols_pool, count_bench=count_bench, next_up_p_change=__next_up_p_change__)
      df = pd.concat([df, result]) if len(df) else result

  if(len(df)):
    # index_change = utils.getStockIndexChange(select_date)
    # shang_df = index_change['shang_df']
    # shen_df = index_change['shen_df']
    # print(f"{index_change['dates']} 大盘指数变化：")
    # print(f"上证指数： {index_change['shang_index']}, sum: {shang_df['p_change'].sum()}, up_count: {len(shang_df[shang_df['p_change'] > 0.0])}")
    # print(f"深圳指数： {index_change['shen_index']}, sum: {shen_df['p_change'].sum()}, up_count: {len(shen_df[shen_df['p_change'] > 0.0])}")

    earning = round(df['盈亏'].sum(), 1)
    df.loc[len(df)] = ['平均', '-', '-', '-', '-', '-', '-', '-', '-', earning]
    print(df)
    print('')
  return df

def test_by_year(year: str, count_bench = 10, all_type: bool = False, filter_by_return = True):
  start_date = f"{year}-01-01";
  end_date = f"{year}-12-31";
  days = utils.get_count_trading_date(start_date, end_date)
  dates = utils.get_next_trading_date(start_date, include_this_day=True, days=days)
  df = pd.DataFrame(columns=["日期", "买入", "盈亏"])
  for trading_date in dates:
    result = test(trading_date, count_bench, all_type=all_type, filter_by_return=filter_by_return)
    if(len(result)):
      buy_code = ', '.join(result.iloc[:, 0].astype(str))
      df.loc[len(df)] = [trading_date, buy_code, result.iloc[-1]['盈亏']]


  total_price_change = df['盈亏'].sum()
  price_change_per_trade = round(total_price_change / len(df), 2) if len(df) else 0
  success_rate = round(len(df[df['盈亏'] > 0]) / len(df) * 100, 2) if len(df) else 0
  print(df)
  print(f"{year}全年盈亏: {df['盈亏'].sum()}, 平均每次交易盈亏: {price_change_per_trade}, 交易成功率: {success_rate}%")
  return { "total_price_change": total_price_change, "price_change_per_trade": price_change_per_trade, "success_rate": success_rate, "trade_count": len(df)  }

def addExtraInfo(df, select_date: str):
  earnings_days = 3
  new_df = pd.DataFrame(columns=['code', 'close', 'free_market_cap', 'volume_radio', 'volume', 'stock_return', 'market_return', 'stock_10_return', 'market_10_return', 'select_date', 'buy_date', 'next_open'])
  for index, row in df.iterrows():
    code = row['code']
    free_market_cap = f"{round(row['free_market_cap'], 1)}亿"
    volume = f"{round(row['volume'], 1)}万"
    
    stock_return = utils.get_stock_earnings(symbol=code, date=select_date, count=earnings_days) * 100
    market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=earnings_days) * 100

    recent_10_return = utils.get_stock_earnings(symbol=code, date=select_date, count=10) * 100
    recent_10_market_return = utils.get_stock_index_earnings(symbol='000300', date=select_date, count=10) * 100

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
      'stock_10_return': recent_10_return,
      'market_10_return': recent_10_market_return,
      'select_date': row['select_date'],
      'buy_date': row['buy_date'],
      'next_open': row['next_open'],
    }
    new_df.loc[len(new_df)] = new_row
  return new_df

def selectDiaoMaoStock1_1(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 150, min_next_open = 1, max_next_open = 2, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)
  
  if filter_by_return:
    df = df[df['stock_return'] - df['market_return'] <= 5]
    df = df[(df['stock_10_return'] <= 30) | (df['market_10_return'] >= 6)]

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'
  return df

def selectDiaoMaoStock1_2(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1.4
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 150, min_next_open = 0.5, max_next_open = 0.8, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if filter_by_return:
    df = df[(df['stock_10_return'] <= 30) | (df['market_10_return'] >= 6)]

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'
  return df

def selectDiaoMaoStock2_1(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1.8
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 20, max_market_cap = 40, min_next_open = 0, max_next_open = 0.5, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

  return df

def selectDiaoMaoStock2_2(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1.6
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap=20, max_market_cap=40, min_next_open=0.5, max_next_open=1, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if filter_by_return:
    df = df[df['stock_return'] - df['market_return'] <= 4]
    df = df[df['market_return'] >= 0]
    df = df[(df['stock_10_return'] <= 15) | (df['market_10_return'] >= 6)]

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

  return df

def selectDiaoMaoStock2_3(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1.0
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap=20, max_market_cap=40, min_next_open=1.0, max_next_open=1.5, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if filter_by_return:
    df = df[df['stock_return'] - df['market_return'] <= 5]
    df = df[df['market_return'] >= 0]
    df = df[(df['stock_10_return'] <= 15) | (df['market_10_return'] >= 6)]

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

  return df

# def selectDiaoMaoStock3(select_date: str, pre_select = False):
#   select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
#   next_up_p_change = 1.0
#   df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap=40, max_market_cap=70, min_next_open=1.0, max_next_open=1.5, next_up_p_change = next_up_p_change)
#   df = filter(df)
#   df = addExtraInfo(df, select_date=select_date)

#   df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

#   return df

def selectDiaoMaoStock4_1(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 1.4
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 100, max_market_cap = 150, min_next_open = 0.5, max_next_open = 1, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

  return df


def selectDiaoMaoStock5_1(select_date: str, pre_select = False, filter_by_return=True):
  select_date = utils.get_previous_trading_date(select_date, include_this_day=True)
  next_up_p_change = 0.6
  df = utils.getDiaoMaoStock(date=select_date, pre_select=pre_select, min_market_cap = 40, max_market_cap = 70, min_next_open = 1.5, max_next_open = 2.0, next_up_p_change = next_up_p_change)
  df = filter(df)
  df = addExtraInfo(df, select_date=select_date)

  if filter_by_return:
    df = df[df['stock_return'] - df['market_return'] <= 5]
    # df = df[df['volume_radio'] >= 1]
    # df = df[df['market_return'] >= 0]

  df['can_buy_price'] = df['next_open'] + df['close'] * next_up_p_change / 100 if len(df) else '-'

  return df


def auto_run_ai():
  years_df = pd.DataFrame({
    'years': pd.Series([
      '2018',
      '2019',
      '2020',
      '2021',
      '2022',
      '2023',
      '2024',
    ])
  })

  next_up_p_change_df = pd.DataFrame({
    # 'next_up_p_change': pd.Series(range(0, 10)) * 0.2
    'next_up_p_change': pd.Series([1.4])
  })

  next_open_p_change_df = pd.DataFrame({
    # 'min_next_open': pd.Series(range(-10, 0)) * 0.1,
    # 'max_next_open': pd.Series(range(-9, 1)) * 0.1
    'min_next_open': pd.Series([0.5]),
    'max_next_open': pd.Series([1.0])
  })

  max_earning_gap_df = pd.DataFrame({
    'max_earning_gap': pd.Series([5])
  })

  global __min_next_open__
  global __max_next_open__
  global __next_up_p_change__
  global __min_market_cap__
  global __max_market_cap__
  global __earnings_days__
  global __max_earnings_gap__
  __min_market_cap__ = 150
  __max_market_cap__ = -1
  __earnings_days__ = 3

  df = pd.DataFrame(columns = ['year', 'open_p_change', 'next_up_p_change', 'trade_count', 'total_price_change', 'price_change_per_trade', 'success_rate'])
  threads = []
  for year in years_df['years'].values:
    for next_up_p_change in next_up_p_change_df['next_up_p_change'].values:
      for index, open_p_change_row in next_open_p_change_df.iterrows(): 
        for max_earning_gap in max_earning_gap_df['max_earning_gap'].values: 
          min_next_open=round(open_p_change_row['min_next_open'], 1)
          max_next_open=round(open_p_change_row['max_next_open'], 1)
          min_next_open = 0 if math.isnan(min_next_open) else min_next_open
          max_next_open = 0 if math.isnan(max_next_open) else max_next_open
          __max_earnings_gap__ = max_earning_gap
          
          
          __min_next_open__=min_next_open
          __max_next_open__=max_next_open
          __next_up_p_change__=next_up_p_change
          result = test_by_year(year, 2, all_type=False)
          df.loc[len(df)] = [year, f"{min_next_open}~{max_next_open}", next_up_p_change, result['trade_count'], result['total_price_change'], result['price_change_per_trade'], result['success_rate']]
      
  for index, row in df.iterrows():
    print(f"{row['year']}_{row['open_p_change']}_{round(row['next_up_p_change'], 1)}: total: {row['total_price_change']}, trade_count: {row['trade_count']}, {row['price_change_per_trade']}/per_trade, success: {row['success_rate']}%")

  print(df)

