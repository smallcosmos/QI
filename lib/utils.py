import akshare as ak
import numpy as np
import pandas as pd
import configparser
import time
import datetime
import common
import os
import one_data

# 获取前一个交易日
def get_previous_trading_date(date: str, include_this_day: bool = False, days: int = 1):
  cur, conn = common.connect_db()
  date = pd.to_datetime(date).date().strftime('%Y-%m-%d')
  condition = '<=' if include_this_day else '<'
  sql = f"select trade_date from stock_trade_calendar where trade_date {condition} '{date}' order by trade_date desc limit {days}"
  cur.execute(sql)
  results = cur.fetchall()
  date = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  if days == 1:
    return date[0] if len(date) else None
  else:
    return date

# 获取下一个交易日
def get_next_trading_date(date: str, include_this_day: bool = False, days: int = 1):
  cur, conn = common.connect_db()
  date = pd.to_datetime(date).date().strftime('%Y-%m-%d')
  condition = '>=' if include_this_day else '>'
  sql = f"select trade_date from stock_trade_calendar where trade_date {condition} '{date}' order by trade_date asc limit {days}"
  cur.execute(sql)
  results = cur.fetchall()
  date = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  if days == 1:
    return date[0] if len(date) else None
  else:
    return date

# 获取交易日历
def get_trading_date(start_date: str, end_date: str):
  cur, conn = common.connect_db()
  start_date = pd.to_datetime(start_date).date().strftime('%Y-%m-%d')
  end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')
  sql = f"select trade_date from stock_trade_calendar where trade_date >= '{start_date}' and trade_date <= '{end_date}' order by trade_date ASC"
  cur.execute(sql)
  results = cur.fetchall()
  date = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  return date

# 获取所有股票列表
def get_all_stock_codes():
	cur, conn = common.connect_db()
	sql = "select code from stock_basic"
	cur.execute(sql)
	results = cur.fetchall()
	codes = pd.Series([item[0] for item in results])
	cur.close()
	conn.close()
	return codes

# 获取已退市股票列表
def get_off_stock_codes():
  cur, conn = common.connect_db()
  sql = "select code from stock_basic where listStatus = '退市'"
  cur.execute(sql)
  results = cur.fetchall()
  codes = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  return codes

# 获取正常上市的所有股票列表
def get_stock_codes():
	cur, conn = common.connect_db()
	sql = "select code from stock_basic where listStatus = '正常上市'"
	cur.execute(sql)
	results = cur.fetchall()
	codes = pd.Series([item[0] for item in results])
	cur.close()
	conn.close()
	return codes

# 更新已退市的所有股票列表
def update_outdate_list_status_code(off_codes: list):
  cur, conn = common.connect_db()
  sql = f"update stock_basic set listStatus = '退市' where code in ({','.join(off_codes)}) and listStatus = '正常上市'"
  cur.execute(sql)
  cur.close()
  conn.close()
  return

# 获取沪深300股票列表
def get_stock_300():
	cur, conn = common.connect_db()
	sql = 'select code from stock_basic where type = 1'
	cur.execute(sql)
	results = cur.fetchall()
	codes = pd.Series([item[0] for item in results])
	cur.close()
	conn.close()
	return codes

# 获取市值、市净率
def get_stock_fin(symbols: list = [], date: str = None):
  cur, conn = common.connect_db()
  if not len(symbols) or date is None:
    return pd.DataFrame()

  date = pd.to_datetime(date).date().strftime('%Y%m%d')
  sql = f"select code, pb, total_market_cap from stock_valuation where code in ({','.join(symbols)}) and date = '{date}'"
  cur.execute(sql)
  results = cur.fetchall()
  columns = ['code', 'pb', 'mc']
  df = pd.DataFrame(results, columns=columns)
  df.set_index('code', inplace = True)
  cur.close()
  conn.close()
  return df

def get_index_p_change_by_stock(symbol: str, date: str):
  if symbol.startswith('0'):
    index_symbol = '000001'
  elif symbol.startswith('60'):
    index_symbol = '399001'
  elif symbol.startswith('8'):
    index_symbol = '899050'
  elif symbol.startswith('3'):
    index_symbol = '399006'
  elif symbol.startswith('688'):
    index_symbol = '000688'
  else:
    index_symbol = '000001'
  
  return get_index_p_change(symbol=index_symbol, date = date)

def get_index_p_change(symbol: str, date: str):
  cur, conn = common.connect_db()
  date = get_previous_trading_date(date, include_this_day=True)
  sql = f"select p_change from stock_index_hist where code = '{symbol}' and date = '{date}'"
  cur.execute(sql)
  results = cur.fetchall()

  if not len(results) or not results[0][0]:
    return 0
  
  p_change = results[0][0]
  cur.close()
  conn.close()
  return p_change

# 获取股票收益率 close[-1] / close[0] - 1
def get_stock_earnings(symbol: str = None, date: str = None, count: int = 2):
  cur, conn = common.connect_db()
  if symbol is None or date is None:
    return 0

  end_date = pd.to_datetime(date).date().strftime('%Y-%m-%d')
  start_date = get_previous_trading_date((pd.to_datetime(date) - datetime.timedelta(days = count)).strftime('%Y-%m-%d'))
  sql = f"select close from stock_hist where code = '{symbol}' and date = '{start_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if not len(results) or not results[0][0]:
    return 0
  
  start_close = results[0][0]

  sql = f"select close from stock_hist where code = '{symbol}' and date = '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if not len(results) or not results[0][0]:
    return 0
  
  end_close = results[0][0]
  cur.close()
  conn.close()
  return end_close / start_close - 1


# 获取股票指数收益率 close[-1] / close[0] - 1
def get_stock_index_earnings(symbol: str = None, date: str = None, count: int = 20):
  cur, conn = common.connect_db()
  if symbol is None or date is None:
    return 0

  end_date = pd.to_datetime(date).date().strftime('%Y-%m-%d')
  start_date = get_previous_trading_date((pd.to_datetime(date) - datetime.timedelta(days = count)).strftime('%Y-%m-%d'))
  sql = f"select close from stock_index_hist where code = '{symbol}' and date = '{start_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if not len(results) or not results[0][0]:
    return 0
  
  start_close = results[0][0]

  sql = f"select close from stock_index_hist where code = '{symbol}' and date = '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if not len(results) or not results[0][0]:
    return 0
  
  end_close = results[0][0]
  cur.close()
  conn.close()
  return end_close / start_close - 1


# 获取指数列表
def get_stock_index():
  cur, conn = common.connect_db()
  sql = f"select code from stock_index"
  cur.execute(sql)
  results = cur.fetchall()
  codes = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  return codes

# 获取指数列表sec_id
def get_stock_index_ids():
  cur, conn = common.connect_db()
  sql = f"select sec_id from stock_index"
  cur.execute(sql)
  results = cur.fetchall()
  ids = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  return ids

# 获取指定时间周期的股票涨跌幅
def get_period_stock_p_change(symbol: str, start_date: str, end_date: str):
  start_date = pd.to_datetime(start_date).date().strftime('%Y-%m-%d')
  end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')
  cur, conn = common.connect_db()

  sql = f"select open from stock_hist where code = '{symbol}' and date = '{start_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if len(results) == 0 or not results[0][0]:
     return -9999
  
  start_open = results[0][0]

  sql = f"select close from stock_hist where code = '{symbol}' and date = '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  if len(results) == 0 or not results[0][0]:
    return -9999
  
  end_close = results[0][0]
  cur.close()
  conn.close()
  return (end_close - start_open) / start_open


# 获取指定时间周期的指数涨跌幅
def get_period_stock_index_p_change(symbol: str, start_date: str, end_date: str):
  start_date = pd.to_datetime(start_date).date().strftime('%Y-%m-%d')
  end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')
  cur, conn = common.connect_db()

  sql = f"select open from stock_index_hist where code = '{symbol}' and date = '{start_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  cur.close()
  conn.close()
  if len(results) == 0 or not results[0][0]:
     return -9999
  
  start_open = results[0][0]

  sql = f"select close from stock_index_hist where code = '{symbol}' and date = '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()

  if len(results) == 0 or not results[0][0]:
    return -9999
  
  end_close = results[0][0]
  return (end_close - start_open) / start_open

# 获取指定周期的集合竞价情况，包含开盘和收盘价
def get_period_stock_call_auction(symbol: str, end_date: str, days: int = 30):
  cur, conn = common.connect_db()

  end_date = pd.to_datetime(end_date).date().strftime('%Y-%m-%d')
  sql = f"SELECT date, open, close FROM stock.stock_hist where date <= '{end_date}' and code = '{symbol}' order by date desc limit {days};"
  cur.execute(sql)
  results = cur.fetchall()
  columns = ['date', 'open', 'close']
  df = pd.DataFrame(results, columns=columns)
  df['pre_close'] = df['close'].shift(-1)
  df = df.drop(df.index[-1])
  cur.close()
  conn.close()
  return df

# 获取指定日期的开盘涨跌幅，(开盘 - 前一日收盘) / 前一日收盘 * 100
def get_stock_open_p_change(symbol: str, date: str):
  cur, conn = common.connect_db()
  trading_date = get_next_trading_date(date, include_this_day=True)
  sql = f"SELECT date, open, close FROM stock_hist where date <= '{trading_date}' and code = '{symbol}' order by date desc limit 2"
  cur.execute(sql)
  results = cur.fetchall()
  columns = ['date', 'open', 'close']
  df = pd.DataFrame(results, columns=columns)
  if df.empty:
    return 0
  
  df['pre_close'] = df['close'].shift(-1)
  df = df.drop(df.index[-1])
  cur.close()
  conn.close()
  return (df.iloc[0].open - df.iloc[0].pre_close) / df.iloc[0].pre_close * 100

# 获取指定日期的涨跌幅
def get_stock_close_p_change(symbol: str, date: str):
  cur, conn = common.connect_db()
  trading_date = get_next_trading_date(date, include_this_day=True)
  sql = f"SELECT p_change FROM stock_hist where date = '{trading_date}' and code = '{symbol}'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  if len(results) == 0 or not results[0][0]:
    return 0
  return results[0][0]


def _custom_check_sequent_down(data):
  # """
  # 检查滚动窗口内的第一个值是否为正数，而其余值都为负数。
  # """

  if data[0] > 0 and all(data[1:len(data)-1] < 0) and all(data[1:len(data)-1] > -5):
    return True
  else:
    return False

# 获取连续跌n天后的下一天涨跌幅
def get_stock_p_change_after_sequent_down(symbol: str, sequent_down_days: int = 5):
  cur, conn = common.connect_db()
  sql = f"SELECT date, p_change FROM stock_hist where code = '{symbol}' and date > '2021-01-01'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['date', 'p_change'])
  df['sequent_valid'] = df['p_change'].rolling(window=sequent_down_days + 2).apply(lambda x: _custom_check_sequent_down(x), raw=True)
 
  filtered_df = df[df['sequent_valid'] == 1.0]
  filtered_df = filtered_df.drop('sequent_valid', axis = 1)
  return filtered_df

  
# 获取成交量大幅扩大后的下一天涨跌幅
def get_stock_p_change_after_volume_up(symbol: str, end_date: str = None, count_bench: int = 365, exclude_cannot_buy: bool = False):
  cur, conn = common.connect_db()
  if end_date is None:
    end_date = datetime.date.today()
  else:
    end_date = pd.to_datetime(end_date)
  start_date =  (end_date - datetime.timedelta(days = count_bench)).strftime('%Y-%m-%d')
  sql = f"SELECT date, p_change, volume FROM stock_hist where code = '{symbol}' and date > '{start_date}' and date <= '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['date', 'p_change', 'volume'])

  df['volume_diff'] = df['volume'].diff()
  df['volume_diff_rate'] = df['volume_diff'] / df['volume'].shift(1)
  df['pre_p_change'] = df['p_change'].shift(1)
  df['pre_volume'] = df['volume'].shift(1)
  df['pre_volume_diff'] = df['volume_diff'].shift(1)
  df['pre_volume_diff_rate'] = df['volume_diff_rate'].shift(1)
  # condition = (df['volume_diff_rate'].shift(1) > 2) & (df['volume_diff_rate'].shift(1) < 4) & (df['p_change'].shift(1) > 0) & (df['p_change'].shift(1) + df['p_change'].shift(2) < 10)
  # condition = (df['volume_diff_rate'].shift(1) > 2) & (df['p_change'].shift(1) > 0) & (df['p_change'].shift(1) + df['p_change'].shift(2) < 10)
  condition = (df['volume_diff_rate'].shift(1) > -0.01) & (df['volume_diff_rate'].shift(1) < 0.01) & (df['p_change'].shift(1) > 0) & (df['p_change'].shift(1) + df['p_change'].shift(2) < 10)
  if exclude_cannot_buy:
    condition = condition & (df['p_change'].shift(1) < 9.8)
  df = df[condition]
  return df

# 获取成交量大幅扩大的股票
def get_stock_volume_up(date: str = None, exclude_cannot_buy: bool = False):
  cur, conn = common.connect_db()
  if date is None:
    date = datetime.date.today()
  trading_date = get_previous_trading_date(date, include_this_day=True)
  trading_date_before_1 = get_previous_trading_date(trading_date)
  sql = f"SELECT code, date, p_change, volume FROM stock_hist WHERE date >= '{trading_date_before_1}' and date <= '{trading_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['code', 'date', 'p_change', 'volume'])
  trading_df = df[df['date'] == trading_date]
  trading_df = trading_df.sort_values('code').set_index('code', drop=False)

  trading_before_1_df = df[df['date'] == trading_date_before_1]
  trading_before_1_df = trading_before_1_df.sort_values('code').set_index('code', drop=False)

  trading_df['pre_p_change'] = trading_before_1_df['p_change']
  trading_df['volume_diff'] = trading_df['volume'] - trading_before_1_df['volume']
  trading_df['volume_diff_rate'] = trading_df['volume_diff'] / trading_before_1_df['volume']
  # condition = (trading_df['volume_diff_rate'] > 2) & (trading_df['volume_diff_rate'] < 4) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  # condition = (trading_df['volume_diff_rate'] > 2) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  condition = (trading_df['volume_diff_rate'] > -0.01) & (trading_df['volume_diff_rate'] < 0.01) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  trading_df = trading_df[condition]
  if exclude_cannot_buy:
    trading_df = trading_df[trading_df['p_change'] < 9.8]
  return trading_df

# 获取今日成交量大幅扩大的股票
def get_today_stock_volume_up(exclude_cannot_buy: bool = False):
  cur, conn = common.connect_db()
  trading_date = get_previous_trading_date(datetime.date.today(), include_this_day=True)
  trading_date_before_1 = get_previous_trading_date(trading_date)
  trading_date_before_2 = get_previous_trading_date(trading_date_before_1)
  sql = f"SELECT code, date, p_change, volume FROM stock_hist WHERE date >= '{trading_date_before_2}' and date <= '{trading_date_before_1}'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['code', 'date', 'p_change', 'volume'])
  trading_df = one_data.get_today_stock_hist()
  
  trading_df = trading_df.sort_values('code').set_index('code', drop=False)

  trading_before_1_df = df[df['date'] == trading_date_before_1]
  trading_before_1_df = trading_before_1_df.sort_values('code').set_index('code', drop=False)

  trading_df['pre_p_change'] = trading_before_1_df['p_change']
  trading_df['volume_diff'] = trading_df['volume'] - trading_before_1_df['volume']
  trading_df['volume_diff_rate'] = trading_df['volume_diff'] / trading_before_1_df['volume']
  # condition = (trading_df['volume_diff_rate'] > 2) & (trading_df['volume_diff_rate'] < 4) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  # condition = (trading_df['volume_diff_rate'] > 2) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  condition = (trading_df['volume_diff_rate'] > -0.01) & (trading_df['volume_diff_rate'] < 0.01) & (trading_df['p_change'] > 0) & (trading_df['p_change'] + trading_df['pre_p_change'] < 10)
  filtered_df = trading_df[condition]
  if exclude_cannot_buy:
    filtered_df = filtered_df[filtered_df['p_change'] < 9.8]
  return filtered_df
  
# 获取指定周期内高涨幅股票的下一天涨跌幅
def get_stock_p_change_after_big_increase(symbol: str, end_date: str = None, count_bench: int = 365, drop: float = 0.01):
  cur, conn = common.connect_db()
  if end_date is None:
    end_date = datetime.date.today()
  else:
    end_date = pd.to_datetime(end_date)
  start_date =  (end_date - datetime.timedelta(days = count_bench)).strftime('%Y-%m-%d')
  sql = f"SELECT code, date, p_change, open, close, high FROM stock_hist where code = '{symbol}' and date > '{start_date}' and date < '{end_date}'"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['code', 'date', 'p_change', 'open', 'close', 'high'])
  condition = (df['p_change'].shift(1).between(5, 9.8)) & (df['close'].shift(1) > df['open'].shift(1)) & ((df['high'].shift(1) - df['close'].shift(1)) / df['close'].shift(1) < drop)
  filtered_df = df[condition]
  return filtered_df

# 获取指定交易日涨幅 > 7【高涨幅】，且收盘价高于开盘价【红线】，且（最高价 - 收盘价） / 收盘价 < 0.01【强势】的股票
def get_big_increase_stock(date: str = None, drop: float = 0.01):
  cur, conn = common.connect_db()
  if date is None:
    date = datetime.date.today()
  
  date = get_previous_trading_date(date, include_this_day=True)
  sql = f"SELECT date, code, p_change FROM stock_hist where date = '{date}' and p_change > 5 and p_change < 9.8 and close > open and (high - close) / close < {drop}"
  cur.execute(sql)
  results = cur.fetchall()
  
  cur.close()
  conn.close()
  df = pd.DataFrame(results, columns=['date', 'code', 'p_change'])

  return df

# 获取小盘股， 流通股 < 1亿
def get_small_stock(date: str):
  cur, conn = common.connect_db()
  if date is None:
    date = datetime.date.today()

  date = get_previous_trading_date(date, include_this_day=True)
  sql = f"SELECT code FROM stock.stock_valuation where free_shares < 100000000 and date = '{date}';"
  cur.execute(sql)
  results = cur.fetchall()
  codes = pd.Series([item[0] for item in results])
  cur.close()
  conn.close()
  return codes
