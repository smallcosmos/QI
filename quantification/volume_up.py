# coding=utf-8
import numpy as np
import pandas as pd
import utils
import time
from pandas import DataFrame
import schedule
import datetime

def algo(date: str, exclude_688: bool = False, exclude_48: bool = False, exclude_3: bool = False, exclude_cannot_buy: bool = False, debug: bool = False):
  stock_volume_up = utils.get_stock_volume_up(date, exclude_cannot_buy = exclude_cannot_buy)
  if exclude_688:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('688')]

  if exclude_48:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('8')]
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('4')]
  
  if exclude_3:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('3')]

  pef = []
  next_1_p_change = []
  next_2_p_change = []
  index_p_change = []
  for code in stock_volume_up['code'].values:
    next_1_date = utils.get_next_trading_date(date)
    next_1_p_change.append(utils.get_stock_close_p_change(code, next_1_date))
    next_2_date = utils.get_next_trading_date(next_1_date)
    next_2_p_change.append(utils.get_stock_close_p_change(code, next_2_date))

    df = utils.get_stock_p_change_after_volume_up(code, end_date=date, exclude_cannot_buy=exclude_cannot_buy)
    pef.append(df['p_change'].mean())
    index_p_change.append(utils.get_index_p_change_by_stock(code, date))
    
  stock_volume_up['next_1_p_change'] = next_1_p_change
  stock_volume_up['next_2_p_change'] = next_2_p_change
  stock_volume_up['pef'] = pef
  stock_volume_up['index_p_change'] = index_p_change
  stock_volume_up = stock_volume_up.sort_values('pef', ascending=False).reset_index(drop=True)

  if debug:
    print(f"{date} volume up: ")
    print(stock_volume_up)
    print('next_1_p_change mean: ', stock_volume_up['next_1_p_change'].mean())
  return stock_volume_up

def algo_today(exclude_688: bool = False, exclude_48: bool = False, exclude_3: bool = False, exclude_cannot_buy: bool = False,  debug: bool = False):
  date = datetime.date.today()
  stock_volume_up = utils.get_today_stock_volume_up(exclude_cannot_buy = exclude_cannot_buy)
  if exclude_688:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('688')]

  if exclude_48:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('8')]
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('4')]

  if exclude_3:
    stock_volume_up = stock_volume_up[~stock_volume_up['code'].str.startswith('3')]

  pef = []
  next_1_p_change = []
  next_2_p_change = []
  for code in stock_volume_up['code'].values:
    next_1_date = utils.get_next_trading_date(date)
    next_1_p_change.append(utils.get_stock_close_p_change(code, next_1_date))
    next_2_date = utils.get_next_trading_date(next_1_date)
    next_2_p_change.append(utils.get_stock_close_p_change(code, next_2_date))

    df = utils.get_stock_p_change_after_volume_up(code, end_date=date, exclude_cannot_buy=exclude_cannot_buy)
    pef.append(df['p_change'].mean())
    
  stock_volume_up['next_1_p_change'] = next_1_p_change
  stock_volume_up['next_2_p_change'] = next_2_p_change
  stock_volume_up['pef'] = pef

  stock_volume_up = stock_volume_up.sort_values('pef', ascending=False).reset_index(drop=True)

  if debug:
    print(f"{date} volume up: ")
    print(stock_volume_up)

  return stock_volume_up

# 测试命中策略且第二天涨停的日期
def test_limit_up(date: str,):
  stock_basic = utils.get_stock_codes()
  # stock_basic = ['603280']
  for code in stock_basic:
    df = utils.get_stock_p_change_after_volume_up(code, end_date=date, exclude_cannot_buy=True)
    df['code'] = code
    for date in df['date'].values:
      df = df[df['p_change'] > 9.8]
      if df['date'].size > 0:
        next_1_p_change = []
        next_2_p_change = []
        for date in df['date'].values:
          next_1_date = utils.get_next_trading_date(date)
          next_1_p_change.append(utils.get_stock_close_p_change(code, next_1_date))
          next_2_date = utils.get_next_trading_date(next_1_date)
          next_2_p_change.append(utils.get_stock_close_p_change(code, next_2_date))
        df['next_1_p_change'] = next_1_p_change
        df['next_2_p_change'] = next_2_p_change
    if not df.empty:
      print(df)
    # pef = df['p_change'].mean()


def test(date: str, exclude_688: bool = False, exclude_48: bool = False, exclude_3: bool = False, exclude_cannot_buy: bool = False):
  # 每股买入
  money_per_code = 10000
  # 每天买几股
  code_size = 1

  trading_date = utils.get_previous_trading_date(date, include_this_day=True)
  next_date = utils.get_next_trading_date(date)
  stock_pool_df = algo(trading_date, exclude_688 = exclude_688, exclude_48 = exclude_48, exclude_3 = exclude_3, exclude_cannot_buy = exclude_cannot_buy)
  stock_pool_df = stock_pool_df.head(code_size)

  df = pd.DataFrame(columns=['买入', '下一日涨跌幅', '盈亏'])
  for code in stock_pool_df['code'].values:
    p_change = utils.get_stock_close_p_change(code, next_date)
    df.loc[len(df)] = [code, p_change, p_change * money_per_code / 100]
  
  total_income = df['盈亏'].sum()
  print(f"{trading_date}: 盈亏{total_income}元, 详情：")
  print(df)
  return [trading_date, ','.join(stock_pool_df['code'].values), total_income]

def test_30_days(date: str, exclude_688: bool = False, exclude_48: bool = False, exclude_3: bool = False, exclude_cannot_buy: bool = False):
  dates = utils.get_previous_trading_date(date, include_this_day=True, days=30)
  results = pd.DataFrame(columns=['trading_date', 'symbols', 'diff'])
  for trading_date in dates:
    result = test(trading_date, exclude_688 = exclude_688, exclude_48 = exclude_48, exclude_3 = exclude_3, exclude_cannot_buy = exclude_cannot_buy)
    results.loc[len(results)] = result
  
  print(results)
  print('30天总盈亏：', results['diff'].sum())
  return results['diff'].sum()


def test_hist():
  test_date = pd.Series([
    ['2023-08-23', '2023-10-11'],
    ['2023-07-12', '2023-08-22'],
    # ['2023-05-29', '2023-07-11'],
    # ['2023-04-13', '2023-05-28'],
    # ['2023-03-01', '2023-04-12'],
    # ['2023-01-11', '2023-02-28'],
    # ['2022-11-29', '2023-01-10'],
    # ['2022-10-18', '2022-11-28'],
    # ['2022-08-29', '2022-10-17'],
    # ['2022-07-18', '2022-08-28'],
    # ['2022-06-06', '2022-07-17'],
    # ['2022-04-19', '2022-06-05']
  ])
  df = pd.DataFrame(columns=['日期区间', '收益'])
  for date in test_date:
    diff = test_30_days(date[1], exclude_688 = True, exclude_48 = True, exclude_3 = True, exclude_cannot_buy = True)
    df.loc[len(df)] = [f"{date[0]} ~ {date[1]}", diff]

  print(df)
  print(f"平均30个交易日收益：{df['收益'].mean()}")
# ######### 每日交易1只，每只股票6万 收益数据 #################
# 不含科创板[688], 不含新三板【4】，不含北交所【8】，含创业板【3】，不包含【涨停不可买进】
#                 日期区间        收益
# 0   2023-08-23 ~ 2023-10-11  101496.0
# 1   2023-07-12 ~ 2023-08-22   73164.0
# 2   2023-05-29 ~ 2023-07-11  107772.0
# 3   2023-04-13 ~ 2023-05-28   73434.0
# 4   2023-03-01 ~ 2023-04-12   85938.0
# 5   2023-01-11 ~ 2023-02-28   73290.0
# 6   2022-11-29 ~ 2023-01-10   65592.0
# 7   2022-10-18 ~ 2022-11-28   93678.0
# 8   2022-08-29 ~ 2022-10-17   39318.0
# 9   2022-07-18 ~ 2022-08-28   91296.0
# 10  2022-06-06 ~ 2022-07-17   68652.0
# 11  2022-04-19 ~ 2022-06-05   62040.0
# 平均30个交易日收益：77972.5


# 不含科创板[688], 不含新三板【4】，不含北交所【8】，不含创业板【3】，不包含【涨停不可买进】
#                  日期区间       收益
# 0   2023-08-23 ~ 2023-10-11  63678.0
# 1   2023-07-12 ~ 2023-08-22  34980.0
# 2   2023-05-29 ~ 2023-07-11  64824.0
# 3   2023-04-13 ~ 2023-05-28  58044.0
# 4   2023-03-01 ~ 2023-04-12  58902.0
# 5   2023-01-11 ~ 2023-02-28  68430.0
# 6   2022-11-29 ~ 2023-01-10  62994.0
# 7   2022-10-18 ~ 2022-11-28  71754.0
# 8   2022-08-29 ~ 2022-10-17  47910.0
# 9   2022-07-18 ~ 2022-08-28  74184.0
# 10  2022-06-06 ~ 2022-07-17  44730.0
# 11  2022-04-19 ~ 2022-06-05  32268.0
# 平均30个交易日收益：56891.5

# ######### 每日交易2只，每只股票3万 收益数据 #################
# 不含科创板[688], 不含新三板【4】，不含北交所【8】，含创业板【3】，不包含【涨停不可买进】
#                  日期区间       收益
# 0   2023-08-23 ~ 2023-10-11  76830.0
# 1   2023-07-12 ~ 2023-08-22  57909.0
# 2   2023-05-29 ~ 2023-07-11  79692.0
# 3   2023-04-13 ~ 2023-05-28  68802.0
# 4   2023-03-01 ~ 2023-04-12  68076.0
# 5   2023-01-11 ~ 2023-02-28  66924.0
# 6   2022-11-29 ~ 2023-01-10  53205.0
# 7   2022-10-18 ~ 2022-11-28  66702.0
# 8   2022-08-29 ~ 2022-10-17  41235.0
# 9   2022-07-18 ~ 2022-08-28  82866.0
# 10  2022-06-06 ~ 2022-07-17  60318.0
# 11  2022-04-19 ~ 2022-06-05  47787.0
# 平均30个交易日收益：64195.5

# 不含科创板[688], 不含新三板【4】，不含北交所【8】，不含创业板【3】，不包含【涨停不可买进】
#                  日期区间       收益
# 0   2023-08-23 ~ 2023-10-11  50994.0
# 1   2023-07-12 ~ 2023-08-22  30894.0
# 2   2023-05-29 ~ 2023-07-11  58344.0
# 3   2023-04-13 ~ 2023-05-28  47520.0
# 4   2023-03-01 ~ 2023-04-12  44955.0
# 5   2023-01-11 ~ 2023-02-28  59379.0
# 6   2022-11-29 ~ 2023-01-10  47556.0
# 7   2022-10-18 ~ 2022-11-28  53154.0
# 8   2022-08-29 ~ 2022-10-17  41700.0
# 9   2022-07-18 ~ 2022-08-28  63084.0
# 10  2022-06-06 ~ 2022-07-17  49257.0
# 11  2022-04-19 ~ 2022-06-05  30531.0
# 平均30个交易日收益：48114.0
