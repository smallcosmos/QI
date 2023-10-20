# coding=utf-8
import numpy as np
import pandas as pd
import utils
# from gm.api import *
from pandas import DataFrame
import schedule
import datetime
# 
# 本策略每隔1个月定时触发,根据Fama-French三因子模型对每只股票进行回归，得到其alpha值。
# 假设Fama-French三因子模型可以完全解释市场，则alpha为负表明市场低估该股，因此应该买入。
# 策略思路：
# 计算市场收益率、个股的账面市值比和市值,并对后两个进行了分类,
# 根据分类得到的组合分别计算其市值加权收益率、SMB和HML.
# 对各个股票进行回归(假设无风险收益率等于0)得到alpha值.
# 选取alpha值小于0并为最小的10只股票进入标的池
# 平掉不在标的池的股票并等权买入在标的池的股票
# 回测数据:SHSE.000300的成份股
# 回测时间:2017-07-01 08:00:00到2017-10-01 16:00:00
#

# 计算市值加权的收益率,MV为市值的分类,BM为账目市值比的分类
def market_value_weighted(stocks, MV, BM):
  select = stocks[(stocks.mv == MV) & (stocks.bm == BM)]
  market_value = select['mv'].values
  mv_total = np.sum(market_value)
  mv_weighted = [mv / mv_total for mv in market_value]
  stock_return = select['return'].values
  # 返回市值加权的收益率的和
  return_total = []
  for i in range(len(mv_weighted)):
    return_total.append(mv_weighted[i] * stock_return[i])

  return_total = np.sum(return_total)
  return return_total

def algo(date: str, count_bench: int):
  context = {
    # 数据滑窗
    'count_bench': count_bench,
    # 账面市值比的大/中/小分类
    'BM_BIG': 3.0,
    'BM_MID': 2.0,
    'BM_SMA': 1.0,
    # 市值大/小分类
    'MV_BIG': 2.0,
    'MV_SMA': 1.0
  }
  # 获取上一个交易日的日期
  last_day = utils.get_previous_trading_date(date=date, include_this_day=True)
  # 获取沪深300成份股
  stock300 = utils.get_stock_300()
  # todo 过滤指定期间无交易的股票
  ###

  # 获取市值、市净率
  fin = utils.get_stock_fin(symbols=stock300, date=last_day)
  # 计算账面市值比,为P/B的倒数
  fin['pb'] = (fin['pb'] ** -1)
  # 计算市值的50%的分位点,用于后面的分类
  size_gate = fin['mc'].quantile(0.50)
  # 计算账面市值比的30%和70%分位点,用于后面的分类
  bm_gate = [fin['pb'].quantile(0.30), fin['pb'].quantile(0.70)]
  x_return = []
  # 对股票进行处理

  for symbol in fin.index:
    # 计算收益率
    stock_return = utils.get_stock_earnings(symbol=symbol, date=last_day, count=context['count_bench'])

    pb = fin['pb'][symbol]
    mc = fin['mc'][symbol]
    # 获取[股票代码. 股票收益率, 账面市值比的分类, 市值的分类, 流通市值]
    if pb < bm_gate[0]:
      if mc < size_gate:
        label = [symbol, stock_return, context['BM_SMA'], context['MV_SMA'], mc]
      else:
        label = [symbol, stock_return, context['BM_SMA'], context['MV_BIG'], mc]
    elif pb < bm_gate[1]:
      if mc < size_gate:
        label = [symbol, stock_return, context['BM_MID'], context['MV_SMA'], mc]
      else:
        label = [symbol, stock_return, context['BM_MID'], context['MV_BIG'], mc]
    elif mc < size_gate:
      label = [symbol, stock_return, context['BM_BIG'], context['MV_SMA'], mc]
    else:
      label = [symbol, stock_return, context['BM_BIG'], context['MV_BIG'], mc]
    
    if len(x_return) == 0:
      x_return = label
    else:
      x_return = np.vstack([x_return, label])

  stocks = DataFrame(data=x_return, columns=['symbol', 'return', 'bm', 'mv', 'mc'])
  stocks.index = stocks.symbol
  columns = ['return', 'bm', 'mv', 'mc']
  for column in columns:
    stocks[column] = stocks[column].astype(np.float64)

  # 计算SMB.HML和市场收益率
  # 获取小市值组合的市值加权组合收益率
  smb_s = (market_value_weighted(stocks, context['MV_SMA'], context['BM_SMA']) + market_value_weighted(stocks, context['MV_SMA'], context['BM_MID']) + market_value_weighted(stocks, context['MV_SMA'], context['BM_BIG'])) / 3
  # 获取大市值组合的市值加权组合收益率
  smb_b = (market_value_weighted(stocks, context['MV_BIG'], context['BM_SMA']) + market_value_weighted(stocks, context['MV_BIG'], context['BM_MID']) + market_value_weighted(stocks, context['MV_BIG'], context['BM_BIG'])) / 3
  smb = smb_s - smb_b
  # 获取大账面市值比组合的市值加权组合收益率
  hml_b = (market_value_weighted(stocks, context['MV_SMA'], context['BM_BIG']) + market_value_weighted(stocks, context['MV_BIG'], context['BM_BIG'])) / 2
  # 获取小账面市值比组合的市值加权组合收益率
  hml_s = (market_value_weighted(stocks, context['MV_SMA'], context['BM_SMA']) + market_value_weighted(stocks, context['MV_BIG'], context['BM_SMA'])) / 2
  hml = hml_b - hml_s

  # 计算沪深300指数收益率
  market_return = utils.get_stock_index_earnings(symbol='000300', date=last_day, count=context['count_bench'])

  coff_pool = []
  # 对每只股票进行回归获取其alpha值
  for stock in stocks.index:
    x_value = np.array([[market_return], [smb], [hml], [1.0]])
    y_value = np.array([stocks['return'][stock]])
    # 最小二乘线性回归, OLS估计系数
    coff = np.linalg.lstsq(x_value.T, y_value)[0][3]
    coff_pool.append(coff)

  # 获取alpha最小并且小于0的10只的股票进行操作(若少于10只则全部买入)
  stocks['alpha'] = coff_pool
  stocks = stocks[stocks.alpha < 0].sort_values(by='alpha', ascending=True).head(30)
  print('symbols_pool: ', stocks)
  symbols_pool = stocks.index.tolist()
  return symbols_pool


def test(test_date: str, count_bench: int = 20, future_days: int = 30):
  symbols_pool = algo(test_date, count_bench)

  start_date = utils.get_next_trading_date(test_date)
  end_date = (pd.to_datetime(test_date) + datetime.timedelta(days=future_days)).strftime('%Y-%m-%d')
  end_date = utils.get_next_trading_date(end_date, include_this_day=True)
  
  stock_300 = utils.get_stock_300()
  stock_300_df = pd.DataFrame(columns=['symbol', 'p_change'])
  for symbol in stock_300:
    p_change = utils.get_period_stock_p_change(symbol=symbol, start_date=start_date, end_date=end_date)
    if p_change == -9999:
      continue
    
    stock_300_df.loc[len(stock_300_df)] = [symbol, p_change]
  
  stock_300_df = stock_300_df.sort_values('p_change', ascending=False).reset_index(drop=True)
  print('stock_300_df: ', stock_300_df)
  stock_pool_df = stock_300_df[stock_300_df['symbol'].isin(symbols_pool)]
  stock_pool_df['symbol'] = pd.Categorical(stock_pool_df['symbol'], categories=symbols_pool, ordered=True)
  stock_pool_df = stock_pool_df.sort_values('symbol')
  print('stock_pool_df: ', stock_pool_df)

  p_change = utils.get_period_stock_index_p_change(symbol='000001', start_date=start_date, end_date=end_date)
  print('stock_index_sh_000001: ', p_change)
  p_change = utils.get_period_stock_index_p_change(symbol='399001', start_date=start_date, end_date=end_date)
  print('stock_index_sz_399001: ', p_change)
  