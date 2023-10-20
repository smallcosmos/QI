import akshare as ak
import pandas as pd
import configparser
import time
import datetime
import common
import os
import one_data
import utils
import ak_share

config = configparser.ConfigParser()
configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config.ini')
config.read(configFile)

def create_db():
	cur, conn = common.connect_db(database='')
	sql = "CREATE DATABASE IF NOT EXISTS `" + common.__db__ + "` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;"
	cur.execute(sql)
	cur.close()
	conn.close()
	if common.__easy__ == "True":
		create_stock_basic_table()
		create_stock_hist_table()
		create_stock_trade_calendar_table()
		create_stock_valuation_table()
		create_stock_index_table()
		create_stock_index_hist_table()

		# 初始化交易日历
		init_stock_trade_calendar()
		# 初始化指数列表
		init_stock_index()


def create_stock_trade_calendar_table():
	cur, conn = common.connect_db()
	sql = f"CREATE TABLE IF NOT EXISTS `{common.__db__}`.`stock_trade_calendar` ( \
	`trade_date` DATE NOT NULL, \
	PRIMARY KEY (`trade_date`)) \
	ENGINE = MyISAM DEFAULT CHARSET=utf8; \
	ALTER TABLE `stock_trade_calendar` ADD UNIQUE KEY `trade_date` (`trade_date`);"
	cur.execute(sql)
	cur.close()
	conn.close()

#"""call once in application"""
# 【 股票代码， 英文全称， 股票全称， 股票名称， 所属行业， 市场类型， 地域， 成立日期， 上市日期， 上市状态, 股票分类(1-沪深300) 】
stockColumns=['code', 'enName', 'originName', 'name', 'industry', 'market', 'area', 'establishDate', 'listDate', 'listStatus', 'type']
def create_stock_basic_table():
	cur, conn = common.connect_db()
	sql = "CREATE TABLE IF NOT EXISTS `" + common.__db__ + "`.`stock_basic` ( \
	`code` char(10) NOT NULL, \
	`enName` varchar(20) DEFAULT NULL, \
	`originName` varchar(30) DEFAULT NULL, \
	`name` varchar(20) DEFAULT NULL, \
	`industry` varchar(20) DEFAULT NULL, \
	`market` varchar(20) DEFAULT NULL, \
	`area` varchar(30) DEFAULT NULL, \
	`establishDate` date DEFAULT NULL, \
	`listDate` date DEFAULT NULL, \
	`listStatus` varchar(5) DEFAULT NULL, \
	`type` varchar(10) DEFAULT NULL, \
	PRIMARY KEY (`code`)) \
	ENGINE = MyISAM DEFAULT CHARSET=utf8;ALTER TABLE `stock_basic` ADD UNIQUE KEY `code`(`code`);"
	cur.execute(sql)
	cur.close()
	conn.close()

#"""call once in application"""
# 【 主键，股票代码， 交易日期， 开盘价， 收盘价， 最高价， 最低价，成交量，成交额，振幅，涨跌幅，涨跌额，换手率 】
stockHisColumns = ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'p_change', 'price_change', 'turnover']
def create_stock_hist_table():
	cur, conn = common.connect_db()
	sql = "CREATE TABLE IF NOT EXISTS `" + common.__db__ +"`.`stock_hist` ( \
	`index` INT(8) NOT NULL AUTO_INCREMENT, \
	`code` char(10) DEFAULT NULL, \
	`date` date DEFAULT NULL, \
	`open` float DEFAULT NULL, \
	`close` float DEFAULT NULL, \
	`high` float DEFAULT NULL, \
	`low` float DEFAULT NULL, \
	`volume` float DEFAULT NULL, \
	`amount` float DEFAULT NULL, \
	`amplitude` float DEFAULT NULL, \
	`p_change` float DEFAULT NULL, \
	`price_change` float DEFAULT NULL, \
	`turnover` float DEFAULT NULL, \
	PRIMARY KEY (`index`)) ENGINE=MyISAM DEFAULT CHARSET=utf8;\
	ALTER TABLE `stock_hist` ADD UNIQUE KEY `code` (`code`,`date`); \
	create index date_index on stock_hist (date)"
	cur.execute(sql)
	cur.close()
	conn.close()


#"""call once in application"""
# 【 主键，股票代码， 交易日期， 开盘价， 收盘价， 最高价， 最低价，成交量，成交额，振幅，涨跌幅，涨跌额，换手率 】
stockIndexHisColumns = ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'p_change', 'price_change', 'turnover']
def create_stock_index_hist_table():
	cur, conn = common.connect_db()
	sql = "CREATE TABLE IF NOT EXISTS `" + common.__db__ +"`.`stock_index_hist` ( \
	`index` INT(8) NOT NULL AUTO_INCREMENT, \
	`code` char(10) DEFAULT NULL, \
	`date` date DEFAULT NULL, \
	`open` float DEFAULT NULL, \
	`close` float DEFAULT NULL, \
	`high` float DEFAULT NULL, \
	`low` float DEFAULT NULL, \
	`volume` float DEFAULT NULL, \
	`amount` float DEFAULT NULL, \
	`amplitude` float DEFAULT NULL, \
	`p_change` float DEFAULT NULL, \
	`price_change` float DEFAULT NULL, \
	`turnover` float DEFAULT NULL, \
	PRIMARY KEY (`index`)) ENGINE=MyISAM DEFAULT CHARSET=utf8;\
	ALTER TABLE `stock_index_hist` ADD UNIQUE KEY `code` (`code`,`date`); \
	create index date_index on stock_index_hist (date)"
	cur.execute(sql)
	cur.close()
	conn.close()

# 【 主键，股票代码，日期， 滚动市盈率， 市盈率，市净率，滚动市销率，市现率，滚动市现率，收盘价， 流通股本，流通市值，总股本，总市值 】
stockValuationColumns = ['code', 'date', 'pe_ttm', 'pe', 'pb', 'ps_ttm', 'pcf', 'pcf_ttm', 'close', 'free_shares', 'free_market_cap', 'total_shares', 'total_market_cap']
def create_stock_valuation_table():
	cur, conn = common.connect_db()
	sql = "CREATE TABLE IF NOT EXISTS `" + common.__db__ +"`.`stock_valuation` ( \
	`index` INT(8) NOT NULL AUTO_INCREMENT, \
	`code` char(10) DEFAULT NULL, \
	`date` date DEFAULT NULL, \
	`pe_ttm` float DEFAULT NULL, \
	`pe` float DEFAULT NULL, \
	`pb` float DEFAULT NULL, \
	`ps_ttm` float DEFAULT NULL, \
	`pcf` float DEFAULT NULL, \
	`pcf_ttm` float DEFAULT NULL, \
	`close` float DEFAULT NULL, \
	`free_shares` float DEFAULT NULL, \
	`free_market_cap` float DEFAULT NULL, \
	`total_shares` float DEFAULT NULL, \
	`total_market_cap` float DEFAULT NULL, \
	PRIMARY KEY (`index`)) ENGINE=MyISAM DEFAULT CHARSET=utf8; \
	ALTER TABLE `stock_valuation` ADD UNIQUE KEY `code` (`code`,`date`); \
	create index date_index on stock_valuation (date)"
	cur.execute(sql)
	cur.close()
	conn.close()


def create_stock_index_table():
	cur, conn = common.connect_db()
	sql = "CREATE TABLE IF NOT EXISTS `" + common.__db__ + "`.`stock_index` ( \
	`index` INT(8) NOT NULL AUTO_INCREMENT, \
	`code` char(10) NOT NULL, \
	`sec_id` char(10) NOT NULL, \
	`name` varchar(20) DEFAULT NULL, \
	PRIMARY KEY (`index`)) \
	ENGINE = MyISAM DEFAULT CHARSET=utf8;ALTER TABLE `stock_index` ADD UNIQUE KEY `code`(`code`);"
	cur.execute(sql)
	cur.close()
	conn.close()

def init_stock_index():
	engine = common.get_engine()
	dfInMarket = one_data.get_stock_index()
	dfInMarket = dfInMarket.rename(columns={'f12' : 'code', 'f13': 'sec_id', 'f14': 'name'})
	dfInMarket['sec_id'] = dfInMarket['sec_id'].astype(str) + '.' + dfInMarket['code']
	dfInMarket.to_sql('stock_index', con=engine, if_exists='replace', method='multi', index=False, chunksize=1000)
	print(f'insert {dfInMarket.index.size} records into stock_index')


def init_stock_trade_calendar():
	engine = common.get_engine()
	dfInMarket = ak.tool_trade_date_hist_sina()
	dfInMarket['trade_date'].to_sql('stock_trade_calendar', con=engine, if_exists='replace', chunksize=1000, index=False)
	print(f'insert {dfInMarket.index.size} records to stock_trade_calendar')

def getTodayNewStock():
	current_date = datetime.date.today()
	stock_dxsyl_em_df = ak.stock_dxsyl_em()
	newStock = stock_dxsyl_em_df[stock_dxsyl_em_df['上市日期'] == current_date]
	return newStock['股票代码']

def update_stock_basic_append():
	engine = common.get_engine()
	stock_codes = one_data.get_stock_a().to_list()
	stock_off_codes = ak_share.get_off_codes()
	total_codes = list(set(stock_codes) | set(stock_off_codes))
	db_codes = utils.get_stock_codes().to_list()
	db_off_codes = utils.get_off_stock_codes().to_list()
	db_all_codes = utils.get_all_stock_codes().to_list()

	updateStock = common.diffList(total_codes, db_all_codes)
	dfInMarket = pd.DataFrame(columns=stockColumns)
	df = one_data.get_stock_basic(updateStock)
	for index, row in df.iterrows():
		new_row = {
			'code': row['SECURITY_CODE'],
			'enName': row['ORG_NAME_EN'][:20] if row['ORG_NAME_EN'] is not None else '', #
			'originName': row['ORG_NAME'],
			'name': row['SECURITY_NAME_ABBR'],
			'industry': row['INDUSTRYCSRC1'][:20] if row['INDUSTRYCSRC1'] is not None else '',
			'market': row['TRADE_MARKETT'],
			'area': row['REG_ADDRESS'][:30] if row['REG_ADDRESS'] is not None else '', #
			'listStatus': '正常上市',
			'establishDate': None, #
			'listDate': None,
			'type': None,
		}
		dfInMarket.loc[len(dfInMarket)] = new_row
	
	dfInMarket = dfInMarket.sort_values('code').reset_index(drop=True)
	dfInMarket.to_sql('stock_basic', con=engine, if_exists='append', chunksize=100, index=False)
	print(f'insert {dfInMarket.index.size} records to stock_basic')

	update_off_codes = common.diffList(stock_off_codes, db_off_codes)
	utils.update_outdate_list_status_code(update_off_codes)

def update_stock_hist_period(start_date: str = None, end_date: str = None, start_code: str = None):
	if end_date is None:
		end_date = datetime.date.today().strftime("%Y-%m-%d")
	if start_date is None:
		start_date = end_date
	
	init_stock_hist(start_date, end_date, start_code)

def init_stock_hist(start_date: str = '1970-01-01', end_date: str = None, start_code: str = None):
	cur, conn = common.connect_db()
	engine = common.get_engine()
	if end_date is None:
		end_date = datetime.date.today().strftime("%Y%m%d")
	
	start_date = pd.to_datetime(start_date).date().strftime('%Y%m%d')
	end_date = pd.to_datetime(end_date).date().strftime('%Y%m%d')
	codes = utils.get_all_stock_codes()
	if start_code:
		codes = codes[codes >= start_code]
	for code in codes:
		dfInMarket = pd.DataFrame(columns=stockHisColumns)
		if (str(code).startswith('400') or str(code).startswith('420') or str(code).startswith('900') ):
			continue
		df = ak.stock_zh_a_hist(symbol=code, start_date=start_date, end_date=end_date)
		for index, row in df.iterrows():
			new_row = {
				'code': code,
				'date': row['日期'],
				'open': row['开盘'],
				'close': row['收盘'],
				'high': row['最高'],
				'low': row['最低'],
				'volume': row['成交量'],
				'amount': row['成交额'],
				'amplitude': row['振幅'],
				'p_change': row['涨跌幅'],
				'price_change': row['涨跌额'],
				'turnover': row['换手率'],
			}
			dfInMarket.loc[len(dfInMarket)] = new_row
		
		dfInMarket.to_sql('stock_hist', con=engine, if_exists='append', chunksize=1000, index=False)
		print(f'insert {code} {start_date} ~ {end_date}: {dfInMarket.index.size} records to stock_basic')
		
		time.sleep(0.1)

	cur.close()
	conn.close()

def update_stock_hist_today():
	engine = common.get_engine()
	cur, conn = common.connect_db()
	stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
	stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['最新价'].notna()]
	
	dfInMarket = pd.DataFrame(columns=stockHisColumns)
	for index, row in stock_zh_a_spot_em_df.iterrows():
		new_row = {
			'code': row['代码'],
			'date': pd.to_datetime(datetime.date.today()),
			'open': row['今开'],
			'close': row['最新价'],
			'high': row['最高'],
			'low': row['最低'],
			'volume': row['成交量'],
			'amount': row['成交额'],
			'amplitude': row['振幅'],
			'p_change': row['涨跌幅'],
			'price_change': row['涨跌额'],
			'turnover': row['换手率'],
		}
		dfInMarket.loc[len(dfInMarket)] = new_row
	
	dfInMarket.to_sql('stock_temp', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

	columns = ','.join(stockHisColumns)
	cur.execute(f'REPLACE INTO stock_hist ({columns}) SELECT {columns} FROM stock_temp;')
	print(f'insert {dfInMarket.index.size} records to stock_hist')

	cur.close()
	conn.close()

def update_stock_300():
	cur, conn = common.connect_db()
	db_stock300 = utils.get_stock_300()
	current_stock300 = one_data.get_stock300()
	outdated_stock = common.diffList(db_stock300.to_list(), current_stock300['SECURITY_CODE'].to_list())
	new_stock300 = common.diffList(current_stock300['SECURITY_CODE'].to_list(), db_stock300.to_list())
	if len(outdated_stock):
		clean_outdated_sql = f'update stock_basic set type = Null where code in ({",".join(outdated_stock)})'
		cur.execute(clean_outdated_sql)
		print(f'stock300 clean: remove {len(outdated_stock)} records')

	if len(new_stock300):
		update_new_sql = f'update stock_basic set type = 1 where code in ({",".join(new_stock300)})'
		cur.execute(update_new_sql)
		print(f'stock300 update: add {len(new_stock300)} records')
	
	cur.close()
	conn.close()

def update_stock_valuation_today(date: str = None):
	if date is None:
		date = datetime.date.today().strftime("%Y-%m-%d")
	
	valuation = one_data.get_stock_valuation(date)
	if valuation.empty:
		print(f'{date} stock valuation empty')
		return

	engine = common.get_engine()
	dfInMarket = pd.DataFrame(columns=stockValuationColumns)
	pre_code = ''
	for index, row in valuation.iterrows():
		if pre_code == row['SECURITY_CODE']:
			# 源数据存在重复的脏数据，清理掉
			continue
		pre_code = row['SECURITY_CODE']
		new_row = {
			'code': row['SECURITY_CODE'],
			'date': pd.to_datetime(date),
			'pe_ttm': row['PE_TTM'],
			'pe': row['PE_LAR'],
			'pb': row['PB_MRQ'],
			'ps_ttm': row['PS_TTM'],
			'pcf': row['PCF_OCF_LAR'],
			'pcf_ttm': row['PCF_OCF_TTM'],
			'close': row['CLOSE_PRICE'],
			'free_shares': row['FREE_SHARES_A'],
			'free_market_cap': row['NOTLIMITED_MARKETCAP_A'],
			'total_shares': row['TOTAL_SHARES'],
			'total_market_cap': row['TOTAL_MARKET_CAP']
		}
		dfInMarket.loc[len(dfInMarket)] = new_row

	dfInMarket.to_sql('stock_valuation', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
	print(f'update {date} stock valuation succeed, insert {dfInMarket.index.size} records')
	
def update_stock_valuation_period(start_date: str = '2018-01-01', end_date: str = None):
	if end_date is None:
		end_date = datetime.date.today().strftime("%Y-%m-%d")
	if start_date is None:
		start_date = end_date
	elif start_date < '2018-01-01':
		start_date = '2018-01-01'

	trading_date = utils.get_trading_date(start_date = start_date, end_date = end_date)

	for date in trading_date:
		update_stock_valuation_today(date)
	
	print('update_stock_valuation_period succeed')


def update_stock_index_hist_period(start_date: str = None, end_date: str = None):
	if end_date is None:
		end_date = datetime.date.today().strftime("%Y-%m-%d")
	if start_date is None:
		start_date = end_date
	
	init_stock_index_hist(start_date, end_date)


def init_stock_index_hist(start_date: str = '1970-01-01', end_date: str = None):
	engine = common.get_engine()
	if end_date is None:
		end_date = datetime.date.today().strftime("%Y-%m-%d")
	
	code_ids = utils.get_stock_index_ids()

	for sec_id in code_ids:
		df = one_data.get_stock_index_hist(sec_id=sec_id, start_date=start_date, end_date=end_date)

		dfInMarket = pd.DataFrame(columns=stockIndexHisColumns)
		for index, row in df.iterrows():
			new_row = {
				'code': sec_id.split('.')[1],
				'date': pd.to_datetime(row['date']),
				'open': row['open'],
				'close': row['close'],
				'high': row['high'],
				'low': row['low'],
				'volume': row['volume'],
				'amount': row['amount'],
				'amplitude': row['amplitude'],
				'p_change': row['p_change'],
				'price_change': row['price_change'],
				'turnover': row['turnover'],
			}
			dfInMarket.loc[len(dfInMarket)] = new_row
		
		dfInMarket.to_sql('stock_index_hist', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
		print(f"insert into stock_index_hist: {sec_id} {dfInMarket.index.size} records succeed")
		time.sleep(0.1)

	print('init_stock_index_hist done')


def update_stock_index_hist_today():
	cur, conn = common.connect_db()
	engine = common.get_engine()
	dfInMarket = one_data.get_stock_index_today()
	dfInMarket.to_sql('stock_temp', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

	columns = ','.join(stockIndexHisColumns)
	cur.execute(f'REPLACE INTO stock_index_hist ({columns}) SELECT {columns} FROM stock_temp;')
	print(f'insert {dfInMarket.index.size} records to stock_hist')

	cur.close()
	conn.close()