# coding=utf-8
import common
import time
import tushare as ts
# filter p_change, default p_change > 5
def filter_p_change(date = False, _filter = 5):
	cur, conn = common.connect_db()
	date = date or time.strftime("%Y-%m-%d", time.localtime(time.time()))
	sql = "SELECT code FROM `stock_hist` where p_change > " + str(_filter) + " and date = '" + date + "';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes

# filter v_change, default v_change > 2
def filter_v_change(date = False, _filter = 2):
	cur, conn = common.connect_db()
	date = date or time.strftime("%Y-%m-%d", time.localtime(time.time()))
	sql = "SELECT code FROM `stock_hist` where volume/v_ma5 > " + str(_filter) + " and date = '" + date + "';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes

# filter close, default close < ma5
def filter_p_compare(date = False, _filter = 5):
	cur, conn = common.connect_db()
	date = date or time.strftime("%Y-%m-%d", time.localtime(time.time()))
	if _filter == 10:
		sql = "SELECT code FROM `stock_hist` where close < ma10 and date = '" + date + "';"
	elif _filter == 20:
		sql = "SELECT code FROM `stock_hist` where close < ma20 and date = '" + date + "';"
	else:
		sql = "SELECT code FROM `stock_hist` where close < ma5 and date = '" + date + "';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes

def filter_utilities():
	cur, conn = common.connect_db(charset = 'utf8')
	utility1 = u'供水供气'
	utility2 = u'电力行业'
	utility3 = u'环保行业'
	sql = "SELECT code FROM `stock_classfy` where c_name in ('" + utility1 + "', '" + utility2 + "', '" + utility3 + "');"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0].encode('utf-8'))
	cur.close()
	conn.close()
	return codes

def filter_financials():
	cur, conn = common.connect_db(charset = 'utf8')
	finance1 = u'房地产'
	finance2 = u'金融行业'
	sql = "SELECT code FROM `stock_classfy` where c_name in ('" + finance1 + "', '" + finance2 + "');"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0].encode('utf-8'))
	cur.close()
	conn.close()
	return codes

# S, ST, STOP
def filter_loss():
	cur, conn = common.connect_db()
	sql = "SELECT code FROM `stock_basic` where name regexp 's';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	# S, ST
	for code in result:
		codes.append(code[0])

	sql = "SELECT date FROM `stock_hist` WHERE 1 ORDER BY date DESC LIMIT 1;"
	count = cur.execute(sql)
	result = cur.fetchone()
	lastDate = result[0]
	lastDate = common.nextDay(lastDate.year, lastDate.month, lastDate.day, _type = "sqlDate", gap = 0)
	sql = "SELECT code FROM `stock_hist` where date = '" + lastDate + "';";
	count = cur.execute(sql)
	result = cur.fetchall()
	normalCodes = []
	for code in result:
		normalCodes.append(code[0])

	sql = "SELECT code FROM `stock_basic`";
	count = cur.execute(sql)
	result = cur.fetchall()
	stopCodes = []
	for code in result:
		if code[0] not in normalCodes:
			stopCodes.append(code[0])

	cur.close()
	conn.close()
	return common.unionList(codes, stopCodes)

# Growth Enterprises Market 
def filter_gem():
	cur, conn = common.connect_db()
	sql = "SELECT code FROM `stock_basic` where code REGEXP '^3';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes

# Small and medium enterprises
def filter_sme():
	cur, conn = common.connect_db()
	sql = "SELECT code FROM `stock_basic` where code REGEXP '^002';"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes


def sort_pe(limit = 100):
	cur, conn = common.connect_db()
	sql = "SELECT code FROM `stock_basic` WHERE esp > 0 and pe > 0 order by pe ASC limit " + str(limit) + ";"
	count = cur.execute(sql)
	result = cur.fetchall()
	codes = []
	for code in result:
		codes.append(code[0])
	cur.close()
	conn.close()
	return codes

def sort_roe(year = False, season = False, limit = 100):
	date = time.localtime(time.time())
	if year == False:
		year = date.tm_year
		month = date.tm_mon
		if month <= 4:
			season = 3
			year = year - 1
		elif month <= 7:
			season = 1
		elif month <= 10:
			season = 2
		else:
			season = 3
	df = ts.get_report_data(year, season)
	df = df.sort_values(by='roe', ascending = False)
	df = df[0:limit]
	df = df.loc[:,['code']];
	df.index = range(0, limit)
	return list(df.code)
