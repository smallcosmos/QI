import types
import time
import datetime
import mysql.connector as db
import configparser
from sqlalchemy import create_engine
import os

config = configparser.ConfigParser()
configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config.ini')
config.read(configFile)

# database default config
database_origin = 'tencent_os_database'
__host__ = config.get(database_origin, "host")
__user__ = config.get(database_origin, "user")
__passwd__ = config.get(database_origin, "passwd")
__db__ = config.get(database_origin, "db")
__easy__ = config.get(database_origin, "easy")

def connect_db(host = __host__, user = __user__, passwd = __passwd__, database = __db__, charset = False):
	if charset == False:
		conn = db.connect(host=host, user=user, passwd=passwd, db=database)
	else:
		conn = db.connect(host=host, user=user, passwd=passwd, db=database, charset=charset)
	cur = conn.cursor()
	return cur, conn

def get_engine(host = __host__, user = __user__, passwd = __passwd__, database = __db__):
	return create_engine('mysql+mysqlconnector://' + user + ':' + passwd + '@' + host + '/' + database +'?charset=utf8')

def nextDay(year, mon=0, day=0, _type='string', gap=1):
	if type(year) == types.StringType:
		if len(year) == 8:
			date = time.strptime(year, "%Y%m%d")
		elif len(year) == 10:
			date= time.strptime(year, "%Y-%m-%d")
		else:
			return False
		year = date.tm_year
		mon = date.tm_mon
		day = date.tm_mday
	elif type(year) == types.IntType:
		pass

	if gap < 0:
		forward = 'back'
		gap = abs(gap)
	else:
		forward = 'forward'

	date = datetime.datetime(year, mon, day)
	if forward == 'back':
		date -= datetime.timedelta(days = gap)
	else:
		date += datetime.timedelta(days = gap)

	if _type == 'string':
		return '{:0>4}'.format(str(date.year)) + '{:0>2}'.format(str(date.month)) + '{:0>2}'.format(str(date.day))
	elif _type == 'sqlDate':
		return '{:0>4}'.format(str(date.year)) + '-' + '{:0>2}'.format(str(date.month)) + '-' + '{:0>2}'.format(str(date.day))
	elif _type == 'ymd':
		return date.year, date.month, date.day

def mergeList(list1 = False, list2 = False, lists = False):
	if lists != False:
		if lists == []:
			return []
		else:
			merge = lists[0]
			for _list in lists:
				merge = [val for val in _list if val in merge]
	else:
		merge = [val for val in list1 if val in list2]
	return merge

def unionList(list1 = False, list2 = False, lists = False):
	if lists != False:
		if lists == []:
			return []
		else:
			union = lists[0]
			for _list in lists:
				union = list(set(union).union(set(_list)))
	else:
		union = list(set(list1).union(set(list2)))
	return union

def diffList(list1 = False, list2 = False, lists = False):
	if lists != False:
		if lists == []:
			return []
		else:
			diff = lists[0]
			lists.remove(lists[0])
			for _list in lists:
				diff = [val for val in diff if val not in _list]
	else:
		diff = [val for val in list1 if val not in list2]
	return diff