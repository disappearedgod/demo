# -*- coding:utf-8 -*-

import os
#from sqlalchemy import create_engin
#from pandas.io.pytables import HDFStore
import tushare as ts
from pymongo import MongoClient
import unittest
#coding=utf8
#import ConfigParser
#config = ConfigParser.ConfiParser()
#path = folder

class storing_test(unittest.TestCase):
    def __init__(self):
        print('I\'m storing_test')
        self.code = '000875'
        self.start = '2017-01-03'
        self.end = '2017-01-07'
        self.year = 2017
        self.quarter = 1
    def set_data(self, json):
        self.code = json['code']
        self.start = json['start']
        self.end = json['end']
        self.year = json['year']
        self.quarter = json['quarter']
        print("set_data fin")
    def csv(self):
        df = ts.get_hist_data(self.code)
        df.to_csv('f:/day/000875.csv',columns=['open','high','low','close'])

    def xls(self):
        df = ts.get_hist_data(self.code)
        #直接保存
        df.to_excel('f:/day/000875.xlsx', startrow=2,startcol=5)

    def hdf(self):
        df = ts.get_hist_data(self.code)
    #     df.to_hdf('c:/day/store.h5','table')

        store = HDFStore('f:/day/store.h5')
        store[self.code] = df
        store.close()

    def json(self):
        df = ts.get_hist_data('000875')
        df.to_json('f+:/day/000875.json',orient='records')

        #或者直接使用
        print(df.to_json(orient='records'))

    def appends(self):
        filename = 'f:/day/bigfile.csv'
        for code in ['000875', '600848', '000981']:
            df = ts.get_hist_data(code)
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=None)
            else:
                df.to_csv(filename)

    def db(self):
        df = ts.get_tick_data(self.code,date=self.start)
        engine = create_engine('mysql://root:23122368@127.0.0.1/mystock?charset=utf8')
    #     db = MySQLdb.connect(host='127.0.0.1',user='root',passwd='23122368',db="mystock",charset="utf8")
    #     df.to_sql('TICK_DATA',con=db,flavor='mysql')
    #     db.close()
        df.to_sql('tick_data',engine,if_exists='append')

    def test_nosql(self):
        import json
        conn = MongoClient('127.0.0.1', port=27017)
        df = ts.get_tick_data(self.code,date='2016-12-22')
        print(df.to_json(orient='records'))

        conn.db.tickdata.insert(json.loads(df.to_json(orient='records')))

    #     print conn.db.tickdata.find()


    def test_historySqlhistorySql(self):
        print "test_historySqlhistorySql"
        tmpString = self.queryFromTime()
        #print tmpString
        conn = MongoClient('127.0.0.1', port=27017)
        conn.db.tickdata.insert(tmpString)
        #print "test_historySql finsihed"

    def queryFromTime(self):
        print ("queryFromTime")
        import datetime
        begin_date = self.start
        end_date = self.end
        begin = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        while begin <= end:
            tmpDate = begin.strftime("%Y-%m-%d")
            tmpString = self.queryForADay(tmpDate)
            #print tmpString
            begin += datetime.timedelta(days=1)
        return tmpString

    def queryForADay(self, str_date):
      import json
      tmpJson=[]
      print str_date
      df = ts.get_tick_data(self.code,date=str_date)
      tmpJson = json.loads(df.to_json(orient='records'))
      print len(tmpJson)
      for i in range(len(tmpJson)):
        tmpJson[i][u'today'] = str_date
        tmpJson[i][u'stock'] = self.code
      #print tmpJson
      return tmpJson
    def readHS300Code(self):
        client = MongoClient('localhost', 27017)
        cursor = client.stockcodes.HS300.find()
        list = []
        for item in cursor:
            list.append(item['stockcode'])
        return list

    def readITCode(self):
        client = MongoClient('localhost', 27017)
        cursor = client.stockcodes.IT.find()
        list = []
        for item in cursor:
            list.append(item['stockcode'])
        return list
if __name__ == '__main__':
    #nosql()
    s = storing_test()
    dic ={
        'code' : '300597',
        'start' : '2017-01-03',
        'end' : '2017-01-07',
        'year' : 2017,
        'quarter' : 1,
    };
    list = s.readHS300Code()
    tmp = {}
    for stockCode in list:
        tmp['code'] = stockCode
        tmp['start'] = '2017-01-01'
        tmp['end'] = '2017-05-03'
        tmp['year'] = 2017
        tmp['quarter'] = 1
        print tmp
        s.set_data(tmp)
        s.test_historySqlhistorySql()
