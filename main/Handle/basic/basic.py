# -*- coding:utf-8 -*-

import os
#from sqlalchemy import create_engin
#from pandas.io.pytables import HDFStore
import tushare as ts
from pymongo import MongoClient
import unittest
import json
import csv
#coding=utf8
#import ConfigParser
#config = ConfigParser.ConfiParser()
#path = folder

class Test(unittest.TestCase):

    def set_data(self, json):
        self.code = json['code']
        self.start = json['start']
        self.end = json['end']
        self.year = json['year']
        self.quarter = json['quarter']
        print("set_data fin")

    def queryForADay(self, str_date):
      import json
      tmpJson=[]
      print (str_date)
      df = ts.get_tick_data(self.code, date=str_date)
      tmpJson = json.loads(df.to_json(orient='records'))
      print (len(tmpJson))
      for i in range(len(tmpJson)):
        tmpJson[i][u'today'] = str_date
        tmpJson[i][u'stock'] = self.code
      #print tmpJson
      return tmpJson
    def readHS300Code(self):
        client = MongoClient('localhost', 27017)
        coll = client['stockcodes']
        cursor = coll.HS300.find()
        list = []
        for item in cursor:
            list.append(item['stockcode'])
        return list
    # def csv(self):
    #     df = ts.get_stock_basics()
    #     # df = ts.get_stock_basics(code,name,industry,area,pe,outstanding,totals,totalAssets,liquidAssets,fixedAssets,reserved,reservedPerShare,esp,bvps,pb,timeToMarket,undp,perundp,rev,profit,gpr,npr,holders,)
    #     print (df)
    #     df.to_csv('E:/test/000875.csv', columns=['代码','名称','所属行业','地区','市盈率','流通股本(亿)','总股本(亿)','总资产(万)','流动资产','固定资产','公积金','每股公积金','每股收益','每股净资','市净率','上市日期','未分利润','每股未分配','收入同比( %)', '利润同比( %)','毛利率( %)',' 净利润率( %)',
    #         '股东人数'])

    def readBasicInfo(self,db, coll_name, condition):
        client = MongoClient('localhost', 27017)
        coll = client[db]
        cursor = coll[coll_name].find()
        list = []
        #isTest = True

        test_len = 0
        for item in cursor:
            if( test_len > 10):
                break
            test_len = test_len + 1
            if item[u'price'] == None:
                continue
            if item[u'code'] == condition:
                list.append(item)
        print(list)


    def test_stratage(self):

        self.readBasicInfo('trading','tick_data','000001')


if __name__ == '__main__':
    #nosql()
    unittest.main()
