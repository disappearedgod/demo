# -*- coding:utf-8 -*-

import os
#from sqlalchemy import create_engin
#from pandas.io.pytables import HDFStore
import re
import tushare as ts
import ConfigParser as ConfigParser
from pymongo import MongoClient
import json
import unittest
import sys


class new_stock(unittest.TestCase):
    def __init__(self):
        cp = ConfigParser.SafeConfigParser()
        cp.read('myapp.conf')
        self.mongo_host = "127.0.0.1"
        self.port = 27017
        self.code = '000875'
        self.start = '2017-01-03'
        self.end = '2017-01-07'
        self.year = 2017
        self.quarter = 1
        # print ('options of [db]:', cp.options('db'))
        # print 'port of mongo:', cp.get('mongo', 'port')  # host of db: 127.0.0.1

    def setUp(self):
        # self.code = json['code']
        # self.start = json['start']
        # self.end = json['end']
        # self.year = json['year']
        # self.quarter = json['quarter']
        self.mongo = MongoClient(self.mongo_host,  self.port)
        print ("set_data fin")

    # def insertion_sort(sort_list):
    #     iter_len = len (sort_list)
    #     if(iter_len < 2):
    #         return sort_list
    #     for i in range(1, iter_len):
    #         key = sort_list[i]['rangeNum']
    #         j = i - 1
    #         while j >= 0 and sort_list[j]['rangeNum'] > key:
    #             sort_list[j+1] = sort_list[j]
    #             j -= 1
    #         sort_list[j+1] = key
    #     return sort_list

    def sortedProfitData(self):
        self.setUp()
        client = MongoClient(self.mongo_host, self.port)
        __cursor = client.db.forecast.find()
        __items = []
        # count = 0
        reload(sys)
        sys.setdefaultencoding("utf-8")
        for item in __cursor:
            # if(count == 5):
            #     break
            # count = count + 1
            # item = json.loads(item)
            code = str(item['code'])
            name = str(item['name'])
            report_date = str(item['report_date'])
            range = str(item['range'])
            pre_eps = str(item['pre_eps'])
            _id = str(item['_id'])
            type = str(item['type'])
            if (re.match(range, "None")):
                continue;
            tmpNumStr = range.split('~')[0]
            if '%'in tmpNumStr:
                tmpNumStr = tmpNumStr.split('%')[0]
            tmpNum = float(tmpNumStr)
            tmpNum = tmpNum / 100
            tmp = {
                'range': range,
                'code': code,
                'name': name,
                'report_date': report_date,
                'pre_eps': pre_eps,
                '_id': _id,
                'type': type,
                'rangeNum':tmpNum
            }
            # #插入
            if len(__items) == 0:
                __items.insert(0,tmp)
            else:
                flag = 0
                for tmpItem  in __items:

                    if(tmpNum > tmpItem['rangeNum']):
                        break;
                    else:
                        flag  = flag + 1
                __items.insert(flag,tmp)

        insert_string = json.dumps(__items)
        self.mongo.db.sortedprofitdata.insert(json.loads(insert_string))

    def testRange(self):
        __items = self.readFromMongo()


    def test_forecast(self):
        # code, 代码
        # name, 名称
        # type, 业绩变动类型【预增、预亏等】
        # report_date, 发布日期
        # pre_eps, 上年同期每股收益
        # range, 业绩变动范围
        self.setUp()
        # 获取2016年中报的业绩预告数据
        df = ts.forecast_data(2017, 1)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        self.mongo.db.forecast.insert(items)

    def test_profitStock(self):
        self.setUp()
        df = ts.profit_data(top=500)
        df.sort('shares', ascending=False)
        insert_string = df.to_json(orient='records')
        self.mongo.db.profitdata.insert(json.loads(insert_string))

    def test_stratagy(self):
        self.sortedProfitData()
        #self.sortedProfitData()

if __name__ == '__main__':
    ns = new_stock()
    ns.test_stratagy()
