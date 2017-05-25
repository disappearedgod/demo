# -*- coding:utf-8 -*-
'''
Created on 2015/3/14
@author: Jimmy Liu
'''
import unittest
import tushare.stock.billboard as fd
import tushare as ts
from pymongo import MongoClient
import json

class Test(unittest.TestCase):
    def set_data(self):
        self.date = '2015-06-12'
        self.days = 5
        self.start = '2017-01-03'
        self.end = '2017-01-07'
        self.host = "127.0.0.1"
        self.port = 27017

    def core_function(self, func):
        self.set_data()
        print (func)
        #mongo = MongoClient(self.host, self.port)
        mongo = MongoClient("127.0.0.1", 27017)
        if (func == 'top_list'):
            self.queryFromTime(mongo, func)
        elif (func == 'inst_detail'):
            df = fd.inst_detail()
        else:
            df = self.queryForDays(func)
        #print(df)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        coll = mongo.billboard[func]
        coll.insert(items)

    def queryFromTime(self, mongo, func):
        print("queryFromTime")
        import datetime
        begin_date = self.start
        end_date = self.end
        begin = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        while begin <= end:
            tmpDate = begin.strftime("%Y-%m-%d")
            tmpString = self.queryForADay(tmpDate)
            # print tmpString
            begin += datetime.timedelta(days=1)
            items = json.loads(tmpString)
            coll = mongo.billboard[func]
            coll.insert(items)

    def queryForADay(self, str_date):
      import json
      tmpJson=[]
      df = fd.top_list(self.date)
      tmpJson = json.loads(df.to_json(orient='records'))
      for i in range(len(tmpJson)):
        tmpJson[i][u'today'] = str_date
      #print (tmpJson)
      return tmpJson

    def queryForDays(self, func):
      import json
      tmpJson=[]
      if (func == 'cap_tops'):
          df = fd.cap_tops(self.days)
      elif (func == 'broker_tops'):
          df = fd.broker_tops(self.days)
      elif (func == 'inst_tops'):
          df = fd.inst_tops(self.days)
      else:
          df = {}
      tmpJson = json.loads(df.to_json(orient='records'))
      #print tmpJson
      return tmpJson

    def test_top_list(self):
        # self.set_data()
        # print(fd.top_list(self.date))
        #########################################################
        # 每日龙虎榜列表
        # 按日期获取历史当日上榜的个股数据，如果一个股票有多个上榜原因，则会出现该股票多条数据。
        #
        # 参数说明：
        #
        # date：日期，格式YYYY - MM - DD
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        #
        # code：代码
        # name: 名称
        # pchange: 当日涨跌幅
        # amount：龙虎榜成交额(万)
        # buy：买入额(万)
        # bratio：买入占总成交比例
        # sell：卖出额(万)
        # sratio：卖出占总成交比例
        # reason：上榜原因
        # date：日期
        ##########################################################
        self.core_function("top_list")

    def test_cap_tops(self):
        # self.set_data()
        # print(fd.cap_tops(self.days))
        #########################################################个股上榜统计
        # 获取近5、10、30、60日个股上榜统计数据,包括上榜次数、累积购买额、累积卖出额、净额、买入席位数和卖出席位数。
        #
        # 参数说明：
        #
        # days：统计周期5、10、30和60日，默认为5日
        # retry_count：当网络异常后重试次数，默认为3
        # pause:重试时停顿秒数，默认为0
        # 返回值说明：
        #
        # code：代码
        # name:名称
        # count：上榜次数
        # bamount：累积购买额(万)
        # samount：累积卖出额(万)
        # net：净额(万)
        # bcount：买入席位数
        # scount：卖出席位数
        #########################################################
        self.core_function("cap_tops")

    def test_broker_tops(self):
        # self.set_data()
        # print(fd.broker_tops(self.days))
        #########################################################
        # 营业部上榜统计
        # 获取营业部近5、10、30、60
        # 日上榜次数、累积买卖等情况。
        #
        # 参数说明：
        #
        # days：统计周期5、10、30
        # 和60日，默认为5日
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        #
        # broker：营业部名称
        # count：上榜次数
        # bamount：累积购买额(万)
        # bcount：买入席位数
        # samount：累积卖出额(万)
        # scount：卖出席位数
        # top3：买入前三股票
        #########################################################
        self.core_function("broker_tops")

    def test_inst_tops(self):
        # self.set_data()
        # print(fd.inst_tops(self.days))
        #########################################################
        # 机构席位追踪
        # 获取机构近5、10、30、60
        # 日累积买卖次数和金额等情况。
        #
        # 参数说明：
        #
        # days：统计周期5、10、30
        # 和60日，默认为5日
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        #
        # code: 代码
        # name: 名称
        # bamount: 累积买入额(万)
        # bcount: 买入次数
        # samount: 累积卖出额(万)
        # scount: 卖出次数
        # net: 净额(万)
        #########################################################
        self.core_function("inst_tops")

    def test_inst_detail(self):
        # print(fd.inst_detail())
        #########################################################
        # 机构成交明细
        # 获取最近一个交易日机构席位成交明细统计数据
        #
        # 参数说明：
        #
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        #
        # code: 代码
        # name: 名称
        # date: 交易日期
        # bamount: 机构席位买入额(万)
        # samount: 机构席位卖出额(万)
        # type: 类型
        #########################################################
        self.core_function("inst_detail")


if __name__ == "__main__":
    unittest.main()