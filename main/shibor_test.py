# -*- coding:utf-8 -*-

import unittest
import tushare.stock.shibor as fd
import tushare as ts
from pymongo import MongoClient
import json
import datetime

class Test(unittest.TestCase):
    def set_data(self):
        self.begin_year = 2014
        self.rang_year = 1
        self.end_year_notinclude = self.begin_year +  self.rang_year
        self.host = "127.0.0.1"
        self.port = 27017
    #         self.year = None

    def core_function(self, func):
        self.set_data()
        print (func)
        mongo = MongoClient(self.host, self.port)
        self.fetchByYear(mongo, func)

    def fetchByYear(self, mongo, func):
        years = range(self.begin_year,self.end_year_notinclude)
        for year in years:
            print (year)
            print (type(year))
            # year = int(year)
            #print (type(year))
            if (func == 'shibor_data'):
                if (year < 2006):
                    print("[Wrong]没有" + str(year) + "年数据")
                    continue
                else:
                    print("有" + str(year) + "年数据")
                    #df = ts.shibor_data()  # 取当前年份的数据
                    print("shibor")
                    print(type(ts.shibor_data(year)))
                    df = ts.shibor_data(year) #取2014年的数据

                    #df.sort('date', ascending=False)
            elif (func == 'shibor_quote_data'):
                if (year < 2006):
                    print("[Wrong]没有" + str(year) + "年数据")
                    continue
                else:
                    print("有" + str(year) + "年数据")
                    #df = ts.shibor_quote_data()  # 取当前年份的数据
                    df = ts.shibor_quote_data(year) #取2014年的数据
                    #df.sort('date', ascending=False)
            elif (func == 'shibor_ma_data'):
                if (year < 2006):
                    print("[Wrong]没有" + str(year) + "年数据")
                    continue
                else:
                    print("有" + str(year) + "年数据")
                    #df = ts.shibor_ma_data()  # 取当前年份的数据

                    df = ts.shibor_ma_data(year) #取2014年的数据
                    print ("4444444444444")
                    print (df)
                    #df.sort('date', ascending=False)
            elif (func == 'lpr_data'):
                if(year < 2013):
                    print("[Wrong]没有" + str(year) + "年数据")
                    continue
                else:
                    print("有" + str(year) + "年数据")
                    #df = ts.lpr_data()  # 取当前年份的数据
                    df = ts.lpr_data(year) #取2014年的数据
                    #df.sort('date', ascending=False)
                    # df =  fd.lpr_data(self.year)
            elif (func == 'lpr_ma_data'):
                if (year < 2013):
                    print("[Wrong]没有" + str(year) + "年数据")
                    continue
                else:
                    print("有" + str(year) + "年数据")
                    # df = ts.lpr_ma_data()  # 取当前年份的数据
                    df = ts.lpr_ma_data(year) #取2014年的数据
                    #df.sort('date', ascending=False)
            else:
                print("error" + func)
                df = {"err":True}
            year = str(year)
            print("###################")
            print (df)
            print (func)
            tmpJson = json.loads(df.to_json(orient='records'))
            # tmpJson = json.dumps(df)
            print ("_________")
            print (tmpJson)
            print(type(tmpJson))
            import time
            for i in range(len(tmpJson)):
                tmpJson[i][u'year'] = int(year)
                d = time.localtime(tmpJson[i][u'date']/1000)
                tmpJson[i][u'date'] =time.strftime('%Y-%m-%d',d)
            coll = mongo.shibor[func]
            coll2 = mongo.shibor[func+'_'+str(year)]
            coll2.insert(tmpJson)
            coll.insert(tmpJson)

    def test_shibor_data(self):
        # self.set_data()
        # print
        # fd.shibor_data(self.year)
        # Shibor拆放利率
        # 获取银行间同业拆放利率数据，目前只提供2006年以来的数据。
        # 参数说明：
        # year: 年份(YYYY), 默认为当前年份
        # 返回值说明：
        # date: 日期
        # ON: 隔夜拆放利率
        # 1W: 1周拆放利率
        # 2W: 2周拆放利率
        # 1M: 1个月拆放利率
        # 3M: 3个月拆放利率
        # 6M: 6个月拆放利率
        # 9M: 9个月拆放利率
        # 1Y: 1年拆放利率
        self.core_function('shibor_data')

    def test_shibor_quote_data(self):
        # self.set_data()
        # fd.shibor_quote_data(self.year)
        # 银行报价数据
        # 获取银行间报价数据，目前只提供2006年以来的数据。
        # 参数说明：
        # year: 年份(YYYY), 默认为当前年份
        # 返回值说明：
        # date: 日期
        # bank: 报价银行名称
        # ON: 隔夜拆放利率
        # ON_B: 隔夜拆放买入价
        # ON_A: 隔夜拆放卖出价
        # 1W_B: 1周买入
        # 1W_A: 1周卖出
        # 2W_B: 买入
        # 2W_A: 卖出
        # 1M_B: 买入
        # 1M_A: 卖出
        # 3M_B: 买入
        # 3M_A: 卖出
        # 6M_B: 买入
        # 6M_A: 卖出
        # 9M_B: 买入
        # 9M_A: 卖出
        # 1Y_B: 买入
        # 1Y_A: 卖出
        self.core_function('shibor_quote_data')

    def test_shibor_ma_data(self):
        # self.set_data()
        # fd.shibor_ma_data(self.year)
        # Shibor均值数据
        # 获取Shibor均值数据，目前只提供2006年以来的数据。
        # 参数说明：
        # year: 年份(YYYY), 默认为当前年份
        # 返回值说明：
        # date: 日期
        # 其它分别为各周期5、10、20
        # 均价，请参考上面的周期含义
        self.core_function('shibor_ma_data')

    def test_lpr_data(self):
        # self.set_data()
        # fd.lpr_data(self.year)
        # 贷款基础利率（LPR）¶
        # 获取贷款基础利率（LPR）数据，目前只提供2013年以来的数据。
        # 参数说明：
        # year: 年份(YYYY), 默认为当前年份
        # 返回值说明：
        # date: 日期
        # 1Y: 1年贷款基础利率
        self.core_function('lpr_data')

    def test_lpr_ma_data(self):
        # self.set_data()
        # fd.lpr_ma_data(self.year)
        # LPR均值数据
        # 获取贷款基础利率均值数据，目前只提供2013年以来的数据。
        # 参数说明：
        # year: 年份(YYYY), 默认为当前年份
        # 返回值说明：
        # date: 日期
        # 1Y_5: 5日均值
        # 1Y_10: 10日均值
        # 1Y_20: 20日均值
        self.core_function('lpr_ma_data')


if __name__ == '__main__':
    unittest.main()