# -*- coding:utf-8 -*-
# import os
# import tushare as ts
# from pymongo import MongoClient
# import unittest
#
# class classify_tushare(unittest.TestCase):
#     def __init__(self):
#         return
import unittest
import tushare.stock.classifying as fd
import tushare as ts
from pymongo import MongoClient
import json


class Test(unittest.TestCase):
    def set_data(self):
        self.port = 27017
        self.mongo_host ="127.0.0..1"
        self.code = '600848'
        self.start = '2015-01-03'
        self.end = '2015-04-07'
        self.year = 2014
        self.quarter = 4

    def core_function(self, func):
        self.set_data()
        mongo = MongoClient("127.0.0.1", 27017)
        if(func == "industry_classified"):
            df = ts.get_industry_classified()
        elif(func == "concept_classified"):
            df = ts.get_concept_classified()
        elif (func == "area_classified"):
            df = ts.get_area_classified()
        elif (func == "gem_classified"):
            df = ts.get_gem_classified()
        elif (func == "sme_classified"):
            df = ts.get_sme_classified()
        elif (func == "st_classified"):
            df = ts.get_st_classified()
        elif (func == "hs300s"):
            df = ts.get_hs300s()
        elif (func == "sz50s"):
            df = ts.get_sz50s()
        elif (func == "zz500s"):
            df = ts.get_zz500s()
            print (df)
        elif (func == "terminated"):
            df = ts.get_terminated()
        else:
            df = {}
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        coll = mongo.classify[func]
        coll.insert(items)

    def test_get_industry_classified(self):
        # ------------------------------------------------
        # 在现实交易中，经常会按行业统计股票的涨跌幅或资金进出，本接口按照sina财经对沪深股票进行的行业分类，返回所有股票所属行业的信息。
        # 考虑到是一次性在线获取数据，调用接口时会有一定的延时，请在数据返回后自行将数据进行及时存储。
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # c_name：行业名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df=fd.get_industry_classified()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.industry_classified.insert(items)
        self.core_function("industry_classified")

    def test_get_concept_classified(self):
        # ------------------------------------------------
        # 返回股票概念的分类数据，现实的二级市场交易中，经常会以”概念”来炒作，在数据分析过程中，可根据概念分类监测资金等信息的变动情况。
        # 本接口是一次性在线获取数据，调用接口时会有一定的延时，请在数据返回后自行将数据进行及时存储。
        # code：股票代码
        # name：股票名称
        # c_name：概念名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_concept_classified())
        # df = fd.get_concept_classified()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.concept_classified.insert(items)
        self.core_function("concept_classified")

    def test_get_area_classified(self):
        # ------------------------------------------------
        # 按地域对股票进行分类，即查找出哪些股票属于哪个省份。
        # code：股票代码
        # name：股票名称
        # area：地域名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_area_classified())
        # df = fd.get_area_classified()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.area_classified.insert(items)
        self.core_function("area_classified")

    def test_get_gem_classified(self):
        # ------------------------------------------------
        # 获取中小板股票数据，即查找所有002开头的股票
        # 参数说明：
        # file_path: 文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_gem_classified())
        # df = fd.get_gem_classified()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.gem_classified.insert(items)
        self.core_function("gem_classified")

    def test_get_sme_classified(self):
        # ------------------------------------------------
        # 获取创业板股票数据，即查找所有300开头的股票
        # 参数说明：
        # file_path: 文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_sme_classified())
        # df = fd.get_sme_classified()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.sme_classified.insert(items)
        self.core_function("sme_classified")

    def test_get_st_classified(self):
        # ------------------------------------------------
        # 获取风险警示板股票数据，即查找所有st股票
        # 参数说明：
        # file_path: 文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_st_classified())
        # df = fd.get_st_classified()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.st_classified.insert(items)
        self.core_function("st_classified")

    def test_get_hs300s(self):
        # ------------------------------------------------
        # 获取沪深300当前成份股及所占权重
        # 返回值说明：
        # code: 股票代码
        # name: 股票名称
        # date: 日期
        # weight: 权重
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_hs300s())
        # df = fd.get_hs300s()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.hs300s.insert(items)
        self.core_function("hs300s")

    def test_get_sz50s(self):
        # ------------------------------------------------
        # 获取上证50成份股
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_sz50s())
        # df = fd.get_sz50s()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.z50s.insert(items)
        self.core_function("sz50s")

    def test_get_zz500s(self):
        # ------------------------------------------------
        # 获取中证500成份股
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_zz500s())
        # df = fd.get_zz500s()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.zz500s.insert(items)
        self.core_function("zz500s")

    def test_get_terminated(self):
        # ------------------------------------------------
        # 获取已经被终止上市的股票列表，数据从上交所获取，目前只有在上海证券交易所交易被终止的股票。
        # 返回值说明：
        # code：股票代码
        # name：股票名称
        # oDate: 上市日期
        # tDate: 终止上市日期
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # print(fd.get_terminated())
        # df = fd.get_terminated()
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.testTU.terminated.insert(items)
        self.core_function("terminated")


if __name__ == "__main__":
    unittest.main()