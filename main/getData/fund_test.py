# -*- coding:utf-8 -*-

import unittest
import tushare.stock.fundamental as fd
import tushare as ts
from pymongo import MongoClient
import json

class Test(unittest.TestCase):
    def set_data(self):
        self.begin_year = 2010
        self.end_year_notinclude = 2017
        self.quarter = 4

    def core_function(self, type):
        self.set_data()
        print (type)
        mongo = MongoClient("127.0.0.1", 27017)
        if(type == 'stock_basics'):
            print ("here")
            df = fd.get_stock_basics()
        else:
            df=self.fetchByYearQuarter(mongo, type)
        #print(df)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        coll = mongo.fundemental[type]
        coll.insert(items)

    def fetchByYearQuarter(self, mongo, type):
        years = range(self.begin_year,self.end_year_notinclude)
        quarters = range(1,5)
        for year in years:
            for quarter in quarters:
                print (str(type)+'_'+str(year)+'_'+str(quarter))
                if (type == 'report_data'):
                    df = fd.get_report_data(year, quarter)
                elif (type == 'profit_data'):
                    df = ts.get_profit_data(year, quarter)
                elif (type == 'operation_data'):
                    df = ts.get_operation_data(year, quarter)
                elif (type == 'growth_data'):
                    df = ts.get_growth_data(year, quarter)
                elif (type == 'debtpaying_data'):
                    df = ts.get_debtpaying_data(year, quarter)
                elif (type == 'cashflow_data'):
                    df = ts.get_cashflow_data(year, quarter)
                else:
                    df = {}
                tmpJson = json.loads(df.to_json(orient='records'))
                for i in range(len(tmpJson)):
                    tmpJson[i][u'year'] = int(year)
                    tmpJson[i][u'quarter'] = int(quarter)
                coll = mongo.fundemental[type]
                coll2 = mongo.fundemental[str(type)+'_'+str(year)+'_'+str(quarter)]
                coll2.insert(tmpJson)
                coll.insert(tmpJson)

    def test_get_stock_basics(self):
        #------------------------------------------------
        # 股票列表
        # 获取沪深上市公司基本情况。属性包括：
        # code, 代码
        # name, 名称
        # industry, 所属行业
        # area, 地区
        # pe, 市盈率
        # outstanding, 流通股本(亿)
        # totals, 总股本(亿)
        # totalAssets, 总资产(万)
        # liquidAssets, 流动资产
        # fixedAssets, 固定资产
        # reserved, 公积金
        # reservedPerShare, 每股公积金
        # esp, 每股收益
        # bvps, 每股净资
        # pb, 市净率
        # timeToMarket, 上市日期
        # undp, 未分利润
        # perundp, 每股未分配
        # rev, 收入同比( %)
        # profit, 利润同比( %)
        # gpr, 毛利率( %)
        # npr, 净利润率( %)
        # holders, 股东人数
        #------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = fd.get_stock_basics()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.stock_basics.insert(items)
        self.core_function('stock_basics')


    def test_get_report_data(self):
        # ------------------------------------------------
        # 业绩报告（主表）
        # 按年度、季度获取业绩报表数据。数据获取需要一定的时间，网速取决于您的网速，请耐心等待。结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # esp, 每股收益
        # eps_yoy, 每股收益同比( %)
        # bvps, 每股净资产
        # roe, 净资产收益率( %)
        # epcf, 每股现金流量(元)
        # net_profits, 净利润(万元)
        # profits_yoy, 净利润同比( %)
        # distrib, 分配方案
        # report_date, 发布日期
        # ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = fd.get_report_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.report_data.insert(items)
        self.core_function('report_data')

    def test_get_profit_data(self):
        # self.set_data()
        # print(fd.get_profit_data(self.year, self.quarter))
        # ------------------------------------------------
        # 盈利能力
        # 按年度、季度获取盈利能力数据，结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # roe, 净资产收益率( %)
        # net_profit_ratio, 净利率( %)
        # gross_profit_rate, 毛利率( %)
        # net_profits, 净利润(万元)
        # esp, 每股收益
        # business_income, 营业收入(百万元)
        # bips, 每股主营业务收入(元)
        # ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = ts.get_profit_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.profit_data.insert(items)
        self.core_function('profit_data')


    def test_get_operation_data(self):
        # self.set_data()
        # print(fd.get_operation_data(self.year, self.quarter))
        # ------------------------------------------------
        # 营运能力
        # 按年度、季度获取营运能力数据，结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # arturnover, 应收账款周转率(次)
        # arturndays, 应收账款周转天数(天)
        # inventory_turnover, 存货周转率(次)
        # inventory_days, 存货周转天数(天)
        # currentasset_turnover, 流动资产周转率(次)
        # currentasset_days, 流动资产周转天数(天)
        # ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = ts.get_operation_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.operation_data.insert(items)
        self.core_function('operation_data')

    def test_get_growth_data(self):
        # self.set_data()
        # print(fd.get_growth_data(self.year, self.quarter))
        # ------------------------------------------------
        # 成长能力
        # 按年度、季度获取成长能力数据，结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # mbrg, 主营业务收入增长率( %)
        # nprg, 净利润增长率( %)
        # nav, 净资产增长率
        # targ, 总资产增长率
        # epsg, 每股收益增长率
        # seg, 股东权益增长率
        #  ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = ts.get_growth_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.growth_data.insert(items)
        self.core_function('growth_data')

    def test_get_debtpaying_data(self):
        # self.set_data()
        # print(fd.get_debtpaying_data(self.year, self.quarter))
        # ------------------------------------------------
        # 偿债能力
        # 按年度、季度获取偿债能力数据，结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # currentratio, 流动比率
        # quickratio, 速动比率
        # cashratio, 现金比率
        # icratio, 利息支付倍数
        # sheqratio, 股东权益比率
        # adratio, 股东权益增长率
        # ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = ts.get_debtpaying_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.debtpaying_data.insert(items)
        self.core_function('debtpaying_data')

    def test_get_cashflow_data(self):
        # self.set_data()
        # print(fd.get_cashflow_data(self.year, self.quarter))
        # ------------------------------------------------
        # 现金流量
        # 按年度、季度获取现金流量数据，结果返回的数据属性说明如下：
        #
        # code, 代码
        # name, 名称
        # cf_sales, 经营现金净流量对销售收入比率
        # rateofreturn, 资产的经营现金流量回报率
        # cf_nm, 经营现金净流量与净利润的比率
        # cf_liabilities, 经营现金净流量对负债比率
        # cashflowratio, 现金流量比率
        # ------------------------------------------------
        # self.set_data()
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = ts.get_cashflow_data(self.year, self.quarter)
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.fundemental.cashflow_data.insert(items)
        self.core_function('cashflow_data')

if __name__ == '__main__':
    unittest.main()