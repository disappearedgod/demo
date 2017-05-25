# -*- coding:utf-8 -*-
'''
Created on 2015/3/14
@author: Jimmy Liu
'''
from pymongo import MongoClient
import json
import unittest
import tushare.stock.macro as fd
import tushare as ts


class Test(unittest.TestCase):
    def set_data(self):
        return

    def core_function(self, type):
        self.set_data()
        print (type)
        mongo = MongoClient("127.0.0.1", 27017)
        if(type == 'gdp_year'):
            print ("gdp_year")
            df = fd.get_gdp_year()
        elif(type == 'gdp_quarter'):
            print ("gdp_quarter")
            df = fd.get_gdp_quarter()
        elif (type == 'gdp_for'):
            print("gdp_for")
            df = fd.get_gdp_for()
        elif (type == 'gdp_pull'):
            print("gdp_pull")
            df = fd.get_gdp_pull()
        elif (type == 'get_money_supply_bal'):
            print("get_money_supply_bal")
            df = fd.get_money_supply_bal()
        elif (type == 'gdp_contrib'):
            print("gdp_contrib")
            df = fd.get_gdp_contrib()
        elif (type == 'get_cpi'):
            print("get_cpi")
            df = ts.get_cpi()
        elif (type == 'get_ppi'):
            print("get_ppi")
            df = ts.get_ppi()
        elif (type == 'get_rrr'):
            print("get_rrr")
            df = ts.get_rrr()
        elif (type == 'money_supply'):
            print("money_supply")
            df = ts.get_money_supply()
        elif (type == 'money_supply_bal'):
            print("money_supply_bal")
            df = ts.get_money_supply_bal()
        else:
            df={}

        print(df)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        mongo.macro.gdp_year.insert(items)

    def test_get_gdp_year(self):
        # ------------------------------------------------
        # 国内生产总值(年度)
        # 返回值说明：
        #
        # year: 统计年度
        # gdp: 国内生产总值(亿元)
        # pc_gdp: 人均国内生产总值(元)
        # gnp: 国民生产总值(亿元)
        # pi: 第一产业(亿元)
        # si: 第二产业(亿元)
        # industry: 工业(亿元)
        # cons_industry: 建筑业(亿元)
        # ti: 第三产业(亿元)
        # trans_industry: 交通运输仓储邮电通信业(亿元)
        # lbdy: 批发零售贸易及餐饮业(亿元)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =fd.get_gdp_year()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.gdp_year.insert(items)
        self.core_function("gdp_year")

    def test_get_gdp_quarter(self):
        # ------------------------------------------------
        # 国内生产总值(季度)
        # 返回值说明：
        #
        # quarter: 季度
        # gdp: 国内生产总值(亿元)
        # gdp_yoy: 国内生产总值同比增长( %)
        # pi: 第一产业增加值(亿元)
        # pi_yoy: 第一产业增加值同比增长( %)
        # si: 第二产业增加值(亿元)
        # si_yoy: 第二产业增加值同比增长( %)
        # ti: 第三产业增加值(亿元)
        # ti_yoy: 第三产业增加值同比增长( %)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =fd.get_gdp_quarter()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.gdp_quarter.insert(items)
        self.core_function("gdp_quarter")


    def test_get_gdp_for(self):
        # ------------------------------------------------
        # 三大需求对GDP贡献
        # 返回值说明：
        #
        # year: 统计年度
        # gdp: 国内生产总值(亿元)
        # pc_gdp: 人均国内生产总值(元)
        # gnp: 国民生产总值(亿元)
        # pi: 第一产业(亿元)
        # si: 第二产业(亿元)
        # industry: 工业(亿元)
        # cons_industry: 建筑业(亿元)
        # ti: 第三产业(亿元)
        # trans_industry: 交通运输仓储邮电通信业(亿元)
        # lbdy: 批发零售贸易及餐饮业(亿元)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df = fd.get_gdp_for()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.gdp_for.insert(items)
        self.core_function("gdp_for")


    def test_get_gdp_pull(self):
        # ------------------------------------------------
        #三大产业对GDP拉动
        # 返回值说明：
        #
        # year: 统计年度
        # gdp_yoy: 国内生产总值同比增长( %)
        # pi: 第一产业拉动率( %)
        # si: 第二产业拉动率( %)
        # industry: 其中工业拉动( %)
        # ti: 第三产业拉动率( %)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =fd.get_gdp_pull()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.gdp_pull.insert(items)
        self.core_function("gdp_pull")


    def test_get_gdp_contrib(self):
        # ------------------------------------------------
        # 三大产业贡献率
        # 返回值说明：
        #
        # year: 统计年度
        # gdp_yoy: 国内生产总值
        # pi: 第一产业献率( %)
        # si: 第二产业献率( %)
        # industry: 其中工业献率( %)
        # ti: 第三产业献率( %)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =fd.get_gdp_contrib()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.gdp_contrib.insert(items)
        self.core_function("gdp_contrib")


    def test_get_cpi(self):
        # ------------------------------------------------
        # 居民消费价格指数
        # 返回值说明：
        #
        # month: 统计月份
        # cpi: 价格指数

        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =ts.get_cpi()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.get_cpi.insert(items)
        self.core_function("get_cpi")


    def test_get_ppi(self):
        # ------------------------------------------------
        # 工业品出厂价格指数
        # 返回值说明：
        #
        # month: 统计月份
        # ppiip: 工业品出厂价格指数
        # ppi: 生产资料价格指数
        # qm: 采掘工业价格指数
        # rmi: 原材料工业价格指数
        # pi: 加工工业价格指数
        # cg: 生活资料价格指数
        # food: 食品类价格指数
        # clothing: 衣着类价格指数
        # roeu: 一般日用品价格指数
        # dcg: 耐用消费品价格指数
        # 结果显示：
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =ts.get_ppi()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.get_ppi.insert(items)
        self.core_function("get_ppi")


    def test_get_deposit_rate(self):
        # ------------------------------------------------
        # 宏观经济数据
        # 宏观经济数据提供国内重要的宏观经济数据，目前只提供比较常用的宏观经济数据，通过简单的接口调用可获取相应的DataFrame格式数据，大项主要包括以下类别：
        #
        # 金融信息数据
        # 国民经济数据
        # 价格指数数据
        # 景气指数数据
        # 对外经济贸易数据
        # 【注：以下所有数据的结果打印只显示了前10行记录】
        # ------------------------------------------------
        mongo = MongoClient("127.0.0.1", 27017)
        df =ts.get_deposit_rate()
        print(df)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        mongo.macro.deposit_rate.insert(items)
        self.core_function("gdp_year")


    def test_get_loan_rate(self):
        # ------------------------------------------------
        # 贷款利率
        # 返回值说明：
        #
        # date: 执行日期
        # loan_type: 存款种类
        # rate: 利率（ % ）
        # ------------------------------------------------
        mongo = MongoClient("127.0.0.1", 27017)
        df =ts.get_loan_rate()
        print(df)
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        mongo.macro.loan_rate.insert(items)
        self.core_function("gdp_year")


    def test_get_rrr(self):
        # ------------------------------------------------
        # 存款准备金率
        # 返回值说明：
        #
        # date: 变动日期
        # before: 调整前存款准备金率( %)
        # now: 调整后存款准备金率( %)
        # changed: 调整幅度( %)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =ts.get_rrr()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.get_rrr.insert(items)
        self.core_function("get_rrr")

    def test_get_money_supply(self):
        # ------------------------------------------------
        # 货币供应量
        # 返回值说明：
        #
        # month: 统计时间
        # m2: 货币和准货币（广义货币M2）(亿元)
        # m2_yoy: 货币和准货币（广义货币M2）同比增长( %)
        # m1: 货币(狭义货币M1)(亿元)
        # m1_yoy: 货币(狭义货币M1)
        # 同比增长( %)
        # m0: 流通中现金(M0)(亿元)
        # m0_yoy: 流通中现金(M0)
        # 同比增长( %)
        # cd: 活期存款(亿元)
        # cd_yoy: 活期存款同比增长( %)
        # qm: 准货币(亿元)
        # qm_yoy: 准货币同比增长( %)
        # ftd: 定期存款(亿元)
        # ftd_yoy: 定期存款同比增长( %)
        # sd: 储蓄存款(亿元)
        # sd_yoy: 储蓄存款同比增长( %)
        # rests: 其他存款(亿元)
        # rests_yoy: 其他存款同比增长( %)
        #  ------------------------------------------------

        # mongo = MongoClient("127.0.0.1", 27017)
        # df =ts.get_money_supply()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.get_money_supply.insert(items)
        self.core_function("money_supply")

    def test_get_money_supply_bal(self):
        # ------------------------------------------------
        # 货币供应量(年底余额)
        # 返回值说明：
        #
        # year: 统计年度
        # m2: 货币和准货币(亿元)
        # m1: 货币(亿元)
        # m0: 流通中现金(亿元)
        # cd: 活期存款(亿元)
        # qm: 准货币(亿元)
        # ftd: 定期存款(亿元)
        # sd: 储蓄存款(亿元)
        # rests: 其他存款(亿元)
        # ------------------------------------------------
        # mongo = MongoClient("127.0.0.1", 27017)
        # df =fd.get_money_supply_bal()
        # print(df)
        # insert_string = df.to_json(orient='records')
        # items = json.loads(insert_string)
        # mongo.macro.money_supply_bal.insert(items)
        self.core_function("money_supply_bal")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()