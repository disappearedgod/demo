# -*- coding:utf-8 -*-


import unittest
from tushare.stock import reference as fd
import tushare as ts
from pymongo import MongoClient
import json

class Test(unittest.TestCase):
    def set_data(self):
        self.quarter = 4
        self.top = 60
        self.show_content = True
        self.begin_year = 2010
        self.rang_year = 7
        self.end_year_notinclude = self.begin_year + self.rang_year
        self.host = "127.0.0.1"
        self.port = 27017
        self.start ='2015-01-01'
        self.end ='2015-04-19'

    def core_function(self, func, isQuarter, isByCode):
        self.set_data()
        print (func)
        mongo = MongoClient(self.host, self.port)
        if(isByCode):
            self.getByCode(mongo, func)
        elif(isQuarter):
            self.fetchByYearQuarter(mongo, func)
        else:
            self.getData(mongo, func)



    def getData(self,mongo, func):
        if (func == 'profit_data'):
            df = fd.profit_data(top=self.top)
        elif (func == 'xsg_data'):
            df = fd.xsg_data()
        elif (func == 'new_stocks'):
            df = fd.new_stocks()
        elif (func == 'sh_margins'):
            df = ts.sh_margins(self.start, self.end)
        elif (func == 'sz_margins'):
            df = ts.sz_margins(self.start, self.end)
        elif (func == 'sz_margin_details'):
            df = ts.sz_margin_details(self.end)
        else:
            df = {}
        print("func:")
        print (func)
        print("type:")
        print(type(df))
        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        # coll = mongo.reference[func]
        # coll.insert(items)

    def fetchByYearQuarter(self, mongo, func):
        years = range(self.begin_year,self.end_year_notinclude)
        quarters = range(1,1+self.quarter)
        for year in years:
            for quarter in quarters:
                print (str(year) + " "+str(quarter))
                if (func == 'forecast_data'):
                    df = ts.forecast_data(year, quarter)
                elif (func == 'fund_holdings'):
                    df = ts.fund_holdings(year, quarter)
                else:
                    print (func)
                    df = {}
                tmpJson = json.loads(df.to_json(orient='records'))
                for i in range(len(tmpJson)):
                    tmpJson[i][u'year'] = int(year)
                    tmpJson[i][u'quarter'] = int(quarter)
                coll = mongo.reference[func]
                coll2 = mongo.reference[str(func)+'_'+str(year)]
                coll2.insert(tmpJson)
                coll.insert(tmpJson)

    def getByCode(self,mongo, func):
        if (func == 'HS300'):
            cursor = mongo.stockcodes.HS300.find()
        elif (func == 'IT'):
            cursor = mongo.stockcodes.IT.find()
        else:
            cursor = {}
        list = []
        for item in cursor:
            code =item['stockcode']
            df = ts.sh_margin_details(self.start, self.end, code)
            tmpJson = json.loads(df.to_json(orient='records'))
            for i in range(len(tmpJson)):
                tmpJson[i][u'code'] = int(code)
            coll = mongo.reference[func]
            coll.insert(tmpJson)

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

    def test_profit_data(self):
        #########################################
        # 分配预案
        # 每到季报、年报公布的时段，就经常会有上市公司利润分配预案发布，而一些高送转高分红的股票往往会成为市场炒作的热点。
        # 及时获取和统计高送转预案的股票是参与热点炒作的关键，TuShare提供了简洁的接口，能返回股票的送转和分红预案情况。
        # 参数说明：
        # year: 预案公布的年份，默认为2014
        # top: 取最新n条数据，默认取最近公布的25条
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # code: 股票代码
        # name: 股票名称
        # year: 分配年份
        # report_date: 公布日期
        # divi: 分红金额（每10股）
        # shares: 转增和送股数（每10股）
        #########################################
        # self.set_data()
        # print(fd.profit_data(top=self.top))
        isQuarter = False
        isByCode = False
        self.core_function('profit_data', isQuarter, isByCode)

    def test_forecast_data(self):
        # self.set_data()
        # print(fd.forecast_data(self.year, self.quarter))
        #########################################
        # 业绩预告¶
        # 按年度、季度获取业绩预告数据，接口提供从1998年以后每年的业绩预告数据，需指定年度、季度两个参数。数据在获取的过程中，会打印进度信息(下同)。
        ##################
        # 参数说明：
        ##################
        # year: int
        # 年度
        # e.g: 2014
        # quarter: int
        # 季度: 1、2、3、4，只能输入这4个季度
        # 结果返回的数据属性说明如下：
        ##################
        # code, 代码
        # name, 名称
        # type, 业绩变动类型【预增、预亏等】
        # report_date, 发布日期
        # pre_eps, 上年同期每股收益
        # range, 业绩变动范围
        #########################################
        isQuarter = True
        isByCode = False
        self.core_function('forecast_data',isQuarter, isByCode)


    def test_xsg_data(self):
        # print(fd.xsg_data())
        #########################################
        # 限售股解禁¶
        # 以月的形式返回限售股解禁情况，通过了解解禁股本的大小，判断股票上行的压力。可通过设定年份和月份参数获取不同时段的数据。
        ##################
        # 参数说明：
        ##################
        # year: 年份, 默认为当前年
        # month: 解禁月份，默认为当前月
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        ##################
        # code：股票代码
        # name：股票名称
        # date: 解禁日期
        # count: 解禁数量（万股）
        # ratio: 占总盘比率
        #########################################
        isQuarter = False
        isByCode = False
        self.core_function('xsg_data',isQuarter, isByCode)


    def test_fund_holdings(self):
        # self.set_data()
        # print(fd.fund_holdings(self.year, self.quarter))
        #########################################
        # 基金持股
        # 获取每个季度基金持有上市公司股票的数据。
        # ##################
        # 参数说明：
        # ##################
        # year: 年份, 默认为当前年
        # quarter: 季度（只能输入1，2，3，4
        # 这个四个数字）
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # ##################
        # code：股票代码
        # name：股票名称
        # date: 报告日期
        # nums: 基金家数
        # nlast: 与上期相比（增加或减少了）
        # count: 基金持股数（万股）
        # clast: 与上期相比
        # amount: 基金持股市值
        # ratio: 占流通盘比率
        #########################################
        isQuarter = True
        isByCode = False
        self.core_function('fund_holdings',isQuarter, isByCode)


    def test_new_stocksa(self):
        # print(fd.new_stocks())
        #########################################
        # 新股数据¶
        # 获取IPO发行和上市的时间列表，包括发行数量、网上发行数量、发行价格已经中签率信息等。
        # ##################
        # 参数说明：
        # ##################
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # ##################
        # code：股票代码
        # name：股票名称
        # ipo_date: 上网发行日期
        # issue_date: 上市日期
        # amount: 发行数量(万股)
        # markets: 上网发行数量(万股)
        # price: 发行价格(元)
        # pe: 发行市盈率
        # limit: 个人申购上限(万股)
        # funds：募集资金(亿元)
        # ballot: 网上中签率( %)
        #########################################
        isQuarter = False
        isByCode = False
        self.core_function('new_stocks',isQuarter, isByCode)



    def test_sh_margin_details(self):
        # self.set_data()
        # print(fd.sh_margin_details(self.start, self.end, self.code))
        #########################################
        # 融资融券（沪市）
        # 沪市的融资融券数据从上海证券交易所网站直接获取，提供了有记录以来的全部汇总和明细数据。
        # 根据上交所网站提示：数据根据券商申报的数据汇总，由券商保证数据的真实、完整、准确。
        # ##################
        # 本日融资融券余额＝本日融资余额＋本日融券余量金额
        # 本日融资余额＝前日融资余额＋本日融资买入额－本日融资偿还额；
        # 本日融资偿还额＝
        #   本日直接还款额＋本日卖券还款额＋本日融资强制平仓额＋本日融资正权益调整－本日融资负权益调整；
        # 本日融券余量 = 前日融券余量 + 本日融券卖出数量 - 本日融券偿还量；
        # 本日融券偿还量＝本日买券还券量＋本日直接还券量＋本日融券强制平仓量＋本日融券正权益调整－本日融券负权益调整－本日余券应划转量；
        # 融券单位：股（标的证券为股票） / 份（标的证券为基金） / 手（标的证券为债券）。
        # 明细信息中仅包含当前融资融券标的证券的相关数据，汇总信息中包含被调出标的证券范围的证券的余额余量相关数据。
        # 沪市融资融券汇总数据
        # ##################
        # 参数说明：
        # ##################
        # start: 开始日期
        # format：YYYY - MM - DD
        # 为空时取去年今日
        # end: 结束日期
        # format：YYYY - MM - DD
        # 为空时取当前日期
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # ##################
        # opDate: 信用交易日期
        # rzye: 本日融资余额(元)
        # rzmre: 本日融资买入额(元)
        # rqyl: 本日融券余量
        # rqylje: 本日融券余量金额(元)
        # rqmcl: 本日融券卖出量
        # rzrqjyzl: 本日融资融券余额(元)
        #########################################
        isQuarter = False
        isByCode = True
        self.core_function('sh_margin_details',isQuarter, isByCode)


    def test_sh_margins(self):
        # self.set_data()
        # print(fd.sh_margins(self.start, self.end))
        #########################################
        # 沪市融资融券明细数据
        # ##################
        # 参数说明：
        # ##################
        # date: 日期
        # format：YYYY - MM - DD
        # 默认为空’‘, 数据返回最近交易日明细数据
        # symbol：标的代码，6
        # 位数字e.g
        # .600848，默认为空’‘
        # start: 开始日期
        # format：YYYY - MM - DD
        # 默认为空’‘
        # end: 结束日期
        # format：YYYY - MM - DD
        # 默认为空’‘
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # ##################
        # opDate: 信用交易日期
        # stockCode: 标的证券代码
        # securityAbbr: 标的证券简称
        # rzye: 本日融资余额(元)
        # rzmre: 本日融资买入额(元)
        # rzche: 本日融资偿还额(元)
        # rqyl: 本日融券余量
        # rqmcl: 本日融券卖出量
        # rqchl: 本日融券偿还量
        #########################################
        isQuarter = False
        isByCode = False
        self.core_function('sh_margins',isQuarter, isByCode)



    def test_sz_margins(self):
        # self.set_data()
        # print(fd.sz_margins(self.start, self.end))
        #########################################
        # 融资融券（深市）
        # 深市的融资融券数据从深圳证券交易所网站直接获取，提供了有记录以来的全部汇总和明细数据。在深交所的网站上，对于融资融券的说明如下：
        # ##################
        # 说明：
        # 本报表基于证券公司报送的融资融券余额数据汇总生成，其中：
        # 本日融资余额(元) = 前日融资余额＋本日融资买入 - 本日融资偿还额
        # 本日融券余量(股) = 前日融券余量＋本日融券卖出量 - 本日融券买入量 - 本日现券偿还量
        # 本日融券余额(元) = 本日融券余量×本日收盘价
        # 本日融资融券余额(元) = 本日融资余额＋本日融券余额；
        # 2014年9月22日起，“融资融券交易总量”数据包含调出标的证券名单的证券的融资融券余额。
        # 深市融资融券汇总数据
        # ##################
        # 参数说明：
        # start: 开始日期
        # format：YYYY - MM - DD
        # 为空时取去年今日
        # end: 结束日期
        # format：YYYY - MM - DD
        # 为空时取当前日期
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # ##################
        # 返回值说明：
        # opDate: 信用交易日期(index)
        # rzmre: 融资买入额(元)
        # rzye: 融资余额(元)
        # rqmcl: 融券卖出量
        # rqyl: 融券余量
        # rqye: 融券余量(元)
        # rzrqye: 融资融券余额(元)
        #########################################
        isQuarter = False
        isByCode = False
        self.core_function('sz_margins',isQuarter, isByCode)


    def test_sz_margin_details(self):
        # self.set_data()
        # print(fd.sz_margin_details(self.end))
        #########################################
        # 深市融资融券明细数据
        # ##################
        # 参数说明：
        # ##################
        # date: 日期
        # format：YYYY - MM - DD
        # 默认为空’‘, 数据返回最近交易日明细数据
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # ##################
        # stockCode: 标的证券代码
        # securityAbbr: 标的证券简称
        # rzmre: 融资买入额(元)
        # rzye: 融资余额(元)
        # rqmcl: 融券卖出量
        # rqyl: 融券余量
        # rqye: 融券余量(元)
        # rzrqye: 融资融券余额(元)
        # opDate: 信用交易日期
        #########################################
        isQuarter = False
        isByCode = False
        self.core_function('sz_margin_details',isQuarter, isByCode)

    def test_str(self):
        self.test_forecast_data()



if __name__ == "__main__":
    unittest.main()



