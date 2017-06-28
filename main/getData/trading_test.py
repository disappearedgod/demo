# -*- coding:utf-8 -*-
'''
Created on 2015/3/14
@author: Jimmy Liu
'''
import unittest
import tushare.stock.trading as fd
import tushare as ts
from pymongo import MongoClient
import json
import datetime

class Test(unittest.TestCase):
    def set_data(self):
        self.code = '000001'
        self.start = '2015-01-03'
        self.end = '2017-01-04'
        self.host = "127.0.0.1"
        self.port = 27017
        self.now = datetime.datetime.today()

    def core_function(self, func, isByAllCode, isByDate, codeType):
        self.set_data()
        print(func)
        # mongo = MongoClient(self.host, self.port)
        mongo = MongoClient(self.host, self.port)
        if(isByAllCode):
            self.search_by_code_date(isByDate, mongo, func, codeType)
        else:
            self.getDataRelateLessWithCode(mongo,func)

    def search_by_code_date(self, isByDate, mongo, func, code_collection):
        print ("getByCode")
        if (code_collection == 'HS300'):
            cursor = mongo.stockcodes.HS300.find()
        elif (code_collection == 'IT'):
            cursor = mongo.stockcodes.IT.find()
        else:
            cursor = {}
        print("_______________________")
        print(cursor)
        for item in cursor:
            # print("_______________________")
            # print(item)
            code = item['stockcode']
            # print ("search_by_code_date", code)
            if(isByDate):
                self.getFromTimeRange(mongo, func,code)
            else:
                self.getOnlyByCode(mongo, func, code)

    def getOnlyByCode(self, mongo, func, code):
        # elif (func == "tick_data"):
        #     df = ts.get_tick_data(code, self.start, self.end)
        if (func == "realtime_quotes"):
            df = ts.get_realtime_quotes(code)
            # # 上证指数
            # ts.get_realtime_quotes('sh')
            # # 上证指数 深圳成指 沪深300指数 上证50 中小板 创业板
            # ts.get_realtime_quotes(['sh', 'sz', 'hs300', 'sz50', 'zxb', 'cyb'])
            # # symbols from a list
            # ts.get_realtime_quotes(['600848', '000980', '000981'])
            # # from a Series
            # ts.get_realtime_quotes(df['code'].tail(10))  # 一次获取10个股票的实时分笔数据
            # # 或者混搭
            # ts.get_realtime_quotes(['sh', '600848'])
        elif (func == "today_ticks"):
            df = ts.get_today_ticks(code)
        else:
            df = {}
        # print(func)
        # print( df )
        # print(type(df))
        tmpJson = json.loads(df.to_json(orient='records'))
        coll = mongo.trading[func]
        for i in range(len(tmpJson)):
            tmpJson[i][u'code'] = code
            coll.insert(tmpJson[i])

    def getFromTimeRange(self, mongo, func, code):
        print("queryFromTime",self.start,self.end,"func:",func,"code:",code)
        import datetime
        begin_date = self.start
        end_date = self.end
        begin = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        while begin <= end:
            tmpDate = begin.strftime("%Y-%m-%d")
            print("code:",code,"tmpDate:",tmpDate)
            self.getByDate(mongo, func,code,tmpDate)
            begin += datetime.timedelta(days=1)

    def getByDate(self, mongo, func, code, date):
        isNeedDate = False

        if (func == "tick_data"):
            df = ts.get_tick_data(code, date=date)
        elif (func == "h_data"):
            df = ts.get_h_data(code, start=date, end=date)
        elif (func == "hist_data"):
            print(date)
            print(type(date))
            df = ts.get_hist_data(code, date=date)
        elif (func == "sina_dd"):
            df = ts.get_sina_dd(code, date=date)
            # df = ts.get_sina_dd('600848', date='2015-12-24')

        else:
            df = {}
        tmpJson = json.loads(df.to_json(orient='records'))
        for i in range(len(tmpJson)):
            tmpJson[i][u'code'] = code
            tmpJson[i][u'date'] = date
            print(tmpJson[i])
        coll = mongo.trading[func]
        coll.insert(tmpJson)

    def getDataRelateLessWithCode(self,mongo,func):
        if(func =="stock_basics"):
            df = ts.get_stock_basics()
            date = df.ix['600848']['timeToMarket']  # 上市日期YYYYMMDD
            # ts.get_h_data('002337')  # 前复权
            # ts.get_h_data('002337', autype='hfq')  # 后复权
            # ts.get_h_data('002337', autype=None)  # 不复权
            # ts.get_h_data('002337', start='2015-01-01', end='2015-03-16')  # 两个日期之间的前复权数据
            #
            # ts.get_h_data('399106', index=True)  # 深圳综合指数
            print(date)
        elif (func == "today_all"):
                df = ts.get_today_all()
                tmpJson = json.loads(df.to_json(orient='records'))
                coll = mongo.trading[func]
                for i in range(len(tmpJson)):
                    tmpJson[i][u'now'] = str(self.now)
                    coll.insert(tmpJson[i])
                return;
        elif (func == "index"):
            df = ts.get_index()
        else:
            df = {}

        insert_string = df.to_json(orient='records')
        items = json.loads(insert_string)
        coll = mongo.trading[func]
        coll.insert(items)



    def test_get_hist_data(self):
        # self.set_data()
        # print(fd.get_hist_data(self.code, self.start))
        #########################################
        # 历史行情
        # 在新版0.5.6中，已经新增了一个接口：get_k_data，具体使用方法请参考tushare公众号文章《全新的免费行情数据接口》 ，建议使用全新接口。
        # 获取个股历史交易数据（包括均线数据），可以通过参数设置获取日k线、周k线、月k线，以及5分钟、15
        # 分钟、30分钟和60分钟k线数据。本接口只能获取近3年的日线数据，适合搭配均线数据进行选股和分析，如果需要全部历史数据，请调用下一个接口get_h_data()。
        # 参数说明：
        # code：股票代码，即6位数字代码，或者指数代码（sh = 上证指数
        # sz = 深圳成指
        # hs300 = 沪深300指数
        # sz50 = 上证50
        # zxb = 中小板
        # cyb = 创业板）
        # start：开始日期，格式YYYY - MM - DD
        # end：结束日期，格式YYYY - MM - DD
        # ktype：数据类型，D = 日k线
        # W = 周
        # M = 月
        # 5 = 5        分钟
        # 15 = 15        分钟
        # 30 = 30        分钟
        # 60 = 60        分钟，默认为D
        # retry_count：当网络异常后重试次数，默认为3
        # pause: 重试时停顿秒数，默认为0
        # 返回值说明：
        # date：日期
        # open：开盘价
        # high：最高价
        # close：收盘价
        # low：最低价
        # volume：成交量
        # price_change：价格变动
        # p_change：涨跌幅
        # ma5：5        日均价
        # ma10：10        日均价
        # ma20: 20        日均价
        # v_ma5: 5        日均量
        # v_ma10: 10        日均量
        # v_ma20: 20        日均量
        # turnover: 换手率[注：指数无此项]
        #########################################
        isCode = True
        isByDate = True
        code_collection = "HS300"
        self.core_function("hist_data", isCode, isByDate, code_collection)

    def test_get_stock_basics(self):
        # self.set_data()
        # print(fd.get_stock_basics(self.code, self.end))
        #########################################
        # 参数说明：
        # code: string, 股票代码        e.g.        600848
        # start: string, 开始日期
        # format：YYYY - MM - DD        为空时取当前日期
        # end: string, 结束日期
        # format：YYYY - MM - DD        为空时取去年今日
        # autype: string, 复权类型，qfq - 前复权
        # hfq - 后复权
        # None - 不复权，默认为qfq
        # index: Boolean，是否是大盘指数，默认为False
        # retry_count: int, 默认3, 如遇网络等问题重复执行的次数
        # pause: int, 默认
        # 0, 重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        # 返回值说明：
        # date: 交易日期(index)
        # open: 开盘价
        # high: 最高价
        # close: 收盘价
        # low: 最低价
        # volume: 成交量
        # amount: 成交金额
        #########################################
        isCode = False
        isByDate = False
        code_collection = "HS300"
        self.core_function("stock_basics", isCode,isByDate, code_collection)

    def test_get_tick_data(self):
        # self.set_data()
        # print(fd.get_tick_data(self.code, self.end))
        #########################################
        # 历史分笔
        # 获取个股以往交易历史的分笔数据明细，通过分析分笔数据，可以大致判断资金的进出情况。在使用过程中，对于获取股票某一阶段的历史分笔数据，需要通过参入交易日参数并append到一个DataFrame或者直接append到本地同一个文件里。历史分笔接口只能获取当前交易日之前的数据，当日分笔历史数据请调用get_today_ticks()
        # 接口或者在当日18点后通过本接口获取。
        # 参数说明：
        # code：股票代码，即6位数字代码
        # date：日期，格式YYYY - MM - DD
        # retry_count: int, 默认3, 如遇网络等问题重复执行的次数
        # pause: int, 默认        0, 重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        # 返回值说明：
        # time：时间
        # price：成交价格
        # change：价格变动
        # volume：成交手
        # amount：成交金额(元)
        # type：买卖类型【买盘、卖盘、中性盘】
        #########################################
        isCode = True
        isByDate = True
        code_collection = "HS300"
        self.core_function("tick_data", isCode, isByDate, code_collection)

    def test_get_today_all(self):
        #########################################
        # print(fd.get_today_all())
        # 返回值说明：
        # code：代码
        # name: 名称
        # changepercent: 涨跌幅
        # trade: 现价
        # open: 开盘价
        # high: 最高价
        # low: 最低价
        # settlement: 昨日收盘价
        # volume: 成交量
        # turnoverratio: 换手率
        # amount: 成交量
        # per: 市盈率
        # pb: 市净率
        # mktcap: 总市值
        # nmc: 流通市值
        #
        #########################################
        code_collection = "HS300"
        isCode = False
        isByDate = False
        self.core_function("today_all", isCode, isByDate, code_collection)

    def test_get_realtime_quotesa(self):
        # self.set_data()
        # print(fd.get_realtime_quotes(self.code))
        #########################################
        # 实时分笔
        # 获取实时分笔数据，可以实时取得股票当前报价和成交信息，其中一种场景是，
        # 写一个python定时程序来调用本接口（可两三秒执行一次，性能与行情软件基本一致），
        # 然后通过DataFrame的矩阵计算实现交易监控，可实时监测交易量和价格的变化。
        # 参数说明：
        # symbols：6
        # 位数字股票代码，或者指数代码（sh = 上证指数
        # sz = 深圳成指
        # hs300 = 沪深300指数
        # sz50 = 上证50
        # zxb = 中小板
        # cyb = 创业板） 可输入的类型：str、list、set或者pandas的Series对象
        # 调用方法：
        # 返回值说明：
        # 0：name，股票名字
        # 1：open，今日开盘价
        # 2：pre_close，昨日收盘价
        # 3：price，当前价格
        # 4：high，今日最高价
        # 5：low，今日最低价
        # 6：bid，竞买价，即“买一”报价
        # 7：ask，竞卖价，即“卖一”报价
        # 8：volume，成交量
        # maybe
        # you
        # need
        # do
        # volume / 100
        # 9：amount，成交金额（元
        # CNY）
        # 10：b1_v，委买一（笔数
        # bid
        # volume）
        # 11：b1_p，委买一（价格
        # bid
        # price）
        # 12：b2_v，“买二”
        # 13：b2_p，“买二”
        # 14：b3_v，“买三”
        # 15：b3_p，“买三”
        # 16：b4_v，“买四”
        # 17：b4_p，“买四”
        # 18：b5_v，“买五”
        # 19：b5_p，“买五”
        # 20：a1_v，委卖一（笔数
        # ask
        # volume）
        # 21：a1_p，委卖一（价格
        # ask
        # price）
        # ...
        # 30：date，日期；
        # 31：time，时间；
        #########################################
        isCode = True
        isByDate = False
        code_collection = "HS300"
        # import tushare as ts
        # df = ts.get_realtime_quotes('000581')  # Single stock symbol
        # df[['code', 'name', 'price', 'bid', 'ask', 'volume', 'amount', 'time']]
        self.core_function("realtime_quotes", isCode, isByDate, code_collection)

    def test_get_h_data(self):
        # self.set_data()
        # print(fd.get_h_data(self.code, self.start, self.end))
        #########################################
        # 复权数据
        # 获取历史复权数据，分为前复权和后复权数据，接口提供股票上市以来所有历史数据，默认为前复权。
        # 如果不设定开始和结束日期，则返回近一年的复权数据，从性能上考虑，推荐设定开始日期和结束日期，
        # 而且最好不要超过三年以上，获取全部历史数据，请分年段分步获取，取到数据后，请及时在本地存储。
        # 获取个股首个上市日期，请参考以下方法：
        # 参数说明：
        #
        # code: string, 股票代码
        # e.g.
        # 600848
        # start: string, 开始日期
        # format：YYYY - MM - DD
        # 为空时取当前日期
        # end: string, 结束日期
        # format：YYYY - MM - DD
        # 为空时取去年今日
        # autype: string, 复权类型，qfq - 前复权
        # hfq - 后复权
        # None - 不复权，默认为qfq
        # index: Boolean，是否是大盘指数，默认为False
        # retry_count: int, 默认3, 如遇网络等问题重复执行的次数
        # pause: int, 默认      0, 重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        # 返回值说明：
        #
        # date: 交易日期(index)
        # open: 开盘价
        # high: 最高价
        # close: 收盘价
        # low: 最低价
        # volume: 成交量
        # amount: 成交金额
        #########################################
        isCode = True
        isByDate = True
        code_collection = "HS300"
        self.core_function("h_data", isCode, isByDate, code_collection)

    # def test_get_index(self):
    #     # self.set_data()
    #     # print(fd.get_h_data(self.code, self.start, self.end))
    #     #########################################
    #     # 大盘指数行情列表
    #     # 获取大盘指数实时行情列表，以表格的形式展示大盘指数实时行情。
    #     #
    #     # 调用方法：
    #     # 返回值说明：
    #     # code: 指数代码
    #     # name: 指数名称
    #     # change: 涨跌幅
    #     # open: 开盘点位
    #     # preclose: 昨日收盘点位
    #     # close: 收盘点位
    #     # high: 最高点位
    #     # low: 最低点位
    #     # volume: 成交量(手)
    #     # amount: 成交金额（亿元）
    #     #########################################
    #     isCode = False
    #     isByDate = False
    #     code_collection = "HS300"
    #     self.core_function("index", isCode,isByDate, code_collection)
    #
    # def test_get_sina_dd(self):
    #     isCode = True
    #     isByDate = True
    #     code_collection = "HS300"
    #     self.core_function("sina_dd", isCode,isByDate, code_collection)
    #
    # def test_get_today_ticks(self):
    #     # self.set_data()
    #     # print(fd.get_today_ticks(self.code))
    #     #########################################
    #     # 当日历史分笔¶
    #     # 获取当前交易日（交易进行中使用）已经产生的分笔明细数据。
    #     # 参数说明：
    #     # code：股票代码，即6位数字代码
    #     # retry_count: int, 默认3, 如遇网络等问题重复执行的次数
    #     # pause: int, 默认        0, 重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
    #     # 返回值说明：
    #     # time：时间
    #     # price：当前价格
    #     # pchange: 涨跌幅
    #     # change：价格变动
    #     # volume：成交手
    #     # amount：成交金额(元)
    #     # type：买卖类型【买盘、卖盘、中性盘】
    #     #########################################
    #     isCode = True
    #     isByDate = False
    #     code_collection = "HS300"
    #     self.core_function("today_ticks", isCode, isByDate, code_collection)
    #

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()