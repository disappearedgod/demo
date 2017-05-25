# -*- coding:utf-8 -*-
'''
Created on 2015/3/14
@author: Jimmy Liu
'''
import unittest
import tushare.stock.newsevent as fd
import tushare as ts
from pymongo import MongoClient
import json

class Test(unittest.TestCase):
    def set_data(self):
        self.code = '600848'
        self.start = '2015-01-03'
        self.end = '2015-04-07'
        self.year = 2014
        self.quarter = 4
        self.top = 60
        self.show_content = True

    def core_function(self, func):
        self.set_data()
        print(func)
        # mongo = MongoClient(self.host, self.port)
        mongo = MongoClient("127.0.0.1", 27017)
        if (func == 'latest_news'):
            df = fd.get_latest_news(self.top, self.show_content)
            insert_string = df.to_json(orient='records')
            #ts.get_latest_news()  # 默认获取最近80条新闻数据，只提供新闻类型、链接和标题
            #ts.get_latest_news(top=5, show_content=True)  # 显示最新5条新闻，并打印出新闻内容
        elif (func == 'notice_content'):
            df = fd.get_notices(self.code)
            m = fd.notice_content(df.ix[0]['url'])
            print (m)
            insert_string = df.to_json(orient='records')
        elif (func == 'guba_sina'):
            #df = fd.guba_sina(self.show_content)
            df = ts.guba_sina()
            insert_string = json.dumps(df)
            print (df)
        else:
            df = {}
            insert_string = df.to_json(orient='records')
        # print(df)

        items = json.loads(insert_string)
        coll = mongo.news[func]
        coll.insert(items)

    def test_get_latest_news(self):
        #################################
        # 即时新闻
        # 获取即时财经新闻，类型包括国内财经、证券、外汇、期货、港股和美股等新闻信息。数据更新较快，使用过程中可用定时任务来获取。
        #  ###########
        # 参数说明：
        # top: int，显示最新消息的条数，默认为80条
        # show_content: boolean, 是否显示新闻内容，默认False
        # 返回值说明：
        # ###########
        # classify: 新闻类别
        # title: 新闻标题
        # time: 发布时间
        # url: 新闻链接
        # content: 新闻内容（在show_content为True的情况下出现）
        #################################
        # self.set_data()
        # print(fd.get_latest_news(self.top, self.show_content))
        self.core_function("latest_news")

    def test_get_notices(self):
        # self.set_data()
        # df = fd.get_notices(self.code)
        # print(fd.notice_content(df.ix[0]['url']))
        #################################
        # 信息地雷
        # 获取个股信息地雷数据。
        # 参数说明：
        # code: 股票代码
        # date: 信息公布日期
        # 返回值说明：
        # title: 信息标题
        # type: 信息类型
        # date: 公告日期
        # url: 信息内容URL
        #################################
        self.core_function("notice_content")

    def test_guba_sina(self):
        # self.set_data()
        # print(fd.guba_sina(self.show_content))
        ###################################
        # 获取sina财经股吧首页的重点消息。股吧数据目前获取大概17条重点数据，可根据参数设置是否显示消息内容，默认情况是不显示。
        #
        # 参数说明：
        #
        # show_content: boolean, 是否显示内容，默认False
        # 返回值说明：
        #
        # title, 消息标题
        # content, 消息内容（show_content = True的情况下）
        # ptime, 发布时间
        # rcounts, 阅读次数
        ###################################
        self.core_function("guba_sina")


if __name__ == "__main__":
    unittest.main()