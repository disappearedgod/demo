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
        ts.set_token('xxxxxxxxxxxxxxxxxxxxxxxxxxxx')

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

    def test_get_growth_data(self):
        mt = ts.Master()
        df = mt.TradeCal(exchangeCD='XSHG', beginDate='20150928', endDate='20151010',
                         field='calendarDate,isOpen,prevTradeDate')
        print (df)





if __name__ == '__main__':
    unittest.main()