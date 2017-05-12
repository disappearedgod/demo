import os
#from sqlalchemy import create_engin
#from pandas.io.pytables import HDFStore
import tushare as ts
from pymongo import MongoClient
import unittest

class read_code:
    def __init__(self):
        self.code = '000875'
        self.start = '2017-01-03'
        self.end = '2017-01-07'
        self.year = 2017
        self.quarter = 1
        self.client = MongoClient('localhost', 27017)
        return;

    def readHS300Code(self):
        cursor = self.client.stockcodes.HS300.find()
        list = []
        for item in cursor:
            list.append(item['stockcode'])
        return list

    def readITCode(self):
        client = self.MongoClient('localhost', 27017)
        cursor = client.stockcodes.IT.find()
        list = []
        for item in cursor:
            list.append(item['stockcode'])
        return list