__author__ = 'Oscar_Yang'
#-*- coding= utf-8 -*-
"""
    查看mongodb存储状况的脚本count.py
"""
import time
import pymongo
client = pymongo.MongoClient("localhost", 27017)
db = client["SCRAPY_anjuke_gz"]
sheet = db["anjuke_doc1"]

while True:
    print(sheet.find().count())
    print("____________________________________")
    time.sleep(3)