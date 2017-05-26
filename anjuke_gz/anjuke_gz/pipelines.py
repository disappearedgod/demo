# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import pymongo
from scrapy.conf import settings

class AnjukeGzPipeline(object):
    def __init__(self):
        host=settings["MONGODB_HOST"]
        port=settings["MONGODB_PORT"]
        dbname=settings["MONGODB_DBNAME"]
        client=pymongo.MongoClient(port=port,host=host)
        tdb = client[dbname]
        self.post=tdb[settings["MONGODB_DOCNAME"]]
    def process_item(self,item,spider):
        info = dict(item)
        self.post.insert(info)
        return item