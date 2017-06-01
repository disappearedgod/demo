# coding: utf-8

from bs4 import BeautifulSoup
from concurrent import futures
import sys
import time
import pandas as pd
import urllib
import random
import json
from pymongo import MongoClient


def mongo_exc(city):
    client = MongoClient("127.0.0.1", 27017)
    chengjiao_coll = client['lianjia']
    xiaoqu_coll = client['lianjia_xiaoqu']
    chengjiao_cursor = chengjiao_coll[city + '_chengjiao'].find()

    countNK=0
    countHP=0
    countHB=0
    countHD=0
    countHX=0
    countHQ=0

    for item in chengjiao_cursor:
        xiaoqu_cursor = xiaoqu_coll[city + '_xiaoqu_info'].find({'ID':item[u'小区ID']})
        count = 0
        for item2 in xiaoqu_cursor:
            if(count==1):
                break;
            chengjiao = item[u'成交日期'].split('.');
            season = int(int(chengjiao[1]) / 4)+1
            season =chengjiao[0]+'Q'+ str(season);
            mouth = chengjiao[0]+'.'+ chengjiao[1];

            tmpJson ={
                "小区ID":item[u'小区ID'],
                "URL":item[u'URL'],
                "小区名称":item[u'小区名称'],
                "户型":item[u'户型'],
                "建筑面积":item[u'建筑面积'],
                "成交价":item[u'成交价'],
                "挂牌价":item[u'挂牌价'],
                "单价":item[u'单价'],
                "成交周期":item[u'成交周期'],
                "成交日期":item[u'成交日期'],
                "朝向":item[u'朝向'],
                "装修":item[u'装修'],
                "电梯":item[u'电梯'],
                "楼层":item[u'楼层'],
                "建筑类型":item[u'建筑类型'],
                "区域":item2[u'区域'],
                "片区":item2[u'片区'],
                "季度":season,
                "成交月份":mouth
            }
            if(item2[u'区域']=='南开'):
                countNK =countNK + 1;
                print("南开：",countNK)
            elif(item2[u'区域']=='和平'):
                countHP =countHP + 1;
                print("和平：",countHP)
            elif (item2[u'区域'] == '河西'):
                countHX = countHX + 1;
                print("河西：",countHX)

            elif (item2[u'区域'] == '红桥'):
                countHQ = countHQ + 1;
                print("红桥：",countHQ)

            elif (item2[u'区域'] == '河北'):
                countHB = countHB + 1;
                print("河北：",countHB)

            elif (item2[u'区域'] == '河东'):
                countHD = countHD + 1;
                print("河东：",countHD)

            #print(tmpJson)
            coll = client.lianjia_data["records"]
            coll.insert(tmpJson)
            count = count + 1


def get_num_from_xiaoqu(city):
    client = MongoClient("127.0.0.1", 27017)
    coll = client['lianjia_chengjiao']
    cursor = coll[city].find()
    list = []
    # count = 0
    fl = open('E:/python/demo/main/getData/foundation/housedata.json', 'w')
    for item in cursor:
        # if(count == 50):
        #     break;
        # count = count + 1
        #print(item)
        tmpJson ={
            "area":item[u'建筑面积'],
            "url":item[u'URL'],
            "average_price":item[u'单价'],
            "floor":item[u'楼层'],
            "build_time":item[u'成交日期'],
            "location":item[u'小区名称'],
            "room_shape":item[u'朝向'],
            "price":item[u'成交价']
        }
        list.append(tmpJson)
        print (tmpJson)
        fl.write(json.dumps(tmpJson))
        fl.write('\n')
    fl.close()
    #print (list)
    return list

if __name__ == '__main__':

    # # 测试区域
    # 测试部分函数的运行结果

    city_dict = {"成都": "cd", "天津": "tj", "北京": "bj", "上海": "sh", "广州": "gz", "深圳": "sz", "南京": "nj", "合肥": "hf",
                 "杭州": "hz", }
    CITY = city_dict["天津"]

    # datas = get_multiple_data()
    # print(datas)
    mongo_exc(CITY)
