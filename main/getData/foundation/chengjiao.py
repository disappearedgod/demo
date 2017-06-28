# coding: utf-8

# In[1]:


# author: Yabin Zheng
# Email: sczhengyabin@hotmail.com

import re
from bs4 import BeautifulSoup
from concurrent import futures
import sys
import time
import pandas as pd
import urllib
import random
import json
from pymongo import MongoClient

# In[2]:

# 多线程数量设置
NUM_THREADS = 8



# 是否打印 HTTP error
PRINT = False

# In[3]:

hds = [{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}, {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},
       {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'}, {
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
       {
           'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
       {
           'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
       {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
       {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
       {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

# # 代理设置

# In[4]:

proxy_support = urllib.request.ProxyHandler({'sock5': 'localhost:1080'})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)


# # 函数定义

# In[5]:

def get_bs_obj_from_url(http_url):
    done = False
    exception_time = 0
    while not done:
        try:
            if PRINT:
                print("Getting {}".format(http_url))
            req = urllib.request.Request(http_url, headers=hds[random.randint(0, len(hds) - 1)])
            html = urllib.request.urlopen(req)
            bs_obj = BeautifulSoup(html.read(), "lxml")
            done = True
        except Exception as e:
            if PRINT:
                print(e)
            exception_time += 1
            time.sleep(2)
            if exception_time > 10:
                return None
    return bs_obj


# In[6]:

def get_district_from_city(city):
    print("********** Processing City: {} **********".format(city))
    city_url = "http://{}.lianjia.com".format(city)
    http_url = city_url + "/xiaoqu"
    bs_obj = get_bs_obj_from_url(http_url)

    parent_div = bs_obj.find("div", {"data-role": "ershoufang"})
    a_list = parent_div.find_all("a")

    district_list = [a.attrs["href"].replace("/xiaoqu/", "")[:-1] for a in a_list]

    print("########## Got {} districts ##########".format(len(district_list)))

    return district_list


# In[7]:

def get_xiaoqu_from_district(city, district):
    print("get_xiaoqu_from_district")
    http_url = "http://{}.lianjia.com/xiaoqu/{}".format(city, district)
    bs_obj = get_bs_obj_from_url(http_url)

    total_pages = int(
        json.loads(bs_obj.find("div", {"class": "page-box house-lst-page-box"}).attrs["page-data"])["totalPage"])
    total_xiaoqu_num = int(bs_obj.find("h2", {"class": "total fl"}).find("span").get_text())

    xiaoqu_list = []

    with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_list = []
        for page_no in range(1, total_pages + 1):
            future_list.append(executor.submit(get_xiaoqu_in_page, city, district, page_no))
        fail_list = []
        for future in futures.as_completed(future_list):
            page_no, xiaoqu_list_partial = future.result()
            if xiaoqu_list_partial is None or len(xiaoqu_list_partial) == 0:
                fail_list.append(page_no)
            else:
                xiaoqu_list += xiaoqu_list_partial
        for page_no in fail_list:
            page_no, xiaoqu_list_partial = get_xiaoqu_in_page(city, district, page_no)
            if xiaoqu_list_partial is not None and len(xiaoqu_list_partial) > 0:
                xiaoqu_list += xiaoqu_list_partial
        #print(xiaoqu_list)
        mongo = MongoClient("127.0.0.1", 27017)
        items = []
        for num in xiaoqu_list:
            item = {
                'num' : num
            }
            items.append(item)
        print(items)
        tablename = city
        coll = mongo.lianjia_xiaoqu_num[tablename]
        coll.insert(items)
    return xiaoqu_list


# In[8]:

def get_xiaoqu_in_page(city, district, page_no):
    http_url = "http://{}.lianjia.com/xiaoqu/{}/pg{}".format(city, district, page_no)
    bs_obj = get_bs_obj_from_url(http_url)

    if bs_obj is None:
        return None

    parent_list = bs_obj.find_all("li", {"class": "clear xiaoquListItem"})

    xiaoqu_list = []

    if not (len(parent_list) == 0):
        for li in parent_list:
            xiaoqu_url = li.find("div", {"class": "title"}).find("a").attrs["href"]
            #             xiaoqu_id = re.sub(r"http://[a-z]*.lianjia.com/xiaoqu/", "", xiaoqu_url)
            xiaoqu_id = "".join(list(filter(str.isdigit, xiaoqu_url)))
            xiaoqu_list.append(xiaoqu_id)
    return page_no, xiaoqu_list


# In[9]:

def get_xiaoqu_of_city(city):
    district_list = get_district_from_city(city)
    xiaoqu_list = []
    for district in district_list:
        xiaoqu_of_district = get_xiaoqu_from_district(city, district)
        xiaoqu_list += xiaoqu_of_district
        print("****** 当前区域小区数: {}, 总小区数: {} ******".format(len(xiaoqu_of_district), len(xiaoqu_list)))


    return xiaoqu_list


# In[10]:

def get_xiaoqu_info(city, xiaoqu_id):
    http_url = "http://{}.lianjia.com/xiaoqu/{}".format(city, xiaoqu_id)
    bs_obj = get_bs_obj_from_url(http_url)

    df = pd.DataFrame()

    if bs_obj is not None:
        try:
            location_list = bs_obj.find("div", {"class": "fl l-txt"}).find_all("a")
            info_city = location_list[1].get_text().replace("小区", "")
            info_district = location_list[2].get_text().replace("小区", "")
            info_area = location_list[3].get_text().replace("小区", "")
            info_name = location_list[4].get_text()

            if bs_obj.find("span", {"class": "xiaoquUnitPrice"}) is not None:
                info_price = bs_obj.find("span", {"class": "xiaoquUnitPrice"}).get_text()
            else:
                info_price = "暂无报价"

            info_address = bs_obj.find("div", {"class": "detailDesc"}).get_text()

            info_list = bs_obj.find_all("span", {"class": "xiaoquInfoContent"})
            info_year = info_list[0].get_text().replace("年建成", "")
            info_type = info_list[1].get_text()
            info_property_fee = info_list[2].get_text()
            info_property_company = info_list[3].get_text()
            info_developer_company = info_list[4].get_text()
            info_building_num = info_list[5].get_text().replace("栋", "")
            info_house_num = info_list[6].get_text().replace("户", "")

            df = pd.DataFrame(data=[[xiaoqu_id, http_url, info_name, info_city,
                                     info_district, info_area, info_price, info_year,
                                     info_building_num, info_house_num, info_developer_company, info_property_fee,
                                     info_property_company, info_type, info_address]],
                              columns=["ID", "URL", "小区名称", "城市",
                                       "区域", "片区", "参考均价", "建筑年代",
                                       "总栋数", "总户数", "开发商", "物业费",
                                       "物业公司", "建筑类型", "地址"])
            mongo = MongoClient("127.0.0.1", 27017)
            loadJson = df.to_json(orient='records')
            items = json.loads(loadJson)
            tablename = city + "_xiaoqu_info"
            coll = mongo.lianjia_xiaoqu[tablename]
            coll.insert(items)
        except Exception as e:
            print(e)

    return xiaoqu_id, df


# In[11]:

def get_xiaoqu_info_from_xiaoqu_list(city, xiaoqu_list):
    df_xiaoqu_info = pd.DataFrame()
    count = 0
    pct = 0

    with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_list = []
        for xiaoqu in xiaoqu_list:
            future_list.append(executor.submit(get_xiaoqu_info, city, xiaoqu))
        fail_list = []
        print(" ")
        for future in futures.as_completed(future_list):
            xiaoqu, df_info_partial = future.result()
            if len(df_info_partial) == 0:
                fail_list.append(xiaoqu)
            else:
                df_xiaoqu_info = df_xiaoqu_info.append(df_info_partial)
                count += 1
                sys.stdout.write("\rProgress: {}/{}".format(count, len(xiaoqu_list)))
        for page_no in fail_list:
            xiaoqu, df_info_partial = get_xiaoqu_info(city, xiaoqu)
            if len(df_info_partial) > 0:
                df_xiaoqu_info = df_xiaoqu_info.append(df_info_partial)
                count += 1
        sys.stdout.write("\rProgress: {}/{}".format(count, len(xiaoqu_list)))

    return df_xiaoqu_info


# In[12]:

def get_xiaoqu_transactions_in_page(city, xiaoqu_id, page_no):
    print("get_xiaoqu_transactions_in_page")
    http_url = "http://{}.lianjia.com/chengjiao/pg{}c{}/".format(city, page_no, xiaoqu_id)
    bs_obj = get_bs_obj_from_url(http_url)
    xiaoqu_name = ""
    df = pd.DataFrame()

    if bs_obj is not None:
        try:
            div_list = bs_obj.find_all("div", {"class": "info"})

            for div in div_list:
                div_title = div.find("div", {"class": "title"}).find("a")
                url = div_title.attrs["href"]
                title_strs = div_title.get_text().split(" ")
                xiaoqu_name = title_strs[0]
                house_type = title_strs[1]
                built_area = title_strs[2].replace("平米", "")

                house_info_strs = div.find("div", {"class": "houseInfo"}).get_text().replace(" ", "").split("|")
                direction = house_info_strs[0]
                decoration = house_info_strs[1].replace("&nbsp;", "")
                if len(house_info_strs) == 3:
                    elevator = house_info_strs[2].replace("电梯", "")
                else:
                    elevator = "未知"

                deal_date = div.find("div", {"class": "dealDate"}).get_text()
                deal_price = div.find("div", {"class": "totalPrice"}).find("span", {"class": "number"}).get_text()

                position_info_strs = div.find("div", {"class": "positionInfo"}).get_text().split(" ")
                floor = position_info_strs[0]
                build_type = position_info_strs[1]

                unit_price = div.find("div", {"class": "unitPrice"}).find("span", {"class": "number"}).get_text()

                span_deal_cycle = div.find("span", {"class": "dealCycleTxt"}).find_all("span")
                if len(span_deal_cycle) == 2:
                    list_price = "".join(
                        list(filter(lambda x: str.isdigit(x) or (x == "."), span_deal_cycle[0].get_text())))
                    deal_cycle = "".join(list(filter(str.isdigit, span_deal_cycle[1].get_text())))
                else:
                    deal_cycle = "".join(list(filter(str.isdigit, span_deal_cycle[0].get_text())))
                    list_price = "无"

                temp_df = pd.DataFrame(data=[[xiaoqu_id, url, xiaoqu_name, house_type,
                                              built_area, deal_price, list_price, unit_price,
                                              deal_cycle, deal_date, direction, decoration,
                                              elevator, floor, build_type]],
                                       columns=["小区ID", "URL", "小区名称", "户型",
                                                "建筑面积", "成交价", "挂牌价", "单价",
                                                "成交周期", "成交日期", "朝向", "装修",
                                                "电梯", "楼层", "建筑类型", ])
                df = df.append(temp_df)
                mongo = MongoClient("127.0.0.1", 27017)
                loadJson = df.to_json(orient='records')
                items = json.loads(loadJson)
                print(items)
                tablename = city
                coll = mongo.lianjia_chengjiao[tablename]
                coll.insert(items)

        except Exception as e:
            print(xiaoqu_id, page_no, e)

    return df


def get_xiaoqu_transactions(city, xiaoqu_id):
    print("get_xiaoqu_transactions")
    print(city,xiaoqu_id)
    df_xiaoqu_transctions = pd.DataFrame()

    for i in range(3):
        try:
            http_url = "http://{}.lianjia.com/chengjiao/c{}/".format(city, xiaoqu_id)
            print(http_url)
            bs_obj = get_bs_obj_from_url(http_url)
            total_transaction_num = int(bs_obj.find("div", {"class": "total fl"}).find("span").get_text())
            if total_transaction_num == 0:
                return df_xiaoqu_transctions
            total_pages = int(
                json.loads(bs_obj.find("div", {"class": "page-box house-lst-page-box"}).attrs["page-data"])[
                    "totalPage"])

            break
        except Exception as e:
            print(xiaoqu_id, e)
            print(xiaoqu_id, e)
            if i == 4:
                return df_xiaoqu_transctions

    fail_list = []
    for page_no in range(1, total_pages + 1):
        xiaoqu_transactions_partial = get_xiaoqu_transactions_in_page(city, xiaoqu_id, page_no)
        if xiaoqu_transactions_partial is None or len(xiaoqu_transactions_partial) == 0:
            fail_list.append(page_no)
        else:
            df_xiaoqu_transctions = df_xiaoqu_transctions.append(xiaoqu_transactions_partial)
    for page_no in fail_list:
        xiaoqu_transactions_partial = get_xiaoqu_transactions_in_page(city, xiaoqu_id, page_no)
        if xiaoqu_transactions_partial is not None and len(xiaoqu_transactions_partial) > 0:
            df_xiaoqu_transctions = df_xiaoqu_transctions.append(xiaoqu_transactions_partial)


    return df_xiaoqu_transctions


# In[13]:

def get_transactions_from_xiaoqu_list(city, xiaoqu_list):
    df = pd.DataFrame()
    print(" get_transactions_from_xiaoqu_list")

    with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_list = []
        for xiaoqu in xiaoqu_list:
            future_list.append(executor.submit(get_xiaoqu_transactions, city, xiaoqu))
        fail_list = []
        count = 0
        for future in futures.as_completed(future_list):
            if future.exception() is not None:
                print(future.exception())
            else:
                xiaoqu_transactions_partial = future.result()
                df = df.append(xiaoqu_transactions_partial)
            count += 1
            sys.stdout.write("\rProgress: {}/{}".format(count, len(xiaoqu_list)))
        mongo = MongoClient("127.0.0.1", 27017)
        loadJson = df.to_json(orient='records')
        items = json.loads(loadJson)
        print(items)
        tablename = city + "_transactions_from_xiaoqu"
        coll = mongo.lianjia[tablename]
        coll.insert(items)
    return df


# # 爬取城市的小区ID列表

# In[ ]:
def test_get_community_ID(city):
    xiaoqu_list = get_xiaoqu_of_city(city)

    # In[ ]:

    with open("{}_list.txt".format(city), mode="w") as f:
        for xiaoqu in xiaoqu_list:
            f.write(xiaoqu + "\n")
    print("list write finished.")

    # In[14]:

    with open("{}_list.txt".format(city), mode="r") as f:
        xiaoqu_list = [line[:-1] for line in f.readlines()]

def get_num_from_xiaoqu(city):
    client = MongoClient("127.0.0.1", 27017)
    coll = client['lianjia_xiaoqu_num']
    cursor = coll[city].find()
    list = []
    for item in cursor:
        list.append(item['num'])
    print ("get_num_from_xiaoqu Finished")
    return list

def test_get_write_community_Info(city):
    # # 爬取小区ID列表对应的小区信息
    xiaoqu_list = get_num_from_xiaoqu(city)
    df_xiaoqu_info = get_xiaoqu_info_from_xiaoqu_list(CITY, xiaoqu_list)
    df_xiaoqu_info.to_csv("{}_info.csv".format(CITY), sep=",", encoding="utf-8")
    print("infos write finished.")

def test_success_record(city):
    xiaoqu_list = get_num_from_xiaoqu(city)
    # # 爬取二手房交易记录
    # 分段爬取，避免失败重新爬，同时ExcelWriter有URL写入最多65530条的限制，根据具体情况设置PART的值
    PART = 5
    for i in range(0, PART):
        start = int(i * len(xiaoqu_list) / PART)
        end = int((i + 1) * len(xiaoqu_list) / PART)
        df_transactions = get_transactions_from_xiaoqu_list(CITY, xiaoqu_list[start:end])
        writer = pd.ExcelWriter("{}_transactions_{}.xlsx".format(CITY, i + 1))
        df_transactions.to_excel(writer, "Data")
        writer.save()
        #     df_transactions.to_csv("{}_transaction_{}.csv".format(CITY, i+1), sep=",", encoding="utf-8")
        print("\nfile {} written.".format(i + 1))


if __name__ == '__main__':

    # # 测试区域
    # 测试部分函数的运行结果

    city_dict = {"成都": "cd", "天津": "tj", "北京": "bj", "上海": "sh", "广州": "gz", "深圳": "sz", "南京": "nj", "合肥": "hf",
                 "杭州": "hz", }
    CITY = city_dict["天津"]

    # In[15]:

    http_url = "http://{}.lianjia.com/chengjiao/c{}/".format(CITY, 1611047831383)
    bs_obj = get_bs_obj_from_url(http_url)

    # In[15]:

    #test_success_record(city=CITY)
    test_get_write_community_Info(city=CITY)
    #get_num(city=CITY)
    #test_get_community_ID(city=CITY)
    #get_xiaoqu_transactions(city=CITY, xiaoqu_id=1111027375590)

# mongoexport -d lia市,区域,片区,参考均价,建筑年代,总栋数,总户数,开发商,物业费,物业公司,建筑类型,地址 --csv -o E:\MongoDB\Server\3.4\bin\test\天津小区信息.csvnjia_chengjiao -c tj_xiaoqu_name -f 小区ID,小区名称,户型,建筑面积,成交价,挂牌价,单价,成交周期,成交日期,朝向,装修,电梯,楼层,建筑类型 --csv -o E:\MongoDB\Server\3.4\bin\天津成交信息.csv
#
# mongoexport -d lianjia_xiaoqu -c tj_xiaoqu_info -f ID,URL,小区名称,城