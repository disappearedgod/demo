from bs4 import BeautifulSoup
from concurrent import futures
import sys
import time
import pandas as pd
import urllib
import random
import json
from pymongo import MongoClient

class Community:
    def __init__(self, city):
        self.isWrite = False
        self.PRINT = False
        # 多线程数量设置
        self.NUM_THREADS = 8
        self.tableNameCommunity = city + '_xiaoqu_num'
    # # 代理设置
        self.hds = [{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}, {
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


    def getCommunity(self):
        return

    def get_bs_obj_from_url(self, http_url):
        done = False
        exception_time = 0
        while not done:
            try:
                if self.PRINT:
                    print("Getting {}".format(http_url))
                req = urllib.request.Request(http_url, headers=self.hds[random.randint(0, len(self.hds) - 1)])
                html = urllib.request.urlopen(req)
                bs_obj = BeautifulSoup(html.read(), "lxml")
                done = True
            except Exception as e:
                if self.PRINT:
                    print(e)
                exception_time += 1
                time.sleep(2)
                if exception_time > 10:
                    return None
        return bs_obj

    def get_xiaoqu_in_page(self, city, district, page_no):
        http_url = "http://{}.lianjia.com/xiaoqu/{}/pg{}".format(city, district, page_no)
        bs_obj = self.get_bs_obj_from_url(http_url)

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

    def get_xiaoqu_from_district(self, city, district):
        print("get_xiaoqu_from_district")
        http_url = "http://{}.lianjia.com/xiaoqu/{}".format(city, district)
        bs_obj = self.get_bs_obj_from_url(http_url)

        total_pages = int(
            json.loads(bs_obj.find("div", {"class": "page-box house-lst-page-box"}).attrs["page-data"])["totalPage"])
        total_xiaoqu_num = int(bs_obj.find("h2", {"class": "total fl"}).find("span").get_text())

        xiaoqu_list = []

        with futures.ThreadPoolExecutor(max_workers=self.NUM_THREADS) as executor:
            future_list = []
            for page_no in range(1, total_pages + 1):
                future_list.append(executor.submit(self.get_xiaoqu_in_page, city, district, page_no))
            fail_list = []
            for future in futures.as_completed(future_list):
                page_no, xiaoqu_list_partial = future.result()
                if xiaoqu_list_partial is None or len(xiaoqu_list_partial) == 0:
                    fail_list.append(page_no)
                else:
                    xiaoqu_list += xiaoqu_list_partial
            for page_no in fail_list:
                page_no, xiaoqu_list_partial = self.get_xiaoqu_in_page(city, district, page_no)
                if xiaoqu_list_partial is not None and len(xiaoqu_list_partial) > 0:
                    xiaoqu_list += xiaoqu_list_partial
            # print(xiaoqu_list)
            mongo = MongoClient("127.0.0.1", 27017)
            items = []
            for num in xiaoqu_list:
                item = {
                    'num': num
                }
                items.append(item)
            # print(items)
            tablename = self.tableNameCommunity
            coll = mongo.lianjia[tablename]
            coll.insert(items)
        return xiaoqu_list

    def get_district_from_city(self, city):
        print("********** Processing City: {} **********".format(city))
        city_url = "http://{}.lianjia.com".format(city)
        http_url = city_url + "/xiaoqu"
        bs_obj = self.get_bs_obj_from_url(http_url)

        parent_div = bs_obj.find("div", {"data-role": "ershoufang"})
        a_list = parent_div.find_all("a")

        district_list = [a.attrs["href"].replace("/xiaoqu/", "")[:-1] for a in a_list]

        print("########## Got {} districts ##########".format(len(district_list)))

        return district_list


    def get_xiaoqu_of_city(self, city):
        district_list = self.get_district_from_city(city)
        xiaoqu_list = []
        for district in district_list:
            xiaoqu_of_district = self.get_xiaoqu_from_district(city, district)
            xiaoqu_list += xiaoqu_of_district
            print("****** 当前区域小区数: {}, 总小区数: {} ******".format(len(xiaoqu_of_district), len(xiaoqu_list)))

        return xiaoqu_list

    def test_get_community_ID(self, city):
        xiaoqu_list = self.get_xiaoqu_of_city(city)

        # In[ ]:
        if (self.isWrite):
            with open("{}_list.txt".format(city), mode="w") as f:
                for xiaoqu in xiaoqu_list:
                    f.write(xiaoqu + "\n")
            print("list write finished.")

            # In[14]:

            with open("{}_list.txt".format(city), mode="r") as f:
                xiaoqu_list = [line[:-1] for line in f.readlines()]


if __name__ == '__main__':
    c = Community()
    c.getCommunity()