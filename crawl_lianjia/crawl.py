import requests
import re
from bs4 import BeautifulSoup
import os
import json
import data_process
import threading
import traceback
import config
import pymysql
import logging


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 Safari/537.36 SE 2.X MetaSr 1.0'
}


def get_url_list(url):
    try:
        r = requests.get(url, headers=headers,timeout=30)
    except:
        logging.error(url)
        logging.error(traceback.format_exc())
    res = re.findall('<div class="title"><a class="" href="(.*?)" target', r.text, re.S)
    return res


def crawl(url, id_set):
    res_list = ['' for i in range(44)]
    house_pic_list = list()

    # 房子链接id
    id = url[len(config.URL):len(url) - 5]
    img_is_crawl = False
    if id in id_set:
        img_is_crawl = True
    res_list[0] = id
    res_list[14] = '0'
    try:
        r = requests.get(url, headers=headers,timeout=30)
        soup = BeautifulSoup(r.text, 'lxml')

        # 房子标题
        title = soup.find('h1', class_='main').text
        res_list[1] = title

        # 房子总价
        price_tag = soup.find('div', class_='price')
        price = price_tag.find('span', class_='total').text + price_tag.find('span', class_='unit').text
        res_list[2] = price

        # 房子关注人数
        fav_count = soup.find('span', id='favCount').text
        res_list[3] = fav_count

        # 单位价格
        unit_price = soup.find('span', class_='unitPriceValue').text
        res_list[4] = unit_price

        # 小区名字
        community_name = soup.find('div', class_='communityName').find('a').text
        res_list[33] = community_name

        # 所处地区
        area_name = soup.find('div', class_='areaName').find('span', class_='info').text
        res_list[5] = area_name

        info = soup.find('div', class_='introContent')
        base_info_soup = info.find('div', class_='base').find('div', class_='content').ul.find_all('li')

        try:
            # 基础属性
            if config.URL[8:10] == 'bj':
                base_info_soup.pop(10)
            for i in range(12):
                res_list[6+i] = base_info_soup[i].contents[1]
        except:
            pass

        sell_info_soup = info.find('div', class_='transaction').find('div', class_='content').ul.find_all('li')

        try:
            # 交易属性
            for i in range(8):
                span_list =sell_info_soup[i].find_all('span')
                string = span_list[1].text
                if i == 6:
                    string = string.replace(' ','')
                res_list[18+i] = string
        except:
            pass

        # 房子特色

        intro_info_list = soup.find('div', class_='showbasemore').find_all('div', recursive=False)
        intro_info = ''
        for i in range(len(intro_info_list)):
            div = intro_info_list[i].find_all('div')
            if len(div) >= 2:
                intro_info += div[0].text + ' ' + div[1].text + '\n'
        res_list[26] = intro_info

        # 布局
        layout_des = ''
        try:
            layout = soup.find('div', id='layout').div.find('div', class_='content')
            row_list = layout.find('div', id='infoList').find_all('div', recursive=False)
            for row in row_list:
                for div in row.find_all('div'):
                    layout_des += div.text + ' '
                layout_des += '\n'
        except:
            pass
        res_list[27] = layout_des

        # 图片
        try:
            if not img_is_crawl:
                house_pic_tag_list = soup.find('div', class_='housePic').find_all('img')
                for tag in house_pic_tag_list:
                    house_pic_list.append(data_process.Img(tag['src'], id, tag['alt']))
        except:
            pass

        # 代看人数
        r1 = requests.get(config.URL + 'houseseerecord', params={'id': id}, headers=headers,timeout=10)
        j = json.loads(r1.text)
        count_7 = j['data']['thisWeek']
        count_30 = j['data']['totalCnt']
        res_list[28] = count_7
        res_list[29] = count_30

        # 经纬度
        jingweidu = re.findall("resblockPosition:'(.*?)'", r.text)
        jingweidu = jingweidu[0]
        res_list[30] = jingweidu

        # 经纪人评论
        comment_data = {}
        comment_data['isContent'] = 1
        comment_data['page'] = 1
        comment_data['order'] = 0
        comment_data['id'] = id
        comment_r = requests.get(config.URL + 'showcomment', headers=headers, params=comment_data,timeout=10)
        comment_dict = json.loads(comment_r.text)
        comment = ''
        if len(comment_dict['data']) != 0:
            for agent in comment_dict['data']['agentList']:
                comment += agent['comment'] + '\n'
        res_list[31] = comment

        # 小区id
        community_url = soup.find('div', class_='communityName').find('a')['href']
        community_id = community_url[8:len(community_url) - 1]
        res_list[32] = community_id

        r = requests.get(config.BASE_URL + community_url, headers=headers,timeout=10)
        soup = BeautifulSoup(r.text, 'lxml')
        try:
            if not img_is_crawl:
                community_img = soup.find('ol', id='overviewThumbnail').find_all('li')[0].img['src']
                house_pic_list.append(data_process.Img(community_img, id, '小区'))
        except:
            pass

        # 小区关注人数
        follow_num_tag = soup.find('div', class_='detailFollowedNum')
        if not follow_num_tag is None:
            follow_count = follow_num_tag.span.text
            res_list[34] = follow_count

        # 小区平均价格
        try:
            info = soup.find('div', class_='xiaoquDescribe')
            price = info.find('div').div.span.text
            res_list[35] = price
        except:
            pass

        # 小区信息
        community_info_tag_list = info.find('div', class_='xiaoquInfo').find_all('div')
        for i in range(8):
            res_list[36 + i] = community_info_tag_list[i].contents[1].text
        '''
        for tag in community_info_tag_list:
            community_base_info += tag.contents[0].text + ' ' + tag.contents[1].text + '\n'
        res_list[18] = community_base_info
        '''


    except:
        logging.error(url)
        logging.error(traceback.format_exc())
    finally:
        return res_list, house_pic_list


def get_small_region_list():
    r = requests.get(config.URL, headers=headers)
    big_region_list = []
    soup = BeautifulSoup(r.text, 'lxml')
    a_list = soup.find('div', class_='position').find_all('dl')[1].dd.find('div').find_all('a')

    length = len(a_list)
    # 北京二手房有两个链接并非北京的，去掉 2 个
    if config.URL == 'https://bj.lianjia.com/ershoufang/':
        length -= 2

    for i in range(length):
        big_region_list.append(a_list[i]['href'])
    small_region = set()
    for big_region in big_region_list:
        r = requests.get(config.BASE_URL + big_region, headers=headers)
        soup = BeautifulSoup(r.text, 'lxml')
        a_list = soup.find('div', class_='position').find_all('dl')[1].dd.find('div').find_all('div')[1].find_all('a')
        for a in a_list:
            if not a['href'] in big_region_list:
                small_region.add(a['href'])
    small_region_list = list(small_region)
    return small_region_list


class CrawlHouseUrlThread(threading.Thread):
    def __init__(self, url_queue, url_set, lock):
        threading.Thread.__init__(self)
        self.queue = url_queue
        self.url_set = url_set
        self.lock = lock

    def run(self):
        while True:
            try:
                small_region = self.queue.get(block=False)
            except:
                break
            url = config.BASE_URL + small_region
            try:
                r = requests.get(url, headers=headers)
            except Exception as e:
                logging.error('爬取' + url + traceback.format_exc())
                continue
            res = re.findall('page-data=\'{"totalPage":(.*?),"', r.text)
            if len(res) == 0:
                continue
            max_page = int(res[0])
            for i in range(1, max_page + 1):
                url_list = get_url_list(url + 'pg' + str(i) + '/')
                if self.lock.acquire():
                    for u in url_list:
                        self.url_set.add(u)
                    self.lock.release()


class CrawlHouseThread(threading.Thread):
    def __init__(self, out_queue, data_p, id_set):
        threading.Thread.__init__(self)
        self.out_queue = out_queue
        self.data_process = data_p
        self.id_set = id_set

    def run(self):
        while True:
            url = self.data_process.get_house_url()
            if url == '':
                break
            res, img_list = crawl(url,self.id_set)
            self.out_queue.put(res)
            if len(img_list) != 0:
                self.data_process.insert_img_url(img_list)


class DownloadImgThread(threading.Thread):
    def __init__(self, data_p):
        threading.Thread.__init__(self)
        self.data_process = data_p
        self.dir = config.IMG_DIR + config.NAME + '/'
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)

    def run(self):
        while True:
            img_list = self.data_process.get_img()
            if len(img_list) == 0:
                break
            for img in img_list:
                try:
                    dir = self.dir + str(img[2])
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    r = requests.get(img[1],headers=headers,timeout=20)
                    with open(dir + '/' + img[3],'wb')as f:
                        f.write(r.content)
                except:
                    logging.error(traceback.format_exc())


class OutThread(threading.Thread):

    def __init__(self, data_queue):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.conn = pymysql.connect("localhost", config.MYSQL_NAME, config.MYSQL_PASSWORD, config.DATABASE_NAME)
        self.house_sql = 'INSERT IGNORE INTO {} VALUES (%s'.format(config.HOUSE_TABLE)
        for i in range(32):
            self.house_sql += ',%s'
        self.house_sql += ')'
        self.community_sql = 'INSERT IGNORE INTO {} VALUES (%s'.format(config.COMMUNITY_TABLE)
        for i in range(11):
            self.community_sql += ',%s'
        self.community_sql += ')'

    def run(self):
        while True:
            try:
                item = self.data_queue.get(timeout=30)
            except:
                break
            c = self.conn.cursor()
            c.execute(self.house_sql, item[:33])
            c.execute(self.community_sql, item[32:])
            self.conn.commit()
        self.conn.close()
        print('所有房子连接已爬取完')

        '''
            items = list()
            try:
                for i in range(100):
                    items.append(self.data_queue.get(timeout=30))
            except:
                state = False
                if len(items) == 0:
                    break
            with open('data/data_' + str(self.count) + '.csv', 'w', encoding='utf-8', newline='')as f:
                writer = csv.writer(f)
                writer.writerow(
                    ['id', '标题', '价格', '关注人数', '单元价格', '小区名字', '所在区域', '基本属性', '交易属性', '特色介绍', '布局', '7天看房人数',
                     '30天看房人数', '经纬度', '带看人评论', '小区关注人数', '小区平均价格', '小区基础信息'])
                writer.writerows(items)
                self.count += 1
        print('所有连接已爬取完')
        '''
