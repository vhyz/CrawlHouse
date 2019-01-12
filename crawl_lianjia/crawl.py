import requests
import re
from bs4 import BeautifulSoup
import os
import json
import data_process
import threading
import wget
import traceback
import config
import pymysql
import logging


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 Safari/537.36 SE 2.X MetaSr 1.0'
}


def get_url_list(url):
    try:
        r = requests.get(url, headers=headers)
    except Exception as e:
        logging.error(traceback.print_exc())
    res = re.findall('<div class="title"><a class="" href="(.*?)" target', r.text, re.S)
    return res


def crawl(url):
    res_list = ['' for i in range(19)]
    house_pic_list = list()

    # 房子链接id
    id = url[len(config.URL):len(url) - 5]
    res_list[0] = id
    res_list[14] = '0'
    try:
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, 'lxml')

        # 房子标题
        title = soup.find('h1', class_='main').text
        res_list[1] = title

        # 房子总价
        price_tag = soup.find('div',class_='price')
        price = price_tag.find('span', class_='total').text + price_tag.find('span',class_='unit').text
        res_list[2] = price

        # 房子关注人数
        fav_count = soup.find('span', id='favCount').text
        res_list[3] = fav_count

        # 单位价格
        unit_price = soup.find('span', class_='unitPriceValue').text
        res_list[4] = unit_price

        # 小区名字
        community_name = soup.find('div', class_='communityName').find('a').text
        res_list[15] = community_name

        # 所处地区
        area_name = soup.find('div', class_='areaName').find('span', class_='info').text
        res_list[5] = area_name

        info = soup.find('div', class_='introContent')
        base_info_soup = info.find('div', class_='base').find('div', class_='content').ul.find_all('li')

        base_info = ''
        for item in base_info_soup:
            base_info += item.contents[0].text + ' ' + item.contents[1] + '\n'
        sell_info_soup = info.find('div', class_='transaction').find('div', class_='content').ul.find_all('li')
        sell_info = ''
        for item in sell_info_soup:
            span_list = item.find_all('span')
            sell_info += span_list[0].text + ' ' + span_list[1].text + '\n'
        # 基础属性
        res_list[6] = base_info
        # 交易属性
        res_list[7] = sell_info

        # 房子特色
        intro_info_list = soup.find('div', class_='showbasemore').find_all('div', recursive=False)
        intro_info = ''
        for i in range(len(intro_info_list)):
            div = intro_info_list[i].find_all('div')
            intro_info += div[0].text + ' ' + div[1].text + '\n'
        res_list[8] = intro_info

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
        res_list[9] = layout_des

        # 图片
        try:
            house_pic_tag_list = soup.find('div', class_='housePic').find_all('img')
            for tag in house_pic_tag_list:
                house_pic_list.append(data_process.Img(tag['src'], id, tag['alt']))
        except:
            pass

        # 代看人数
        r1 = requests.get(config.URL + 'houseseerecord', params={'id': id}, headers=headers)
        j = json.loads(r1.text)
        count_7 = j['data']['thisWeek']
        count_30 = j['data']['totalCnt']
        res_list[10] = count_7
        res_list[11] = count_30

        # 经纬度
        jingweidu = re.findall("resblockPosition:'(.*?)'", r.text)
        jingweidu = jingweidu[0]
        res_list[12] = jingweidu

        # 经纪人评论
        comment_data = {}
        comment_data['isContent'] = 1
        comment_data['page'] = 1
        comment_data['order'] = 0
        comment_data['id'] = id
        comment_r = requests.get(config.URL + 'showcomment', headers=headers, params=comment_data)
        comment_dict = json.loads(comment_r.text)
        comment = ''
        if len(comment_dict['data']) != 0:
            for agent in comment_dict['data']['agentList']:
                comment += agent['comment'] + '\n'
        res_list[13] = comment

        # 小区id

        community_url = soup.find('div', class_='communityName').find('a')['href']
        community_id = community_url[8:len(community_url)-1]
        res_list[14] = community_id

        r = requests.get(config.BASE_URL + community_url, headers=headers)
        soup = BeautifulSoup(r.text, 'lxml')
        try:
            community_img = soup.find('ol', id='overviewThumbnail').find_all('li')[0].img['src']
            house_pic_list.append(data_process.Img(community_img, id, '小区'))
        except:
            pass

        # 小区关注人数
        follow_count = soup.find('div', class_='detailFollowedNum').span.text
        res_list[16] = follow_count

        info = soup.find('div', class_='xiaoquDescribe')
        price = info.find('div').div.span.text
        # 小区品滚价格
        res_list[17] = price
        community_info_tag_list = info.find('div', class_='xiaoquInfo').find_all('div')
        community_base_info = ''
        for tag in community_info_tag_list:
            community_base_info += tag.contents[0].text + ' ' + tag.contents[1].text + '\n'
        # 小区信息
        res_list[18] = community_base_info


    except:
        logging.error(traceback.print_exc())
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
                logging.error('爬取' + url + traceback.print_exc())
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
    def __init__(self, out_queue):
        threading.Thread.__init__(self)
        self.out_queue = out_queue

    def run(self):
        while True:
            url = data_process.get_house_url()
            if url == '':
                break
            print(url)
            res, img_list = crawl(url)
            self.out_queue.put(res)
            if len(img_list) != 0:
                data_process.insert_img_url(img_list)


class DownloadImgThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                img_list = data_process.get_img_url()
                if len(img_list) == 0:
                    break
                if not os.path.exists('img'):
                    os.mkdir('img')
                for img in img_list:
                    dir = 'img/' + str(img[2])
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    wget.download(img[1], dir + '/' + img[3])
            except:
                continue


class OutThread(threading.Thread):

    def __init__(self, data_queue):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.conn = pymysql.connect("localhost",config.MYSQL_NAME,config.MYSQL_PASSWORD,config.DATABASE_NAME)

    def run(self):
        while True:
            try:
                item = self.data_queue.get(timeout=30)
            except:
                break
            c = self.conn.cursor()
            house_sql = 'INSERT IGNORE INTO {} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(config.HOUSE_TABLE)
            c.execute(house_sql,item[:15])
            community_sql = 'INSERT IGNORE INTO {} VALUES (%s,%s,%s,%s,%s)'.format(config.COMMUNITY_TABLE)
            c.execute(community_sql,item[14:])
            self.conn.commit()
        self.conn.close()
        print('所有连接已爬取完')

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