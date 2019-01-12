import crawl
import data_process
from queue import Queue
import logging
import threading
import report
import datetime


def get_house_url():
    if data_process.house_url_count() == 0 and data_process.img_url_count() == 0:
        print('数据库无可爬连接，开始收集连接')
        href_list = crawl.get_small_region_list()
        print('已找到{}个小地点'.format(str(len(href_list))))
        lock_set = threading.Lock()
        href_queue = Queue()
        for href in href_list:
            href_queue.put(href)
        url_set = set()
        thread_list = list()
        thread_count = 15
        for i in range(thread_count):
            thread_list.append(crawl.CrawlHouseUrlThread(href_queue, url_set, lock_set))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        print('已找到{}个房子链接'.format(str(len(url_set))))
        data_process.insert_house_url_set(url_set)

def main():
    '''
    爬虫程序分为几个部分
    crawl.py: 爬虫部分，包括线程
    其中的crawl函数为解析网页函数，返回爬取信息的list
    report.py: 发邮件报告程序进度
    data_process.py: 封装数据库的一些操作
    sqlite3_connector_pool: sqlite3的连接池

    运行程序只需要运行  run.py
    '''
    data_process.drop()

    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    data_process.create_table()
    get_house_url()

    report_thread = report.RepoterThread()
    report_thread.setDaemon(True)
    report_thread.start()

    print('数据库已有连接，开始爬取')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    out_queue = Queue()
    out_thread = crawl.OutThread(out_queue)
    out_thread.start()

    thread_count = 15
    thread_list = list()
    for i in range(thread_count):
        thread_list.append(crawl.CrawlHouseThread(out_queue))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

    print('开始爬取图片')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)
    thread_count = 15
    thread_list = list()
    for i in range(thread_count):
        thread_list.append(crawl.DownloadImgThread())
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

    print('爬取结束')
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime)

    data_process.release()


if __name__ == '__main__':
    main()
