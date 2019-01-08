from sqlite3_connector_pool import *
import traceback
import logging
logging.basicConfig(filename='log.txt')


class Img:

    def __init__(self, url, id, name):
        self.url = url
        self.id = id
        self.name = name


class DataProcess():

    def __init__(self):
        self.url_pool = Sqlite3Pool('url_data.db')
        self.img_pool = Sqlite3Pool('img_data.db')
        self.url_lock = threading.Lock()
        self.img_lock = threading.Lock()

    def get_house_url(self):
        conn = self.url_pool.get()
        c = conn.cursor()
        if self.url_lock.acquire():
            try:
                c.execute('select * from url limit 1')
                f = c.fetchone()
                if f is None:
                    self.url_lock.release()
                    self.url_pool.free(conn)
                    return ''
                c.execute('delete from url where id = ' + str(f[0]))
                conn.commit()
            except:
                logging.exception(traceback.print_exc())
            self.url_lock.release()
        self.url_pool.free(conn)
        return f[1]

    def get_img(self):
        conn = self.img_pool.get()
        c = conn.cursor()
        if self.img_lock.acquire():
            try:
                c.execute('select * from img limit 10')
                f = c.fetchall()
                if len(f) == 0:
                    self.img_lock.release()
                    self.img_pool.free(conn)
                    return list()
                for i in range(len(f)):
                    c.execute('delete from img where id = ' + str(f[i][0]))
                conn.commit()
            except:
                logging.exception(traceback.print_exc())
            self.img_lock.release()
        self.img_pool.free(conn)
        return f

    def insert_img_url(self, img_list):
        conn = self.img_pool.get()
        c = conn.cursor()
        if self.img_lock.acquire():
            try:
                sql = 'Insert into img (url, house_id, name) VALUES ("{}", "{}","{}") '.format(img_list[0].url,
                                                                                               str(img_list[0].id),
                                                                                               img_list[
                                                                                                   0].name + '.jpg')
                for i in range(1, len(img_list)):
                    sql += ',("{}", "{}","{}") '.format(img_list[i].url, str(img_list[i].id),
                                                        img_list[i].name + '.jpg')
                c.execute(sql)
                conn.commit()
            except:
                logging.exception(traceback.print_exc())
            self.img_lock.release()
            self.img_pool.free(conn)

    def create_table(self):
        conn = self.url_pool.get()
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS url (id INTEGER PRIMARY KEY autoincrement , url VARCHAR(60))')
        conn.commit()
        self.url_pool.free(conn)

        conn = self.img_pool.get()
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS img (id INTEGER PRIMARY KEY autoincrement , url VARCHAR(110), house_id VARCHAR(12), name VARCHAR(15))')
        conn.commit()
        self.img_pool.free(conn)

    def house_url_count(self):
        conn = self.url_pool.get()
        c = conn.cursor()
        c.execute('select count(*) from url')
        f = c.fetchone()
        self.url_pool.free(conn)
        return f[0]

    def img_url_count(self):
        conn = self.img_pool.get()
        c = conn.cursor()
        c.execute('select count(*) from img')
        f = c.fetchone()
        self.img_pool.free(conn)
        return f[0]

    def insert_house_url_set(self, url_set):
        url_list = list()
        conn = self.url_pool.get()
        c = conn.cursor()
        count = 0
        url_set_len = len(url_set)
        for url in url_set:
            url_list.append(url)
            count += 1
            if len(url_list) == 1000 or count == url_set_len:
                if len(url_list) == 0:
                    continue
                try:
                    sql = 'Insert into url (url) VALUES ("{}") '.format(url_list[0])
                    for i in range(1, len(url_list)):
                        sql += ',("{}")'.format(url_list[i])
                    c.execute(sql)
                    conn.commit()
                except:
                    logging.exception(traceback.print_exc())
                url_list.clear()
        self.url_pool.free(conn)



data_process = DataProcess()
get_house_url = data_process.get_house_url
get_img_url = data_process.get_img
insert_img_url = data_process.insert_img_url
create_table = data_process.create_table
house_url_count = data_process.house_url_count
img_url_count = data_process.img_url_count
insert_house_url_set = data_process.insert_house_url_set


def release():
    data_process.img_pool.release()
    data_process.url_pool.release()