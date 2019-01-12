import mysql_connector_pool
import traceback
import logging
import threading
import config
logging.basicConfig(filename='log.txt')


class Img:

    def __init__(self, url, id, name):
        self.url = url
        self.id = id
        self.name = name


class DataProcess():

    def __init__(self):
        self.pool = mysql_connector_pool.MysqlPool()
        self.url_lock = threading.Lock()
        self.img_lock = threading.Lock()
        self.img_table = config.HOUSE_TABLE + '_img'
        self.url_table = config.HOUSE_TABLE + '_url'

    def get_house_url(self):
        conn = self.pool.get()
        c = conn.cursor()
        if self.url_lock.acquire():
            try:
                c.execute('select * from {} limit 1 for update'.format(self.url_table))
                f = c.fetchone()
                if f is None:
                    conn.commit()
                    self.url_lock.release()
                    self.pool.free(conn)
                    return ''
                c.execute('delete from {} where id = '.format(self.url_table) + str(f[0]))
                conn.commit()
            except:
                logging.exception(traceback.print_exc())
            self.url_lock.release()
        self.pool.free(conn)
        return f[1]

    def get_img(self):
        conn = self.pool.get()
        c = conn.cursor()
        if self.img_lock.acquire():
            try:
                c.execute('select * from {} limit 10 for update '.format(self.img_table))
                f = c.fetchall()
                if len(f) == 0:
                    conn.commit()
                    self.img_lock.release()
                    self.pool.free(conn)
                    return list()
                for i in range(len(f)):
                    c.execute('delete from {} where id = '.format(self.img_table) + str(f[i][0]))
                conn.commit()
            except:
                logging.exception(traceback.print_exc())
            self.img_lock.release()
        self.pool.free(conn)
        return f

    def insert_img_url(self, img_list):
        conn = self.pool.get()
        c = conn.cursor()
        if self.img_lock.acquire():
            try:
                sql = 'Insert into {} (url, house_id, name) VALUES ("{}", "{}","{}") '.format(self.img_table, img_list[0].url,
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
            self.pool.free(conn)

    def create_table(self):
        conn = self.pool.get()
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY auto_increment , url VARCHAR(60))'.format(self.url_table))
        c.execute('CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY auto_increment , url TEXT, house_id VARCHAR(15), name VARCHAR(15))'.format(self.img_table))
        create_house_table_sql = """CREATE TABLE IF NOT EXISTS {}(
                                id BIGINT PRIMARY KEY,
                                title VARCHAR(50),
                                price VARCHAR(20),
                                favor_count VARCHAR(10),
                                unit_price VARCHAR(20),
                                location VARCHAR(50),
                                base_info TEXT,
                                sell_info TEXT,
                                intro TEXT,
                                layout TEXT,
                                count_7 VARCHAR(10),
                                count_30 VARCHAR(10),
                                jingweidu VARCHAR(50),
                                comment TEXT,
                                community_id VARCHAR(20) )
                              """.format(config.HOUSE_TABLE)
        create_community_table_sql = """CREATE TABLE IF NOT EXISTS {}(
                                id BIGINT PRIMARY KEY,
                                community_name VARCHAR(20),
                                favor_count VARCHAR(10),
                                unit_price VARCHAR(20),
                                info TEXT )
                              """.format(config.COMMUNITY_TABLE)
        c.execute(create_community_table_sql)
        c.execute(create_house_table_sql)
        conn.commit()
        self.pool.free(conn)

    def house_url_count(self):
        conn = self.pool.get()
        c = conn.cursor()
        c.execute('select count(*) from {}'.format(self.url_table))
        f = c.fetchone()
        self.pool.free(conn)
        return f[0]

    def img_url_count(self):
        conn = self.pool.get()
        c = conn.cursor()
        c.execute('select count(*) from {}'.format(self.img_table))
        f = c.fetchone()
        self.pool.free(conn)
        return f[0]
    
    def house_and_community_count(self):
        conn = self.pool.get()
        c = conn.cursor()
        c.execute('select count(*) from {}'.format(config.HOUSE_TABLE))
        house_count = c.fetchone()[0]
        c.execute('select count(*) from {}'.format(config.COMMUNITY_TABLE))
        community_count = c.fetchone()[0]
        self.pool.free(conn)
        return house_count,community_count

    def insert_house_url_set(self, url_set):
        url_list = list()
        conn = self.pool.get()
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
                    sql = 'Insert into {} (url) VALUES ("{}") '.format(self.url_table, url_list[0])
                    for i in range(1, len(url_list)):
                        sql += ',("{}")'.format(url_list[i])
                    c.execute(sql)
                    conn.commit()
                except:
                    logging.exception(traceback.print_exc())
                url_list.clear()
        self.pool.free(conn)

    def drop(self):
        conn = self.pool.get()
        c = conn.cursor()
        c.execute('drop table {}'.format(config.HOUSE_TABLE))
        c.execute('drop table {}'.format(config.COMMUNITY_TABLE))
        c.execute('drop table {}'.format(self.img_table))
        c.execute('drop table {}'.format(self.url_table))
        conn.commit()
        self.pool.free(conn)

data_process = DataProcess()
get_house_url = data_process.get_house_url
get_img_url = data_process.get_img
insert_img_url = data_process.insert_img_url
create_table = data_process.create_table
house_url_count = data_process.house_url_count
img_url_count = data_process.img_url_count
insert_house_url_set = data_process.insert_house_url_set
house_and_community_count = data_process.house_and_community_count
drop = data_process.drop


def release():
    data_process.pool.release()