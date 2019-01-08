import json
import logging
logging.basicConfig(filename='log.txt')


with open('config.json')as f:
    j = json.loads(f.read())
    URL = j['url']
    BASE_URL = URL[:len(URL) - 12]
    HOUSE_TABLE = j['house_table']
    COMMUNITY_TABLE = j['community_table']
    MYSQL_NAME = j['mysql_name']
    MYSQL_PASSWORD = j['mysql_password']
    DATABASE_NAME = j['database_name']