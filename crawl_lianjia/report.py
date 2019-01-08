import smtplib
from email.mime.text import MIMEText
import threading
import time
import sqlite3
import config


class RepoterThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.mail_host = "smtp.163.com"  # SMTP服务器
        self.mail_user = config.EMAIL_NAME  # 用户名
        self.mail_pass = config.EMAIL_PW  # 授权密码，非登录密码
        self.sender = 'python_reporter@163.com'
        self.receivers = ['32564682@qq.com']

    def sendEmail(self, title, content):
        message = MIMEText(content, 'plain', 'utf-8')  # 内容, 格式, 编码
        message['From'] = "{}".format(self.sender)
        message['To'] = ",".join(self.receivers)
        message['Subject'] = title

        try:
            smtpObj = smtplib.SMTP_SSL(self.mail_host, 465)  # 启用SSL发信, 端口一般是465
            smtpObj.login(self.mail_user, self.mail_pass)  # 登录验证
            smtpObj.sendmail(self.sender, self.receivers, message.as_string())  # 发送
        except smtplib.SMTPException as e:
            print(e)

    def count(self, db, table):
        try:
            conn = sqlite3.connect(db)
            c = conn.cursor()
            c.execute('select count(*) from ' + table)
            f = c.fetchone()
            conn.close()
            return f[0]
        except:
            return 0

    def run(self):
        if not config.USE_REPORT:
            return
        while True:
            content = '程序运行正常\n'
            n = 0
            url_count = self.count('url_data.db','url')
            img_count = self.count('img_data.db','img')
            content += 'url数量:' + str(url_count) + '\n'
            content += 'img数量:' + str(img_count) + '\n'
            self.sendEmail('程序运行报告', content)
            time.sleep(1800)
