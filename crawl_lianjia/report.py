import smtplib
from email.mime.text import MIMEText
import threading
import time
import config
import data_process


class RepoterThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.mail_host = "smtp.163.com"  # SMTP服务器
        self.mail_user = config.EMAIL_NAME  # 用户名
        self.mail_pass = config.EMAIL_PW  # 授权密码，非登录密码
        self.sender = config.EMAIL_NAME
        self.receivers = []
        self.receivers.append(config.EMAIL_RECEIVER)

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

    def run(self):
        if not config.USE_REPORT:
            return
        while True:
            content = '程序运行正常\n'
            url_count = data_process.house_url_count()
            img_count = data_process.img_url_count()
            house_count,community_count = data_process.house_and_community_count()
            content += 'url数量:' + str(url_count) + '\n'
            content += 'img数量:' + str(img_count) + '\n'
            content += 'house数量' + str(house_count) + '\n'
            content += 'community数量' + str(community_count) + '\n'
            self.sendEmail('程序运行报告', content)
            time.sleep(1800)
