# coding: utf-8

import ConfigParser
import hashlib
import httplib
import time
import urlparse
import hmac
from hashlib import sha1
import logging

class AliMQProducer(object):
    def __init__(self, config):
        self.url = config.get('Aliyun MQ', 'url')
        self.host = urlparse.urlparse(self.url).netloc
        self.producer_id = config.get('Aliyun MQ', 'producer_id')
        self.topic = config.get('Aliyun MQ', 'topic')
        self.tag = config.get('Aliyun MQ', 'tag')
        self.ak = config.get('Aliyun MQ', 'access_key')
        self.sk = config.get('Aliyun MQ', 'secret_key')

    def send_msg(self, content):
        newline = '\n'
        conn = httplib.HTTPConnection(self.host)
        date = repr(int(time.time() * 1000))[0:13]
        try:
            sign_str = str(self.topic + newline + self.producer_id + newline + \
                           hashlib.md5(content).hexdigest() + newline + date)
            signature = self.cal_signature(sign_str)
            headers = {
                'Signature' : signature,
                'AccessKey' : self.ak,
                'ProducerID' : self.producer_id,
                'Content-Type' : 'text/html;charset=UTF-8'
            }
            post_url = self.url + '/message/?topic='+self.topic+'&time='+date+'&tag='+self.tag
            conn.request(method='POST',
                         url=post_url,
                         body=content,
                         headers=headers)
            response = conn.getresponse()
            if response.status != httplib.CREATED:
                msg = response.read()
                raise Exception('Bad response when send message to MQ: %s %s' % (response.status, msg))
        finally:
            conn.close()

    def cal_signature(self, sign_str):
        mac = hmac.new(self.sk, sign_str, sha1)
        return mac.digest().encode('base64').rstrip()
