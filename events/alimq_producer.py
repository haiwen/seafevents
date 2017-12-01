# coding: utf-8

import hashlib
import httplib
import time
import hmac
from hashlib import sha1

from urllib3 import HTTPConnectionPool
from seafevents.app.config  import appconfig


class AliMQProducer(object):
    def __init__(self):
        self.pool = HTTPConnectionPool(appconfig.ali.host, maxsize=5)

    def send_msg(self, content):
        newline = '\n'
        date = repr(int(time.time() * 1000))[0:13]
        sign_str = str(appconfig.ali.topic + newline + appconfig.ali.producer_id + \
                       newline + hashlib.md5(content).hexdigest() + newline + date)
        signature = self.cal_signature(sign_str)
        headers = {
            'Signature' : signature,
            'AccessKey' : appconfig.ali.ak,
            'ProducerID' : appconfig.ali.producer_id,
            'Content-Type' : 'text/html;charset=UTF-8'
        }
        post_url = appconfig.ali.url + '/message/?topic='+appconfig.ali.topic+'&time='+date+'&tag='+appconfig.ali.tag
        res = self.pool.request('POST',
                                post_url,
                                body=content,
                                headers=headers,
                                retries=2)
        if res.status != httplib.CREATED:
            msg = res.data
            raise Exception('Bad response when send message to MQ: %s %s' % (res.status, msg))

    def cal_signature(self, sign_str):
        mac = hmac.new(appconfig.ali.sk, sign_str, sha1)
        return mac.digest().encode('base64').rstrip()


ali_mq = AliMQProducer()
