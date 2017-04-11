# coding: utf-8

import hashlib
import httplib
import time
import hmac
from hashlib import sha1

from seafevents.app.config  import appconfig


class AliMQProducer(object):
    def send_msg(self, content):
        newline = '\n'
        conn = httplib.HTTPConnection(appconfig.ali.host)
        date = repr(int(time.time() * 1000))[0:13]
        try:
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
        mac = hmac.new(appconfig.ali.sk, sign_str, sha1)
        return mac.digest().encode('base64').rstrip()


ali_mq = AliMQProducer()
