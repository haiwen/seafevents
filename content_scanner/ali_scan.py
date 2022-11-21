# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkcore.profile import region_provider
from aliyunsdkgreen.request.v20180509 import TextScanRequest
from aliyunsdkgreen.request.extension import HttpContentHelper
import json
import uuid
import datetime
import logging

MAX_SIZE=25000

class AliScanner(object):
    def __init__(self, key, key_id, region,):
        domain = 'green.' + region + '.aliyuncs.com'
        self.clt = client.AcsClient(key_id, key, region)
        region_provider.modify_point('Green', region, domain)
        self.request = TextScanRequest.TextScanRequest()
        self.request.set_accept_format('JSON')

    def scan(self, content):
        try:
            content.decode('utf8')
        except:
            logging.warning('Only \'utf8\' is supported.')
            return -1

        ret = {}
        remain = content
        while remain:
            if len(remain) > MAX_SIZE:
                scan_content = remain[:MAX_SIZE]
                offset = self.strip_to_utf8(scan_content)
                if offset >= 0:
                    scan_content = remain[:MAX_SIZE-offset]
                    remain = remain[MAX_SIZE-offset:]
            else:
                scan_content = remain
                remain = None

            task2 = {"dataId": str(uuid.uuid1()),
                     "content": scan_content.decode('utf-8'),
                     "time":datetime.datetime.now().microsecond
                    }
            self.request.set_content(HttpContentHelper.toValue({"tasks": [task2], "scenes": ["antispam"]}))
            response = self.clt.do_action_with_exception(self.request)
            response_json = json.loads(response)
            results = self.parse_response(response_json)
            for task_id in results:
                ret[task_id] = results[task_id]

        return ret

    def parse_response(self, response):
        if response['code'] != 200:
            logging.warning('Error when calling ali API, code: %d', response['code'])
            return -1

        # ret format: {"task_id1": {"label": label, "suggestion": suggestion},
        #              "task_id2": {"label": label, "suggestion": suggestion}}
        ret = {}
        taskResults = response["data"]
        for taskResult in taskResults:
            if (200 == taskResult["code"]):
                task_id = taskResult["taskId"]
                sceneResults = taskResult["results"]
                for sceneResult in sceneResults:
                    label = sceneResult["label"]
                    suggestion = sceneResult["suggestion"]
                    if suggestion == 'block':
                        ret[task_id] = {"label": label, "suggestion": suggestion}
                        break
                    elif suggestion == 'review':
                        ret[task_id] = {"label": label, "suggestion": suggestion}

        return ret

    def strip_to_utf8(self, text):
        offset=0
        while text:
            try:
                text.decode('utf8')
            except:
                text=text[:-1]
                offset += 1
                continue
            break
        if text:
            return offset
        else:
            return -1
