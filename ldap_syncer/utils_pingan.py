# coding: utf8
import os
import json
import sys
import logging
import requests

logger = logging.getLogger(__name__)

seahub_dir = os.environ.get('SEAHUB_DIR', '')
sys.path.insert(0, seahub_dir)


try:
    import seahub.settings as settings
except ImportError as e:
    raise ('Can not import seahub.settings: %s.' % e)


PINGAN_626888_REPORT_EVENT_ENABLE = getattr(settings, 'PINGAN_626888_REPORT_EVENT_ENABLE', False)
PINGAN_626888_GET_ACCESS_TOKEN_URL = getattr(settings, 'PINGAN_626888_GET_ACCESS_TOKEN_URL', '')
PINGAN_626888_CLIENT_ID = getattr(settings, 'PINGAN_626888_CLIENT_ID', '')
PINGAN_626888_CLIENT_SECRET = getattr(settings, 'PINGAN_626888_CLIENT_SECRET', '')
PINGAN_626888_GRANT_TYPE = getattr(settings, 'PINGAN_626888_GRANT_TYPE', 'client_credentials')
PINGAN_626888_send_um = getattr(settings, 'PINGAN_626888_send_um', 'sunruili267')
PINGAN_626888_affectRange = getattr(settings, 'PINGAN_626888_affectRange', 'SLA_031_2_1')
PINGAN_626888_affectLevel = getattr(settings, 'PINGAN_626888_affectLevel', 'SLA_031_1_1')
PINGAN_626888_channelId = getattr(settings, 'PINGAN_626888_channelId', 2415)
PINGAN_626888_group_lst = getattr(settings, 'PINGAN_626888_group_lst', ['stg-GS_mobilefile_free'])

PINGAN_626888_EVENT_URL = getattr(settings, 'PINGAN_626888_EVENT_URL', '')

SPECIAL_ACCOUNT_TYPE = getattr(settings, 'SPECIAL_ACCOUNT_TYPE', ['GW','OPR','TEST','MON','SYS','PRA','ADM'])

def get_access_token():
    url = PINGAN_626888_GET_ACCESS_TOKEN_URL
    logger.info({
        "client_id": PINGAN_626888_CLIENT_ID,
        "grant_type": PINGAN_626888_GRANT_TYPE,
        "client_secret": PINGAN_626888_CLIENT_SECRET,
    })
    try:
        res = requests.post(url, headers={
            "content-type": "application/json",
        }, data=json.dumps({
            "client_id": PINGAN_626888_CLIENT_ID,
            "grant_type": PINGAN_626888_GRANT_TYPE,
            "client_secret": PINGAN_626888_CLIENT_SECRET,
        }))
        logger.info('{}, {}'.format(res.status_code, json.loads(res.text)))
        tmp = json.loads(res.text)
        access_token = tmp["data"]["access_token"]
        return access_token
    except Exception as e:
        logger.error('626888 move gs_free event get accesstoken error : {}'.format(e))
        return None


# add /  del um
def deal_event(payload, acctoken):
    url_event = PINGAN_626888_EVENT_URL + '?access_token=' + acctoken
    headers = {"Content-Type": "application/json"}
    resp = None
    try:
        response = requests.post(url_event, data=json.dumps(payload), headers=headers, timeout=10)
        status_code = response.status_code
        result = response.content
        str_result = result.decode('utf-8').strip()
        logger.info(' Response: {}'.format(str_result))
        if status_code != 200:
            logger.error('626888 move gs_free event, requests.post failure: {}'.format(status_code))
        paras = json.dumps(payload)
        str_result = str_result.replace('"{', '{').replace('}"', '}')
        res_dict = json.loads(str_result)
        if res_dict['ret'] == '0' and res_dict['data'] is not None:
            resp = res_dict['data']
            success = resp.get('success', 'false')
            value = resp.get('value', None)
            logger.info('626888 move gs_free event value: {}'.format(value))
            return success
        else:
            logger.warning(' 626888 move gs_free event fail : {}'.format(res_dict))
            return None
    except Exception as e:
        logger.error(' 626888 move gs_free event Exception: {}\r\n'.format(e))
        return None


# 主函数

def handle_mobfile(uids, inputor_name):
    # 获取认证
    acctoken = get_access_token()
    # 事件单请求（增加/删除用户）
    payload = {
        "authKey": "fc1789bd-8d69-4e31-a3fd-dc38c9ff0898",
        "um": PINGAN_626888_send_um,
        "affectRange": PINGAN_626888_affectRange,
        "affectLevel": PINGAN_626888_affectLevel,
        "attachFlag": "N",
        "channelId": PINGAN_626888_channelId,
        "flowFlag": "N",
        "channelName": "GS安全组成员修改",
        "eventName": "网盘无邮箱用户安全群组维护",
        "eventSource": "mobfile",
        "reportType": "0",
        "inputors": [
            {
                "inputorName": "ml_name",
                "selectorType": "class",
                "values": PINGAN_626888_group_lst
            }
        ]
    }
    
    for i in range(0, len(uids), 1000):
        add_del_par = {
            "inputorName": inputor_name,
            "selectorType": "id",
            "values": uids[i:i+1000]
        }
        
        if not PINGAN_626888_REPORT_EVENT_ENABLE:
            logger.warning("626888 move gs_free event is disabled")
            flag_event = 0
        else:
            flag_event = 1
        
        if flag_event:
            # [根据逻辑需要选择 1,2。 1和2可以同时存在，也就是用户列表不同add_um_list/del_um_list，删除权限/加入权限可以同时执行。]
            payload['inputors'].extend([add_del_par])  # A 用户加入权限组，作为入参。
            result = deal_event(payload, acctoken)
            if result:
                logger.info('626888 move gs_free event deal success')