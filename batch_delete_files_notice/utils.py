# -*- coding: utf-8 -*-
import os
import sys
import json
import uuid
import logging
from urllib.parse import urlparse, urljoin, quote

from seafobj import fs_mgr

from seafevents.app.config import appconfig, load_config

logger = logging.getLogger(__name__)

seahub_dir = os.environ.get('SEAHUB_DIR', '')
if not seahub_dir:
    logger.critical('seahub_dir is not set')
    raise RuntimeError('seahub_dir is not set')
if not os.path.exists(seahub_dir):
    logger.critical('seahub_dir %s does not exist' % seahub_dir)
    raise RuntimeError('seahub_dir does not exist')

sys.path.append(seahub_dir)
try:
    import seahub.settings as seahub_settings
    SITE_ROOT = getattr(seahub_settings, 'SITE_ROOT', '/')
    SITE_NAME = getattr(seahub_settings, 'SITE_NAME', '')
    SERVICE_URL = getattr(seahub_settings, 'SERVICE_URL', '')
    ALIBABA_DINGDING_TALK_URL = getattr(seahub_settings, 'ALIBABA_DINGDING_TALK_URL',
                                        'dingtalk://dingtalkclient/page/link?url=%s&pc_slide=false')
    ALIBABA_MESSAGE_TOPIC_PUSH_MESSAGE = getattr(seahub_settings, 'ALIBABA_MESSAGE_TOPIC_PUSH_MESSAGE',
                                                 '01_push_message')
except Exception as err:
    logger.critical("Can not import seahub settings: %s." % err)
    raise RuntimeError("Can not import seahub settings: %s" % err)

if not appconfig.get('session_cls'):
    if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
        load_config(os.path.join(os.environ['SEAFILE_CENTRAL_CONF_DIR'], 'seafevents.conf'))


def count_deleted_files(dir_obj, files_dict):

    for item in dir_obj.dirents.values():
        if item.type == 1:
            files_dict['files'].append(item)
        else:
            sub_dir = fs_mgr.load_seafdir(dir_obj.store_id, dir_obj.version, item.id)
            count_deleted_files(sub_dir, files_dict)

    return files_dict


def get_deleted_files_count(repo_id, version, deleted_files, deleted_dirs):

    files_count = len(deleted_files)
    for deleted in deleted_dirs:
        directory = fs_mgr.load_seafdir(repo_id, version, deleted.obj_id)
        files_dict = {'files': []}
        files_dict = count_deleted_files(directory, files_dict)
        files_count += len(files_dict['files'])

    return files_count


def save_ding_talk_msg(repo_id, repo_name, username):
    session1 = appconfig.session_cls()
    try:
        sql1 = """SELECT * FROM alibaba_profile WHERE `uid`=:username"""
        res = session1.execute(sql1, {'username': username}).first()
        if not res:
            logger.error('%s not found in alibaba profile table.' % username)
            return
        to_work_no_list = [res['work_no']]
    except Exception as e:
        logger.error(e)
        return
    finally:
        session1.close()

    parse_result = urlparse(SERVICE_URL)
    base_url = "%s://%s" % (parse_result.scheme, parse_result.netloc)
    endpoint = '%srepo/%s/trash/' % (SITE_ROOT, repo_id)
    url = quote(urljoin(base_url, endpoint))

    content_cn = {
        "message_url": ALIBABA_DINGDING_TALK_URL % url,
        "head": {"bgcolor": "FFF17334", "text": SITE_NAME},
        "body": {
            "title": "检测到您名下文档库近期有大量文件被删除，将在删除后30天内被永久清理，点击查看详细信息",
            "form": [
                {
                    "key": "文件库名称:",
                    "value": "%s" % repo_name
                },
                {
                    "key": "系统消息:",
                    "value": SITE_NAME
                }
            ]
        }
    }

    content_en = {
        "message_url": ALIBABA_DINGDING_TALK_URL % url,
        "head": {"bgcolor": "FFF17334", "text": SITE_NAME},
        "body": {
            "title": "You recently deleted a large number of files from your library, "
                     "these files will be permanently removed from your trash 30 days "
                     "after they're deleted, go to the trash to get details.",
            "form": [
                {
                    "key": "Library Name:",
                    "value": "%s" % repo_name
                },
                {
                    "key": "Message From:",
                    "value": SITE_NAME
                }
            ]
        }
    }

    message_body = {
        "pushType": "dingding",
        "contentCN": content_cn,
        "contentEN": content_en,
        "pushWorkNos": to_work_no_list
    }

    session2 = appconfig.session_cls()
    try:
        message_body_json = json.dumps(message_body, ensure_ascii=False).encode('utf8')
        sql2 = """INSERT INTO `message_queue` (`topic`, `message_body`, `lock_version`, `is_consumed`, `message_key`)
                  VALUES (:topic, :message_body, :lock_version, :is_consumed, :message_key)"""
        session2.execute(sql2, {
            'topic': ALIBABA_MESSAGE_TOPIC_PUSH_MESSAGE,
            'message_body': message_body_json,
            'lock_version': 0,
            'is_consumed': 0,
            'message_key': uuid.uuid4(),
        })
        session2.commit()
    except Exception as e:
        logger.error(e)
        return
    finally:
        session2.close()
