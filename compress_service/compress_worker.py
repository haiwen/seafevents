# -*- coding: utf-8 -*-
import os
import sys
import json
import base64
import logging
from datetime import datetime
from Crypto.Cipher import AES
from threading import Thread, Event

from seaserv import seafile_api

from seafevents.app.config import appconfig
from seafevents.compress_service.task_manager import task_manager

logger = logging.getLogger(__name__)

seahub_dir = os.environ.get('SEAHUB_DIR', '')
if not seahub_dir:
    logging.critical('seahub_dir is not set')
    raise RuntimeError('seahub_dir is not set')
if not os.path.exists(seahub_dir):
    logging.critical('seahub_dir %s does not exist' % seahub_dir)
    raise RuntimeError('seahub_dir does not exist')

sys.path.insert(0, seahub_dir)
try:
    from seahub.settings import PWD_SECRET_KEY, PINGAN_SHARE_LINK_SEND_TO_VISITS_LIMIT_BASE
except ImportError as err:
    logging.warning('Can not import seahub.settings: %s.' % err)
    PWD_SECRET_KEY = None
    PINGAN_SHARE_LINK_SEND_TO_VISITS_LIMIT_BASE = 2


def add_share_links_to_task_queue():
    session = appconfig.session_cls()

    sql = """SELECT `token`, `repo_id`, `path`, `app_info` FROM `share_fileshare`
             WHERE `deleted`=0 AND `expire_date` > :datetime_now"""
    results = session.execute(sql, {'datetime_now': datetime.now()}).fetchall()
    for token, repo_id, path, app_info in results:
        if app_info:
            try:
                info_dict = json.loads(app_info)
            except Exception as e:
                logger.error('Failed to get share link app_info: %s' % e)
                continue

            if info_dict and info_dict['flag'] == 'simplerisk' and \
                    info_dict['extra'] and info_dict['extra'].get('isLock', '') == 'Y':
                try:
                    dirent = seafile_api.get_dirent_by_path(repo_id, path)
                except Exception as e:
                    logger.error('Failed to get dirent: %s. repo_id: %s path: %s' % (e, repo_id, path))
                    continue
                if not dirent:
                    logger.error('dirent is None. repo_id: %s path: %s' % (repo_id, path))
                    continue

                # decrypt pwd
                pwd = info_dict['extra'].get('encryptedPwd', '')
                base_key = base64.b64decode(PWD_SECRET_KEY.encode("utf-8"))
                cipher = AES.new(base_key, AES.MODE_ECB)
                base64_decrypted = base64.b64decode(pwd.encode(encoding='utf-8'))
                content = cipher.decrypt(base64_decrypted).decode('utf-8')
                un_pad = lambda s: s[0:-ord(s[-1])]
                decrypted_text = un_pad(content)
                decrypted_pwd = decrypted_text.rstrip('\0')

                last_modify = dirent.mtime
                task_manager.add_compress_task(token, repo_id, path, last_modify, decrypted_pwd)
                logger.info('add compress task success, token %s' % token)

    session.close()


def delete_useless_zip_files():
    session = appconfig.session_cls()

    sql1 = """SELECT `id`, `repo_id`, `path`, `app_info` FROM `share_fileshare`
              WHERE `deleted`=0 AND `expire_date` > :datetime_now"""
    results1 = session.execute(sql1, {'datetime_now': datetime.now()}).fetchall()
    need_del_link_id_set = set()
    cannot_del_files_map = dict()
    for link_id, repo_id, path, app_info in results1:
        if app_info:
            try:
                info_dict = json.loads(app_info)
            except Exception as e:
                logger.error('Failed to get share link app_info: %s' % e)
                continue
            if info_dict and info_dict['flag'] == 'simplerisk' and \
                    info_dict['extra'] and info_dict['extra'].get('isLock', '') == 'Y':
                need_del_link_id_set.add(link_id)
                cannot_del_files_map[link_id] = (repo_id + path)

    if need_del_link_id_set:
        sql2 = """SELECT `share_link_id`, COUNT(`share_link_id`) AS download_num FROM `share_filesharedownloads`
                  WHERE `share_link_id` IN :share_link_ids GROUP BY `share_link_id`"""
        results2 = session.execute(sql2, {'share_link_ids': list(need_del_link_id_set)}).fetchall()

        sql3 = """SELECT `share_link_id`, COUNT(`share_link_id`) AS sent_tos FROM `share_fileshareextrainfo`
                  WHERE `share_link_id` IN :share_link_ids GROUP BY `share_link_id`"""
        results3 = session.execute(sql3, {'share_link_ids': list(need_del_link_id_set)}).fetchall()

        download_num_map = dict()
        sent_to_map = dict()
        for link_id, download_num in results2:
            download_num_map[link_id] = download_num
        for link_id, sent_tos in results3:
            sent_to_map[link_id] = sent_tos

        for link_id in download_num_map:
            if int(download_num_map[link_id]) < int(sent_to_map[link_id]) * PINGAN_SHARE_LINK_SEND_TO_VISITS_LIMIT_BASE:
                need_del_link_id_set.discard(link_id)
            else:
                cannot_del_files_map.pop(link_id, None)

    if need_del_link_id_set:
        sql4 = """SELECT `repo_id`, `path` FROM `share_fileshare` WHERE `id` IN :share_link_ids
                  OR `deleted`=1 OR `expire_date` <= :datetime_now"""
        results4 = session.execute(
            sql4, {'share_link_ids': list(need_del_link_id_set), 'datetime_now': datetime.now()}).fetchall()
    else:
        sql4 = """SELECT `repo_id`, `path` FROM `share_fileshare` WHERE `deleted`=1
                  OR `expire_date` <= :datetime_now"""
        results4 = session.execute(sql4, {'datetime_now': datetime.now()}).fetchall()

    for repo_id, path in results4:
        if (repo_id + path) not in cannot_del_files_map.values():
            filename = os.path.basename(path)
            file_name, file_ext = os.path.splitext(filename)

            tmp_file_dir = os.path.join('/tmp/temp_file', repo_id, os.path.dirname(path).strip('/'))
            tmp_file = os.path.join(tmp_file_dir, filename)
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                    logger.info('Succeed to remove temp file %s' % tmp_file)
                except Exception as e:
                    logger.error('Failed to remove zip file %s, %s' % (tmp_file, e))
            tmp_zip_dir = os.path.join('/tmp/temp_zip', repo_id, os.path.dirname(path).strip('/'))
            tmp_zip = os.path.join(tmp_zip_dir, '%s_zip.zip' % file_name)
            if os.path.exists(tmp_zip):
                try:
                    os.remove(tmp_zip)
                    logger.info('Succeed to remove zip file %s' % tmp_zip)
                except Exception as e:
                    logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))

    session.close()


class CompressWorker(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        AddTaskWorker().start()
        CleanFilesWorker().start()


class AddTaskWorker(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()

    def run(self):
        while not self._finished.is_set():
            self._finished.wait(300)
            try:
                add_share_links_to_task_queue()
            except Exception as e:
                logger.error(e)


class CleanFilesWorker(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()

    def run(self):
        while not self._finished.is_set():
            self._finished.wait(86400)
            try:
                delete_useless_zip_files()
            except Exception as e:
                logger.error(e)
