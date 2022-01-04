# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import Queue
import threading
import logging
import requests

from django.utils.http import urlquote

from seaserv import seafile_api

from seafevents.app.config import appconfig
from seafevents.compress_service.models import CompressRecords


logger = logging.getLogger(__name__)


def get_compress_file_last_modified(token):
    session = appconfig.session_cls()
    last_modified = None
    try:
        result = session.query(CompressRecords).filter(CompressRecords.token == token).first()
        last_modified = result.last_modified if result else None
    except Exception as e:
        logger.error(e)
    finally:
        session.close()
    return last_modified


def generate_tmp_paths(repo_id, file_path):
    filename = os.path.basename(file_path)
    file_name, file_ext = os.path.splitext(filename)

    tmp_file_dir = os.path.join('/tmp/temp_file', repo_id, os.path.dirname(file_path).strip('/'))
    tmp_zip_dir = os.path.join('/tmp/temp_zip', repo_id, os.path.dirname(file_path).strip('/'))

    if not os.path.exists(tmp_file_dir):
        os.makedirs(tmp_file_dir)
    if not os.path.exists(tmp_zip_dir):
        os.makedirs(tmp_zip_dir)

    tmp_file = os.path.join(tmp_file_dir, filename)
    tmp_zip = os.path.join(tmp_zip_dir, '%s_zip.zip' % file_name)

    return tmp_file, tmp_zip


class CompressTask(object):

    def __init__(self, token, repo_id, file_path, last_modify, decrypted_pwd):
        self.token = token
        self.repo_id = repo_id
        self.file_path = file_path
        self.last_modify = last_modify
        self.decrypted_pwd = decrypted_pwd


class TaskManager(object):

    def __init__(self):
        self.task_queue = Queue.Queue()
        self.task_map = set()
        self.workers = 4
        self.file_server_port = 8082

    def init(self, workers, file_server_port):
        self.workers = workers
        self.file_server_port = file_server_port

    def add_compress_task(self, token, repo_id, file_path, last_modify, decrypted_pwd):
        if (repo_id + file_path) not in self.task_map:
            tmp_file, tmp_zip = generate_tmp_paths(repo_id, file_path)
            if os.path.exists(tmp_zip):
                try:
                    dirent = seafile_api.get_dirent_by_path(repo_id, file_path)
                except Exception as e:
                    logger.error('Failed to get dirent: %s. repo_id: %s file_path: %s' % (e, repo_id, file_path))
                    return False, 'Failed to get dirent: %s. repo_id: %s file_path: %s' % (e, repo_id, file_path)
                if not dirent:
                    logger.error('dirent is None. repo_id: %s file_path: %s' % (repo_id, file_path))
                    return False, 'dirent is None. repo_id: %s file_path: %s' % (repo_id, file_path)
                last_modified = get_compress_file_last_modified(token)
                if (last_modified and (str(last_modified) != str(dirent.mtime)))\
                        or str(last_modify) != str(dirent.mtime):
                    logger.info('This file was modified after it was shared.')
                    try:
                        os.remove(tmp_zip)
                    except Exception as e:
                        logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                    return False, 'This file was modified after it was shared: %s file_path: %s' % (repo_id, file_path)
                result = None
                session = appconfig.session_cls()
                try:
                    result = session.query(CompressRecords).filter(CompressRecords.token == token).first()
                except Exception as e:
                    logger.error(e)
                finally:
                    session.close()
                if not result:
                    try:
                        os.remove(tmp_zip)
                    except Exception as e:
                        logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                    compress_task = CompressTask(token, repo_id, file_path, last_modify, decrypted_pwd)
                    self.task_queue.put(compress_task)
            else:
                compress_task = CompressTask(token, repo_id, file_path, last_modify, decrypted_pwd)
                self.task_queue.put(compress_task)

        return True, None

    def query_compress_status(self, token, repo_id, file_path, last_modify):
        if (repo_id + file_path) in self.task_map:
            return 2
        else:
            tmp_file, tmp_zip = generate_tmp_paths(repo_id, file_path)
            if os.path.exists(tmp_zip):
                try:
                    dirent = seafile_api.get_dirent_by_path(repo_id, file_path)
                except Exception as e:
                    logger.error('Failed to get dirent: %s. repo_id: %s file_path: %s' % (e, repo_id, file_path))
                    return 0
                if not dirent:
                    logger.error('dirent is None. repo_id: %s file_path: %s' % (repo_id, file_path))
                    return 0
                last_modified = get_compress_file_last_modified(token)
                if (last_modified and (str(last_modified) != str(dirent.mtime)))\
                        or str(last_modify) != str(dirent.mtime):
                    logger.info('This file was modified after it was shared.')
                    try:
                        os.remove(tmp_zip)
                    except Exception as e:
                        logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                    return 4
                else:
                    result = None
                    session = appconfig.session_cls()
                    try:
                        result = session.query(CompressRecords).filter(CompressRecords.token == token).first()
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
                    if not result:
                        try:
                            os.remove(tmp_zip)
                        except Exception as e:
                            logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                        return 1
                    else:
                        return 3
            else:
                return 1

    def handle_task(self):
        while 1:
            try:
                compress_task = self.task_queue.get(timeout=1)
            except Queue.Empty:
                continue
            except Exception as e:
                logger.error(e)
                continue

            token = compress_task.token
            repo_id = compress_task.repo_id
            file_path = compress_task.file_path
            last_modify = compress_task.last_modify
            decrypted_pwd = compress_task.decrypted_pwd

            if (repo_id + file_path) in self.task_map:
                logger.info('compress task is doing by other worker')
                continue

            tmp_file, tmp_zip = generate_tmp_paths(repo_id, file_path)
            if os.path.exists(tmp_zip):
                try:
                    dirent = seafile_api.get_dirent_by_path(repo_id, file_path)
                except Exception as e:
                    logger.error('Failed to get dirent: %s. repo_id: %s file_path: %s' % (e, repo_id, file_path))
                    continue
                if not dirent:
                    logger.error('dirent is None. repo_id: %s file_path: %s' % (repo_id, file_path))
                    continue
                if str(last_modify) != str(dirent.mtime):
                    logger.info('This file was modified after it was shared.')
                    try:
                        os.remove(tmp_zip)
                    except Exception as e:
                        logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                    continue

                result = None
                session = appconfig.session_cls()
                try:
                    result = session.query(CompressRecords).filter(CompressRecords.token == token).first()
                except Exception as e:
                    logger.error(e)
                finally:
                    session.close()
                if not result:
                    try:
                        os.remove(tmp_zip)
                    except Exception as e:
                        logger.error('Failed to remove zip file %s, %s' % (tmp_zip, e))
                else:
                    continue

            self.task_map.add(repo_id + file_path)

            filename = os.path.basename(file_path)
            try:
                obj_id = seafile_api.get_file_id_by_path(repo_id, file_path)
                dl_token = seafile_api.get_fileserver_access_token(repo_id, obj_id, 'view', '', use_onetime=False)
            except Exception as e:
                logger.error(e)
                self.task_map.discard(repo_id + file_path)
                continue

            try:
                inner_url = '%s/files/%s/%s' % (
                    'http://127.0.0.1:%s' % self.file_server_port, dl_token, urlquote(filename))
            except Exception as e:
                logger.error(e)
                self.task_map.discard(repo_id + file_path)
                self.task_queue.put(compress_task)
                continue

            try:
                logger.info('Starting get file %s content' % file_path)
                resp = requests.get(inner_url)
                logger.info('Succeed get file content')
            except Exception as e:
                logger.error(e)
                self.task_map.discard(repo_id + file_path)
                self.task_queue.put(compress_task)
                continue

            try:
                logger.info('Starting write file content')
                with open(tmp_file, 'wb') as f:
                    f.write(resp.content)
                logger.info('Starting compress file')
                status = os.system("zip -P '%s' -j '%s' '%s'" % (decrypted_pwd, tmp_zip, tmp_file))
                logger.info('Compress file %s status: %s' % (file_path, status))
            except Exception as e:
                logger.error(e)
                self.task_map.discard(repo_id + file_path)
                self.task_queue.put(compress_task)
                continue

            self.task_map.discard(repo_id + file_path)
            session = appconfig.session_cls()
            try:
                sql = """REPLACE INTO `compress_records` (`token`, `last_modified`) VALUES(:token, :last_modified)"""
                session.execute(sql, {'token': token, 'last_modified': last_modify})
                session.commit()
            except Exception as e:
                logger.error('Failed to record compress task: %s' % e)
            finally:
                session.close()

    def run(self):
        for i in range(self.workers):
            thread_name = 'compress-worker' + str(i)
            logger.info('Starting %s for compress file.' % thread_name)
            t = threading.Thread(target=self.handle_task, name=thread_name)
            t.start()


task_manager = TaskManager()
