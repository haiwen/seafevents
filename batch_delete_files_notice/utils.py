# -*- coding: utf-8 -*-
import json
import logging

from sqlalchemy import text
from seafobj import fs_mgr

from .db import clean_deleted_files_count

logger = logging.getLogger(__name__)


def count_deleted_files(dir_obj, files_dict):
    # 递归计算删除的文件
    # dir_obj: SeafDir 对象
    # files_dict: {'files': []} 字典，用于存储删除的文件
    # 1. 遍历 dir_obj 的 dirents，dirents 是一个字典，key 是文件或目录的id，value 是该文件或目录的信息
    # 2. 如果遍历的对象是文件（type=1），则将该文件添加到 files_dict['files'] 中
    # 3. 如果遍历的对象是目录（type=0），则递归调用 count_deleted_files 函数，计算该目录下的所有文件
    for item in dir_obj.dirents.values():
        if item.type == 1:
            files_dict['files'].append(item)
        else:
            sub_dir = fs_mgr.load_seafdir(dir_obj.store_id, dir_obj.version, item.id)
            count_deleted_files(sub_dir, files_dict)

    return files_dict


def get_deleted_files_count(repo_id, version, deleted_files, deleted_dirs):
    # 获取已删除文件数量

    # 1. 遍历 deleted_files，deleted_dirs，计算所有文件的数量
    # 2. 遍历 deleted_dirs，递归调用 count_deleted_files 函数，计算每个目录下的所有文件
    files_count = len(deleted_files)
    for deleted in deleted_dirs:
        directory = fs_mgr.load_seafdir(repo_id, version, deleted.obj_id)
        files_dict = {'files': []}
        # 计算每个目录下的所有文件
        files_dict = count_deleted_files(directory, files_dict)
        files_count += len(files_dict['files'])

    return files_count


def save_deleted_files_msg(session, username, repo_id, timestamp):
    # 保存已删除文件的消息
    try:
        sql = """INSERT INTO notifications_usernotification (`to_user`, `msg_type`, `detail`, `timestamp`, `seen`)
                 VALUES (:to_user, :msg_type, :detail, :timestamp, :seen)"""
        # detail 是一个 Python 对象，需要将其转换为 JSON 字符串，以便存储到 MySQL 数据库中
        detail = json.dumps(detail)         
        detail = {"repo_id": repo_id}
        session.execute(text(sql), {
            'to_user': username,
            'msg_type': 'deleted_files',
            'detail': detail,
            'timestamp': timestamp,
            'seen': 0,
        })
        session.commit()
        # 写入信息后，清空今天的已删除文件数量
        clean_deleted_files_count(session, repo_id)
    except Exception as e:
        logger.error(e)
