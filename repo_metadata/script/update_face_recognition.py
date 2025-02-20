import argparse
import logging
import os
import sys

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_metadata.utils import get_face_recognition_enabled_repo_list, get_faces_rows
from seafevents.repo_metadata.constants import FACES_TABLE
from seafevents.app.config import get_config
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI

logger = logging.getLogger('face_recognition')


# 更新人脸识别信息

# 清除人脸识别信息（删除表格的行）
def clean_faces(repo_id, metadata_server_api):
    metadata = metadata_server_api.get_metadata(repo_id)
    tables = metadata.get('tables', [])
    faces_table = [table for table in tables if table['name'] == FACES_TABLE.name]
    if not faces_table:
        return
    faces_table_id = faces_table[0]['id']

    old_faces = get_faces_rows(repo_id, metadata_server_api)
    row_ids = [item[FACES_TABLE.columns.id.name] for item in old_faces]
    # 删除全部的行
    metadata_server_api.delete_rows(repo_id, faces_table_id, row_ids)


# 更新人脸识别信息
def update_face_info(face_recognition_manager, session):
    metadata_server_api = MetadataServerAPI('seafevents')

    start, count = 0, 1000
    while True:
        try:
            repos = get_face_recognition_enabled_repo_list(session, start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000

        if len(repos) == 0:
            break

        for repo in repos:
            repo_id = repo[0]
            logger.info('start refresh face info for repo: %s' % repo_id)
            clean_faces(repo_id, metadata_server_api)
            face_recognition_manager.init_face_recognition(repo_id)

    logger.info("Finish refresh face info")


# 初始化日志
def init_logging(args):
    seafile_log_to_stdout = os.getenv('SEAFILE_LOG_TO_STDOUT', 'false') == 'true'
    stream = args.logfile
    format = '[%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(funcName)s %(message)s'
    if seafile_log_to_stdout:
        stream = sys.stdout
        format = '[seafevents] [%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(funcName)s %(message)s'

    kw = {
        'format': format,
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': logging.INFO,
        'stream': stream
    }

    logging.basicConfig(**kw)
    logger.setLevel(logging.INFO)


# 主函数
# 它是 Python 脚本的入口点。
def main():
    # 它使用 argparse 库来解析命令行参数
    parser = argparse.ArgumentParser()

    # 特别是 --logfile 和 --loglevel。
    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='log file')

    parser.add_argument(
        '--loglevel',
        default='info',
        help='log level')

    args = parser.parse_args()
    init_logging(args)

    # 在解析参数后，它初始化日志记录，加载配置文件，创建一个 FaceRecognitionManager 实例，并启动一个数据库会话。
    seafevents_conf = os.environ.get('EVENTS_CONFIG_FILE')
    config = get_config(seafevents_conf)
    face_recognition_manager = FaceRecognitionManager(config)
    session = init_db_session_class(config)

    # 最后，它调用 update_face_info 函数来执行人脸识别更新。
    update_face_info(face_recognition_manager, session)


if __name__ == "__main__":
    main()
