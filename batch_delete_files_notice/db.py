# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import select, delete

from .models import DeletedFilesCount


def save_deleted_files_count(session, repo_id, files_count, deleted_time):
    # 保存已删除文件的数量
    count = DeletedFilesCount(repo_id, files_count, deleted_time)
    session.add(count)
    session.commit()


def get_deleted_files_total_count(session, repo_id, deleted_time):
    # 获取已删除文件的总数
    # 1. session.scalars()：select语句执行后返回一个可迭代对象，每个元素是一个DeletedFilesCount对象。
    # 2. select(DeletedFilesCount)：select语句，选取DeletedFilesCount表
    # 3. where()：过滤条件，repo_id和deleted_time都要等于传入的参数
    # 4. .all()：将可迭代对象转换为list
    counts = session.scalars(select(DeletedFilesCount).where(
        DeletedFilesCount.repo_id == repo_id, DeletedFilesCount.deleted_time == deleted_time)).all()
    # 累加数量
    total_count = 0
    for count in counts:
        total_count += count.files_count

    return total_count


def clean_deleted_files_count(session, repo_id):
    # 清除今天已删除文件的数量
    today = datetime.date.today()

    # session.execute(delete(DeletedFilesCount)...)：执行删除操作，从DeletedFilesCount表中删除满足条件的记录。
    # where()：删除条件，repo_id和deleted_time需要等于传入的参数repo_id和今天的日期。
    # session.commit()：提交事务，将删除操作持久化到数据库。
    session.execute(delete(DeletedFilesCount).where(
        DeletedFilesCount.repo_id == repo_id, DeletedFilesCount.deleted_time == today))
    session.commit()
