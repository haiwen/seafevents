# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, DateTime, BigInteger

from seafevents.db import Base

# sqlalchemy
# Column: 表示数据库表中的一个列（字段）。
# String(length=36): 表示一个字符串类型的列，长度为36个字符。
# DateTime: 表示一个日期和时间类型的列。
# BigInteger: 表示一个大整数类型的列，通常用于存储非常大的整数值。

# 已删除文件的数量统计
class DeletedFilesCount(Base):
    __tablename__ = 'deleted_files_count'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    repo_id = Column(String(length=36), nullable=False, index=True)
    deleted_time = Column(DateTime, nullable=False, index=True)
    files_count = Column(BigInteger, nullable=False)

    def __init__(self, repo_id, files_count, deleted_time):
        self.repo_id = repo_id
        self.deleted_time = deleted_time
        self.files_count = files_count
