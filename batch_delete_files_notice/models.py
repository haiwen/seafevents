# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, DateTime, Index, BigInteger

from seafevents.db import Base


class DeletedFilesCount(Base):
    __tablename__ = 'deleted_files_count'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    repo_id = Column(String(length=36), nullable=False)
    deleted_time = Column(DateTime, nullable=False)
    files_count = Column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_repo_id_deleted_time', 'repo_id', 'deleted_time'),)

    def __init__(self, repo_id, files_count, deleted_time):
        self.repo_id = repo_id
        self.deleted_time = deleted_time
        self.files_count = files_count
