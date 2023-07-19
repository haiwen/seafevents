# coding: utf-8
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Text

from seafevents.db import Base


class ContentScanRecord(Base):
    __tablename__ = 'ContentScanRecord'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id = mapped_column(String(length=36), nullable=False, index=True)
    commit_id = mapped_column(String(length=40), nullable=False)
    timestamp = mapped_column(DateTime(), nullable=False)

    def __init__(self, repo_id, commit_id, timestamp):
        super().__init__()
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.timestamp = timestamp


class ContentScanResult(Base):
    __tablename__ = 'ContentScanResult'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id = mapped_column(String(length=36), nullable=False, index=True)
    path = mapped_column(Text, nullable=False)
    platform = mapped_column(String(length=32), nullable=False)
    # detail format: {"task_id1": {"label": "abuse", "suggestion": "block"},
    #                 "task_id2": {"label": "customized", "suggestion": "block"}}
    detail = mapped_column(Text, nullable=False)

    def __init__(self, repo_id, path, platform, detail):
        super().__init__()
        self.repo_id = repo_id
        self.path = path
        self.platform = platform
        self.detail = detail
