# coding: utf-8

from sqlalchemy import Column, Integer, String, DateTime, Text, Index, BigInteger

from seafevents.db import Base

class ContentScanRecord(Base):
    __tablename__ = 'ContentScanRecord'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(String(length=36), nullable=False, index=True)
    commit_id = Column(String(length=40), nullable=False)
    timestamp = Column(DateTime(), nullable=False)

    def __init__(self, repo_id, commit_id, timestamp):
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.timestamp = timestamp

class ContentScanResult(Base):
    __tablename__ = 'ContentScanResult'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(String(length=36), nullable=False, index=True)
    path = Column(Text, nullable=False)
    platform = Column(String(length=32), nullable=False)
    # detail format: {"task_id1": {"label": "abuse", "suggestion": "block"},
    #                 "task_id2": {"label": "customized", "suggestion": "block"}}
    detail = Column(Text, nullable=False)

    def __init__(self, repo_id, path, platform, detail):
        self.repo_id = repo_id
        self.path = path
        self.platform = platform
        self.detail = detail
