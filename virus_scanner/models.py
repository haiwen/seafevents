from sqlalchemy import Column, Integer, String, Text, Boolean
from sqlalchemy import Index, Sequence

from seafevents.db import Base

class VirusScanRecord(Base):
    __tablename__ = 'VirusScanRecord'

    repo_id = Column(String(length=36), nullable=False, primary_key=True)
    scan_commit_id = Column(String(length=40), nullable=False)
    __table_args__ = {'extend_existing':True}

    def __init__(self, repo_id, scan_commit_id):
        self.repo_id = repo_id
        self.scan_commit_id = scan_commit_id

class VirusFile(Base):
    __tablename__ = 'VirusFile'

    vid = Column(Integer, Sequence('virus_file_seq'), primary_key=True)
    repo_id = Column(String(length=36), nullable=False, index=True)
    commit_id = Column(String(length=40), nullable=False)
    file_path = Column(Text, nullable=False)
    has_handle = Column(Boolean, nullable=False)
    __table_args__ = {'extend_existing':True}

    def __init__(self, repo_id, commit_id, file_path, has_handle):
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.file_path = file_path
        self.has_handle = has_handle
