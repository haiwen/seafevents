from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, String, Text, Boolean

from seafevents.db import Base


class VirusScanRecord(Base):
    __tablename__ = 'VirusScanRecord'

    repo_id = mapped_column(String(length=36), nullable=False, primary_key=True)
    scan_commit_id = mapped_column(String(length=40), nullable=False)
    __table_args__ = {'extend_existing': True}

    def __init__(self, repo_id, scan_commit_id):
        super().__init__()
        self.repo_id = repo_id
        self.scan_commit_id = scan_commit_id


class VirusFile(Base):
    __tablename__ = 'VirusFile'

    vid = mapped_column(Integer, primary_key=True, autoincrement=True)
    repo_id = mapped_column(String(length=36), nullable=False, index=True)
    commit_id = mapped_column(String(length=40), nullable=False)
    file_path = mapped_column(Text, nullable=False)
    has_deleted = mapped_column(Boolean, nullable=False, index=True)
    has_ignored = mapped_column(Boolean, nullable=False, index=True)
    __table_args__ = {'extend_existing': True}

    def __init__(self, repo_id, commit_id, file_path, has_deleted, has_ignored):
        super().__init__()
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.file_path = file_path
        self.has_deleted = has_deleted
        self.has_ignored = has_ignored
