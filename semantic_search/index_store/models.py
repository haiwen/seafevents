from sqlalchemy import Column, Integer, String, DateTime
from seafevents.db import Base


class IndexRepo(Base):
    __tablename__ = 'index_repo'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(String(length=36), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated = Column(DateTime, nullable=False)

    def __init__(self, repo_id, created_at, updated=None):
        self.repo_id = repo_id
        self.created_at = created_at
        self.updated = updated

    def to_dict(self):
        res = {
            'id': self.id,
            'dtable_uuid': self.repo_id,
            'created_at': self.created_at.isoformat(),
            'updated': self.updated.isoformat() if self.updated else None,
        }
        return res
