import os

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

__all__ = [
    "init_db_session",
    "RepoUpdateEvent",
]

Base = declarative_base()

class RepoUpdateEvent(Base):
    __tablename__ = 'RepoUpdateEvent'

    id = Column(Integer, primary_key=True)
    repo_id = Column(String)
    commit_id = Column(String)
    timestamp = Column(DateTime)

    def __init__(self, repo_id, commit_id, timestamp):
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.timestamp = timestamp

def init_db_session():
    fn = os.path.join(os.path.dirname(__file__), "seafevents.db")
    engine = create_engine('sqlite:///' + fn, echo=True)
    Base.metadata.create_all(engine) 

    Session = sessionmaker(bind=engine)
    session = Session()
    return session
