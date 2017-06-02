from seafevents.db import Base
from sqlalchemy import Column, Integer, DateTime, BigInteger, String

class UserActivityStat(Base):
    __tablename__ = 'UserActivityStat'
    
    timestamp = Column(DateTime, primary_key=True)
    number = Column(Integer, nullable=False)

    def __init__(self, timestamp, number):
        self.timestamp = timestamp
        self.number = number

    def __str__(self):
        return ''

    def as_dict(self):
        return None

class TotalStorageStat(Base):
    __tablename__ = 'TotalStorageStat'

    timestamp = Column(DateTime, primary_key=True)
    total_size = Column(BigInteger, nullable=False)

    def __init__(self, timestamp, total_size):
        self.timestamp = timestamp
        self.total_size = total_size

class FileAuditStat(Base):
    __tablename__ = 'FileAuditStat'

    a_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    a_type = Column(String(length=16), nullable=False)
    number = Column(Integer, nullable=False)
    
    def __init__(self, timestamp, a_type, number):
        self.timestamp = timestamp
        self.a_type = a_type
        self.number = number

