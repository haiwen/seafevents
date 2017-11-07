from seafevents.db import Base
from sqlalchemy import Column, Integer, DateTime, BigInteger, String

class TotalStorageStat(Base):
    __tablename__ = 'TotalStorageStat'

    timestamp = Column(DateTime, primary_key=True)
    total_size = Column(BigInteger, nullable=False)

    def __init__(self, timestamp, total_size):
        self.timestamp = timestamp
        self.total_size = total_size

class FileOpsStat(Base):
    __tablename__ = 'FileOpsStat'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    op_type = Column(String(length=16), nullable=False)
    number = Column(Integer, nullable=False)
    
    def __init__(self, timestamp, op_type, number):
        self.timestamp = timestamp
        self.op_type = op_type
        self.number = number

