from seafevents.db import Base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime


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

class UserTrafficStat(Base):
    __tablename__ = 'UserTrafficStat'

    email = Column(String(length=255), primary_key=True)
    month = Column(String(length=6), primary_key=True, index=True)

    block_download = Column(BigInteger, nullable=False)
    file_view = Column(BigInteger, nullable=False)
    file_download = Column(BigInteger, nullable=False)
    dir_download = Column(BigInteger, nullable=False)

    def __init__(self, email, month, block_download=0, file_view=0, file_download=0, dir_download=0):
        self.email = email
        self.month = month
        self.block_download = block_download
        self.file_view = file_view
        self.file_download = file_download
        self.dir_download = dir_download

    def __str__(self):
        return '''UserTraffic<email: %s, month: %s, block: %s, file view: %s, \
file download: %s, dir download: %s>''' % (self.email, self.month, self.block_download,
                         self.file_view, self.file_download, self.dir_download)

    def as_dict(self):
        return dict(email=self.email,
                    month=self.month,
                    block_download=self.block_download,
                    file_view=self.file_view,
                    file_download=self.file_download,
                    dir_download=self.dir_download)

class UserActivityStat(Base):
    __tablename__ = 'UserActivityStat'

    name_time_md5 = Column(String(length=32), primary_key=True)
    username = Column(String(length=255))
    timestamp = Column(DateTime, nullable=False, index=True)

    def __init__(self, username, timestamp):
        self.username = username
        self.timestamp = timestamp

class UserTraffic(Base):
    __tablename__ = 'UserTraffic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String(length=255), nullable=False, index=True)
    org_id = Column(Integer, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    op_type = Column(String(length=48), nullable=False, index=True)
    size = Column(BigInteger, nullable=False)

    def __init__(self, user, timestamp, op_type, size, org_id=0):
        self.user = user
        self.timestamp = timestamp
        self.op_type = op_type
        self.size = size
        self.org_id = org_id
