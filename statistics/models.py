from seafevents.db import Base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Index


class TotalStorageStat(Base):
    __tablename__ = 'TotalStorageStat'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    total_size = Column(BigInteger, nullable=False)
    org_id = Column(Integer, nullable=False)

    __table_args__ = (Index('idx_storage_time_org', 'timestamp', 'org_id'), )

    def __init__(self, org_id, timestamp, total_size):
        self.timestamp = timestamp
        self.total_size = total_size
        self.org_id = org_id

class FileOpsStat(Base):
    __tablename__ = 'FileOpsStat'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    op_type = Column(String(length=16), nullable=False)
    number = Column(Integer, nullable=False)
    org_id = Column(Integer, nullable=False)
    
    __table_args__ = (Index('idx_file_ops_time_org', 'timestamp', 'org_id'), )

    def __init__(self, org_id, timestamp, op_type, number):
        self.timestamp = timestamp
        self.op_type = op_type
        self.number = number
        self.org_id = org_id

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    name_time_md5 = Column(String(length=32), unique=True)
    username = Column(String(length=255))
    timestamp = Column(DateTime, nullable=False, index=True)
    org_id = Column(Integer, nullable=False)

    __table_args__ = (Index('idx_activity_time_org', 'timestamp', 'org_id'), )

    def __init__(self, name_time_md5, org_id, username, timestamp):
        self.name_time_md5 = name_time_md5
        self.username = username
        self.timestamp = timestamp
        self.org_id = org_id

class UserTraffic(Base):
    __tablename__ = 'UserTraffic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String(length=255), nullable=False)
    org_id = Column(Integer, index=True)
    timestamp = Column(DateTime, nullable=False)
    op_type = Column(String(length=48), nullable=False)
    size = Column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_traffic_time_user', 'timestamp', 'user', 'org_id'), )

    def __init__(self, user, timestamp, op_type, size, org_id):
        self.user = user
        self.timestamp = timestamp
        self.op_type = op_type
        self.size = size
        self.org_id = org_id

class SysTraffic(Base):
    __tablename__ = 'SysTraffic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer, index=True)
    timestamp = Column(DateTime, nullable=False)
    op_type = Column(String(length=48), nullable=False)
    size = Column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_systraffic_time_org', 'timestamp', 'org_id'), )

    def __init__(self, timestamp, op_type, size, org_id):
        self.timestamp = timestamp
        self.op_type = op_type
        self.size = size
        self.org_id = org_id

class MonthlyUserTraffic(Base):
    __tablename__ = 'MonthlyUserTraffic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String(length=255), nullable=False)
    org_id = Column(Integer)
    timestamp = Column(DateTime, nullable=False)
    web_file_upload = Column(BigInteger, nullable=False)
    web_file_download = Column(BigInteger, nullable=False)
    sync_file_upload = Column(BigInteger, nullable=False)
    sync_file_download = Column(BigInteger, nullable=False)
    link_file_upload = Column(BigInteger, nullable=False)
    link_file_download = Column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_monthlyusertraffic_time_org_user', 'timestamp', 'user', 'org_id'), )

    def __init__(self, user, org_id, timestamp, size_dict):
        self.user = user
        self.org_id = org_id
        self.timestamp = timestamp
        self.web_file_upload = size_dict['web_file_upload']
        self.web_file_download = size_dict['web_file_download']
        self.sync_file_upload = size_dict['sync_file_upload']
        self.sync_file_download = size_dict['sync_file_download']
        self.link_file_upload = size_dict['link_file_upload']
        self.link_file_download = size_dict['link_file_download']

class MonthlySysTraffic(Base):
    __tablename__ = 'MonthlySysTraffic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    org_id = Column(Integer)
    timestamp = Column(DateTime, nullable=False)
    web_file_upload = Column(BigInteger, nullable=False)
    web_file_download = Column(BigInteger, nullable=False)
    sync_file_upload = Column(BigInteger, nullable=False)
    sync_file_download = Column(BigInteger, nullable=False)
    link_file_upload = Column(BigInteger, nullable=False)
    link_file_download = Column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_monthlysystraffic_time_org', 'timestamp', 'org_id'), )

    def __init__(self, timestamp, org_id, size_dict):
        self.timestamp = timestamp
        self.org_id = org_id
        self.web_file_upload = size_dict['web_file_upload']
        self.web_file_download = size_dict['web_file_download']
        self.sync_file_upload = size_dict['sync_file_upload']
        self.sync_file_download = size_dict['sync_file_download']
        self.link_file_upload = size_dict['link_file_upload']
        self.link_file_download = size_dict['link_file_download']
