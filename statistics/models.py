from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, BigInteger, String, DateTime
from sqlalchemy.sql.schema import Index

from seafevents.db import Base


class TotalStorageStat(Base):
    __tablename__ = 'TotalStorageStat'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp = mapped_column(DateTime, nullable=False)
    total_size = mapped_column(BigInteger, nullable=False)
    org_id = mapped_column(Integer, nullable=False)

    __table_args__ = (Index('idx_storage_time_org', 'timestamp', 'org_id'), )

    def __init__(self, org_id, timestamp, total_size):
        super().__init__()
        self.timestamp = timestamp
        self.total_size = total_size
        self.org_id = org_id


class FileOpsStat(Base):
    __tablename__ = 'FileOpsStat'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp = mapped_column(DateTime, nullable=False)
    op_type = mapped_column(String(length=16), nullable=False)
    number = mapped_column(Integer, nullable=False)
    org_id = mapped_column(Integer, nullable=False)
    
    __table_args__ = (Index('idx_file_ops_time_org', 'timestamp', 'org_id'), )

    def __init__(self, org_id, timestamp, op_type, number):
        super().__init__()
        self.timestamp = timestamp
        self.op_type = op_type
        self.number = number
        self.org_id = org_id


class UserActivityStat(Base):
    __tablename__ = 'UserActivityStat'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    name_time_md5 = mapped_column(String(length=32), unique=True)
    username = mapped_column(String(length=255))
    timestamp = mapped_column(DateTime, nullable=False, index=True)
    org_id = mapped_column(Integer, nullable=False)

    __table_args__ = (Index('idx_activity_time_org', 'timestamp', 'org_id'), )

    def __init__(self, name_time_md5, org_id, username, timestamp):
        super().__init__()
        self.name_time_md5 = name_time_md5
        self.username = username
        self.timestamp = timestamp
        self.org_id = org_id


class UserTraffic(Base):
    __tablename__ = 'UserTraffic'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user = mapped_column(String(length=255), nullable=False)
    org_id = mapped_column(Integer, index=True)
    timestamp = mapped_column(DateTime, nullable=False)
    op_type = mapped_column(String(length=48), nullable=False)
    size = mapped_column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_traffic_time_user', 'timestamp', 'user', 'org_id'), )

    def __init__(self, user, timestamp, op_type, size, org_id):
        super().__init__()
        self.user = user
        self.timestamp = timestamp
        self.op_type = op_type
        self.size = size
        self.org_id = org_id


class SysTraffic(Base):
    __tablename__ = 'SysTraffic'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    org_id = mapped_column(Integer, index=True)
    timestamp = mapped_column(DateTime, nullable=False)
    op_type = mapped_column(String(length=48), nullable=False)
    size = mapped_column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_systraffic_time_org', 'timestamp', 'org_id'), )

    def __init__(self, timestamp, op_type, size, org_id):
        super().__init__()
        self.timestamp = timestamp
        self.op_type = op_type
        self.size = size
        self.org_id = org_id


class MonthlyUserTraffic(Base):
    __tablename__ = 'MonthlyUserTraffic'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user = mapped_column(String(length=255), nullable=False)
    org_id = mapped_column(Integer)
    timestamp = mapped_column(DateTime, nullable=False)
    web_file_upload = mapped_column(BigInteger, nullable=False)
    web_file_download = mapped_column(BigInteger, nullable=False)
    sync_file_upload = mapped_column(BigInteger, nullable=False)
    sync_file_download = mapped_column(BigInteger, nullable=False)
    link_file_upload = mapped_column(BigInteger, nullable=False)
    link_file_download = mapped_column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_monthlyusertraffic_time_org_user', 'timestamp', 'user', 'org_id'), )

    def __init__(self, user, org_id, timestamp, size_dict):
        super().__init__()
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

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    org_id = mapped_column(Integer)
    timestamp = mapped_column(DateTime, nullable=False)
    web_file_upload = mapped_column(BigInteger, nullable=False)
    web_file_download = mapped_column(BigInteger, nullable=False)
    sync_file_upload = mapped_column(BigInteger, nullable=False)
    sync_file_download = mapped_column(BigInteger, nullable=False)
    link_file_upload = mapped_column(BigInteger, nullable=False)
    link_file_download = mapped_column(BigInteger, nullable=False)

    __table_args__ = (Index('idx_monthlysystraffic_time_org', 'timestamp', 'org_id'), )

    def __init__(self, timestamp, org_id, size_dict):
        super().__init__()
        self.timestamp = timestamp
        self.org_id = org_id
        self.web_file_upload = size_dict['web_file_upload']
        self.web_file_download = size_dict['web_file_download']
        self.sync_file_upload = size_dict['sync_file_upload']
        self.sync_file_download = size_dict['sync_file_download']
        self.link_file_upload = size_dict['link_file_upload']
        self.link_file_download = size_dict['link_file_download']
