from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, BigInteger, String, DateTime
from sqlalchemy.sql.schema import Index

from seafevents.db import Base

# statistics/models.py 定义了各种用于存储统计数据的数据库模型。这些模型包括：

# * SysTraffic：代表系统流量数据
# * MonthlyUserTraffic：代表月度用户流量数据
# * MonthlySysTraffic：代表月度系统流量数据
# * UserTraffic：代表用户流量数据
# * FileOpsStat：代表文件操作统计数据
# * UserActivityStat：代表用户活动统计数据
# * TotalStorageStat：代表总存储统计数据

# 这些模型可能用于存储和管理数据库中的统计数据，并可能与 statistics 目录中的其他文件（如 counter.py 和 db.py）一起使用，以收集、处理和分析统计数据。

class TotalStorageStat(Base):
    # 总存储统计对象
    # __tablename__是 SQLAlchemy 模型的特殊变量，指定了这个模型对应的数据库表名
    __tablename__ = 'TotalStorageStat'

    # id是该对象的唯一标识符
    # mapped_column是 SQLAlchemy 的一个函数，用于将该对象的属性，映射到对应的数据库表的某个字段上
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # timestamp是该对象的创建时间
    timestamp = mapped_column(DateTime, nullable=False)

    # total_size是该对象的总存储大小
    total_size = mapped_column(BigInteger, nullable=False)

    # org_id是该对象所属的组织id
    org_id = mapped_column(Integer, nullable=False)

    # __table_args__ 是 SQLAlchemy 模型的一个特殊变量，指定了该模型对应的数据库表的其他参数
    # 例如，可以在这里指定该表的索引
    __table_args__ = (Index('idx_storage_time_org', 'timestamp', 'org_id'), )

    def __init__(self, org_id, timestamp, total_size):
        super().__init__()
        self.timestamp = timestamp
        self.total_size = total_size
        self.org_id = org_id


class FileOpsStat(Base):
    # FileOpsStat 文件操作数量的统计对象
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
    # 用户活动统计
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
    # 用户流量
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
    # 系统流量
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
    # 每月用户流量
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

    # 将对象的属性映射到数据库表的字段上
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
    # 每月系统流量
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
