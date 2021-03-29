# coding: utf-8

import json
import uuid
import hashlib

from sqlalchemy import Column, Integer, String, DateTime, Text, Index, BigInteger
from sqlalchemy import ForeignKey

from seafevents.db import Base

class Activity(Base):
    """
    """
    __tablename__ = 'Activity'

    id = Column(Integer, primary_key=True, autoincrement=True)
    op_type = Column(String(length=128), nullable=False)
    op_user = Column(String(length=255), nullable=False)
    obj_type = Column(String(length=128), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

    repo_id = Column(String(length=36), nullable=False)
    commit_id = Column(String(length=40))
    path = Column(Text, nullable=False)
    detail = Column(Text, nullable=False)

    def __init__(self, record):
        self.op_type = record['op_type']
        self.obj_type = record['obj_type']
        self.repo_id = record['repo_id']
        self.timestamp = record['timestamp']
        self.op_user = record['op_user']
        self.path = record['path']
        self.commit_id = record.get('commit_id', None)


        detail = {}
        detail_keys = ['size', 'old_path', 'days', 'repo_name', 'obj_id', 'old_repo_name']
        for k in detail_keys:
            if k in record and record.get(k, None) is not None:
                detail[k] = record.get(k, None)

        self.detail = json.dumps(detail)

    def __str__(self):
        return 'Activity<id: %s, type: %s, repo_id: %s>' % \
            (self.id, self.op_type, self.repo_id)


class UserActivity(Base):
    """
    """
    __tablename__ = 'UserActivity'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(length=255), nullable=False)
    activity_id = Column(Integer, ForeignKey('Activity.id', ondelete='CASCADE'))
    timestamp = Column(DateTime, nullable=False, index=True)

    __table_args__ = (Index('idx_username_timestamp',
                            'username', 'timestamp'),)

    def __init__(self, username, activity_id, timestamp):
        self.username = username
        self.activity_id = activity_id
        self.timestamp = timestamp

    def __str__(self):
        return 'UserActivity<username: %s, activity id: %s>' % \
                (self.username, self.activity_id)


class Event(Base):
    """General class for events. Specific information is stored in json format
    in Event.detail.

    """
    __tablename__ = 'Event'

    uuid = Column(String(length=36), primary_key=True)
    etype = Column(String(length=128), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Json format detail for this event
    detail = Column(Text, nullable=False)

    def __init__(self, timestamp, etype, detail):
        self.uuid = str(uuid.uuid4())
        self.timestamp = timestamp
        self.etype = etype
        self.detail = json.dumps(detail)

    def __str__(self):
        return 'Event<uuid: %s, type: %s, detail: %s>' % \
            (self.uuid, self.etype, self.detail)

class UserEvent(Base):
    __tablename__ = 'UserEvent'

    id = Column(Integer, primary_key=True, autoincrement=True)

    org_id = Column(Integer)

    username = Column(String(length=255), nullable=False, index=True)

    eid = Column(String(length=36), ForeignKey('Event.uuid', ondelete='CASCADE'), index=True)

    def __init__(self, org_id, username, eid):
        self.org_id = org_id
        self.username = username
        self.eid = eid

    def __str__(self):
        if self.org_id > 0:
            return "UserEvent<org = %d, user = %s, event id = %s>" % \
                (self.org_id, self.username, self.eid)
        else:
            return "UserEvent<user = %s, event id = %s>" % \
                (self.username, self.eid)

class FileHistory(Base):
    __tablename__ = 'FileHistory'

    id = Column(Integer, primary_key=True, autoincrement=True)
    op_type = Column(String(length=128), nullable=False)
    op_user = Column(String(length=255), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

    repo_id = Column(String(length=36), nullable=False)
    commit_id = Column(String(length=40))
    file_id =  Column(String(length=40), nullable=False)
    file_uuid = Column(String(length=40), index=True)
    path = Column(Text, nullable=False)
    repo_id_path_md5 = Column(String(length=32), index=True)
    size = Column(BigInteger, nullable=False)
    old_path = Column(Text, nullable=False)

    def __init__(self, record):
        self.op_type = record['op_type']
        self.op_user = record['op_user']
        self.timestamp = record['timestamp']
        self.repo_id = record['repo_id']
        self.commit_id = record.get('commit_id', '')
        self.file_id = record.get('obj_id')
        self.file_uuid = record.get('file_uuid')
        self.path = record['path']
        self.repo_id_path_md5 = hashlib.md5((self.repo_id + self.path).encode('utf8')).hexdigest()
        self.size = record.get('size')
        self.old_path = record.get('old_path', '')

class FileAudit(Base):
    __tablename__ = 'FileAudit'

    eid = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    etype = Column(String(length=128), nullable=False)
    user = Column(String(length=255), nullable=False)
    ip = Column(String(length=45), nullable=False)
    device = Column(Text, nullable=False)
    org_id = Column(Integer, nullable=False)
    repo_id = Column(String(length=36), nullable=False)
    file_path = Column(Text, nullable=False)
    __table_args__ = (Index('idx_file_audit_orgid_eid',
                            'org_id', 'eid'),
                      Index('idx_file_audit_user_orgid_eid',
                            'user', 'org_id', 'eid'),
                      Index('idx_file_audit_repo_org_eid',
                            'repo_id', 'org_id', 'eid'))

    def __init__(self, timestamp, etype, user, ip, device,
                 org_id, repo_id, file_path):
        self.timestamp = timestamp
        self.etype = etype
        self.user = user
        self.ip = ip
        self.device = device
        self.org_id = org_id
        self.repo_id = repo_id
        self.file_path = file_path

    def __str__(self):
        if self.org_id > 0:
            return "FileAudit<EventType = %s, User = %s, IP = %s, Device = %s, \
                    OrgID = %s, RepoID = %s, FilePath = %s>" % \
                    (self.etype, self.user, self.ip, self.device,
                     self.org_id, self.repo_id, self.file_path)
        else:
            return "FileAudit<EventType = %s, User = %s, IP = %s, Device = %s, \
                    RepoID = %s, FilePath = %s>" % \
                    (self.etype, self.user, self.ip, self.device,
                     self.repo_id, self.file_path)

class FileUpdate(Base):
    __tablename__ = 'FileUpdate'

    eid = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String(length=255), nullable=False)
    org_id = Column(Integer, nullable=False)
    repo_id = Column(String(length=36), nullable=False)
    commit_id = Column(String(length=40), nullable=False)
    file_oper = Column(Text, nullable=False)
    __table_args__ = (Index('idx_file_update_orgid_eid',
                            'org_id', 'eid'),
                      Index('idx_file_update_user_orgid_eid',
                            'user', 'org_id', 'eid'),
                      Index('idx_file_update_repo_org_eid',
                            'repo_id', 'org_id', 'eid'))

    def __init__(self, timestamp, user, org_id, repo_id, commit_id, file_oper):
        self.timestamp = timestamp
        self.user = user
        self.org_id = org_id
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.file_oper = file_oper

    def __str__(self):
        if self.org_id > 0:
            return "FileUpdate<User = %s, OrgID = %s, RepoID = %s, CommitID = %s \
                   FileOper = %s>" % (self.user, self.org_id, self.repo_id,
                                      self.commit_id, self.file_oper)
        else:
            return "FileUpdate<User = %s, RepoID = %s, CommitID = %s, \
                    FileOper = %s>" % (self.user, self.repo_id,
                                       self.commit_id, self.file_oper)

class PermAudit(Base):
    __tablename__ = 'PermAudit'

    eid = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    etype = Column(String(length=128), nullable=False)
    from_user = Column(String(length=255), nullable=False)
    to = Column(String(length=255), nullable=False)
    org_id = Column(Integer, nullable=False)
    repo_id = Column(String(length=36), nullable=False)
    file_path = Column(Text, nullable=False)
    permission = Column(String(length=15), nullable=False)
    __table_args__ = (Index('idx_perm_audit_orgid_eid',
                            'org_id', 'eid'),
                      Index('idx_perm_audit_user_orgid_eid',
                            'from_user', 'org_id', 'eid'),
                      Index('idx_perm_audit_repo_org_eid',
                            'repo_id', 'org_id', 'eid'))

    def __init__(self, timestamp, etype, from_user, to, org_id, repo_id,
                 file_path, permission):
        self.timestamp = timestamp
        self.etype = etype
        self.from_user = from_user
        self.to = to
        self.org_id = org_id
        self.repo_id = repo_id
        self.file_path = file_path
        self.permission = permission

    def __str__(self):
        if self.org_id > 0:
            return "PermAudit<EventType = %s, FromUser = %s, To = %s, \
                   OrgID = %s, RepoID = %s, FilePath = %s, Permission = %s>" % \
                    (self.etype, self.from_user, self.to, self.org_id,
                     self.repo_id, self.file_path, self.permission)
        else:
            return "PermAudit<EventType = %s, FromUser = %s, To = %s, \
                   RepoID = %s, FilePath = %s, Permission = %s>" % \
                    (self.etype, self.from_user, self.to,
                     self.repo_id, self.file_path, self.permission)
