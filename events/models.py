import json
import uuid

from sqlalchemy import Column, Integer, String, DateTime, Text, Index
from sqlalchemy import ForeignKey, Sequence

from seafevents.db import Base

class Event(Base):
    """General class for events. Specific information is stored in json format
    in Event.detail.

    """
    __tablename__ = 'Event'

    uuid = Column(String(length=36), primary_key=True)
    etype = Column(String(length=128), nullable=False)
    timestamp = Column(DateTime, nullable=False)

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

    id = Column(Integer, Sequence('user_event_eid_seq'), primary_key=True)

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

class FileAudit(Base):
    __tablename__ = 'FileAudit'

    eid = Column(Integer, Sequence('file_audit_eid_seq'), primary_key=True)
    timestamp = Column(DateTime, nullable=False)
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

    def __init__(self, timestamp, etype, user, ip, device, \
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
                    (self.etype, self.user, self.ip, self.device, \
                     self.org_id, self.repo_id, self.file_path)
        else:
            return "FileAudit<EventType = %s, User = %s, IP = %s, Device = %s, \
                    RepoID = %s, FilePath = %s>" % \
                    (self.etype, self.user, self.ip, self.device, \
                     self.repo_id, self.file_path)

class FileUpdate(Base):
    __tablename__ = 'FileUpdate'

    eid = Column(Integer, Sequence('file_update_eid_seq'), primary_key=True)
    timestamp = Column(DateTime, nullable=False)
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
                   FileOper = %s>" % (self.user, self.org_id, self.repo_id, \
                                      self.commit_id, self.file_oper)
        else:
            return "FileUpdate<User = %s, RepoID = %s, CommitID = %s, \
                    FileOper = %s>" % (self.user, self.repo_id, \
                                       self.commit_id, self.file_oper)

class PermAudit(Base):
    __tablename__ = 'PermAudit'

    eid = Column(Integer, Sequence('user_perm_audit_eid_seq'), primary_key=True)
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

    def __init__(self, timestamp, etype, from_user, to, org_id, repo_id, \
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
                    (self.etype, self.from_user, self.to, self.org_id, \
                     self.repo_id, self.file_path, self.permission)
        else:
            return "PermAudit<EventType = %s, FromUser = %s, To = %s, \
                   RepoID = %s, FilePath = %s, Permission = %s>" % \
                    (self.etype, self.from_user, self.to, \
                     self.repo_id, self.file_path, self.permission)
