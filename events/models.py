import uuid
import simplejson as json

from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Text
from sqlalchemy import ForeignKey

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

    id = Column(Integer, primary_key=True)

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