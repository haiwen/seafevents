import uuid
import simplejson as json

from sqlalchemy import Column, BigInteger, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey

__all__ = [
    "Base",
    "Event",
    "UserEvent",
]

Base = declarative_base()

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

    id = Column(BigInteger, primary_key=True)

    username = Column(String(length=256), nullable=False, index=True)

    eid = Column(String(length=36), ForeignKey('Event.uuid', ondelete='CASCADE'), index=True)

    def __init__(self, username, eid):
        self.username = username
        self.eid = eid

    def __str__(self):
        return "UserEvent<user = %s, event id = %s>" % \
            (self.username, self.eid)
