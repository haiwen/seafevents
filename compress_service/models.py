# -*- coding: utf-8 -*-
from seafevents.db import Base
from sqlalchemy import Column, BigInteger, String


class CompressRecords(Base):
    __tablename__ = 'compress_records'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    token = Column(String(length=36), nullable=False, index=True, unique=True)
    last_modified = Column(BigInteger, nullable=False)

    def __init__(self, token, last_modified):
        self.token = token
        self.last_modified = last_modified
