#!/usr/bin/env python3
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
Base = declarative_base()
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

class Page(Base):
    __tablename__ = 'page'
    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    size = Column(Integer)
    file_path = Column(String, index=True)
    content_hash = Column(String)
    mtime = Column(DateTime, index=True)

class FetchQueue(Base):
    __tablename__ = 'fetch_queue'
    url = Column(String, primary_key=True)
    mtime = Column(DateTime, index=True)

    