from flask import current_app
from dynamicannotationdb.materialization_client import DynamicMaterializationClient
from sqlalchemy.engine.url import make_url
from sqlalchemy import create_engine, engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from functools import lru_cache

cache = {}


def get_db(aligned_volume) -> DynamicMaterializationClient:
    if aligned_volume not in cache:
        sql_uri_config = current_app.config["SQLALCHEMY_DATABASE_URI"]
        cache[aligned_volume] = DynamicMaterializationClient(aligned_volume, sql_uri_config)

    return cache[aligned_volume]


def create_session(sql_uri: str = None):
    engine = create_engine(sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50)
    Session = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))
    session = Session()
    return session, engine

class SqlAlchemyCache:

    def __init__(self):
        self._engine = None
        self._sessions = {}

    @property
    def engine(self):
        return self._engine

    def get(self, aligned_volume):
        if aligned_volume not in self._sessions:
            SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
            self._engine = create_engine(sql_uri, pool_recycle=3600,
                                                  pool_size=20,
                                                  max_overflow=50)
            Session = scoped_session(sessionmaker(bind=self._engine))
            self._sessions[aligned_volume] = Session
        return self._sessions[aligned_volume]

sqlalchemy_cache = SqlAlchemyCache()