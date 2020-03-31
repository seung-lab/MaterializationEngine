from flask import g, current_app
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from celery import Celery

# create database
Base = declarative_base()
db = SQLAlchemy(model_class=Base)

# create celery
celery = Celery(include=['app.tasks'])


def create_session(sql_uri: str = None):
    engine = create_engine(sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50)
    Session = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))
    session = Session()
    return session, engine
