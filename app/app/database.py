from flask import g, current_app
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

db = SQLAlchemy(model_class=Base)

