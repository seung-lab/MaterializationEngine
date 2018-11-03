from flask import g, current_app
from flask_sqlalchemy import SQLAlchemy
# from flask_marshmallow import Marshmallow
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

db = SQLAlchemy(model_class=Base)

