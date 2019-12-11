from app import materialize, materializationmanager
import argschema
import os
import marshmallow as mm
import datetime
import time
import pickle as pkl
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from emannotationschemas.models import format_version_db_uri, Base

HOME = os.path.expanduser("~")

class BatchMaterializationSchema(argschema.ArgSchema):
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")

if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=BatchMaterializationSchema)
    if 'sql_uri' not in mod.args:
        if 'MATERIALIZATION_POSTGRES_URI' in os.environ.keys():
            sql_uri = os.environ['MATERIALIZATION_POSTGRES_URI']
        else:
            raise Exception(
                'need to define a postgres uri via command line or MATERIALIZATION_POSTGRES_URI env')
    else:
        sql_uri = mod.args['sql_uri']

   
    engine = create_engine(sql_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    