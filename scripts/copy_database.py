import sqlalchemy
import os
from sqlalchemy.orm import sessionmaker
from emannotationschemas.models import AnalysisVersion, AnalysisTable
import emannotationschemas.models as em_models
import psycopg2
import time

dataset = "pinky100"
base_version = 58
new_version = 60
sql_uri = os.environ['MATERIALIZATION_POSTGRES_URI']

sql_uri_base = "/".join(sql_uri.split('/')[:-1])
print(sql_uri_base)

new_db_name = f"{dataset}_v{new_version}"
base_db_name = f"{dataset}_v{base_version}"
new_db_uri = sql_uri_base + f"/{new_db_name}"
base_db_uri = sql_uri_base + f"/{base_db_name}"

base_engine = sqlalchemy.create_engine(sql_uri)

start = time.time()
conn = base_engine.connect()
conn.execute("commit")
conn.execute(f"create database {new_db_name} TEMPLATE {base_db_name}")
print(time.time()-start)

Session = sessionmaker(bind=base_engine)
session = Session()

old_version = session.query(AnalysisVersion)