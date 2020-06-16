from flask import current_app
from dynamicannotationdb.client import AnnotationDBMeta


cache = {}

def get_db():
    if 'db' not in cache:
        sql_uri_config = current_app.config['MATERIALIZATION_POSTGRES_URI']
        # cache["db"] = AnnotationDBMeta(sql_uri=sql_uri_config)
    return sql_uri_config