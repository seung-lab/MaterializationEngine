from flask import current_app
from dynamicannotationdb.materialization_client import DynamicMaterializationClient

cache = {}


def get_db(aligned_volume) -> DynamicMaterializationClient:
    if aligned_volume not in cache:
        sql_uri_config = current_app.config["SQLALCHEMY_DATABASE_URI"]
        cache[aligned_volume] = DynamicMaterializationClient(aligned_volume, sql_uri_config)

    return cache[aligned_volume]
