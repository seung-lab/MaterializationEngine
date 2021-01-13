"""
Create frozen dataset.
"""
from datetime import datetime

import psycopg2
from celery.utils.log import get_task_logger
from flask import current_app
from materializationengine.database import create_session
from materializationengine.info_client import get_aligned_volumes
from materializationengine.models import AnalysisVersion
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

celery_logger = get_task_logger(__name__)


SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


def get_aligned_volumes_databases():
    aligned_volumes = get_aligned_volumes()
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]

    engine = create_engine(sql_base_uri)
    with engine.connect() as connection:
        result = connection.execute("SELECT datname FROM pg_database;")
        databases = [database[0] for database in result]
    aligned_volume_databases = list(
        set(aligned_volumes).intersection(databases))
    return aligned_volume_databases


def remove_expired_databases(aligned_volume_databases) -> None:
    """
    Remove expired database from time this method is called.
    """
    current_time = datetime.utcnow()

    for aligned_volume in aligned_volume_databases:
        sql_uri = f"postgres://postgres:materialize@localhost:5432/{aligned_volume}"
        session, engine = create_session(sql_uri)

        expired_versions = (session.query(AnalysisVersion).
                            filter(AnalysisVersion.expires_on <= current_time).all())

        session.close()
        engine.dispose()

        con = psycopg2.connect(dbname='postgres',
                               user='postgres', host='localhost',
                               password='materialize')

        con.set_isolation_level(
            ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        for database in expired_versions:
            try:
                result = cur.execute(sql.SQL("DROP DATABASE {}").format(
                    sql.Identifier(str(database)))
                )
            except Exception as e:
                celery_logger.error(f"ERROR: {e}: {database} does not exist")
        con.close()
        return True


if __name__ == "__main__":

    remove_expired_databases(get_aligned_volumes_databases())
