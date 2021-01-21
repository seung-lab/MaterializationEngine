"""
Create frozen dataset.
"""
from datetime import datetime

import psycopg2
from celery.utils.log import get_task_logger
from flask import current_app
from materializationengine.database import create_session
from materializationengine.info_client import get_aligned_volumes
from materializationengine.celery_worker import celery
from materializationengine.models import AnalysisVersion
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

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

@celery.task(name="process:remove_expired_databases", acks_late=True, bind=True)
def remove_expired_databases(self, aligned_volume_databases) -> None:
    """
    Remove expired database from time this method is called.
    """
    current_time = datetime.datetime.utcnow()
    dropped_dbs = []                            

    for aligned_volume in aligned_volume_databases:
        sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
        session, engine = create_session(sql_uri)

        expired_versions = session.query(AnalysisVersion).\
                            filter(AnalysisVersion.expires_on <= current_time).all()
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            for database in expired_versions:
                try:
                    sql = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
                
                    result_proxy = conn.execute(sql)
                    result = result_proxy.scalar()

                    if result:
                        sql = "DROP DATABASE %s" % database
                        result_proxy = conn.execute(sql)
                        
                        expired_database = session.query(AnalysisVersion).\
                                    filter(AnalysisVersion.version==database.version).one()
                        expired_database.valid = False
                        dropped_dbs.append(expired_database)
                        celery_logger.info(f"Database '{expired_database}' dropped")
                except Exception as e:
                    celery_logger.error(f"ERROR: {e}: {database} does not exist")
        session.commit()
        session.close()
    return dropped_dbs

if __name__ == "__main__":
    aligned_volume_databases = get_aligned_volumes_databases()
    dropped_dbs = remove_expired_databases.s(aligned_volume_databases).apply_async()
