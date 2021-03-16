"""
Periodically clean up expired materialized databases.
"""
import itertools
from datetime import datetime

from celery.utils.log import get_task_logger
from materializationengine.celery_init import celery
from materializationengine.database import create_session
from materializationengine.info_client import get_aligned_volumes
from materializationengine.models import AnalysisVersion
from materializationengine.utils import get_config_param
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

celery_logger = get_task_logger(__name__)


def get_aligned_volumes_databases():
    aligned_volumes = get_aligned_volumes()
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]

    engine = create_engine(sql_base_uri)
    with engine.connect() as connection:
        result = connection.execute("SELECT datname FROM pg_database;")
        databases = [database[0] for database in result]
    aligned_volume_databases = list(
        set(aligned_volumes).intersection(databases))
    return aligned_volume_databases

@celery.task(name="process:remove_expired_databases")
def remove_expired_databases(delete_threshold: int=5) -> str:
    """
    Remove expired database from time this method is called.
    """
    aligned_volume_databases = get_aligned_volumes_databases()
    current_time = datetime.utcnow()
    remove_db_cron_info = []

    for aligned_volume in aligned_volume_databases:
        SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
        sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
        session, engine = create_session(sql_uri)
        session.expire_on_commit = False
        # get number of expired dbs that are ready for deletion
        try:
            expired_versions = session.query(AnalysisVersion).\
                filter(AnalysisVersion.expires_on <= current_time).\
                filter(AnalysisVersion.valid == True).all()
        except Exception as sql_error:
            celery_logger.error(f"Error: {sql_error}")
            continue
        
        # get databases that exist currently, filter by materializied dbs            
        result = engine.execute('SELECT datname FROM pg_database;').fetchall()
        database_list = list(itertools.chain.from_iterable(result))
        databases = [
            database for database in database_list if database.startswith(aligned_volume)]
        # remove the aligned volume database to get accurate count of mat dbs
        databases.remove(aligned_volume)
        dropped_dbs_info = {
            "aligned_volume": aligned_volume,
            "materialized_databases": (databases, f'count={len(databases)}'),
            "expired_databases": (expired_versions, f'count={len(expired_versions)}'),
            "delete_threshold": delete_threshold,
        }
        dropped_dbs = []

        if len(databases) > delete_threshold:
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
                                filter(AnalysisVersion.version ==
                                    database.version).one()
                            expired_database.valid = False
                            session.commit()
                            celery_logger.info(f"Database '{expired_database}' dropped")
                            dropped_dbs.append(expired_database)
                            dropped_dbs_info['dropped_databases'] = dropped_dbs
                    except Exception as e:
                        celery_logger.error(
                            f"ERROR: {e}: {database} does not exist")
        remove_db_cron_info.append(dropped_dbs_info)
        session.close()
    return remove_db_cron_info


