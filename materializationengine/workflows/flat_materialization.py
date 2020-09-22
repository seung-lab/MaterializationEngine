import datetime
import logging
import numpy as np
from flask import current_app
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func, or_
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
import cloudvolume
from celery import group, chain, chord, subtask, signature

from emannotationschemas import models as em_models
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import Base, create_table_dict, format_version_db_uri
from dynamicannotationdb.key_utils import (
    build_segmentation_table_name,
)
from materializationengine import materializationmanager as manager
from materializationengine import materialize
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.extensions import create_session
from materializationengine.models import AnalysisMetadata, Base
from materializationengine.errors import AnnotationParseFailure
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from typing import List

BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ADDRESS = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]


def create_flat_materialization(aligned_volume: str):
    """[summary]

    Parameters
    ----------
    aligned_volume : str
        [description]
    """
    # mat_db_sql_uri = create_materialized_database(aligned_volume, version)
    # create_materialized_tables.s(aligned_volume, mat_db_sql_uri).apply_async()
    pass

def create_materialized_database(aligned_volume: str) -> str:
    """Create a new database to store materialized annotation tables

    Parameters
    ----------
    sql_uri : str
        base path to the sql server
    aligned_volume : str
        name of aligned volume which the database name will inherent
    Returns
    -------
    return True
    """
    sql_uri_config = current_app.config["SQLALCHEMY_DATABASE_URI"]
    sql_base_uri = sql_uri_config.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/materiailzed_{aligned_volume}")

    engine = create_engine(sql_base_uri)
    with engine.connect() as connection:
        connection.execute("commit")
        result = connection.execute(
            f"SELECT 1 FROM pg_catalog.pg_database \
                    WHERE datname = '{sql_uri.database}'"
        )
        if not result.fetchone():
            # create new database from template_postgis database
            logging.info(f"Creating new materialized database {sql_uri.database}")
            connection.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                        WHERE pid <> pg_backend_pid() \
                        AND datname = '{sql_uri.database}';"
            )
            connection.execute(
                f"CREATE DATABASE {sql_uri.database} \
                                TEMPLATE template_postgis"
            )
            result = connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database \
                    WHERE datname = '{sql_uri.database}'"
            )
    engine.dispose()

    table_objects = [
        AnalysisMetadata.__tablename__,
    ]

    mat_engine = create_engine(sql_uri)
    # create analysis metadata table if not exists
    if not engine.dialect.has_table(mat_engine, AnalysisMetadata.__tablename__):
        for table in table_objects:
            Base.metadata.tables[table].create(bind=mat_engine)
    mat_engine.dispose()
    return str(sql_uri)


@celery.task(name="threads:create_materialized_tables")
def create_materialized_tables(aligned_volume: str, mat_sql_uri: str):
    """Create all tables in flat materialized format.

    Parameters
    ----------
    aligned_volume : str
        aligned volume name
    mat_sql_uri : str
        target database sql url to use

    Returns
    -------
    [type]
        [description]

    Raises
    ------
    e
        [description]
    """
    anno_db = get_db(aligned_volume)
    tables = anno_db._get_all_tables()
    try:
        mat_engine = create_engine(mat_sql_uri)
        Session = scoped_session(sessionmaker(bind=mat_engine, autocommit=False, autoflush=False))
        session = Session()
        materialize_base = declarative_base(bind=mat_engine)
    except Exception as e:
        raise e
    materialized_tables = []
    for table in tables:
        # only create table if marked as valid in the metadata table
        if table.valid:
            table_name = table.table_name
            # create name of table to be materialized
            mat_table_name = build_materailized_table_id(table_name)
            if not mat_engine.dialect.has_table(mat_engine, mat_table_name):
                schema_name = anno_db.get_table_schema(aligned_volume, table_name)
                anno_schema = get_schema(schema_name)
                flat_schema = create_flattened_schema(anno_schema)
                # construct dict of sqlalchemy columns
                annotation_dict = create_table_dict(
                    table_name=mat_table_name,
                    Schema=flat_schema,
                    table_metadata=None,
                    with_crud_columns=False,
                )
                creation_time = datetime.datetime.now()

                mat_table = type(mat_table_name, (materialize_base,), annotation_dict)
                mat_table.__table__.create(bind=mat_engine)
                analysis_metadata_dict = {
                    "schema": schema_name,
                    "table_name": mat_table_name,
                    "valid": True,
                    "created": creation_time,
                }
                analysis_table = AnalysisMetadata(**analysis_metadata_dict)
                try:
                    session.add(analysis_table)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logging.error(e)
                finally:
                    materialized_tables.append(mat_table_name)
                    session.close()
                logging.info(
                    f"Table: {table_name} created using {mat_table} \
                            model at {creation_time}"
                )
    return materialized_tables


@celery.task(name="threads:get_missing_tables")
def get_missing_tables(
    aligned_volume: str, table_name: str, pcg_table_name: str, mat_sql_uri: str
) -> List:
    """Compare list of tables in a materialized database with a
    target annotation database. Returns a list of missing tables to
    generate during materialization.


    Parameters
    ----------
    aligned_volume : str
        [description]
    table_name : str
        [description]
    mat_sql_uri : str
        [description]

    Returns
    -------
    List
        [description]

    Raises
    ------
    e
        [description]
    """
    anno_db = get_db(aligned_volume)

    try:
        session, mat_engine = create_session(mat_sql_uri)
        materialize_base = declarative_base(bind=mat_engine)
    except Exception as e:
        raise e

    analysis_table_results = anno_db._get_all_tables()

    if not analysis_table_results:
        return None
    # compare set of tables in annotation db and analysis db and find
    # whats not present in analysis table
    analysis_tables = [table.__dict__ for table in analysis_table_results]
    missing_tables = []
    # TODO check if table exists in analysis metadata table and check if table
    # is valid, return all tables not present in the analysis metadata
    for table in analysis_tables:
        if table.valid:
            if table.annotation_table not in analysis_tables:
                table_name = table.table_name
                # create name of table to be materialized
                mat_table_name = build_materailized_table_id(table_name)
                if not mat_engine.dialect.has_table(mat_engine, mat_table_name):
                    schema_name = anno_db.get_table_schema(aligned_volume, table_name)
                    anno_schema = get_schema(schema_name)
                    flat_schema = create_flattened_schema(anno_schema)
                    # construct dict of sqlalchemy columns
                    annotation_dict = create_table_dict(
                        table_name=mat_table_name,
                        Schema=flat_schema,
                        table_metadata=None,
                        version=None,
                        with_crud_columns=False,
                    )
                    creation_time = datetime.datetime.now()

                    mat_table = type(mat_table_name, (materialize_base,), annotation_dict)
                    mat_table.__table__.create(bind=mat_engine)
                    analysis_metadata_dict = {
                        "schema": schema_name,
                        "table_name": mat_table_name,
                        "valid": True,
                        "created": creation_time,
                    }
                    analysis_table = AnalysisMetadata(**analysis_metadata_dict)
                    try:
                        session.add(analysis_table)
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        logging.error(e)
                    finally:
                        missing_tables.append(mat_table_name)
                        session.close()
    return missing_tables


def build_materailized_table_id(table_name: str) -> str:
    return f"mat__{table_name}"
