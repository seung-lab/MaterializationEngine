import datetime
import logging
import numpy as np
from annotationframeworkclient.annotationengine import AnnotationClient
from annotationframeworkclient.infoservice import InfoServiceClient
from celery import group
from flask import current_app
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
import cloudvolume

# from dynamicannotationdb.client import AnnotationDBMeta
from emannotationschemas import models as em_models
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import Base, create_table_dict, format_version_db_uri
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from materializationengine import materializationmanager as manager
from materializationengine import materialize
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.extensions import create_session
from materializationengine.models import AnalysisMetadata, Base
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from typing import List

BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ADDRESS = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]


def start_materialization(aligned_volume: str, version: int = 1):
    mat_db_sql_uri = create_materialized_database(aligned_volume, version)
    create_materialized_tables.s(aligned_volume, mat_db_sql_uri).apply_async()
    # add_analysis_tables.s() |
    # materialize_root_ids.s() |
    # materialize_annotations.s() |
    # materialize_annotations_delta.s()).apply_async()
    return "Test"


def create_materialized_database(aligned_volume: str, version: int = 1) -> str:
    """Create a new database to store materialized annotation tables

    Parameters
    ----------
    sql_uri : str
        base path to the sql server
    aligned_volume : str
        name of aligned volume which the database name will inherent
    version: int
        optional version tag for materialiazed database name, defaults to 1
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
            logging.info(
                f"Creating new materialized database {sql_uri.database}"
            )
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
    return sql_uri

@celery.task(name="threads:materializationengine.mat_tasks.create_materialized_tables")
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
    mat_client = get_db(aligned_volume)
    tables = mat_client._get_all_tables()
    try:
        mat_engine = create_engine(mat_sql_uri)
        Session = scoped_session(sessionmaker(bind=mat_engine,
                                              autocommit=False,
                                              autoflush=False))
        session = Session()
        materialize_base = declarative_base(bind=mat_engine)
    except Exception as e:
        raise e
    materialized_tables = []
    for table in tables:
        # only create table if marked as valid in the metadata table
        if table.valid:
            table_name = get_table_name_from_table_id(table.table_id)
            # create name of table to be materialized
            mat_table_id = build_materailized_table_id(aligned_volume, table_name)
            if not mat_engine.dialect.has_table(mat_engine, mat_table_id):
                schema_name = mat_client.get_table_schema(aligned_volume, table_name)
                anno_schema = get_schema(schema_name)
                flat_schema = create_flattened_schema(anno_schema)
                # construct dict of sqlalchemy columns
                annotation_dict = create_table_dict(
                    table_id=mat_table_id,
                    Schema=flat_schema,
                    table_metadata=None,
                    version=None,
                    with_crud_columns=False,
                )
                creation_time = datetime.datetime.now()

                mat_table = type(mat_table_id, (materialize_base,), annotation_dict)
                mat_table.__table__.create(bind=mat_engine)
                analysis_metadata_dict = {
                    "schema": schema_name,
                    "table_id": mat_table_id,
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
                    materialized_tables.append(mat_table_id)
                    session.close()
                logging.info(
                    f"Table: {table_name} created using {mat_table} \
                            model at {creation_time}"
                )
    return materialized_tables


@celery.task(name="threads:materializationengine.mat_tasks.get_missing_tables")
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
    pcg_table_name : str
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
    mat_db = get_db(aligned_volume)

    try:
        session, mat_engine = create_session(mat_sql_uri)
        # materialize_base = declarative_base(bind=mat_engine)
    except Exception as e:
        raise e

    analysis_table_results = session.query(AnalysisMetadata).all()

    if not analysis_table_results:
        return None
    # compare set of tables in annotation db and analysis db and find
    # whats not present in analysis table
    # list(set(analysis_tables).difference(annotation_tables))
    annotation_tables = mat_db.get_linked_tables(table_name, pcg_table_name)
    analysis_tables = [table.__dict__ for table in analysis_table_results]
    missing_tables = list(set(analysis_tables).difference(annotation_tables))
    tables = []
    # TODO check if table exists in analysis metadata table and check if table
    # is valid, return all tables not present in the analysis metadata
    for table in analysis_tables:
        if table.annotation_table:
            # if annotation_tables in analysis_tables:
            #    tables.append(annotation_table)
            pass
    # missing_tables_info = [
    #     t
    #     for t in all_tables
    #     if (t["table_name"] not in [t.annotation_table for t in tables])
    #     and (t["table_name"]) not in BLACKLIST
    # ]
    # logging.info(missing_tables_info)
    return missing_tables


def update_root_ids():
    raise NotImplementedError


def update_segment_ids():
    raise NotImplementedError


def update_annotations():
    raise NotImplementedError


def materialize_annotations():
    raise NotImplementedError


def create_versioned_materialized_table():
    raise NotImplementedError


def build_materailized_table_id(aligned_volume: str, table_name: str) -> str:
    return f"mat__{aligned_volume}__{table_name}"
