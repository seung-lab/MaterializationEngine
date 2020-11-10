import datetime
import logging
from typing import List

from celery import chain, chord, group, signature, subtask
from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import AnnoMetadata, SegmentationMetadata
from emannotationschemas import get_schema
from emannotationschemas import models as em_models
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import create_table_dict, format_version_db_uri
from flask import current_app
from materializationengine.celery_worker import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.database import (create_session, get_db,
                                            sqlalchemy_cache)
from materializationengine.errors import AnnotationParseFailure
from materializationengine.models import (AnalysisMetadata, AnalysisTable,
                                          AnalysisVersion, Base)
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                query_id_range)
from materializationengine.utils import (create_annotation_model,
                                         create_segmentation_model)
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func, or_

celery_logger = get_task_logger(__name__)

SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


@celery.task(name="process:versioned_materialization",
             bind=True,
             acks_late=True,)
def versioned_materialization(self, datastack_info: dict):
    """Create a timelocked database of materialization annotations
    and asociated segmentation data.

    if not version:
        version = 1
    else:
        query_lastest_version

    result = get_analysis_metadata()
    mat_info = result.get()
    create_materialized_database()
    for metadata in mat_info:
        chunk_ids 
    create_materialized_tables()


    Parameters
    ----------
    aligned_volume : str
        [description]
    """
    
    new_version_number = create_new_version(datastack_info)
    mat_info = get_analysis_info(new_version_number, datastack_info)
    
    for mat_metadata in mat_info:
        if mat_metadata:
            setup_tables = chain(
            create_analysis_database.s(mat_metadata),
            create_analysis_tables.s())
            setup_tables.apply_async()

            supervoxel_chunks = chunk_supervoxel_ids_task(mat_metadata)
            process_chunks_workflow = chain(
                chord([
                    chain(insert_annotation_data.si(chunk, mat_metadata)) for chunk in supervoxel_chunks], fin.si()), 
                    update_analysis_metadata.si(mat_metadata))
            process_chunks_workflow.apply_async()
    
def create_new_version(datastack_info: dict):
    aligned_volume_name = datastack_info['aligned_volume']['name']
    datastack = datastack_info.get('datastack')
    
    table_objects = [
        AnalysisVersion.__tablename__,
        AnalysisTable.__tablename__,
    ]

    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume_name}")

    mat_engine = create_engine(sql_uri)
    # create analysis metadata table if not exists
    for table in table_objects:
        if not mat_engine.dialect.has_table(mat_engine, table):
            Base.metadata.tables[table].create(bind=mat_engine)
    mat_engine.dispose()

    session = sqlalchemy_cache.get(aligned_volume_name)

    top_version = (session.query(AnalysisVersion)
                .order_by(AnalysisVersion.version.desc())
                .first())

    if top_version is None:
        new_version_number = 1
    else:
        new_version_number = top_version.version + 1
    
    time_stamp = datetime.datetime.utcnow()

    analysisversion = AnalysisVersion(datastack=datastack,
                                      time_stamp=time_stamp,
                                      version=new_version_number,
                                      valid=False)
    session.add(analysisversion)
    session.commit()
    return new_version_number

def get_analysis_info(analysis_version: int, datastack_info: dict) -> List[dict]:
    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization.

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    pcg_table_name: str
        cg_table_name
    segmentation_source:
        infoservice data
    Returns
    -------
    List[dict]
        list of dicts containing metadata for each table
    """

    aligned_volume_name = datastack_info['aligned_volume']['name']
    pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]
    segmentation_source = datastack_info.get('segmentation_source')
    db = get_db(aligned_volume_name)

    annotation_tables = db.get_valid_table_names()
    metadata = []
    for annotation_table in annotation_tables:
        max_id = db.get_max_id_value(annotation_table)
        if max_id:
            segmentation_table_name = build_segmentation_table_name(
                annotation_table, pcg_table_name)

            materialization_time_stamp = datetime.datetime.utcnow()

            table_metadata = {
                'datastack': datastack_info['datastack'],
                'aligned_volume': str(aligned_volume_name),
                'schema': db.get_table_schema(annotation_table),
                'max_id': int(max_id),
                'segmentation_table_name': segmentation_table_name,
                'annotation_table_name': annotation_table,
                'pcg_table_name': pcg_table_name,
                'segmentation_source': segmentation_source,
                'materialization_time_stamp': str(materialization_time_stamp),
                'analysis_version': analysis_version,
                'chunk_size': 100000,
            }
            metadata.append(table_metadata.copy())
    db.cached_session.close()
    return metadata

@celery.task(name="process:create_analysis_database",
             bind=True,
             acks_late=True,)
def create_analysis_database(self, mat_metadata: dict) -> str:
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

    aligned_volume = mat_metadata['aligned_volume']
    analysis_version = mat_metadata['analysis_version']
    datastack = mat_metadata['datastack']
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

    analysis_sql_uri = make_url(
        f"{sql_base_uri}/{datastack}__mat{analysis_version}")
            
    engine = create_engine(sql_uri)


    with engine.connect() as connection:
        connection.execute("commit")
        result = connection.execute(
            f"SELECT 1 FROM pg_catalog.pg_database \
                    WHERE datname = '{analysis_sql_uri.database}'"
        )
        if not result.fetchone():
            # create new database from template_postgis database
            logging.info(
                f"Creating new materialized database {analysis_sql_uri.database}")
            connection.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                        WHERE pid <> pg_backend_pid() \
                        AND datname = '{analysis_sql_uri.database}';"
            )
            connection.execute(
                f"CREATE DATABASE {analysis_sql_uri.database} \
                                TEMPLATE postgres"
            )
            result = connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database \
                    WHERE datname = '{analysis_sql_uri.database}'"
            )
    engine.dispose()

    return mat_metadata


@celery.task(name="process:create_analysis_tables",
             bind=True,
             acks_late=True,)
def create_analysis_tables(self, mat_metadata: dict):
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

    aligned_volume = mat_metadata['aligned_volume']
    analysis_version = mat_metadata['analysis_version']
    datastack = mat_metadata['datastack']
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    
    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version)
    try:
        analysis_session, analysis_engine = create_session(analysis_sql_uri)
        session, engine = create_session(sql_uri)
        analysis_base = declarative_base(bind=analysis_engine)
    except Exception as e:
        raise e

    tables = session.query(AnnoMetadata).all()

    for table in tables:
        # only create table if marked as valid in the metadata table
        if table.valid:
            table_name = table.table_name
            # create name of table to be materialized
            if not engine.dialect.has_table(analysis_engine, table_name):
                schema_type = session.query(AnnoMetadata.schema_type).\
                    filter(AnnoMetadata.table_name == table_name).first()

                anno_schema = get_schema(schema_type[0])
                flat_schema = create_flattened_schema(anno_schema)
                # construct dict of sqlalchemy columns

                annotation_dict = create_table_dict(
                    table_name=table_name,
                    Schema=flat_schema,
                    segmentation_source=None,
                    table_metadata=None,
                    with_crud_columns=False,
                )

                flat_table = type(
                    table_name, (analysis_base,), annotation_dict)
                flat_table.__table__.create(bind=analysis_engine)

    session.close()
    engine.dispose()
    analysis_session.close()
    analysis_engine.dispose()
    return mat_metadata

@celery.task(name="process:insert_annotation_data",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def insert_annotation_data(self, chunk: List[int], mat_metadata: dict):

    aligned_volume = mat_metadata['aligned_volume']
    analysis_version = mat_metadata['analysis_version']
    annotation_table_name = mat_metadata['annotation_table_name']
    datastack = mat_metadata['datastack']
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

    session, engine = create_session(sql_uri)

    AnnotationModel = create_annotation_model(mat_metadata)
    SegmentationModel = create_segmentation_model(mat_metadata)
    analysis_table = get_analysis_table(aligned_volume, datastack, annotation_table_name, analysis_version)
    
    chunked_id_query = query_id_range(AnnotationModel.id, chunk[0], chunk[1])
  
    r = session.query(AnnotationModel, SegmentationModel).\
                join(SegmentationModel).\
                filter((AnnotationModel.deleted <= datetime.datetime.utcnow()) | (AnnotationModel.valid == True)).\
                filter(SegmentationModel.id == AnnotationModel.id).\
                filter(chunked_id_query).order_by(AnnotationModel.id)

    annotation_data = r.all()
    annotations = []
    for (anno, seg) in annotation_data:
        annotation = {**anno.__dict__, **seg.__dict__}
        del annotation['_sa_instance_state']
        del annotation['created']
        del annotation['deleted']
        del annotation['superceded_id']
        annotations.append(annotation)

    analysys_sql_uri = create_analysis_sql_uri(SQL_URI_CONFIG, datastack, analysis_version)
    analysis_session, analysis_engine = create_session(analysys_sql_uri)      

    try:
        analysis_engine.execute(
            analysis_table.insert(),
            [data for data in annotations]
        )
    except Exception as e:
        celery_logger.error(e)
        analysis_session.rollback()
    finally:
        analysis_session.close()
        analysis_engine.dispose()
        session.close()
        engine.dispose()

@celery.task(name="process:update_analysis_metadata",
             bind=True,
             acks_late=True,)
def update_analysis_metadata(self, mat_metadata: dict):
    aligned_volume = mat_metadata['aligned_volume']
    session = sqlalchemy_cache.get(aligned_volume)

    analysis_table_dict = {
        "aligned_volume": aligned_volume,
        "schema": mat_metadata['schema'],
        "table_name": mat_metadata['annotation_table_name'],
        "valid": True,
        "created": mat_metadata['materialization_time_stamp'],
        "analysisversion_id": mat_metadata['analysis_version']
    }
    analysis_table = AnalysisTable(**analysis_table_dict)
    session.add(analysis_table)\

    try:
        session.commit()
    except Exception as e:
        session.rollback()
        celery_logger.error(e)
    finally:
        session.close()

def create_analysis_sql_uri(sql_uri: str, datastack: str, mat_version: int):
    sql_base_uri = sql_uri.rpartition("/")[0]
    analysis_sql_uri = make_url(
        f"{sql_base_uri}/{datastack}__mat{mat_version}")
    return analysis_sql_uri


def get_analysis_table(aligned_volume: str, datastack: str, table_name: str, mat_version: int = 1):

    anno_db = get_db(aligned_volume)
    schema_name = anno_db.get_table_schema(table_name)

    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, mat_version)
    analysis_engine = create_engine(analysis_sql_uri)

    meta = MetaData()
    meta.reflect(bind=analysis_engine)

    anno_schema = get_schema(schema_name)
    flat_schema = create_flattened_schema(anno_schema)

    if not analysis_engine.dialect.has_table(analysis_engine, table_name):
        annotation_dict = create_table_dict(
            table_name=table_name,
            Schema=flat_schema,
            segmentation_source=None,
            table_metadata=None,
            with_crud_columns=False,
        )
        analysis_table = type(table_name, (Base,), annotation_dict)
    else:
        analysis_table = meta.tables[table_name]

    analysis_engine.dispose()
    return analysis_table

