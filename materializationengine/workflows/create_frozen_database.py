import datetime
import logging
import re
import time
from collections import OrderedDict
from typing import Iterator, Dict, Any, Optional
from typing import List
from io import TextIOBase
import pandas as pd
import psycopg2
from celery import chain, chord
from celery.utils.log import get_task_logger
from dynamicannotationdb.models import AnnoMetadata
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import create_table_dict, make_flat_model
from flask import current_app
from materializationengine.celery_worker import celery
from materializationengine.database import (create_session, get_db,
                                            sqlalchemy_cache, get_sql_url_params)
from materializationengine.index_manager import index_cache
from materializationengine.models import AnalysisTable, AnalysisVersion, Base
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                get_materialization_info,
                                                query_id_range)
from materializationengine.utils import (create_annotation_model,
                                         create_segmentation_model)
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base

celery_logger = get_task_logger(__name__)

SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


@celery.task(name="process:create_versioned_materialization_workflow",
             bind=True,
             acks_late=True,)
def create_versioned_materialization_workflow(self, datastack_info: dict):
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
    mat_info = get_materialization_info(datastack_info, new_version_number)
    database = create_analysis_database(datastack_info, new_version_number),
    materialized_tables = create_analysis_tables(datastack_info, new_version_number)

    frozen_workflow = []
    for mat_metadata in mat_info:
        if mat_metadata:
            # supervoxel_chunks = chunk_supervoxel_ids_task(mat_metadata)
            process_chunks_workflow = chain(
                drop_indexes.si(mat_metadata),
                copy_data_from_live_table.si(mat_metadata),
                # chord([
                #     chain(insert_annotation_data.si(chunk, mat_metadata)) for chunk in supervoxel_chunks], fin.si()),
                update_analysis_metadata.si(mat_metadata),
                add_indexes.si(mat_metadata),
                check_tables.si(mat_metadata))
            frozen_workflow.append(process_chunks_workflow)
            
    workflow = chord(frozen_workflow, fin.s())
    status = workflow.apply_async()
    return status


def create_new_version(datastack_info: dict):
    aligned_volume_name = datastack_info['aligned_volume']['name']
    datastack = datastack_info.get('datastack')

    table_objects = [
        AnalysisVersion.__tablename__,
        AnalysisTable.__tablename__,
    ]

    mat_engine = sqlalchemy_cache.get_engine(aligned_volume_name)

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

    expiration_date = datetime.datetime.utcnow() + datetime.timedelta(days=5)

    analysisversion = AnalysisVersion(datastack=datastack,
                                      time_stamp=time_stamp,
                                      version=new_version_number,
                                      valid=False,
                                      expires_on=expiration_date)
    session.add(analysisversion)
    session.commit()
    return new_version_number


def create_analysis_database(datastack_info: dict, analysis_version: int) -> str:
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

    aligned_volume = datastack_info['aligned_volume']['name']
    datastack = datastack_info['datastack']

    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    analysis_sql_uri = create_analysis_sql_uri(
        str(sql_uri), datastack, analysis_version)

    engine = sqlalchemy_cache.get_engine(aligned_volume)

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
                        AND datname = 'postgres';"
            )
            connection.execute(
                f"CREATE DATABASE {analysis_sql_uri.database} \
                                TEMPLATE postgres")

       
    mat_engine = sqlalchemy_cache.get_engine(analysis_sql_uri.database)
    with mat_engine.connect() as mat_connection:
            # mat_engine.execute(
            #     "CREATE EXTENSION IF NOT EXISTS postgres_fdw;")

            # mat_engine.execute(
            #     f"CREATE SERVER foreign_server \
            #         FOREIGN DATA WRAPPER postgres_fdw \
            #             OPTIONS(host '{sql_uri.host}', port '{sql_uri.port}', dbname '{sql_uri.database}', sslmode 'disable');"
            # )
            # mat_engine.execute(
            #     f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER  \
            #          SERVER foreign_server \
            #               OPTIONS (user '{sql_uri.username}', password '{sql_uri.password}');"
            # )
            # mat_connection.execute(
            #     f"CREATE SCHEMA IF NOT EXISTS foreign_live_schema;")
            
            # mat_connection.execute(
            #     f"IMPORT FOREIGN SCHEMA public FROM SERVER foreign_server INTO foreign_live_schema;")


            result = mat_connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database \
                    WHERE datname = '{analysis_sql_uri.database}'"
            )
    engine.dispose()
    mat_engine.dispose()
    return True


def create_analysis_tables(datastack_info: dict, analysis_version: int):
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
    aligned_volume = datastack_info['aligned_volume']['name']
    datastack = datastack_info['datastack']
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
    return True

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

    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.get_engine(aligned_volume)
    
    # build table models
    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=False)
    SegmentationModel = create_segmentation_model(mat_metadata)
    analysis_table = get_analysis_table(
        aligned_volume, datastack, annotation_table_name, analysis_version)
    
    query_columns = []
    for col in AnnotationModel.__table__.columns:
        query_columns.append(col)
    for col in SegmentationModel.__table__.columns:
        if not col.name == 'id':
            query_columns.append(col)
    
    chunked_id_query = query_id_range(AnnotationModel.id, chunk[0], chunk[1])

    anno_ids = session.query(AnnotationModel.id).filter(chunked_id_query).filter(AnnotationModel.valid == True)
    query = session.query(*query_columns).join(SegmentationModel).\
                        filter(SegmentationModel.id == AnnotationModel.id).\
                        filter(SegmentationModel.id.in_(anno_ids))
    data = query.all()
    mat_df = pd.DataFrame(data)
    mat_df = mat_df.to_dict(orient="records")
   
    analysys_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version)
    analysis_session, analysis_engine = create_session(analysys_sql_uri)

    try:
        analysis_engine.execute(
            analysis_table.insert(),
            [data for data in mat_df]
        )
    except Exception as e:
        celery_logger.error(e)
        analysis_session.rollback()
    finally:
        analysis_session.close()
        analysis_engine.dispose()
        session.close()
        engine.dispose()



@celery.task(name="process:copy_data_from_live_table",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def copy_data_from_live_table(self, mat_metadata: dict):

    aligned_volume = mat_metadata['aligned_volume']
    analysis_version = mat_metadata['analysis_version']
    annotation_table_name = mat_metadata['annotation_table_name']
    schema = mat_metadata['schema']

    datastack = mat_metadata['datastack']
    # create dynamic sql_uri
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]

    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version)

    # get schema and match column order for sql query
    anno_schema = get_schema(schema)
    flat_schema = create_flattened_schema(anno_schema)

    ordered_model_columns = create_table_dict(
        table_name=annotation_table_name,
        Schema=flat_schema,
        segmentation_source=None,
        table_metadata=None,
        with_crud_columns=False,
    )

    AnnotationModel = create_annotation_model(
        mat_metadata, with_crud_columns=False)
    SegmentationModel = create_segmentation_model(mat_metadata)

    query_columns = {}
    crud_columns = ['created', 'deleted', 'superceded_id']
    for col in AnnotationModel.__table__.columns:
        if col.name not in crud_columns:
            query_columns[col.name] = col
    for col in SegmentationModel.__table__.columns:
        if not col.name == 'id':
            query_columns[col.name] = col

    sorted_columns = OrderedDict([(key, query_columns[key])
                                  for key in ordered_model_columns if key in query_columns.keys()])
    sorted_columns_list = list(sorted_columns.values())

    # generate db url for psycopg2
    live_db_uri = get_sql_url_params(sql_uri)
    mat_db_uri = get_sql_url_params(analysis_sql_uri)

    # create psycopg2 db connections
    live_engine = psycopg2.connect(**live_db_uri)
    mat_engine = psycopg2.connect(**mat_db_uri)

    mat_cur = mat_engine.cursor(name=f"{annotation_table_name}_mat_cursor")
    session, __ = create_session(sql_uri)

    start = time.time()
    try:
        # generate sql string query
        query = session.query(*sorted_columns_list).join(SegmentationModel).\
            filter(SegmentationModel.id == AnnotationModel.id).\
            filter(AnnotationModel.valid == True)
        statement = str(query)

        # drop ST_AsEWKB prefix and parentheses of geometry columns
        geo_prefix = re.sub('ST_AsEWKB', '', re.sub(
            r'\((.*?)\)', r'(\1)', statement))
        sql_query_string = re.sub('[()]', '', geo_prefix)

        with live_engine.cursor(name=f'{annotation_table_name}_live_cursor') as live_cur:
            # input = StringIO()
            live_cur.itersize = 100_000
            live_cur.execute(sql_query_string)
            while True:
                rows = live_cur.fetchmany(live_cur.itersize)
                if not rows:
                    print("finished")
                    break

                data_iterator = StringIteratorIO((','.join(map(clean_csv_value,(
                                                        row
                                                        ))) + '\n'
                                                        for row in rows
                                                    ))            

                mat_cur.copy_from(data_iterator, annotation_table_name, sep=',')
                mat_engine.commit()
            # live_cur.copy_expert(f'COPY ({sql_query_string}) TO STDOUT', input)
            # input.seek(0)
            # mat_cur.copy_expert(
            #     f'COPY {annotation_table_name} FROM STDOUT', input)
            # mat_engine.commit()

    except Exception as e:
        session.rollback()
        celery_logger.error(e)
    finally:
        mat_cur.close()
        live_engine.close()
        mat_engine.close()
        session.close()
    total_time = time.time() - start
    celery_logger.info(f"TOTAL QUERY RUN TIME: {total_time}")

    return f"TOTAL QUERY RUN TIME: {total_time}"

class StringIteratorIO(TextIOBase):
    """https://stackoverflow.com/a/12604375/2221667
    """
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buffer = ''

    def readable(self) -> bool:
        return True

    def _read_single_line(self, n: Optional[int] = None) -> str:
        while not self._buffer:
            try:
                self._buffer = next(self._iter)
            except StopIteration:
                break
        ret = self._buffer[:n]
        self._buffer = self._buffer[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read_single_line()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read_single_line(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

@celery.task(name="process:update_analysis_metadata",
             bind=True,
             acks_late=True,)
def update_analysis_metadata(self, mat_metadata: dict):
    aligned_volume = mat_metadata['aligned_volume']
    version = mat_metadata['analysis_version']
    session = sqlalchemy_cache.get(aligned_volume)

    version_id = session.query(AnalysisVersion.id).filter(
        AnalysisVersion.version == version).first()

    analysis_table = AnalysisTable(aligned_volume=aligned_volume,
                                   schema=mat_metadata['schema'],
                                   table_name=mat_metadata['annotation_table_name'],
                                   valid=True,
                                   created=mat_metadata['materialization_time_stamp'],
                                   analysisversion_id=version_id)

    try:
        session.add(analysis_table)
        session.commit()
    except Exception as e:
        session.rollback()
        celery_logger.error(e)
        raise e
    finally:
        session.close()


@celery.task(name="process:check_tables",
             bind=True,
             acks_late=True,)
def check_tables(self, mat_metadata: dict):
    aligned_volume = mat_metadata['aligned_volume']
    analysis_version = mat_metadata['analysis_version']
    table_count = mat_metadata['table_count']

    session = sqlalchemy_cache.get(aligned_volume)
    version_id = session.query(AnalysisVersion.id).filter(AnalysisVersion.version==analysis_version).first()
    num_tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion_id==version_id).\
        filter(AnalysisTable.valid==True).count()
    if num_tables == table_count:
        validity = session.query(AnalysisVersion).filter(AnalysisVersion.version==analysis_version).first()
        validity.valid = True
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            celery_logger.error(e)
        finally:
            session.close()
    else:
        return None


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

@celery.task(name="process:drop_indexes",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def drop_indexes(self, mat_metadata: dict):
    drop_indexes = mat_metadata.get('drop_indexes', None)
    if drop_indexes:
        analysis_version = mat_metadata.get('analysis_version', None)
        datastack = mat_metadata['datastack']
        annotation_table_name = mat_metadata.get('annotation_table_name', None)

        analysis_sql_uri = create_analysis_sql_uri(
            SQL_URI_CONFIG, datastack, analysis_version)

        analysis_session, analysis_engine = create_session(analysis_sql_uri)
        index_cache.drop_table_indexes(annotation_table_name, analysis_engine)
        analysis_session.close()
        analysis_engine.dispose()
        return "INDEXES DROPPED"  
    return "No indexes dropped"


@celery.task(name="process:add_indexes",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def add_indexes(self, mat_metadata: dict):
    drop_indexes = mat_metadata.get('drop_indexes', None)
    if drop_indexes:
        analysis_version = mat_metadata.get('analysis_version', None)
        datastack = mat_metadata['datastack']
        analysis_sql_uri = create_analysis_sql_uri(
            SQL_URI_CONFIG, datastack, analysis_version)

        analysis_session, analysis_engine = create_session(analysis_sql_uri)


        annotation_table_name = mat_metadata.get('annotation_table_name', None)
        schema = mat_metadata.get('schema', None)

        model = make_flat_model(annotation_table_name, schema)

        index_cache.add_indexes(annotation_table_name, model, analysis_engine, is_flat=True)

        analysis_session.close()
        analysis_engine.dispose()
        return "Indexes Added"  
    return "Indexes already exist"
