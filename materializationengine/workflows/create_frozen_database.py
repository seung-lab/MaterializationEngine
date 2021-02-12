import datetime
from collections import OrderedDict
from typing import List
import pandas as pd
from celery import chain, chord
from celery.utils.log import get_task_logger
from dynamicannotationdb.models import AnnoMetadata
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import create_table_dict, make_flat_model
from materializationengine.celery_init import celery
from materializationengine.database import (create_session, get_db,
                                            reflect_tables, sqlalchemy_cache)
from materializationengine.index_manager import index_cache
from materializationengine.models import (AnalysisTable, 
                                          AnalysisVersion,
                                          MaterializedMetadata,
                                          Base)
from materializationengine.errors import IndexMatchError                                          
from materializationengine.shared_tasks import (fin, get_materialization_info,
                                                query_id_range)
from materializationengine.utils import (create_annotation_model,
                                         create_segmentation_model,
                                         get_config_param)
from psycopg2 import sql
from sqlalchemy import MetaData, create_engine, func
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import reflection

celery_logger = get_task_logger(__name__)

# SQL_URI_CONFIG = get_config_param('SQLALCHEMY_DATABASE_URI')


@celery.task(name="process:create_versioned_materialization_workflow")
def create_versioned_materialization_workflow(datastack_info: dict):
    """Create a timelocked database of materialization annotations
    and associated segmentation data.

    Parameters
    ----------
    aligned_volume : str
        [description]
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    new_version_number = create_new_version(datastack_info, materialization_time_stamp)
    mat_info = get_materialization_info(datastack_info, new_version_number, materialization_time_stamp)
    
    setup_versioned_database = chain(
        create_analysis_database.si(datastack_info, new_version_number),
        create_materialized_metadata.si(datastack_info, new_version_number, materialization_time_stamp),
        update_table_metadata.si(mat_info),
        drop_tables.si(datastack_info, new_version_number)) #TODO drop tables should come after update_metadata, break out 

    frozen_workflow = []
    for mat_metadata in mat_info:
        process_chunks_workflow = chain(
            # drop_indices.si(mat_metadata),
            merge_tables.si(mat_metadata),
            add_indices.si(mat_metadata))
        frozen_workflow.append(process_chunks_workflow)
            
    workflow = chain(setup_versioned_database, chord(frozen_workflow, fin.s()), check_tables.si(mat_info, new_version_number))
    status = workflow.apply_async()
    return True


def create_new_version(datastack_info: dict, 
                       materialization_time_stamp: datetime.datetime.utcnow,
                       days_to_expire: int = 5):
    """Create new versioned database row in the anaylsis_version table.
    Sets the expiration date for the database.

    Args:
        datastack_info (dict): [description]
        materialization_time_stamp (datetime.datetime.utcnow): [description]
        days_to_expire (int, optional): [description]. Defaults to 5.

    Returns:
        [int]: version number of materialzied database
    """
    aligned_volume = datastack_info['aligned_volume']['name']
    datastack = datastack_info.get('datastack')
    database_expires = datastack_info['database_expires']
    
    table_objects = [
        AnalysisVersion.__tablename__,
        AnalysisTable.__tablename__,
    ]
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

    session, engine = create_session(sql_uri)
    
    # create analysis metadata table if not exists
    for table in table_objects:
        if not engine.dialect.has_table(engine, table):
            Base.metadata.tables[table].create(bind=engine)

    top_version = session.query(func.max(AnalysisVersion.version)).scalar()

    if top_version is None:
        new_version_number = 1
    else:
        new_version_number = top_version + 1
    if database_expires:
        expiration_date = materialization_time_stamp + datetime.timedelta(days=days_to_expire)
    else:
        expiration_date = None

    analysisversion = AnalysisVersion(datastack=datastack,
                                      time_stamp=materialization_time_stamp,
                                      version=new_version_number,
                                      valid=False,
                                      expires_on=expiration_date)
    try:                                      
        session.add(analysisversion)
        session.commit()
    except Exception as e:
        session.rollback()
        celery_logger.error(e)
    finally:
        session.close()
        engine.dispose()
    return new_version_number

@celery.task(name="process:create_analysis_database",
             bind=True,
             acks_late=True,)
def create_analysis_database(self, datastack_info: dict, analysis_version: int) -> str:
    """Copies live database to new versioned database for materializied annotations.

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
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    analysis_sql_uri = create_analysis_sql_uri(
        str(sql_uri), datastack, analysis_version)

    engine = create_engine(sql_uri, isolation_level='AUTOCOMMIT', pool_pre_ping=True)

    connection = engine.connect()
    connection.connection.set_session(autocommit=True)

    result = connection.execute(
        f"SELECT 1 FROM pg_catalog.pg_database \
                WHERE datname = '{analysis_sql_uri.database}'"
    )
    if not result.fetchone():
        # create new database from template_postgis database
        celery_logger.info(
            f"Creating new materialized database {analysis_sql_uri.database}")

        connection.execute(
            f"""SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = '{aligned_volume}'
                AND pid <> pg_backend_pid();""")
        try:
            connection.execute(
                f"""CREATE DATABASE {analysis_sql_uri.database} 
                    WITH TEMPLATE {aligned_volume}"""
            )
        except Exception as e:
            celery_logger.error(f"Connection was lost: {e}")
        # lets reconnect 
        connection = engine.connect()
        # check if database exists
        connection.execute(
                f"SELECT 1 FROM pg_catalog.pg_database \
                WHERE datname = '{analysis_sql_uri.database}'")

        if connection is not None:
            connection.close()
    engine.dispose()
    sqlalchemy_cache.invalidate_cache()
    return True

@celery.task(name="process:create_materialized_metadata",
             bind=True,
             acks_late=True,)
def create_materialized_metadata(self, datastack_info: dict, 
                                 analysis_version: int,
                                 materialization_time_stamp: datetime.datetime.utcnow):
    """Creates a metadata table in a materialized database.Reads row counts
    from annotation tables copied to the materialzied database. Inserts row count 
    and table info into the metadata table.

    Parameters
    ----------
    aligned_volume : str
        aligned volume name
    mat_sql_uri : str
        target database sql url to use

    Returns
    -------
    True
        If Metadata table was created and table info was inserted.

    Raises
    ------
    database_error
        sqlalchemy connection error
    """
    aligned_volume = datastack_info['aligned_volume']['name']
    datastack = datastack_info['datastack']
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version)
    
    session, engine = create_session(sql_uri)
    analysis_session, analysis_engine = create_session(analysis_sql_uri)
    
    try:
        mat_table = MaterializedMetadata()
        mat_table.__table__.create(bind=analysis_engine) # pylint: disable=maybe-no-member
    except Exception as e:
        celery_logger.error(f"Materialized Metadata table creation failed {e}")

    mat_client = get_db(f"{datastack}__mat{analysis_version}")

    tables = session.query(AnnoMetadata).all()
    try:   
        for table in tables:
            # only create table if marked as valid in the metadata table
            if table.valid:
                table_name = table.table_name
                schema_type = session.query(AnnoMetadata.schema_type).\
                    filter(AnnoMetadata.table_name == table_name).one()


                valid_row_count = mat_client._get_table_row_count(table_name, filter_valid=True)
                celery_logger.info(f"Row count {valid_row_count}")
                mat_metadata = MaterializedMetadata(schema=schema_type[0],
                                                    table_name=table_name,
                                                    row_count=valid_row_count,
                                                    materialized_timestamp=materialization_time_stamp)
                analysis_session.add(mat_metadata)
                analysis_session.commit()
    except Exception as database_error:
        analysis_session.rollback()
        session.rollback()
        celery_logger.error(database_error)            
    finally:
        session.close()
        engine.dispose()
        mat_client.cached_session.close()
        analysis_session.close()
        analysis_engine.dispose()
    return True

@celery.task(name="process:update_table_metadata",
             bind=True,
             acks_late=True,)
def update_table_metadata(self, mat_info: List[dict]):
    """Update 'analysistables' with all the tables
    to be created in the frozen materialized database. 

    Args:
        mat_info (List[dict]): list of dicts containing table metadata

    Returns:
        list: list of tables that were added to 'analysistables' 
    """
    aligned_volume = mat_info[0]['aligned_volume']
    version = mat_info[0]['analysis_version']

    session = sqlalchemy_cache.get(aligned_volume)()

    tables = []
    for mat_metadata in mat_info:
        version_id = session.query(AnalysisVersion.id).filter(
            AnalysisVersion.version == version).first()
        analysis_table = AnalysisTable(aligned_volume=aligned_volume,
                                    schema=mat_metadata['schema'],
                                    table_name=mat_metadata['annotation_table_name'],
                                    valid=False,
                                    created=mat_metadata['materialization_time_stamp'],
                                    analysisversion_id=version_id)
        tables.append(analysis_table.table_name)
        session.add(analysis_table)
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        celery_logger.error(e)
    finally:
        session.close()
    return tables

@celery.task(name="process:drop_tables",
             bind=True,
             acks_late=True,)
def drop_tables(self, datastack_info: dict, analysis_version: int):
    """Drop all tables that dont match valid in the live 'aligned_volume' database
    as well as tables that were copied from the live table that are not needed in
    the frozen version (e.g. metadata tables).

    Args:
        datastack_info (dict): datastack info for the aligned_volume from the infoservice
        analysis_version (int): materialized verison number

    Raises:
        e: [description]

    Returns:
        [type]: [description]
    """
    aligned_volume = datastack_info['aligned_volume']['name']
    datastack = datastack_info['datastack']
    pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]

    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version)
    
    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.get_engine(aligned_volume)   
    
    mat_engine = create_engine(analysis_sql_uri)

    mat_inspector = reflection.Inspector.from_engine(mat_engine)
    mat_table_names = mat_inspector.get_table_names()
    mat_table_names.remove('materializedmetadata') 

    anno_tables = session.query(AnnoMetadata).filter(AnnoMetadata.valid==True).all()
        
    annotation_tables = [table.__dict__['table_name'] for table in anno_tables]
    segmentation_tables = [f"{anno_table}__{pcg_table_name}" for anno_table in annotation_tables]

    filtered_tables = annotation_tables + segmentation_tables
        
    tables_to_drop = set(mat_table_names) - set(filtered_tables)
    tables_to_drop.remove('spatial_ref_sys') # keep postgis spatial info table

    try:    
        connection = mat_engine.connect()
        for table in tables_to_drop:
            drop_statement = f"DROP TABLE {table} CASCADE"
            connection.execute(drop_statement)
    except Exception as e:
        celery_logger.error(e)
        raise e
    finally:
        connection.close()
        session.close()
        engine.dispose()
        mat_engine.dispose()

    return {f"Tables dropped {tables_to_drop}"}


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
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
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


@celery.task(name="process:merge_tables",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def merge_tables(self, mat_metadata: dict):
    """Merge all the annotation and segmentation rows into a new table that are
    flagged as valid. Drop the original split tables after inserting all the rows
    into the new table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume from the infoservice
        analysis_version (int): materialized verison number

    Raises:
        e: [description]

    Returns:
        [type]: [description]
    """
    analysis_version = mat_metadata['analysis_version']
    annotation_table_name = mat_metadata['annotation_table_name']
    segmentation_table_name = mat_metadata['segmentation_table_name']
    temp_table_name = mat_metadata['temp_mat_table_name']
    schema = mat_metadata['schema']
    datastack = mat_metadata['datastack']
    
    # create dynamic sql_uri
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
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

    sorted_columns = OrderedDict([(key, query_columns[key]) for key in ordered_model_columns if key in query_columns.keys()])
    sorted_columns_list = list(sorted_columns.values())            
    columns = ([f"{col.table}.{col.name}" for col in sorted_columns_list])
   
    mat_session, mat_engine = create_session(analysis_sql_uri)

    query = f"""
        SELECT 
            {", ".join(str(col) for col in columns)}
        FROM 
            {AnnotationModel.__table__.name}
        JOIN 
            {SegmentationModel.__table__.name}
            ON {AnnotationModel.id} = {SegmentationModel.id}
        WHERE
            {AnnotationModel.id} = {SegmentationModel.id}
        AND {AnnotationModel.valid} = true

    """
    
    try:
        mat_db_connection = mat_engine.connect()
        with mat_db_connection.begin():
            insert_query = mat_db_connection.execute(f"CREATE TABLE {temp_table_name} AS ({query});")  
            row_count = insert_query.rowcount     
            drop_query = mat_db_connection.execute(f"DROP TABLE {annotation_table_name}, {segmentation_table_name};")
            alter_query = mat_db_connection.execute(f"ALTER TABLE {temp_table_name} RENAME TO {annotation_table_name};")
        mat_session.close()
        mat_engine.dispose()
        
        return f"Number of rows copied: {row_count}"
    except Exception as e:
        celery_logger.error(e)
        raise(e)
     

def insert_chunked_data(annotation_table_name: str, sql_statement: str, cur, engine, next_key: int, batch_size: int=100_000):
    pagination_query = f"""AND 
                {annotation_table_name}.id > {next_key} 
            ORDER BY {annotation_table_name}.id ASC 
            LIMIT {batch_size} RETURNING  {annotation_table_name}.id"""
    insert_statement = sql.SQL(sql_statement + pagination_query)

    try:
        cur.execute(insert_statement)
        engine.commit()
    except Exception as e:
        celery_logger.error(e)
    
    results = cur.fetchmany(batch_size)

    # If there were less than the limit then it is the last page of data
    if len(results) < batch_size:
        return  
    else:
    # Find highest returned uid in results to get next key
        next_key = results[-1][0]
        return insert_chunked_data(annotation_table_name, sql_statement, cur, engine, next_key)


@celery.task(name="process:check_tables",
             bind=True,
             acks_late=True,)
def check_tables(self, mat_info: list, analysis_version: int):
    """Check if each materialized table has the same number of rows as 
    the aligned volumes tables in the live databse that are set as valid. 
    If row numbers match, set the validity of both the analysis tables as well
    as the analysis version (materialzied database) as True.

    Args:
        mat_info (list): list of dicts containing metadata for each materialzied table
        analysis_version (int): the materialized version number

    Returns:
        [str]: [description]
    """
    aligned_volume = mat_info[0]['aligned_volume'] # get aligned_volume name from datastack
    table_count = mat_info[0]['table_count']
    analysis_database = mat_info[0]['analysis_database']

    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.get_engine(aligned_volume)
    mat_session = sqlalchemy_cache.get(analysis_database)
    mat_engine = sqlalchemy_cache.get_engine(analysis_database)
    mat_client = get_db(analysis_database)
    versioned_database = session.query(AnalysisVersion).filter(AnalysisVersion.version==analysis_version).one()

    valid_row_counts = 0
    for mat_metadata in mat_info:
        annotation_table_name = mat_metadata['annotation_table_name']
     
        live_table_row_count = mat_session.query(MaterializedMetadata.row_count).\
            filter(MaterializedMetadata.table_name==annotation_table_name).one()
        mat_row_count = mat_client._get_table_row_count(annotation_table_name)

        celery_logger.info(f"ROW COUNTS: {live_table_row_count[0]} {mat_row_count}")

        if live_table_row_count[0] == mat_row_count:
            celery_logger.info(f"{annotation_table_name} row counts match")
            schema = mat_metadata['schema']
            anno_model = make_flat_model(annotation_table_name, schema)
            live_mapped_indexes = index_cache.get_index_from_model(anno_model)
            mat_mapped_indexes = index_cache.get_table_indices(annotation_table_name, mat_engine)
            
            if live_mapped_indexes == mat_mapped_indexes:

                celery_logger.info(f"Indexes matches: {live_mapped_indexes} {mat_mapped_indexes}")
                
                table_validity = session.query(AnalysisTable).filter(AnalysisTable.analysisversion_id==versioned_database.id).\
                    filter(AnalysisTable.table_name==annotation_table_name).one()
                table_validity.valid = True
                valid_row_counts += 1
            else:
                raise IndexMatchError(f"Indexes did not match: annotation indexes {live_mapped_indexes}; materialzied indexes {mat_mapped_indexes}")         

    celery_logger.info(f"Valid tables {valid_row_counts}, Mat tables {table_count}")

    if valid_row_counts == table_count:
        versioned_database.valid = True
    try:
        session.commit()
        return f"All materialized tables match valid row number from live tables"
    except Exception as e:
        session.rollback()
        celery_logger.error(e)
    finally:
        session.close()
        mat_client.cached_session.close()
        mat_session.close()
        engine.dispose()
        mat_engine.dispose()

def create_analysis_sql_uri(sql_uri: str, datastack: str, mat_version: int):
    sql_base_uri = sql_uri.rpartition("/")[0]
    analysis_sql_uri = make_url(
        f"{sql_base_uri}/{datastack}__mat{mat_version}")
    return analysis_sql_uri


def get_analysis_table(aligned_volume: str, datastack: str, table_name: str, mat_version: int = 1):

    anno_db = get_db(aligned_volume)
    schema_name = anno_db.get_table_schema(table_name)
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
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

@celery.task(name="process:drop_indices",
             bind=True,
             acks_late=True)
def drop_indices(self, mat_metadata: dict):
    add_indices = mat_metadata.get('add_indices', False)
    if add_indices:
        analysis_version = mat_metadata.get('analysis_version', None)
        datastack = mat_metadata['datastack']
        temp_mat_table_name = mat_metadata['temp_mat_table_name']
        SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
        analysis_sql_uri = create_analysis_sql_uri(
            SQL_URI_CONFIG, datastack, analysis_version)

        analysis_session, analysis_engine = create_session(analysis_sql_uri)
        index_cache.drop_table_indices(temp_mat_table_name, analysis_engine)
        analysis_session.close()
        analysis_engine.dispose()
        return "Indices DROPPED"  
    return "No indices dropped"


@celery.task(name="process:add_indices",
             bind=True)
def add_indices(self, mat_metadata: dict):
    add_indices = mat_metadata.get('add_indices', False)
    if add_indices:
        analysis_version = mat_metadata.get('analysis_version', None)
        datastack = mat_metadata['datastack']
        SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")        
        analysis_sql_uri = create_analysis_sql_uri(
            SQL_URI_CONFIG, datastack, analysis_version)

        analysis_session, analysis_engine = create_session(analysis_sql_uri)


        annotation_table_name = mat_metadata.get('annotation_table_name', None)
        schema = mat_metadata.get('schema', None)

        model = make_flat_model(annotation_table_name, schema)

        index_cache.add_indices(annotation_table_name, model, analysis_engine, is_flat=True)

        analysis_session.close()
        analysis_engine.dispose()
        return "Indices Added"  
    return "Indices already exist"
