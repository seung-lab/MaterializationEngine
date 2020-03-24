import datetime
import logging

import cloudvolume
import numpy as np
from annotationframeworkclient.annotationengine import AnnotationClient
from annotationframeworkclient.infoservice import InfoServiceClient
from celery import group

from dynamicannotationdb.annodb_meta import AnnotationMetaDB
from emannotationschemas import models as em_models
from emannotationschemas.models import (AnalysisTable, AnalysisVersion, Base,
                                        format_version_db_uri)
from flask import current_app
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.exc import ProgrammingError

from app import materializationmanager as manager
from app import materialize
from app.celery_worker import celery
from pychunkedgraph.backend import chunkedgraph

SQL_URI = current_app.config['MATERIALIZATION_POSTGRES_URI']
BIGTABLE = current_app.config['BIGTABLE_CONFIG']
PROJECT_ID = BIGTABLE['project_id']
CG_INSTANCE_ID = BIGTABLE['instance_id']
AMDB_INSTANCE_ID = BIGTABLE['amdb_instance_id']
CHUNKGRAPH_TABLE_ID = current_app.config['CHUNKGRAPH_TABLE_ID']
INFOSERVICE_ADDRESS = current_app.config['INFOSERVICE_ENDPOINT']
ANNO_ADDRESS = current_app.config['ANNO_ENDPOINT']

BLACKLIST = ["pni_synapses", "pni_synapses_i2",  "is_chandelier"]


def create_session(sql_uri: str = SQL_URI):
    engine = create_engine(sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50)
    Session = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))
    session = Session()
    return session, engine


def get_missing_tables(session, dataset_name: str, dataset_version: int, server_address: str = None) -> list:
    """Get list of analysis tables from database. If there are blacklisted
    tables they will not be included.

    Arguments:
        dataset_name {str} -- Name of dataset
        analysisversion {int} -- Version of dataset to use

    Returns:
        list -- Tables that appear from versioned dataset
    """
    if server_address is None:
        server_address = ANNO_ADDRESS
    logging.info(f"DATASET VERSION IS {dataset_version}")

    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == dataset_version).all()

    anno_client = AnnotationClient(dataset_name=dataset_name,
                                   server_address=server_address)
    all_tables = anno_client.get_tables()
    missing_tables_info = [t for t in all_tables
                           if (t['table_name'] not in [t.tablename for t in tables])
                           and (t['table_name']) not in BLACKLIST]
    logging.info(missing_tables_info)
    return missing_tables_info


def run_materialization(dataset_name: str, database_version: int,
                        use_latest: bool, server: str = None):
    """Start celery based materialization. A database and version are used as a
    base to update annotations. A series of tasks are chained in sequence passing
    metadata between each task. Requires pickle celery serialization.

    Arguments:
        dataset_name {str} -- Name of dataset to use as a base.
        database_version {int} -- Version of database to use to update from.
    """
    logging.info("STARTING MATERIALIZATION")
    logging.info(f"DATASET_NAME: {dataset_name} | DATABASE_VERSION: {database_version}")
    session, engine = create_session(SQL_URI)
    try:
        if use_latest:
            base_mat_version = (session.query(AnalysisVersion).order_by(AnalysisVersion.version.desc()).first())
        else:
            base_mat_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==database_version).first()
        logging.info(f"CURRENT VERSION: {base_mat_version}")
        version = base_mat_version.version
        ret = (get_materialization_metadata.s(dataset_name, version, base_mat_version, server) |
               create_database_from_template.s() |
               add_analysis_tables.s() |
               materialize_root_ids.s() |
               materialize_annotations.s() |
               materialize_annotations_delta.s()).apply_async()
    except ProgrammingError:
        logging.info("NO TABLE EXISTS")
        version = database_version
        base_mat_version = None
        ret = (get_materialization_metadata.s(dataset_name, version, base_mat_version, server) |
               setup_new_database.s() |
               materialize_root_ids.s() |
               materialize_annotations.s() |
               materialize_annotations_delta.s()).apply_async()



@celery.task(name='process:app.tasks.get_materialization_metadata')
def get_materialization_metadata(dataset_name: str, database_version: int,
                                 base_mat_version=None, server_address: str = None) -> dict:
    """Generate metadata for materialization, returns infoclient data and generates the template
    analysis database version for updating.

    Arguments:
        dataset_name {str} -- Dataset name to use as template.
        database_version {int} -- Version to use as template.

    Keyword Arguments:
        use_latest {bool} -- Use latest version of databaes if True.
                             Gets specific version if false (default: {False})

    Raises:
        info_service_error: Fails to connect to infoservice.

    Returns:
        dict -- Metadata used in proceeding celery tasks.
    """
    logging.info("GET MATERIALIZATION METADATA")

    if server_address is None:
        server_address = INFOSERVICE_ADDRESS
    metadata = {}
    metadata.update(base_mat_version=base_mat_version)
    metadata.update(dataset_name=str(dataset_name),
                    database_version=database_version)
    try:
        info_client = InfoServiceClient(dataset_name=metadata['dataset_name'],
                                        server_address=server_address)
        logging.info(info_client)
        data = info_client.get_dataset_info()
        cg_table_id = data['graphene_source'].rpartition('/')[-1]
        metadata.update(cg_table_id=str(cg_table_id))
    except Exception as info_service_error:
        logging.error(f"Could not connect to infoservice: {info_service_error}")
        raise info_service_error
    logging.info(f"MATERIALIZATION TASK METADATA:{metadata}")
    return metadata


@celery.task(name='process:app.tasks.setup_new_database')
def setup_new_database(metadata: dict) -> dict:
    """Create new analysis database for materialization.

    Arguments:
        dataset_name {str} -- Name of dataset to use as base.

    Keyword Arguments:
        version {int} -- Analysis version number to use (default: {1})

    Returns:
        AnalysisVersion -- AnalysisVersion model of created database.
    """
    logging.info("SETUP NEW DATABASE TASK")

    # populate database metadata
    session, engine = create_session(SQL_URI)
    Base.metadata.create_all(engine)

    base_mat_version = manager.create_new_version(SQL_URI,
                                                  metadata['dataset_name'],
                                                  str(datetime.datetime.utcnow()))
    base_mat_version_db_uri = format_version_db_uri(SQL_URI,
                                                    metadata['dataset_name'],
                                                    base_mat_version.version)

    base_db_name = f"{metadata['dataset_name']}_v{base_mat_version.version}"
    metadata.update(base_db_name=base_db_name)

    with engine.connect() as connection:
        connection.execute("commit")
        logging.info("CONNECTING TO DB....")
        connection.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                  WHERE pid <> pg_backend_pid() AND datname = '{base_db_name}';")
        connection.execute(f"create database {base_db_name}")
        logging.info(f"CREATED NEW DATABASE {base_mat_version}")

    logging.info(base_mat_version_db_uri)


    new_database_session, new_engine = create_session(base_mat_version_db_uri)
    Base.metadata.create_all(new_engine)
    base_mat_version = manager.create_new_version(base_mat_version_db_uri,
                                                  metadata['dataset_name'],
                                                  base_mat_version.time_stamp,
                                                  base_mat_version.version)

    tables = get_missing_tables(session, metadata['dataset_name'], base_mat_version)

    for table in tables:
        if table['schema_name'] != em_models.root_model_name.lower():
            new_analysistable = AnalysisTable(schema=table['schema_name'],
                                              tablename=table['table_name'],
                                              valid=False,
                                              analysisversion_id=base_mat_version.id)
            new_database_session.add(new_analysistable)
            new_database_session.commit()

    metadata['base_mat_version'] = base_mat_version
    # assume new version is what was just created
    metadata['new_mat_version'] = metadata['base_mat_version']
    metadata.update(base_mat_version_db_uri=base_mat_version_db_uri)
    return metadata


@celery.task(name='process:app.tasks.create_database_from_template')
def create_database_from_template(metadata: dict) -> dict:
    """ Create a new database from existing version.

    Arguments:
        metadata {dict} -- Metadata used for materialization

    Returns:
        dict -- Appends metadata to dict to use in subsequent celery tasks in the chain.
    """
    logging.info("CREATING DATABASE FROM TEMPLATE")
    logging.info(f"BASE MATERIALIZATION VERSION: {metadata['base_mat_version']}")
    base_mat_version_db_uri = format_version_db_uri(SQL_URI,
                                                    metadata['dataset_name'],
                                                    metadata['base_mat_version'].version)
    metadata.update(base_mat_version_db_uri=str(base_mat_version_db_uri))

    session, engine = create_session(SQL_URI)
    new_mat_version = manager.create_new_version(sql_uri=SQL_URI,
                                                 dataset=metadata['dataset_name'],
                                                 time_stamp=str(datetime.datetime.utcnow())
                                                 )
    new_mat_version_db_name = f"{metadata['dataset_name']}_v{new_mat_version.version}"
    base_db_name = f"{metadata['dataset_name']}_v{metadata['base_mat_version'].version}"
    new_mat_version_db_uri = format_version_db_uri(SQL_URI,
                                                   metadata['dataset_name'],
                                                   new_mat_version.version)
    metadata['new_mat_version'] = new_mat_version
    metadata['new_mat_version_db_name'] = str(new_mat_version_db_name)
    metadata['new_mat_version_db_uri'] = str(new_mat_version_db_uri)
    logging.info(f"______________________________TEMPLATE TASK METADATA:{metadata}")

    conn = engine.connect()
    conn.execute("commit")
    logging.info("CONNECTING TO DB....")
    conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                  WHERE pid <> pg_backend_pid() AND datname = '{base_db_name}';")
    conn.execute(f"create database {new_mat_version_db_name} TEMPLATE {base_db_name}")

    return metadata


@celery.task(name='process:app.tasks.add_analysis_tables')
def add_analysis_tables(metadata: dict) -> dict:
    logging.info("ADDING MISSING ANALYSIS TABLES")

    session, engine = create_session(SQL_URI)
    tables = session.query(AnalysisTable).filter(
             AnalysisTable.analysisversion == metadata['base_mat_version']).all()
    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            new_analysistable = AnalysisTable(schema=table.schema,
                                              tablename=table.tablename,
                                              valid=False,
                                              analysisversion_id=metadata['new_mat_version'].id)
            session.add(new_analysistable)
            session.commit()
    return metadata


@celery.task(name='process:app.tasks.materialize_root_ids')
def materialize_root_ids(metadata: dict) -> dict:
    logging.info("MATERIALIZNG ROOT IDS")
    base_mat_version_engine = create_engine(metadata['base_mat_version_db_uri'])
    BaseMatVersionSession = sessionmaker(bind=base_mat_version_engine)
    base_mat_version_session = BaseMatVersionSession()
    Base.metadata.create_all(base_mat_version_engine)
    root_model = em_models.make_cell_segment_model(metadata['dataset_name'],
                                                   version=metadata['new_mat_version'].version)
    try:
        prev_max_id = int(base_mat_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
    except Exception as e:
        logging.error(e)
        prev_max_id = 1
    cg = chunkedgraph.ChunkedGraph(table_id=metadata['cg_table_id'])
    max_root_id = materialize.find_max_root_id_before(cg,
                                                      metadata['base_mat_version'].time_stamp,
                                                      2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
                                                      start_id=np.uint64(prev_max_id),
                                                      delta_id=100)
    max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
    multi_args, new_roots, old_roots = materialize.materialize_root_ids_delta(cg_table_id=metadata['cg_table_id'],
                                                                              dataset_name=metadata['dataset_name'],
                                                                              time_stamp=metadata['new_mat_version'].time_stamp,
                                                                              time_stamp_base=metadata['base_mat_version'].time_stamp,
                                                                              min_root_id=max_seg_id,
                                                                              analysisversion=metadata['new_mat_version'],
                                                                              sqlalchemy_database_uri=metadata['new_mat_version_db_uri'],
                                                                              cg_instance_id=CG_INSTANCE_ID)
    subtasks = [materialize_root_ids_subtask.s(args) for args in multi_args]
    results = group(subtasks)()
    metadata['old_roots'] = old_roots
    return metadata


@celery.task(name='process:app.tasks.materialize_annotations')
def materialize_annotations(metadata: dict) -> dict:
    logging.info("MATERIALIZNG ANNOTATIONS")

    session, engine = create_session(SQL_URI)

    missing_tables_info = get_missing_tables(session,
                                             metadata['dataset_name'],
                                             metadata['new_mat_version'])

    logging.info(f"MISSING TABLES: {missing_tables_info}")

    for table_info in missing_tables_info:
        materialized_info = materialize.materialize_all_annotations(metadata['cg_table_id'],
                                                                    metadata['dataset_name'],
                                                                    table_info['schema_name'],
                                                                    table_info['table_name'],
                                                                    analysisversion=metadata['new_mat_version'],
                                                                    time_stamp=metadata['new_mat_version'].time_stamp,
                                                                    cg_instance_id=CG_INSTANCE_ID,
                                                                    sqlalchemy_database_uri=metadata['new_mat_version_db_uri'],
                                                                    block_size=1000)
        process_all_annotations_subtask.delay(materialized_info)
    for table_info in missing_tables_info:
        at = AnalysisTable(schema=table_info['schema_name'],
                           tablename=table_info['table_name'],
                           valid=True,
                           analysisversion=metadata['new_mat_version'])
        session.add(at)
        session.commit()
    return metadata


@celery.task(name='process:app.tasks.materialize_annotations_delta')
def materialize_annotations_delta(metadata: dict) -> dict:
    logging.info("MATERIALIZNG ANNOTATIONS DELTAS")

    session, engine = create_session(SQL_URI)
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == metadata['new_mat_version']).all()
    version_engine = create_engine(metadata['new_mat_version_db_uri'])
    VersionSession = sessionmaker(bind=version_engine)
    version_session = VersionSession()
    version_session.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analysis_user;')
    version_session.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO analysis_user;')

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            delta_info = materialize.materialize_annotations_delta(metadata['cg_table_id'],
                                                                   metadata['dataset_name'],
                                                                   table.tablename,
                                                                   table.schema,
                                                                   metadata['old_roots'],
                                                                   metadata['new_mat_version'],
                                                                   metadata['new_mat_version_db_uri'],
                                                                   cg_instance_id=CG_INSTANCE_ID,)
            materialize_delta_annotation_subtask.delay(delta_info)

    root_model = em_models.make_cell_segment_model(metadata['dataset_name'], version=metadata['new_mat_version'].version)
    version_session.query(root_model).filter(root_model.id.in_(metadata['old_roots'].tolist())).delete(synchronize_session=False)

    version_session.commit()

    new_mat_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==metadata['new_mat_version'].version).first()
    new_mat_version.valid = True
    session.commit()
    return metadata


@celery.task(name='threads:app.tasks.process_all_annotations_task')
def process_all_annotations_subtask(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, \
        table_name, schema_name, version, \
        time_stamp, cg_table_id, serialized_amdb_info, \
        serialized_cg_info, serialized_mm_info, \
        serialized_cv_info, pixel_ratios = args

    amdb = AnnotationMetaDB(**serialized_amdb_info)

    cg = chunkedgraph.ChunkedGraph(**serialized_cg_info)

    cv = cloudvolume.CloudVolume(**serialized_cv_info)
    mm = manager.MaterializationManager(**serialized_mm_info)

    annos_dict = {}
    annos_list = []
    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data_b, bsps = amdb.get_annotation(
            dataset_name, table_name, annotation_id, time_stamp=time_stamp)

        if annotation_data_b is None:
            continue

        deserialized_annotation = mm.deserialize_single_annotation(annotation_data_b,
                                                                   cg, cv,
                                                                   pixel_ratios=pixel_ratios,
                                                                   time_stamp=time_stamp)
        deserialized_annotation['id'] = int(annotation_id)

        if mm.is_sql:
            annos_list.append(deserialized_annotation)
        annos_dict[annotation_id] = deserialized_annotation

    try:
        mm.bulk_insert_annotations(annos_list)
        mm.commit_session()
    except Exception as e:
        logging.error(f"Failed to insert annotations: {e}")


@celery.task(name='threads:app.tasks.materialize_delta_annotation_subtask')
def materialize_delta_annotation_subtask(args):
    """ Helper for materialize_annotations_delta """
    (block, col, time_stamp,  mm_info, cg_info) = args
    cg = chunkedgraph.ChunkedGraph(**cg_info)
    mm = manager.MaterializationManager(**mm_info)
    annos_list = []
    for id_, sup_id in block:
        new_root = cg.get_root(sup_id, time_stamp=time_stamp)
        annos_list.append({
            'id': id_,
            col: int(new_root)
        })
    try:
        mm.bulk_update_annotations(annos_list)
        mm.commit_session()
    except Exception as e:
        logging.error(e)
        logging.error(f"ANNOTATION LIST ERROR:{annos_list}")
        raise Exception(e)


@celery.task(name='threads:app.tasks.materialize_root_ids_subtask')
def materialize_root_ids_subtask(args):
    root_ids, serialized_mm_info = args
    model = em_models.make_cell_segment_model(serialized_mm_info['dataset_name'],
                                              serialized_mm_info['database_version'])
    mm = manager.MaterializationManager(**serialized_mm_info, annotation_model=model)
    annos_dict = {}
    annos_list = []
    for root_id in root_ids:
        ann = {"id": int(root_id)}
        if mm.is_sql:
            annos_list.append(ann)
        else:
            annos_dict[root_id] = ann
    if not mm.is_sql:
        return annos_dict
    else:
        mm.bulk_insert_annotations(annos_list)
        mm.commit_session()
