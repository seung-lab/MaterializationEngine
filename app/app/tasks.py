import time
import random
import numpy as np
import cloudvolume
from app import materialize
from celery import Task
from celery import group, chord, chain
from app.schemas import MaterializationSchema, AnalysisVersionSchema
from app import materializationmanager
from annotationframeworkclient.annotationengine import AnnotationClient
from dynamicannotationdb.annodb_meta import AnnotationMetaDB
from annotationframeworkclient.infoservice import InfoServiceClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql import func
from pychunkedgraph.backend import chunkedgraph
from emannotationschemas.models import format_version_db_uri, Base
from emannotationschemas.models import AnalysisVersion, AnalysisTable
from emannotationschemas import models as em_models
from emannotationschemas.base import flatten_dict
from emannotationschemas import get_schema
from app.celery_worker import celery
from celery.concurrency import eventlet
from flask import current_app
import datetime
import logging

# logging.debug(f"Celery Settings: {current_app.config}")

SQL_URI = current_app.config['MATERIALIZATION_POSTGRES_URI']
BIGTABLE = current_app.config['BIGTABLE_CONFIG']
CG_TABLE = BIGTABLE['instance_id']
DATASET = BIGTABLE['project_id']
CG_INSTANCE_ID = BIGTABLE['instance_id']
AMDB_INSTANCE_ID = BIGTABLE['amdb_instance_id']
CHUNKGRAPH_TABLE_ID = current_app.config['CHUNKGRAPH_TABLE_ID']

engine = create_engine(SQL_URI, pool_recycle=3600, pool_size=20, max_overflow=50)
Session = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))
session = Session()
Base.metadata.create_all(engine)

BLACKLIST = ["pni_synapses", "pni_synapses_i2",  "is_chandelier"]

@celery.task(name='process:app.tasks.test_add')
def test_celery(x,y):
    return x + y

class SqlAlchemyTask(Task):
    """An abstract Celery Task class that ensures that the connection the the
    database is closed on task completion"""

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        if session is not None:
            Session.remove()

def get_missing_tables(dataset_name: str, analysisversion: int) -> list:    
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    
    anno_client = AnnotationClient(dataset_name=dataset_name)
    all_tables = anno_client.get_tables()
    missing_tables_info = [t for t in all_tables 
                           if (t['table_name'] not in [t.tablename for t in tables]) 
                           and (t['table_name']) not in BLACKLIST]
    return missing_tables_info

def run_materialization(dataset_name, database_version):
    logging.info(f"DATASET_NAME: {dataset_name} | DATABASE_VERSOIN: {database_version}")
    ret = (get_materialization_metadata.s(dataset_name, database_version) | create_database_from_template.s() | materialize_root_ids.s() | materialize_annotations.s() | materialize_annotations_delta.s()).apply_async()
    if ret.status == u'SUCCESS':
        return ("result:", ret.get())

@celery.task(base=SqlAlchemyTask, name='process:app.tasks.get_materialization_metadata')   
def get_materialization_metadata(dataset_name, database_version, auto=False): 
    if auto:
        base_version_number = (session.query(AnalysisVersion)
                    .order_by(AnalysisVersion.version.desc())
                    .first())
    else:
        base_version_number = database_version

    base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==base_version_number).first()
    logging.info(f"BASE URI :{SQL_URI} \
                DATASAET:{dataset_name} \
                VERSION: {base_version} \
                ")

    base_db_name = f'{dataset_name}_v{base_version_number}'
    base_version_db_uri = format_version_db_uri(SQL_URI, dataset_name,  base_version_number)
    try:
        info_client  = InfoServiceClient(dataset_name=dataset_name)
        logging.info(info_client)
        data = info_client.get_dataset_info()
        cg_table_id = data['graphene_source'].rpartition('/')[-1]
        metadata = {'dataset_name': dataset_name,
                'base_version': base_version_number,
                'base_version_db_uri': base_version_db_uri,
                'base_db_name': base_db_name,
                'cg_table_id': cg_table_id,
                }
        logging.info(f"METADATA:{metadata}")

    except Exception as e:
        logging.error(f"Could not connect to infoservice: {e}") 
        raise e
   
    return metadata

@celery.task(base=SqlAlchemyTask, name='process:app.tasks.create_database_from_template')   
def create_database_from_template(metadata: dict):
    dataset_name = metadata['dataset_name']
    new_version = materializationmanager.create_new_version(SQL_URI, dataset_name, str(datetime.datetime.utcnow()))
    new_version_db_name = f'{dataset_name}_v{new_version.version}'
    new_version_db_uri = format_version_db_uri(SQL_URI, dataset_name, new_version)

    metadata['new_version'] = new_version
    metadata['new_version_db_name'] = new_version_db_name
    metadata['new_version_db_uri'] = new_version_db_uri

    conn = engine.connect()
    conn.execute("commit")
    conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{metadata['base_db_name']}';")
    conn.execute(f"create database {new_version_db_name} TEMPLATE {metadata['base_db_name']}")
    logging.info("Connecting....")

    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == metadata['base_version']).all()

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            new_analysistable = AnalysisTable(schema=table.schema,
                                              tablename=table.tablename,
                                              valid=False,
                                              analysisversion_id=new_version.id)
            session.add(new_analysistable)
            session.commit()
    return metadata

@celery.task(base=SqlAlchemyTask, name='process:app.tasks.materialize_root_ids')   
def materialize_root_ids(metadata: dict):
    base_version_engine = create_engine(metadata['base_version_db_uri'])
    BaseVersionSession = sessionmaker(bind=base_version_engine)
    base_version_session = BaseVersionSession()
    root_model = em_models.make_cell_segment_model(metadata['dataset_name'], 
                                                    version=metadata['new_version'])

    prev_max_id = int(base_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
    cg = chunkedgraph.ChunkedGraph(table_id=metadata['cg_table_id'])
    max_root_id = materialize.find_max_root_id_before(cg,
                                                      metadata['base_version_timestamp'],
                                                      2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
                                                      start_id=np.uint64(prev_max_id),
                                                      delta_id=100)
    max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
    multi_args, new_roots, old_roots = materialize.materialize_root_ids_delta(cg_table_id=metadata['cg_table_id'],
                                                                  dataset_name=metadata['dataset_name'],
                                                                  time_stamp=metadata['new_version'].timestamp,
                                                                  time_stamp_base=metadata['base_version'].timestamp,
                                                                  min_root_id = max_seg_id,
                                                                  analysisversion=metadata['new_version'],
                                                                  sqlalchemy_database_uri=metadata['new_version_db_uri'],
                                                                  cg_instance_id=CG_INSTANCE_ID)
    subtasks = []
    for args in multi_args:
        subtasks.append(materialize_root_ids_subtask.s(args))
    results = group(subtasks)()
    metadata['old_roots'] = old_roots
    return metadata

@celery.task(name='process:app.tasks.materialize_annotations')
def materialize_annotations(metadata: dict):
    missing_tables_info = get_missing_tables(metadata['dataset_name'], metadata['base_version'])
    logging.info(missing_tables_info)
    for table_info in missing_tables_info:
        materialized_info = materialize.materialize_all_annotations(metadata["cg_table_id"],
                                                metadata["dataset_name"],
                                                table_info['schema_name'],
                                                table_info['table_name'],
                                                analysisversion=metadata['new_version'],
                                                time_stamp=metadata['new_version'].time_stamp,
                                                cg_instance_id=CG_INSTANCE_ID,
                                                sqlalchemy_database_uri=metadata['version_db_uri'],
                                                block_size=100)
        process_all_annotations_subtask.delay(materialized_info)
    for table_info in missing_tables_info:
        at = AnalysisTable(schema=table_info['schema_name'],        
                           tablename=table_info['table_name'],
                           valid=True,
                           analysisversion=metadata['analysisversion'])
        session.add(at)
        session.commit()
    return metadata

@celery.task(base=SqlAlchemyTask, name='process:app.tasks.materialize_annotations_delta')   
def materialize_annotations_delta(metadata: dict):
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == metadata['new_version']).all()
    version_engine = create_engine(metadata['new_version_db_uri'])
    VersionSession = sessionmaker(bind=version_engine)
    version_session = VersionSession()
    version_session.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analysis_user;')
    version_session.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO analysis_user;')

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            delta_info = materialize.materialize_annotations_delta(metadata["cg_table_id"],
                                                                   metadata["dataset_name"],
                                                                   table.tablename,
                                                                   table.schema,
                                                                   metadata['old_roots'],
                                                                   metadata['new_version'],
                                                                   metadata['new_version_db_uri'],
                                                                   cg_instance_id=CG_INSTANCE_ID,)
            materialize_delta_annotation_subtask.delay(delta_info)

    root_model = em_models.make_cell_segment_model(metadata['dataset_name'], version=metadata['new_version'].version)
    version_session.query(root_model).filter(root_model.id.in_(metadata['old_roots'].tolist())).delete(synchronize_session=False)

    version_session.commit()
    
    new_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==metadata['new_version'].version).first()
    new_version.valid = True
    session.commit()
    return metadata

@celery.task(name='process:app.tasks.process_all_annotations_task')
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
    mm = materializationmanager.MaterializationManager(**serialized_mm_info)

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

@celery.task(name='process:app.tasks.materialize_delta_annotation_subtask')
def materialize_delta_annotation_subtask(args):
    """ Helper for materialize_annotations_delta """
    (block, col, time_stamp,  mm_info, cg_info) = args
    cg = chunkedgraph.ChunkedGraph(**cg_info)
    mm = materializationmanager.MaterializationManager(**mm_info)
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
        logging.error(f"Annotation list error:{annos_list}")
        raise Exception(e)

@celery.task(name='process:app.tasks.materialize_root_ids_subtask')
def materialize_root_ids_subtask(args):
    root_ids, serialized_mm_info = args
    model = em_models.make_cell_segment_model(serialized_mm_info["dataset_name"],
                                              serialized_mm_info["version"])
    mm = materializationmanager.MaterializationManager(**serialized_mm_info,
                                                       annotation_model=model)
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