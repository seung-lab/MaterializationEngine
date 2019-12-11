from app import materializationmanager
from emannotationschemas.models import AnalysisVersion, AnalysisTable
import emannotationschemas.models as em_models
import argschema
import os
import marshmallow as mm
import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pychunkedgraph.backend import chunkedgraph
from app import materialize
from emannotationschemas.models import format_version_db_uri
import logging
from sqlalchemy.sql import func
from pytz import UTC
import numpy as np
from annotationframeworkclient.annotationengine import AnnotationClient


def __download_annotation_thread(multiargs):
    (dataset, table_name, ann_id)
HOME = os.path.expanduser("~")
class IncrementalMaterializationSchema(argschema.ArgSchema):
    cg_table_id = mm.fields.Str(default="pinky100_sv16",
                                description="PyChunkedGraph table id")
    dataset_name = mm.fields.Str(default="pinky100",
                                 description="name of dataset in DynamicAnnotationDb")
    cg_instance_id = mm.fields.Str(default="pychunkedgraph",
                                   description="name of google instance for PyChunkedGraph")
    base_version = mm.fields.Int(default=0,
                                 description="previous base version # to use as base")
    time_stamp = mm.fields.DateTime(default=str(datetime.datetime.utcnow()),
                                    description="time to use for materialization")
    n_threads = mm.fields.Int(default=4,
                              description="number of threads to use in parallelization")
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")


if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=IncrementalMaterializationSchema)
    if 'sql_uri' not in mod.args:
        if 'MATERIALIZATION_POSTGRES_URI' in os.environ.keys():
            sql_uri = os.environ['MATERIALIZATION_POSTGRES_URI']
        else:
            raise Exception(
                'need to define a postgres uri via command line or MATERIALIZATION_POSTGRES_URI env')
    else:
        sql_uri = mod.args['sql_uri']
    dataset = mod.args['dataset_name']
    

    # analysisversion = materializationmanager.create_new_version(
    #     sql_uri, dataset, mod.args['time_stamp'])

    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))
    blacklist = ["pni_synapses", "pni_synapses_i2",  "is_chandelier"]
    
    engine = create_engine(sql_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
 
    analysisversion = materializationmanager.create_new_version(
        sql_uri, dataset, mod.args['time_stamp'])
    version = analysisversion.version
    base_version_number = mod.args['base_version']
    base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==base_version_number).first()

    new_db_name = f'{dataset}_v{analysisversion.version}'
    base_db_name = f'{dataset}_v{base_version_number}'

    version_db_uri = format_version_db_uri(sql_uri, dataset, version)
    base_version_db_uri = format_version_db_uri(sql_uri, dataset,  base_version_number)
    print(version_db_uri)

    print(f'making new version {analysisversion.version} with timestamp {analysisversion.time_stamp}')

    start = time.time()
    conn = engine.connect()
    conn.execute("commit")
    conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{base_db_name}';")
    conn.execute(f"create database {new_db_name} TEMPLATE {base_db_name}")
    copy_time = time.time()-start

    timings = {}
    timings['copy database'] = copy_time 

    tables = session.query(AnalysisTable).filter(
             AnalysisTable.analysisversion == base_version).all()

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            time_start = time.time()
            new_analysistable = AnalysisTable(schema=table.schema,
                                              tablename=table.tablename,
                                              valid=False,
                                              analysisversion_id=analysisversion.id)
            session.add(new_analysistable)
            session.commit()
            timings[table.tablename] = time.time() - time_start
            print(table.tablename)

    for k in timings:
        print("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))

    assert(base_version is not None)
    timings = {}
    
    base_version_engine = create_engine(base_version_db_uri)
    BaseVersionSession = sessionmaker(bind=base_version_engine)
    base_version_session = BaseVersionSession()
    root_model = em_models.make_cell_segment_model(dataset, version=analysisversion.version)

    prev_max_id = int(base_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
    cg = chunkedgraph.ChunkedGraph(table_id=mod.args['cg_table_id'])
    max_root_id = materialize.find_max_root_id_before(cg,
                                                      base_version.time_stamp,
                                                      2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
                                                      start_id=np.uint64(prev_max_id),
                                                      delta_id=100)
    max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
    time_start = time.time()
    new_roots, old_roots = materialize.materialize_root_ids_delta(mod.args["cg_table_id"],
                                                                  dataset_name=mod.args["dataset_name"],
                                                                  time_stamp=analysisversion.time_stamp,
                                                                  time_stamp_base=base_version.time_stamp,
                                                                  min_root_id = max_seg_id,
                                                                  analysisversion=analysisversion,
                                                                  sqlalchemy_database_uri=version_db_uri,
                                                                  cg_instance_id=mod.args["cg_instance_id"],
                                                                  n_threads=mod.args["n_threads"])
    timings['root_ids'] = time.time()-time_start
    
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    
    anno_client = AnnotationClient(dataset_name=dataset)
    all_tables = anno_client.get_tables()
    missing_tables_info = [t for t in all_tables 
                           if (t['table_name'] not in [t.tablename for t in tables]) 
                           and (t['table_name']) not in blacklist]

    for table_info in missing_tables_info:
        materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                                mod.args["dataset_name"],
                                                table_info['schema_name'],
                                                table_info['table_name'],
                                                analysisversion=analysisversion,
                                                time_stamp=analysisversion.time_stamp,
                                                cg_instance_id=mod.args["cg_instance_id"],
                                                sqlalchemy_database_uri=version_db_uri,
                                                block_size=100,
                                                n_threads=25*mod.args["n_threads"])
        at = AnalysisTable(schema=table_info['schema_name'],        
                           tablename=table_info['table_name'],
                           valid=True,
                           analysisversion=analysisversion)
        session.add(at)
        session.commit()
        
    
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    version_engine = create_engine(version_db_uri)
    VersionSession = sessionmaker(bind=version_engine)
    version_session = VersionSession()
    version_session.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analysis_user;')
    version_session.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO analysis_user;')

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            time_start = time.time()
            materialize.materialize_annotations_delta(mod.args["cg_table_id"],
                                                      mod.args["dataset_name"],
                                                      table.tablename,
                                                      table.schema,
                                                      old_roots,
                                                      analysisversion,
                                                      version_db_uri,
                                                      cg_instance_id=mod.args["cg_instance_id"],
                                                      n_threads=3*mod.args["n_threads"])
            timings[table.tablename] = time.time() - time_start
    root_model = em_models.make_cell_segment_model(dataset, version=analysisversion.version)
    version_session.query(root_model).filter(root_model.id.in_(old_roots.tolist())).delete(synchronize_session=False)

    version_session.commit()
    
    new_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==analysisversion.version).first()
    new_version.valid = True
    session.commit()

    for k in timings:
        print("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))