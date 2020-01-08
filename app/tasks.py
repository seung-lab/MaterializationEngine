import time
import random

from flask import current_app
from app.database import db
from app import celery



@celery.task(bind=True)
def add_together(self, x, y):
    time.sleep(random.randint(6, 20))
    return x + y

@celery.task(bind=True)
def get_status(self):
    return self.AsyncResult(self.request.id).state

@celery.task(bind=True)
def long_task():
    time.sleep(random.randint(6, 20))
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}

@celery.task(bind=True)
def incremental_materialization(self):
    time.sleep(random.randint(6, 20))
    return "Materializing..."

# @celery.task   
# def incremental_materialization_task(self, dataset_name):

#     materialized_schema = IncrementalMaterializationSchema()
#     dataset = dataset_name
#     sql_uri = current_app.config['MATERIALIZATION_POSTGRES_URI']
#     cg_table = current_app.config['CHUNKGRAPH_TABLE_ID']
#     cg_instance_id = current_app.config['BIG_TABLE_CONFIG']['instance_id']
#     amdb_instance_id = current_app.config['BIG_TABLE_CONFIG']['amdb_instance_id']


#     # can we talk about this???
#     blacklist = ["pni_synapses", "pni_synapses_i2",  "is_chandelier"]
    
#     engine = create_engine(sql_uri)
#     Session = sessionmaker(bind=engine)
#     session = Session()
 
#     analysisversion = materializationmanager.create_new_version(
#         sql_uri, dataset_name, materialized_schema['time_stamp'])
#     version = analysisversion.version
#     base_version_number = materialized_schema['base_version']
#     base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==base_version_number).first()

#     new_db_name = f'{dataset_name}_v{analysisversion.version}'
#     base_db_name = f'{dataset_name}_v{base_version_number}'

#     version_db_uri = format_version_db_uri(sql_uri, dataset_name, version)
#     base_version_db_uri = format_version_db_uri(sql_uri, dataset_name,  base_version_number)
#     current_app.logger.info(version_db_uri)
#     current_app.logger.info(f'making new version {analysisversion.version} with timestamp {analysisversion.time_stamp}')
#     start = time.time()
#     conn = engine.connect()
#     conn.execute("commit")
#     conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{base_db_name}';")
#     conn.execute(f"create database {new_db_name} TEMPLATE {base_db_name}")
#     copy_time = time.time()-start

#     timings = {}
#     timings['copy database'] = copy_time 

#     tables = session.query(AnalysisTable).filter(
#              AnalysisTable.analysisversion == base_version).all()

#     for table in tables:
#         if table.schema != em_models.root_model_name.lower():
#             time_start = time.time()
#             new_analysistable = AnalysisTable(schema=table.schema,
#                                               tablename=table.tablename,
#                                               valid=False,
#                                               analysisversion_id=analysisversion.id)
#             session.add(new_analysistable)
#             session.commit()
#             timings[table.tablename] = time.time() - time_start
#             current_app.logger.info(table.tablename)

#     for k in timings:
#         current_app.logger.info("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))

#     assert(base_version is not None)
#     timings = {}
    
#     base_version_engine = create_engine(base_version_db_uri)
#     BaseVersionSession = sessionmaker(bind=base_version_engine)
#     base_version_session = BaseVersionSession()
#     root_model = em_models.make_cell_segment_model(dataset_name, version=analysisversion.version)

#     prev_max_id = int(base_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
#     cg = chunkedgraph.ChunkedGraph(table_id=materialized_schema['cg_table_id'])
#     max_root_id = materialize.find_max_root_id_before(cg,
#                                                       base_version.time_stamp,
#                                                       2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
#                                                       start_id=np.uint64(prev_max_id),
#                                                       delta_id=100)
#     max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
#     time_start = time.time()
#     new_roots, old_roots = materialize.materialize_root_ids_delta(materialized_schema["cg_table_id"],
#                                                                   dataset_name=materialized_schema["dataset_name"],
#                                                                   time_stamp=analysisversion.time_stamp,
#                                                                   time_stamp_base=base_version.time_stamp,
#                                                                   min_root_id = max_seg_id,
#                                                                   analysisversion=analysisversion,
#                                                                   sqlalchemy_database_uri=version_db_uri,
#                                                                   cg_instance_id=materialized_schema["cg_instance_id"],
#                                                                   n_threads=materialized_schema["n_threads"])
#     timings['root_ids'] = time.time()-time_start
    
#     tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    
#     anno_client = AnnotationClient(dataset_name=dataset_name)
#     all_tables = anno_client.get_tables()
#     missing_tables_info = [t for t in all_tables 
#                            if (t['table_name'] not in [t.tablename for t in tables]) 
#                            and (t['table_name']) not in blacklist]

#     for table_info in missing_tables_info:
#         materialize.materialize_all_annotations(materialized_schema["cg_table_id"],
#                                                 materialized_schema["dataset_name"],
#                                                 table_info['schema_name'],
#                                                 table_info['table_name'],
#                                                 analysisversion=analysisversion,
#                                                 time_stamp=analysisversion.time_stamp,
#                                                 cg_instance_id=materialized_schema["cg_instance_id"],
#                                                 sqlalchemy_database_uri=version_db_uri,
#                                                 block_size=100,
#                                                 n_threads=25*materialized_schema["n_threads"])
#         at = AnalysisTable(schema=table_info['schema_name'],        
#                            tablename=table_info['table_name'],
#                            valid=True,
#                            analysisversion=analysisversion)
#         session.add(at)
#         session.commit()
        
    
#     tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
#     version_engine = create_engine(version_db_uri)
#     VersionSession = sessionmaker(bind=version_engine)
#     version_session = VersionSession()
#     version_session.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analysis_user;')
#     version_session.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO analysis_user;')

#     for table in tables:
#         if table.schema != em_models.root_model_name.lower():
#             time_start = time.time()
#             materialize.materialize_annotations_delta(materialized_schema["cg_table_id"],
#                                                       materialized_schema["dataset_name"],
#                                                       table.tablename,
#                                                       table.schema,
#                                                       old_roots,
#                                                       analysisversion,
#                                                       version_db_uri,
#                                                       cg_instance_id=materialized_schema["cg_instance_id"],
#                                                       n_threads=3*materialized_schema["n_threads"])
#             timings[table.tablename] = time.time() - time_start
#     root_model = em_models.make_cell_segment_model(dataset_name, version=analysisversion.version)
#     version_session.query(root_model).filter(root_model.id.in_(old_roots.tolist())).delete(synchronize_session=False)

#     version_session.commit()
    
#     new_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==analysisversion.version).first()
#     new_version.valid = True
#     session.commit()

#     for k in timings:
#         current_app.logger.info("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))
#     return {'new_version': analysisversion.version,
#             'old_version': base_version_number}