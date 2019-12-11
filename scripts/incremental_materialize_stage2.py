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
    version = mm.fields.Int(required=True, description="version to update with stage 2")
    base_version = mm.fields.Int(default=0,
                                 description="previous base version # to use as base")
    n_threads = mm.fields.Int(default=25,
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
    version = mod.args['version']
    version_db_uri = format_version_db_uri(sql_uri, dataset, version)
    base_version_db_uri = format_version_db_uri(sql_uri, dataset,  mod.args['base_version'])
    print(version_db_uri)

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
    engine.echo = False
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    logging.getLogger('sqlalchemy.dialects').setLevel(logging.ERROR)

    analysisversion = session.query(AnalysisVersion).filter(AnalysisVersion.version == mod.args['version']).first()
    base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version == mod.args['base_version']).first()
    assert(analysisversion is not None)
    assert(base_version is not None)
    timings = {}
    
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
                                                block_size=50,
                                                n_threads=12*mod.args["n_threads"])
        at = AnalysisTable(schema=table_info['schema_name'],        
                           tablename=table_info['table_name'],
                           valid=True,
                           analysisversion=analysisversion)
        session.add(at)
        session.commit()
        
    # base_version_engine = create_engine(base_version_db_uri)
    # BaseVersionSession = sessionmaker(bind=base_version_engine)
    # base_version_session = BaseVersionSession()
    # root_model = em_models.make_cell_segment_model(dataset, version=analysisversion.version)


    # prev_max_id = int(base_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
    # cg = chunkedgraph.ChunkedGraph(table_id=mod.args['cg_table_id'])
    # max_root_id = materialize.find_max_root_id_before(cg,
    #                                              base_version.time_stamp,
    #                                              2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
    #                                              start_id=prev_max_id,
    #                                              delta_id=100)
    # max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
    # time_start = time.time()
    # new_roots, old_roots = materialize.materialize_root_ids_delta(mod.args["cg_table_id"],
    #                                                               dataset_name=mod.args["dataset_name"],
    #                                                               time_stamp=analysisversion.time_stamp,
    #                                                               time_stamp_base=base_version.time_stamp,
    #                                                               min_root_id = max_seg_id,
    #                                                               analysisversion=analysisversion,
    #                                                               sqlalchemy_database_uri=version_db_uri,
    #                                                               cg_instance_id=mod.args["cg_instance_id"],
    #                                                               n_threads=mod.args["n_threads"])
    # timings['root_ids'] = time.time()-time_start

    # version_engine = create_engine(version_db_uri)
    # VersionSession = sessionmaker(bind=version_engine)
    # version_session = VersionSession()

    # for table in tables:
    #     if table.schema != em_models.root_model_name.lower():
    #         time_start = time.time()
    #         materialize.materialize_annotations_delta(mod.args["cg_table_id"],
    #                                                   mod.args["dataset_name"],
    #                                                   table.tablename,
    #                                                   table.schema,
    #                                                   old_roots,
    #                                                   analysisversion,
    #                                                   version_db_uri,
    #                                                   cg_instance_id=mod.args["cg_instance_id"],
    #                                                   n_threads=10*mod.args["n_threads"])
    #         timings[table.tablename] = time.time() - time_start
    # root_model = em_models.make_cell_segment_model(dataset, version=analysisversion.version)
    # version_session.query(root_model).filter(root_model.id.in_(old_roots.tolist())).delete(synchronize_session=False)
    # version_session.commit()

    # for k in timings:
    #     print("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))
