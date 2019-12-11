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


HOME = os.path.expanduser("~")


class IncrementalMaterializationSchema(argschema.ArgSchema):
    dataset_name = mm.fields.Str(default="pinky100",
                                 description="name of dataset in DynamicAnnotationDb")
    amdb_instance_id = mm.fields.Str(default="pychunkedgraph",
                                     description="name of google instance for DynamicAnnotationDb")
    base_version = mm.fields.Int(default=0,
                                 description="previous base version # to use as base")
    n_threads = mm.fields.Int(default=150,
                              description="number of threads to use in parallelization")
    time_stamp = mm.fields.DateTime(default=str(datetime.datetime.utcnow()),
                                    description="time to use for materialization")
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


    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))
    blacklist = ["pni_synapses", "pni_synapses_i2", "interneurons_putative_ai"]

    print(mod.args)
    dataset = mod.args['dataset_name']
    assert(sql_uri)
    analysisversion = materializationmanager.create_new_version(
        sql_uri, dataset, mod.args['time_stamp'])
    print(f'making new version {analysisversion.version} with timestamp {analysisversion.time_stamp}')
    engine = create_engine(sql_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==mod.args['base_version']).first()

    new_db_name = f'{dataset}_v{analysisversion.version}'
    base_db_name = f'{dataset}_v{base_version.version}'
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
