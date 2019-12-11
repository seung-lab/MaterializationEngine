from app import materialize, materializationmanager
import argschema
import os
import marshmallow as mm
import datetime
import time
import pickle as pkl
import requests

HOME = os.path.expanduser("~")

class BatchMaterializationSchema(argschema.ArgSchema):
    cg_table_id = mm.fields.Str(default="pinky100_sv16",
                                description="PyChunkedGraph table id")
    dataset_name = mm.fields.Str(default="pinky100",
                                 description="name of dataset in DynamicAnnotationDb")
    amdb_instance_id = mm.fields.Str(default="pychunkedgraph",
                                     description="name of google instance for DynamicAnnotationDb")
    cg_instance_id = mm.fields.Str(default="pychunkedgraph",
                                   description="name of google instance for PyChunkedGraph")
    n_threads = mm.fields.Int(default=150,
                              description="number of threads to use in parallelization")
    time_stamp = mm.fields.DateTime(default=str(datetime.datetime.utcnow()),
                                    description="time to use for materialization")
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")

if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=BatchMaterializationSchema)
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

    print(sql_uri)
    print(mod.args)

    if sql_uri is not None:
        analysisversion = materializationmanager.create_new_version(
            sql_uri, mod.args['dataset_name'], mod.args['time_stamp'])
    else:
        analysisversion = None

    print(analysisversion)

    annotation_endpoint = "https://www.dynamicannotationframework.com/annotation"

    print("INFO:", mod.args, analysisversion)
    print("sql_uri:", sql_uri)

    with open("{}/materialization_log_v{}.pkl".format(HOME, analysisversion.version), "wb") as f:
        pkl.dump(mod.args, f)
        pkl.dump(sql_uri, f)

    timings = {}

    time_start = time.time()
    root_df = materialize.materialize_root_ids(mod.args["cg_table_id"],
                                               dataset_name=mod.args["dataset_name"],
                                               time_stamp=mod.args['time_stamp'],
                                               analysisversion=analysisversion,
                                               sqlalchemy_database_uri=sql_uri,
                                               cg_instance_id=mod.args["cg_instance_id"],
                                               n_threads=mod.args["n_threads"])

    timings["root_ids"] = time.time() - time_start

    print("Time(root ids): %.2fs" % timings["root_ids"])

    url = annotation_endpoint + "/dataset/{}".format(mod.args["dataset_name"])
    tables = requests.get(url).json()

    schema_tables = [(t['schema_name'], t['table_name']) for t in tables]

    for schema_name, table_name in schema_tables:
        if table_name in blacklist:
            continue

        print(schema_name, table_name)
        time_start = time.time()
        syn_df = materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                                         mod.args["dataset_name"],
                                                         schema_name,
                                                         table_name,
                                                         analysisversion=analysisversion,
                                                         time_stamp=mod.args['time_stamp'],
                                                         amdb_instance_id=mod.args["amdb_instance_id"],
                                                         cg_instance_id=mod.args["cg_instance_id"],
                                                         sqlalchemy_database_uri=sql_uri,
                                                         n_threads=mod.args["n_threads"])

        timings[table_name] = time.time() - time_start

    for k in timings:
        print("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))
