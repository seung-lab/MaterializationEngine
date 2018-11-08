from materializationengine import materialize
from emannotationschemas.models import make_annotation_model, get_next_version
import argschema
import os
import marshmallow as mm
import datetime
import time
import pickle as pkl

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
    n_threads = mm.fields.Int(default=200,
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
            raise(
                'need to define a postgres uri via command line or MATERIALIZATION_POSTGRES_URI env')
    else:
        sql_uri = mod.args['sql_uri']
    # types = get_types()

    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))

    # engine = sqlalchemy.create_engine(sql_uri)
    #new_version = get_next_version(sql_uri, mod.args['dataset_name'])
    new_version = 17
    schema_name = "cell_type_local"
    table_name = "soma_valence"
    # sql_uri = None

    print("INFO:", mod.args, new_version)
    print("sql_uri:", sql_uri)

    with open("{}/materialization_log_v{}.pkl".format(HOME, new_version), "rb") as f:
        mod.args = pkl.load(f)
        # sql_uri = pkl.load(f)


    timings = {}

    time_start = time.time()
    syn_df = materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                                     mod.args["dataset_name"],
                                                     schema_name,
                                                     table_name,
                                                     version=new_version,
                                                     time_stamp=mod.args['time_stamp'],
                                                     amdb_instance_id=mod.args["amdb_instance_id"],
                                                     cg_instance_id=mod.args["cg_instance_id"],
                                                     sqlalchemy_database_uri=sql_uri,
                                                     n_threads=mod.args["n_threads"])
    # syn_df.to_csv("%s/syn_df_v%d.csv" % (HOME, new_version))
    timings[table_name] = time.time() - time_start

    for k in timings:
        print("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))
