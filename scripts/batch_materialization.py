from materializationengine import materialize
from emannotationschemas import get_types, get_schema
from emannotationschemas.base import ReferenceAnnotation
from emannotationschemas.models import make_annotation_model
import sqlalchemy
import argschema
import os
import marshmallow as mm


class BatchMaterializationSchema(argschema.ArgSchema):
    cg_table_id = mm.fields.Str(default="pinky100_sv11",
                                description="PyChunkedGraph table id")
    dataset_name = mm.fields.Str(default="pinky100",
                                 description="name of dataset in DynamicAnnotationDb")
    amdb_instance_id = mm.fields.Str(default="pychunkedgraph",
                                     description="name of google instance for DynamicAnnotationDb")
    cg_instance_id = mm.fields.Str(default="pychunkedgraph",
                                   description="name of google instance for PyChunkedGraph")
    n_threads = mm.fields.Int(default=128,
                              description="number of threads to use in parallelization")
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")


if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=BatchMaterializationSchema)
    sql_uri = mod.args.get('sql_uri',
                           os.environ['MATERIALIZATION_POSTGRES_URI'])

    # types = get_types()

    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))

    engine = sqlalchemy.create_engine(sql_uri)

    # for type_ in sorted_types:

    schema_name = "synapse"
    table_name = 'pni_synapses'
    materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                            mod.args["dataset_name"],
                                            schema_name,
                                            table_name,
                                            version='v1',
                                            amdb_instance_id=mod.args["amdb_instance_id"],
                                            cg_instance_id=mod.args["cg_instance_id"],
                                            sqlalchemy_database_uri=sql_uri,
                                            n_threads=mod.args["n_threads"])