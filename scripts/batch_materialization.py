from materializationengine import materialize
from emannotationschemas import get_types, get_schema
from emannotationschemas.base import ReferenceAnnotation
import argschema
import os
import marshmallow as mm


class BatchMaterializationSchema(argschema.ArgSchema):
    cg_table_id = mm.fields.Str(default="pinky40_fanout2_v7",
                                description="PyChunkedGraph table id")
    dataset_name = mm.fields.Str(default="pinky40",
                                 description="name of dataset in DynamicAnnotationDb")
    amdb_instance_id = mm.fields.Str(default="pychunkedgraph",
                                     description="name of google instance for DynamicAnnotationDb")
    cg_instance_id = mm.fields.Str(default="pychunkedgraph",
                                   description="name of google instance for PyChunkedGraph")
    n_threads = mm.fields.Int(default=20,
                              description="number of threads to use in parallelization")
    sql_uri = mm.fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")


if __name__ == '__main__':
    mod = argschema.ArgSchemaParser(schema_type=BatchMaterializationSchema)
    sql_uri = mod.args.get(
        'sql_uri', os.environ['MATERIALIZATION_POSTGRES_URI'])

    types = get_types()

    normal_types = [type_ for type_ in types if not issubclass(get_schema(type_),
                                                               ReferenceAnnotation)]
    reference_types = [type_ for type_ in types if issubclass(get_schema(type_),
                                                              ReferenceAnnotation)]
    for type_ in normal_types:
        materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                                mod.args["dataset_name"],
                                                type_,
                                                amdb_instance_id=mod.args["amdb_instance_id"],
                                                cg_instance_id=mod.args["cg_instance_id"],
                                                sqlalchemy_database_uri=sql_uri,
                                                n_threads=mod.args["n_threads"])

    for type_ in reference_types:
        materialize.materialize_all_annotations(mod.args["cg_table_id"],
                                                mod.args["dataset_name"],
                                                type_,
                                                amdb_instance_id=mod.args["amdb_instance_id"],
                                                cg_instance_id=mod.args["cg_instance_id"],
                                                sqlalchemy_database_uri=sql_uri,
                                                n_threads=mod.args["n_threads"])
