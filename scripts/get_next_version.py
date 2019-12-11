# from materializationengine import materialize
# from emannotationschemas.models import make_annotation_model, get_next_version
from app.database import Base
from emannotationschemas.models import AnalysisVersion
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
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
            raise Exception(
                'need to define a postgres uri via command line or MATERIALIZATION_POSTGRES_URI env')
    else:
        sql_uri = mod.args['sql_uri']
    dataset = 'pinky100'
    # types = get_types()

    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))

    engine = create_engine(sql_uri)
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    top_version = session.query(AnalysisVersion).order_by(AnalysisVersion.version.desc()).first()

    if top_version is None:
        new_version_number = 1
    else:
        new_version_number = top_version.version+1
    
    time_stamp = datetime.datetime.utcnow()
    version = AnalysisVersion(dataset=dataset,
                              time_stamp = time_stamp,
                              version = new_version_number)
    session.add(version)
    session.commit()
    print(version)
