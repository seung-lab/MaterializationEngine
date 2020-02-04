import emannotationschemas.models as models
from flask_marshmallow import Marshmallow
from marshmallow import Schema, fields
import datetime as dt

ma = Marshmallow()


class AnalysisVersionSchema(ma.ModelSchema):
    class Meta:
        model = models.AnalysisVersion


class AnalysisTableSchema(ma.ModelSchema):
    class Meta:
        model = models.AnalysisTable


class MaterializationSchema(Schema):
    cg_table_id = fields.Str(default="pinky100_sv16",
                                description="PyChunkedGraph table id")
    dataset_name = fields.Str(default="pinky100",
                                 description="name of dataset in DynamicAnnotationDb")
    cg_instance_id = fields.Str(default="pychunkedgraph",
                                   description="name of google instance for PyChunkedGraph")
    base_version = fields.Int(default=0,
                                 description="previous base version # to use as base")
    time_stamp = fields.DateTime(default=str(dt.datetime.utcnow()),
                                    description="time to use for materialization")
    n_threads = fields.Int(default=4,
                              description="number of threads to use in parallelization")
    sql_uri = fields.Str(
        description="connection uri string to postgres database"
        "(default to MATERIALIZATION_POSTGRES_URI environment variable")