from materializationengine.models import AnalysisTable, AnalysisVersion
from flask_marshmallow import Marshmallow
from marshmallow_sqlalchemy import ModelSchema

ma = Marshmallow()


class AnalysisVersionSchema(ModelSchema):
    class Meta:
        model = AnalysisVersion


class AnalysisTableSchema(ModelSchema):
    class Meta:
        model = AnalysisTable

