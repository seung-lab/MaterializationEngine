import emannotationschemas.models as models
from flask_marshmallow import Marshmallow

ma = Marshmallow()


class AnalysisVersionSchema(ma.ModelSchema):
    class Meta:
        model = models.AnalysisVersion


class AnalysisTableSchema(ma.ModelSchema):
    class Meta:
        model = models.AnalysisTable