import materializationengine.models as models
from flask_marshmallow import Marshmallow

ma = Marshmallow()

class AnalysisVersionSchema(ma.ModelSchema):
    class Meta:
        model = models.AnalysisVersion
