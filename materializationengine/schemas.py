from materializationengine.models import AnalysisTable, AnalysisVersion
from flask_marshmallow import Marshmallow
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

ma = Marshmallow()


class AnalysisVersionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisVersion
        load_instance = True

class AnalysisTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisTable
        load_instance = True
