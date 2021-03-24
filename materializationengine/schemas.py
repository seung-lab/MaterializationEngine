from materializationengine.models import AnalysisTable, AnalysisVersion
from flask_marshmallow import Marshmallow
from marshmallow_sqlalchemy import ModelSchema
from marshmallow import fields, ValidationError, Schema

ma = Marshmallow()


class AnalysisVersionSchema(ModelSchema):
    class Meta:
        model = AnalysisVersion


class AnalysisTableSchema(ModelSchema):
    class Meta:
        model = AnalysisTable


class CronField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, str) or isinstance(value, int) or isinstance(value, list):
            return value
        else:
            raise ValidationError('Field should be str, int or list')


class CeleryBeatSchema(Schema):
    name = fields.Str(required=True)
    minute = CronField(default='*')
    hour = CronField(default='*')
    day_of_week = CronField(default='*')
    day_of_month = CronField(default='*')
    month_of_year = CronField(default='*')
    task = fields.Str(required=True)
