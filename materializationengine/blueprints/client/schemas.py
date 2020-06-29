from marshmallow import fields, Schema, post_load


class Metadata(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class SegmentationTableSchema(Schema):
    table_name = fields.Str(order=0, required=True)
    pcg_table_name = fields.Str(order=1, required=True)
    pcg_version = fields.Int(default=2, required=True)


class CreateTableSchema(SegmentationTableSchema):
    metadata = fields.Nested(
        Metadata, required=True, example={"description": "my description"}
    )


class DeleteAnnotationSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=True)


class PutAnnotationSchema(Schema):
    annotations = fields.List(fields.Dict, required=True)
