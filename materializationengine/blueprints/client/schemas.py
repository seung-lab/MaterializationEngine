from marshmallow import fields, Schema


class Metadata(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class SegmentationInfoSchema(Schema):
    pcg_table_name = fields.Str(required=True)
    pcg_version = fields.Int(required=True)


class SegmentationTableSchema(SegmentationInfoSchema):
    table_name = fields.Str(order=0, required=True)


class CreateTableSchema(SegmentationTableSchema):
    metadata = fields.Nested(Metadata, required=True, example={"description": "my description"})


class GetDeleteAnnotationSchema(SegmentationInfoSchema):
    annotation_ids = fields.List(fields.Int, required=True)


class PostPutAnnotationSchema(SegmentationInfoSchema):
    annotations = fields.List(fields.Dict, required=True)


class SegmentationDataSchema(Schema):
    pcg_table_name = fields.Str(required=True)
    pcg_version = fields.Int(required=True)
    segmentations = fields.List(fields.Dict, required=True)
