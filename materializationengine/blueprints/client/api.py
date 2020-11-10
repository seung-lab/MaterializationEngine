from flask import abort, current_app, request, Response, stream_with_context
from flask_restx import Namespace, Resource, reqparse
from flask_accepts import accepts, responds
import pyarrow as pa
from materializationengine.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.blueprints.client.query import specific_query, _execute_query
from materializationengine.info_client import get_aligned_volumes, get_datastacks, get_datastack_info
from materializationengine.database import get_db, sqlalchemy_cache, create_session
from materializationengine.blueprints.client.schemas import (
    Metadata,
    SegmentationTableSchema,
    SimpleQuerySchema,
    CreateTableSchema,
    PostPutAnnotationSchema,
    GetDeleteAnnotationSchema,
    SegmentationDataSchema
)
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import create_table_dict, Base, annotation_models
from sqlalchemy.engine.url import make_url
from middle_auth_client import auth_required, auth_requires_permission
import logging

__version__ = "0.2.35"

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'query',
        'name': 'middle_auth_token'
    }
}

client_bp = Namespace("Materialization Client",
                      authorizations=authorizations,
                      description="Materialization Client")

annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument('annotation_ids', type=int, action='split', help='list of annotation ids')    
annotation_parser.add_argument('pcg_table_name', type=str, help='name of pcg segmentation table')    


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")

def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info['segmentation_source']
    pcg_table_name = seg_source.split('/')[-1]
    aligned_volume_name = ds_info['aligned_volume']['name']
    return aligned_volume_name, pcg_table_name

@client_bp.route("/datastack/<string:datastack_name>/versions")
class DatastackVersions(Resource):
    @auth_required
    @client_bp.doc("datastack_versions", security="apikey")
    def get(self, datastack_name: str):
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        #db = get_db(aligned_volume_name)
        session = sqlalchemy_cache.get(aligned_volume_name)

        response = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack_name)
            .filter(AnalysisVersion.valid == True)
            .all()
        )

        versions = [av.version for av in response]
        return versions, 200

@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/tables")
class FrozenTableVersions(Resource):
    @auth_required
    @client_bp.doc("get_frozen_tables", security="apikey")
    def get(self, datastack_name: str, version:int):
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        session = sqlalchemy_cache.get(aligned_volume_name)

        av = (session.query(AnalysisVersion)
            .filter(AnalysisVersion.version == version)
            .first()
        )
        if av is None:
            return None, 404
        response = (
            session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion_id == av.id)
            .all()
        )
        
        if response is None:
            return None, 404
        return [r.table_name for r in response], 200

@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/metadata")
class FrozenTableMetadata(Resource):
    @auth_required
    @client_bp.doc("get_frozen_table_metadata", security="apikey")
    def get(self, datastack_name: str, version:int, table_name: str):
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        session = sqlalchemy_cache.get(aligned_volume_name)

        av = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack== datastack_name)
            .filter(AnalysisVersion.version == version)
            .first()
        )
        if av is None:
            return None,404
        response = (
            session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion_id == av.id)
            .filter(AnalysisTable.table_name == table_name)
            .first()
        )
        schema = AnalysisTableSchema()
        tables = schema.dump(response)

        if tables:
            return tables, 200
        else:
            logging.error(error)
            return abort(404)



@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/count")
class FrozenTableQuery(Resource):
    @auth_required
    @client_bp.doc("simple_query", security="apikey")
    def get(self, datastack_name, version, table_name):
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        
        Session = sqlalchemy_cache.get(aligned_volume_name)
       
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        
        analysis_version = Session.query(AnalysisVersion)\
            .filter(AnalysisVersion.datastack == datastack_name)\
            .filter(AnalysisVersion.version == version)\
            .first()
        analysis_table = Session.query(AnalysisTable)\
            .filter(AnalysisTable.analysisversion_id==AnalysisVersion.id)\
            .filter(AnalysisTable.table_name == table_name)\
            .first()

        anno_schema = get_schema(analysis_table.schema)
        FlatSchema = create_flattened_schema(anno_schema)
        if annotation_models.contains_model(table_name, flat=True):
            Model = annotation_models.get_model(table_name, flat=True)
        else:
            annotation_dict = create_table_dict(
                table_name=table_name,
                Schema=FlatSchema,
                segmentation_source=None,
                table_metadata=None,
                with_crud_columns=False,
            )
            Model = type(table_name, (Base,), annotation_dict)
            annotation_models.set_model(table_name, Model, flat=True)

        Session = sqlalchemy_cache.get("{}__mat{}".format(datastack_name, version))
        return Session().query(Model).count(), 200

@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/query")
class FrozenTableQuery(Resource):
    @auth_required
    @client_bp.doc("simple_query", security="apikey")
    @accepts("SimpleQuerySchema", schema=SimpleQuerySchema, api=client_bp)
    def post(self, datastack_name, version, table_name):
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        
        Session = sqlalchemy_cache.get(aligned_volume_name)
       
        data = request.parsed_obj
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
        
        analysis_version = Session.query(AnalysisVersion)\
            .filter(AnalysisVersion.datastack == datastack_name)\
            .filter(AnalysisVersion.version == version)\
            .first()
        analysis_table = Session.query(AnalysisTable)\
            .filter(AnalysisTable.analysisversion_id==AnalysisVersion.id)\
            .filter(AnalysisTable.table_name == table_name)\
            .first()
        

        anno_schema = get_schema(analysis_table.schema)
        FlatSchema = create_flattened_schema(anno_schema)
        if annotation_models.contains_model(table_name, flat=True):
            Model = annotation_models.get_model(table_name, flat=True)
        else:
            annotation_dict = create_table_dict(
                table_name=table_name,
                Schema=FlatSchema,
                segmentation_source=None,
                table_metadata=None,
                with_crud_columns=False,
            )
            Model = type(table_name, (Base,), annotation_dict)
            annotation_models.set_model(table_name, Model, flat=True)

        Session = sqlalchemy_cache.get(datastack_name)
        engine = sqlalchemy_cache.engine

        data = request.parsed_obj
        logging.info('query {}'.format(data))
        df=specific_query(Session, engine, {table_name: Model}, [table_name],
                      filter_in_dict=data.get('filter_in_dict', {}),
                      filter_notin_dict=data.get('filter_notin_dict', {}),
                      filter_equal_dict=data.get('filter_equal_dict', {}),
                      select_columns=data.get('select_columns', None),
                      offset=data.get('offset', None))
        context = pa.default_serialization_context()
        serialized = context.serialize(df)
        return Response(serialized.to_buffer().to_pybytes(),
                        mimetype='x-application/pyarrow')


@client_bp.route("/aligned_volume/<string:aligned_volume_name>/table")
class SegmentationTable(Resource):
    @auth_required
    @client_bp.doc("create_segmentation_table", security="apikey")
    @accepts("SegmentationTableSchema", schema=SegmentationTableSchema, api=client_bp)
    def post(self, aligned_volume_name: str):
        """ Create a new segmentation table"""
        check_aligned_volume(aligned_volume_name)

        data = request.parsed_obj
        db = get_db(aligned_volume_name)

        annotation_table_name = data.get("table_name")
        pcg_table_name = data.get("pcg_table_name")

        table_info = db.create_and_attach_seg_table(
            annotation_table_name, pcg_table_name)

        return table_info, 200

    @auth_required
    @client_bp.doc("get_aligned_volume_tables", security="apikey")
    def get(self, aligned_volume_name: str):
        """ Get list of segmentation tables for an aligned_volume"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        tables = db.get_existing_segmentation_table_ids()
        return tables, 200


@client_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/segmentations"
)
class LinkedSegmentations(Resource):
    @auth_required
    @client_bp.doc("post linked annotations", security="apikey")
    @accepts("SegmentationDataSchema", schema=SegmentationDataSchema, api=client_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Insert linked segmentations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        segmentations = data.get("segmentations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)
        try:
            db.insert_linked_segmentation(table_name,
                                          pcg_table_name,
                                          segmentations)
        except Exception as error:
            logging.error(f"INSERT FAILED {segmentations}")
            abort(404, error)

        return f"Inserted {len(segmentations)} annotations", 200
@client_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/annotations"
)
class LinkedAnnotations(Resource):
    @auth_required
    @client_bp.doc("get linked annotations", security="apikey")
    @client_bp.expect(annotation_parser)
    def get(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Get annotations and segmentation from list of IDs"""
        check_aligned_volume(aligned_volume_name)
        args = annotation_parser.parse_args()

        ids = args["annotation_ids"]
        pcg_table_name = args["pcg_table_name"]

        db = get_db(aligned_volume_name)
        annotations = db.get_linked_annotations(table_name,
                                                pcg_table_name,
                                                ids)

        if annotations is None:
            msg = f"annotation_id {ids} not in {table_name}"
            abort(404, msg)

        return annotations, 200

    @auth_required
    @client_bp.doc("post linked annotations", security="apikey")
    @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Insert linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        annotations = data.get("annotations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)
        try:
            db.insert_linked_annotations(table_name,
                                         pcg_table_name,
                                         annotations)
        except Exception as error:
            logging.error(f"INSERT FAILED {annotations}")
            abort(404, error)

        return f"Inserted {len(annotations)} annotations", 200

    @auth_required
    @client_bp.doc("update linked annotations", security="apikey")
    @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
    def put(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Update linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        annotations = data.get("annotations")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)

        metadata = db.get_table_metadata(aligned_volume_name, table_name)
        schema = metadata.get("schema_type")

        if schema:
            for annotation in annotations:
                db.update_linked_annotations(table_name,
                                             pcg_table_name,
                                             annotation)

        return f"Updated {len(data)} annotations", 200

    @auth_required
    @client_bp.doc("delete linked annotations", security="apikey")
    @accepts("GetDeleteAnnotationSchema", schema=GetDeleteAnnotationSchema, api=client_bp)
    def delete(self, aligned_volume_name: str, table_name: str, **kwargs):
        """ Delete linked annotations """
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj

        ids = data.get("annotation_ids")
        pcg_table_name = data.get("pcg_table_name")
        db = get_db(aligned_volume_name)

        for anno_id in ids:
            ann = db.delete_linked_annotation(table_name,
                                              pcg_table_name,
                                              ids)

        if ann is None:
            msg = f"annotation_id {ids} not in {table_name}"
            abort(404, msg)

        return ann, 200
