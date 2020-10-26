from flask import (
    Blueprint,
    jsonify,
    abort,
    current_app,
    request,
    render_template,
    url_for,
    redirect,
)
from flask_restx import Namespace, Resource, reqparse, fields
from flask_accepts import accepts, responds
from emannotationschemas.models import format_version_db_uri
from materializationengine.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.views import get_datasets
from materializationengine.database import get_db, sqlalchemy_cache, create_session
from materializationengine.info_client import get_aligned_volumes
from middle_auth_client import auth_required, auth_requires_permission
import requests
import logging
import numpy as np
import json
from sqlalchemy import Table
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.url import make_url
import datetime


__version__ = "0.2.35"


bulk_upload_parser = reqparse.RequestParser()
bulk_upload_parser.add_argument('column_mapping', type=json.loads, location='json')
bulk_upload_parser.add_argument('project', type=str)
bulk_upload_parser.add_argument('file_path', type=str)
bulk_upload_parser.add_argument('schema', type=str)

missing_chunk_parser = reqparse.RequestParser()
missing_chunk_parser.add_argument('chunks', type=json.loads, location='json')
missing_chunk_parser.add_argument('column_mapping', type=json.loads, location='json')
missing_chunk_parser.add_argument('project', type=str)
missing_chunk_parser.add_argument('file_path', type=str)
missing_chunk_parser.add_argument('schema', type=str)


authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'query',
        'name': 'middle_auth_token'
    }
}

mat_bp = Namespace("Materialization Engine",
                   authorizations=authorizations,
                   description="Materialization Engine")


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")
    
@mat_bp.route("/materialize/live/datastack/<string:datastack_name>")
class RunMaterializeResource(Resource):
    @auth_required
    @mat_bp.doc("run updating materialization", security="apikey")
    def post(self, datastack_name):
        """Run live materialization to update segmentation data 

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.live_materialization import start_materialization
        INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
        url = INFOSERVICE_ENDPOINT + f"/api/v2/datastack/full/{datastack_name}"
        try:
            auth_header = {"Authorization": f"Bearer {current_app.config['AUTH_TOKEN']}"}
            r = requests.get(url, headers=auth_header)
            r.raise_for_status()
            logging.info(url)
            datastack_info = r.json()
            aligned_volume_name = datastack_info['aligned_volume']['name']
            pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]
            segmentation_source = datastack_info.get('segmentation_source')
            start_materialization.s(aligned_volume_name, pcg_table_name, segmentation_source).apply_async()
            return 200
        except requests.exceptions.RequestException as e:
            logging.error(f"ERROR {e}. Cannot connect to {INFOSERVICE_ENDPOINT}")


@mat_bp.route("/materialize/frozen/datastack/<string:datastack_name>")
class CreateFrozenMaterializationResource(Resource):
    @auth_required
    @mat_bp.doc("create frozen materialization", security="apikey")
    def post(self, datastack_name: str):
        """Create a new frozen (versioned) materialization

        Args:
            datastack_name (str): name of datastack from infoservice
        """
        from materializationengine.workflows.versioned_materialization import versioned_materialization
        INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
        url = INFOSERVICE_ENDPOINT + f"/api/v2/datastack/full/{datastack_name}"
        try:
            auth_header = {"Authorization": f"Bearer {current_app.config['AUTH_TOKEN']}"}
            r = requests.get(url, headers=auth_header)
            r.raise_for_status()
            logging.info(url)
            datastack_info = r.json()
            datastack_info['datastack'] = datastack_name
            versioned_materialization(datastack_info)
        except requests.exceptions.RequestException as e:
            logging.error(f"ERROR {e}. Cannot connect to {INFOSERVICE_ENDPOINT}")

@mat_bp.expect(bulk_upload_parser)
@mat_bp.route("/bulk_upload/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>")
class BulkUploadResource(Resource):
    @auth_required
    @mat_bp.doc("bulk upload", security="apikey")
    def post(self, datastack_name: str, table_name: str, segmentation_source: str, description: str):
        """Run bulk upload from npy files

        Args:
            column_mappings (dict): dict mapping file names to column names in database
            project (str): bucket project path
            file_path (str): bucket project path
            schema (str): type of schema from emannotationschemas
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table in database to create
            segmentation_source (str): source of segmentation data
            description (str): text field added to annotation metadata table for reference
        """
        from materializationengine.workflows.bulk_upload import bulk_upload

        args = bulk_upload_parser.parse_args()


        INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
        url = INFOSERVICE_ENDPOINT + f"/api/v2/datastack/full/{datastack_name}"
        try:
            auth_header = {"Authorization": f"Bearer {current_app.config['AUTH_TOKEN']}"}
            r = requests.get(url, headers=auth_header)
            r.raise_for_status()
            logging.info(url)
            bulk_upload_info = r.json()
            
            bulk_upload_info.update({
                'column_mapping': args['column_mapping'],
                'project': args['project'],
                'file_path': args['file_path'],
                'schema': args['schema'],
                'datastack': datastack_name,
                'description': description,
                'annotation_table_name': table_name,
                'segmentation_source': segmentation_source,
            })
            bulk_upload(bulk_upload_info)
            return f"Uploading : {datastack_name}", 200
        except requests.exceptions.RequestException as e:
            logging.error(f"ERROR {e}. Cannot connect to {INFOSERVICE_ENDPOINT}")


@mat_bp.expect(missing_chunk_parser)
@mat_bp.route("/missing_chunks/<string:datastack_name>/<string:table_name>/<string:segmentation_source>/<string:description>")
class InsertMissingChunks(Resource):
    @auth_required
    @mat_bp.doc("insert missing chunks", security="apikey")
    def post(self, datastack_name: str, table_name: str, segmentation_source: str, description: str):
        """Insert missing chunks of data into database

        Args:
            chunks (list): list mapping file names to column names in database
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table in database to create
            segmentation_source (str): source of segmentation data
            description (str): text field added to annotation metadata table for reference

        """
        from materializationengine.workflows.bulk_upload import insert_missing_data

        args = missing_chunk_parser.parse_args()
        INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
        url = INFOSERVICE_ENDPOINT + f"/api/v2/datastack/full/{datastack_name}"
        try:
            auth_header = {"Authorization": f"Bearer {current_app.config['AUTH_TOKEN']}"}
            r = requests.get(url, headers=auth_header)
            r.raise_for_status()
            logging.info(url)
            bulk_upload_info = r.json()
            bulk_upload_info.update({
                'chunks': args['chunks'],
                'column_mapping': args['column_mapping'],
                'project': args['project'],
                'file_path': args['file_path'],
                'schema': args['schema'],
                'datastack': datastack_name,
                'description': description,
                'annotation_table_name': table_name,
                'segmentation_source': segmentation_source,
            })
            insert_missing_data(bulk_upload_info)
            return f"Uploading : {datastack_name}", 200
        except requests.exceptions.RequestException as e:
            logging.error(f"ERROR {e}. Cannot connect to {INFOSERVICE_ENDPOINT}")


@mat_bp.route("/aligned_volume/<aligned_volume_name>")
class DatasetResource(Resource):
    @auth_required
    @mat_bp.doc("get_aligned_volume_versions", security="apikey")
    def get(self, aligned_volume_name: str):
        db = get_db(aligned_volume_name)
        response = db.session.query(AnalysisVersion.datastack).distinct()
        aligned_volumes = [r._asdict() for r in response]
        return aligned_volumes

@mat_bp.route("/aligned_volumes/<aligned_volume_name>")
class VersionResource(Resource):
    @auth_required
    @mat_bp.doc("get_analysis_versions", security="apikey")
    def get(self, aligned_volume_name):
        check_aligned_volume(aligned_volume_name)
        session = sqlalchemy_cache.get(aligned_volume_name)
        
        response = (
            session.query(AnalysisVersion).filter(AnalysisVersion.datastack == aligned_volume_name).all()
        )
        schema = AnalysisVersionSchema(many=True)
        versions, error = schema.dump(response)
        logging.info(versions)
        if versions:
            return versions, 200
        else:
            logging.error(error)
            return abort(404)

@mat_bp.route("/aligned_volumes/<aligned_volume_name>/version/<version>")
class TableResource(Resource):
    @auth_required
    @mat_bp.doc("get_all_tables", security="apikey")
    def get(self, aligned_volume_name, version):
        check_aligned_volume(aligned_volume_name)
        session = sqlalchemy_cache.get(aligned_volume_name)

        response = (
            session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion)
            .filter(AnalysisVersion.version == version)
            .filter(AnalysisVersion.datastack == aligned_volume_name)
            .all()
        )
        schema = AnalysisTableSchema(many=True)
        tables, error = schema.dump(response)
        if tables:
            return tables, 200
        else:
            logging.error(error)
            return abort(404)

@mat_bp.route("/aligned_volumes/<aligned_volume_name>/version/<version>/tablename/<tablename>")
class AnnotationResource(Resource):
    @auth_required
    @mat_bp.doc("get_top_materialized_annotations", security="apikey")
    def get(self, aligned_volume_name, version, tablename):
        check_aligned_volume(aligned_volume_name)
        SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]
        sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
        sql_uri = make_url(f"{sql_base_uri}/{aligned_volume_name}")
        session, engine = create_session(sql_uri)
        metadata = MetaData()
        try:
            annotation_table = Table(tablename, metadata, autoload=True, autoload_with=engine)
        except NoSuchTableError as e:
            logging.error(f"No table exists {e}")
            return abort(404)
        response = session.query(annotation_table).limit(10).all()
        annotations = [r._asdict() for r in response]
        if annotations:
            return annotations, 200
        else:
            return abort(404)
