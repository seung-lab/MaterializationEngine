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
from materializationengine.extensions import create_session
from materializationengine.views import get_datasets
from materializationengine.database import get_db
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

__version__ = "0.2.35"

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

@mat_bp.route("/metadata/<aligned_volume>/<version>")
class MetaDataResource(Resource):
    @auth_required
    @mat_bp.doc("get_materialized_metadata", security="apikey")
    def get(self, aligned_volume, version):
        from materializationengine.tasks import get_materialization_metadata

        results = get_materialization_metadata(aligned_volume, version)
        logging.info(f"Results are: {results}")
        return results


@mat_bp.route("/run/<string:aligned_volume>/<int:version>/<use_latest>")
class RunMaterializeResource(Resource):
    @auth_required
    @mat_bp.doc("run updating materialization", security="apikey")
    def get(self, aligned_volume, version, use_latest):
        from materializationengine.tasks import run_materialization

        run_materialization(aligned_volume, version, use_latest)
        return jsonify({"Aligned Volume": aligned_volume, "Version": version}), 200

@mat_bp.route("/new/<aligned_volume>/<version>")
class NewMaterializeResource(Resource):
    @auth_required
    @mat_bp.doc("create new materialized version", security="apikey")
    def get(self, aligned_volume, version):
        from materializationengine.tasks import new_materialization

        new_materialization(aligned_volume, version)
        return jsonify({"Aligned Volume": aligned_volume, "Version": version}), 200

@mat_bp.route("/aligned_volumes")
class DatasetResource(Resource):
    @auth_required
    @mat_bp.doc("get_aligned_volume_versions", security="apikey")
    def get(self):
        response = db.session.query(AnalysisVersion.dataset).distinct()
        aligned_volumes = [r._asdict() for r in response]
        return jsonify(aligned_volumes)

@mat_bp.route("/aligned_volumes/<aligned_volume>")
class VersionResource(Resource):
    @auth_required
    @mat_bp.doc("get_analysis_versions", security="apikey")
    def get(self, aligned_volume):
        response = (
            db.session.query(AnalysisVersion).filter(AnalysisVersion.dataset == aligned_volume).all()
        )
        schema = AnalysisVersionSchema(many=True)
        versions, error = schema.dump(response)
        logging.info(versions)
        if versions:
            return jsonify(versions), 200
        else:
            logging.error(error)
            return abort(404)

@mat_bp.route("/aligned_volumes/<aligned_volume>/<version>")
class TableResource(Resource):
    @auth_required
    @mat_bp.doc("get_all_tables", security="apikey")
    def get(self, aligned_volume, version):
        response = (
            db.session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion)
            .filter(AnalysisVersion.version == version)
            .filter(AnalysisVersion.dataset == aligned_volume)
            .all()
        )
        schema = AnalysisTableSchema(many=True)
        tables, error = schema.dump(response)
        if tables:
            return jsonify(tables), 200
        else:
            logging.error(error)
            return abort(404)

@mat_bp.route("/aligned_volumes/<aligned_volume>/<version>/<tablename>")
class AnnotationResource(Resource):
    @auth_required
    @mat_bp.doc("get_top_materialized_annotations", security="apikey")
    def get(self, aligned_volume, version, tablename):
        db = get_db()
        sql_uri = format_version_db_uri(db, aligned_volume, version)
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
            return jsonify(annotations), 200
        else:
            return abort(404)
