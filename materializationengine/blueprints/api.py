from flask import Blueprint, jsonify, abort, current_app, request,\
                  render_template, url_for, redirect
from flask_restx import Namespace, Resource, reqparse, fields
from flask_accepts import accepts, responds

from emannotationschemas.models import format_version_db_uri
from materializationengine.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.extensions import db, create_session
from materializationengine.blueprints.routes import get_datasets
from materializationengine.database import get_db
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

# api = Blueprint("api", __name__, url_prefix='/materialize/api/v1/')
api_bp = Namespace("Materialization Engine", description="Materialization Engine")



@api_bp.route("/metadata/<dataset_name>/<version>")
class MetaDataResource(Resource):

    def get(self, dataset_name, version):
        from materializationengine.tasks import get_materialization_metadata
        results = get_materialization_metadata(dataset_name, version)
        logging.info(f"Results are: {results}")
        return results

@api_bp.route("/run/<string:dataset_name>/<int:dataset_version>/<use_latest>")
class RunMaterializeResource(Resource):
    def get(self, dataset_name, dataset_version, use_latest):
        from materializationengine.tasks import run_materialization
        run_materialization(dataset_name, dataset_version, use_latest)
        return jsonify({"Dataset Name": dataset_name, "Version": dataset_version}), 200

@api_bp.route("/new/<dataset_name>/<dataset_version>")
class NewMaterializeResource(Resource):
    def get(self, dataset_name, dataset_version):
        from materializationengine.tasks import new_materialization
        new_materialization(dataset_name, dataset_version)
        return jsonify({"Dataset Name": dataset_name, "Version": dataset_version}), 200


@api_bp.route('/datasets')
class DatasetResource(Resource):

    def get(self):
        response = db.session.query(AnalysisVersion.dataset).distinct()
        datasets = [r._asdict() for r in response]
        return jsonify(datasets)


@api_bp.route("/datasets/<dataset_name>")
class VersionResource(Resource):

    def get(self, dataset_name):
        response = db.session.query(AnalysisVersion).filter(AnalysisVersion.dataset == dataset_name).all()
        schema = AnalysisVersionSchema(many=True)
        versions, error = schema.dump(response)
        logging.info(versions)
        if versions:
            return jsonify(versions), 200
        else:
            logging.error(error)
            return abort(404)

@api_bp.route("/datasets/<dataset_name>/<version>")
class TableResource(Resource):

    def get(self, dataset_name, version):
        response = (db.session.query(AnalysisTable)
                    .filter(AnalysisTable.analysisversion)
                    .filter(AnalysisVersion.version == version)
                    .filter(AnalysisVersion.dataset == dataset_name)
                    .all())
        schema = AnalysisTableSchema(many=True)
        tables, error = schema.dump(response)
        if tables:
            return jsonify(tables), 200
        else:
            logging.error(error)
            return abort(404)


@api_bp.route("/datasets/<dataset_name>/<version>/<tablename>")
class AnnotationResource(Resource):

    def get(self, dataset_name, version, tablename):
        db = get_db()
        sql_uri = format_version_db_uri(db, dataset_name, version)
        session, engine = create_session(sql_uri)
        metadata = MetaData()
        try:
            annotation_table = Table(tablename, metadata,
                                     autoload=True, autoload_with=engine)
        except NoSuchTableError as e:
            logging.error(f'No table exists {e}')
            return abort(404)
        response = session.query(annotation_table).limit(10).all()
        annotations = [r._asdict() for r in response]
        if annotations:
            return jsonify(annotations), 200
        else:
            return abort(404)
