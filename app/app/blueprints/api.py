from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from emannotationschemas.models import AnalysisTable, AnalysisVersion, format_version_db_uri
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema
from app.extensions import db, create_session
from app.blueprints.routes import get_datasets
import requests
import logging
import numpy as np
import json
from flask import current_app
from sqlalchemy import Table
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import NoSuchTableError
__version__ = "0.2.35"

api = Blueprint("api", __name__, url_prefix='/materialize/api/v1/')

SQL_URI = current_app.config['MATERIALIZATION_POSTGRES_URI']


@api.route("/test_celery")
def celery_test():
    from app.tasks import test_celery
    tasks = 10000
    results = [test_celery.delay(1, 2) for i in range(tasks)]
    return f"Creating {tasks} tasks"

@api.route("/metadata/<dataset_name>/<version>", methods=('GET', ))
def get_metadata(dataset_name, version):
    from app.tasks import get_materialization_metadata
    results = get_materialization_metadata(dataset_name, version)
    logging.info(f"Results are: {results}")
    return results

@api.route("/run/<dataset_name>/<dataset_version>/<use_latest>")
def materialize_annotations(dataset_name, dataset_version, use_latest):
    from app.tasks import run_materialization
    run_materialization(dataset_name, dataset_version, use_latest)
    return jsonify({"Dataset Name": dataset_name, "Version": dataset_version}), 200

@api.route("/new/<dataset_name>/<dataset_version>")
def create_new_materialization(dataset_name, dataset_version):
    from app.tasks import new_materialization
    new_materialization(dataset_name, dataset_version)
    return jsonify({"Dataset Name": dataset_name, "Version": dataset_version}), 200


@api.route('/datasets')
def datasets():
    response = db.session.query(AnalysisVersion.dataset).distinct()
    datasets = [r._asdict() for r in response]
    return jsonify(datasets)


@api.route("/datasets/<dataset_name>", methods=["GET"])
def get_dataset_versions(dataset_name):
    response = db.session.query(AnalysisVersion).filter(AnalysisVersion.dataset == dataset_name).all()
    schema = AnalysisVersionSchema(many=True)
    versions, error = schema.dump(response)
    logging.info(versions)
    if versions:
        return jsonify(versions), 200
    else:
        logging.error(error)
        return abort(404)

@api.route("/datasets/<dataset_name>/<version>", methods=["GET"])
def get_dataset_tables(dataset_name, version):
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


@api.route("/datasets/<dataset_name>/<version>/<tablename>", methods=["GET"])
def get_annotations(dataset_name, version, tablename):
    sql_uri = format_version_db_uri(SQL_URI, dataset_name, version)
    session, engine = create_session(sql_uri)
    metadata = MetaData()
    try:
        annotation_table = Table(tablename, metadata, autoload=True, autoload_with=engine)
    except NoSuchTableError as e:
        logging.error(f'No table exists {e}')
        return abort(404)
    response = session.query(annotation_table).limit(10).all()
    annotations = [r._asdict() for r in response]
    if annotations:
        return jsonify(annotations), 200
    else:
        return abort(404)
