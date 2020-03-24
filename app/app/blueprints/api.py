from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from emannotationschemas.models import AnalysisTable, AnalysisVersion
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema, MaterializationSchema
from app.extensions import db
from app.blueprints.routes import get_datasets
import requests
import logging
import numpy as np
import json

__version__ = "0.2.24"

api = Blueprint("api", __name__, url_prefix='/materialize/api/v1/')

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
    return jsonify({"Dataset Name": dataset_name, "Version":dataset_version}), 200

@api.route("/new/<dataset_name>/<dataset_version>")
def create_new_materialization(dataset_name, dataset_version):
    from app.tasks import new_materialization
    new_materialization(dataset_name, dataset_version)
    return jsonify({"Dataset Name": dataset_name, "Version":dataset_version}), 200

# @api.route("/new/<dataset_name>/<dataset_version>/<use_latest>/<increment>")
# def setup_db(dataset_name, dataset_version, use_latest, increment):
#     from app.tasks import setup_new_database
#     setup_new_database(dataset_name, dataset_version, use_latest, increment)
#     return jsonify({"Dataset Name": dataset_name, "Version":dataset_version}), 200