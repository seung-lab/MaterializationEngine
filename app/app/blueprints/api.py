from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from emannotationschemas.models import AnalysisTable, AnalysisVersion
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema, MaterializationSchema
from app.extensions import db
import requests
import logging
# from app.tasks import add_together, get_status, process_ids, increment_version, create_database_from_template # incremental_materialization_task
import numpy as np

__version__ = "0.2.1"

api = Blueprint("api", __name__, url_prefix='/materialize/api/v1/')


@api.route("/chain/<name>", methods=('GET', ))
def test_chain(name):
    from app.tasks import test_chain
    task = test_chain(name)
    print(task)
    return 'Test'

@api.route("/incrementalization/<name>")
def run_increment(name):
    from app.tasks import run_incrementalization
    run_incrementalization(name)
    return "Running..."

@api.route("/<int:x>/<int:y>", methods=('GET', ))
def add(x,y):
    from app.tasks import add_together
    task = add_together.delay(x,y)
    print(task.id)
    return 'Test'

@api.route("/status")
def status():
    from app.tasks import get_status
    result = get_status.delay()
    return jsonify(name='celery', task=str(result.task_id), status=str(result.state))

@api.route("/increment/<dataset_name>", methods=('GET', ))
def new_version(dataset_name):
    from app.tasks import increment_version
    increment_version(dataset_name)
    return jsonify(name=f'increment version:{dataset_name}')

@api.route("/create_database/<new_name>:<old_name>")
def create_database(new_name, old_name):
    from app.tasks import create_database_from_template
    create_database_from_template.apply_async([new_name,old_name])
    return jsonify(name=f'{new_name}:{old_name}')

# @api.route("/dataset/<dataset_name>/new_version", methods=["POST"])
# def materialize_dataset(dataset_name):
#     if (dataset_name not in get_datasets()):
#         abort(404, "Dataset name not valid")
#     incremental_materialization_task.apply_async()
#     return jsonify({}), 200