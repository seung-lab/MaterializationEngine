from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from emannotationschemas.models import AnalysisTable, AnalysisVersion
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema, MaterializationSchema
from app.extensions import db
import requests
import logging
# from app.tasks import add_together, get_status, process_ids, increment_version, create_database_from_template # incremental_materialization_task
import numpy as np
import json

__version__ = "0.2.2"

api = Blueprint("api", __name__, url_prefix='/materialize/api/v1/')

@api.route("/test_celery")
def celery_test():
    from app.tasks import test_celery
    tasks = 10000
    results = [test_celery.delay(1, 2) for i in range(tasks)]
    return f"Creating {tasks} tasks"

@api.route("/metadata/<dataset_name>", methods=('GET', ))
def get_metadata(dataset_name):
    from app.tasks import get_materialization_metadata
    results = get_materialization_metadata(dataset_name)
    logging.info("Results are: ", results)
    return results
   

@api.route("/max_root_id/<dataset_name>", methods=('GET', ))
def get_max_root_id(dataset_name):
    from app.tasks import get_max_root_id
    results = get_max_root_id(dataset_name)
    logging.info("Results are: ", results)
    return results

@api.route("/create_database/<template_dataset_name>/<new_database_name>", methods=('GET', ))
def create_database(template_dataset_name: str, new_database_name: str):
    from app.tasks import create_database_from_template
    task = create_database_from_template.delay(template_dataset_name, new_database_name)
    return f"Creating database {new_database_name} from template {template_dataset_name}"

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

@api.route("/materialize/<dataset_name>")
def materialize_annotations(dataset_name):
    from app.tasks import materialize_annotations
    # if (dataset_name not in get_datasets()):
    #     abort(404, "Dataset name not valid")
    materialize_annotations.apply_async([dataset_name])
    return jsonify({}), 200