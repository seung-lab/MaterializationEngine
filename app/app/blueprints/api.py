from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema, IncrementalMaterializationSchema
from app.database import db
import requests
import logging
from app.tasks import add_together, get_status

__version__ = "0.2.0"

api = Blueprint("api", __name__, url_prefix='/api/v1/')


def get_datasets():
    url = current_app.config['INFOSERVICE_ENDPOINT'] + "/api/datasets"
    return requests.get(url).json()


@api.route("/<int:x>/<int:y>", methods=('GET', ))
def add(x,y):
    task = add_together.delay(x,y)
    print(task.id)
    return 'Test'


@api.route("/status")
def status():
    result = get_status.delay()
    return jsonify(name='celery', task=str(result.task_id), status=str(result.state))
 

@api.route('/dataset')
def datasets():
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # TODO wire to info service
    return jsonify(get_datasets())


@api.route("/dataset/<dataset_name>", methods=["GET"])
def get_dataset_version(dataset_name):
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    if (dataset_name not in get_datasets()):
        abort(404, "dataset not valid")

    if (request.method == 'GET'):
        versions = (db.session.query(AnalysisVersion).all())
        schema = AnalysisVersionSchema(many=True)
        results = schema.dump(versions)
        return jsonify(schema.dump(versions).data)


@api.route("/dataset/<dataset_name>/new_version", methods=["POST"])
def materialize_dataset(dataset_name):
    if (dataset_name not in get_datasets()):
        abort(404, "Dataset name not valid")
    incremental_materialization_task.apply_async()
    return jsonify({}), 200