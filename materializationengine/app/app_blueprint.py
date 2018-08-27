from flask import Blueprint, request, make_response, g
from flask import current_app
# from google.cloud import pubsub_v1
import json
import numpy as np
import time
import datetime
from materializationengine.app import app_utils
from emannotationschemas.models import root_model_name
from emannotationschemas import get_types, get_schema
from emannotationschemas.base import ReferenceAnnotation
from materializationengine import materialize

bp = Blueprint('materializationengine', __name__, url_prefix="/")

# -------------------------------
# ------ Access control and index
# -------------------------------


@bp.route('/')
@bp.route("/index")
def index():
    return "MaterializationEngine -- 0.1"


@bp.route
def home():
    resp = make_response()
    resp.headers['Access-Control-Allow-Origin'] = '*'
    acah = "Origin, X-Requested-With, Content-Type, Accept"
    resp.headers["Access-Control-Allow-Headers"] = acah
    resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    resp.headers["Connection"] = "keep-alive"
    return resp


# -------------------------------
# ------ Measurements and Logging
# -------------------------------

@bp.before_request
def before_request():
    print("NEW REQUEST:", datetime.datetime.now(), request.url)
    g.request_start_time = time.time()


@bp.after_request
def after_request(response):
    dt = (time.time() - g.request_start_time) * 1000

    url_split = request.url.split("/")
    current_app.logger.info("%s - %s - %s - %s - %f.3" %
                            (request.path.split("/")[-1], "1",
                             "".join([url_split[-2], "/", url_split[-1]]),
                             str(request.data), dt))

    print("Response time: %.3fms" % (dt))
    return response


@bp.errorhandler(500)
def internal_server_error(error):
    dt = (time.time() - g.request_start_time) * 1000

    url_split = request.url.split("/")
    current_app.logger.error("%s - %s - %s - %s - %f.3" %
                             (request.path.split("/")[-1],
                              "Server Error: " + error,
                              "".join([url_split[-2], "/", url_split[-1]]),
                              str(request.data), dt))
    return 500


@bp.errorhandler(Exception)
def unhandled_exception(e):
    dt = (time.time() - g.request_start_time) * 1000

    url_split = request.url.split("/")
    current_app.logger.error("%s - %s - %s - %s - %f.3" %
                             (request.path.split("/")[-1],
                              "Exception: " + str(e),
                              "".join([url_split[-2], "/", url_split[-1]]),
                              str(request.data), dt))
    return 500


# @bp.route('/dataset/', methods=['GET'])
# def get_dataset_info(dataset):
#     """ Get a list of all datasets

#     """


# @bp.route('/dataset/<dataset>', methods=['GET'])
# def get_dataset_info(dataset):
#     """ Get a summary of all information about a dataset

#     """

@bp.route('/dataset/<dataset>/materialize/all', methods=['POST'])
def materialize_all(dataset):
    """ Rematerialize all annotations of all types in dataset

    """
    materialize_specific(dataset, root_model_name.lower())
    types = get_types()

    sorted_types = sorted(types, key=lambda x: issubclass(get_schema(x),
                                                          ReferenceAnnotation))
    for type_ in sorted_types:
        materialize_specific(dataset, type_)


@bp.route('/dataset/<dataset>/materialize/<annotation_type>', methods=['POST'])
def materialize_specific(dataset, annotation_type):
    """ Rematerialize a specific annotation type

    """
    timestamp = datetime.datetime.utcnow()
    cfg = current_app.config
    if annotation_type == root_model_name.lower():
        materialize.materialize_root_ids(cfg.CG_TABLE_ID,
                                         dataset,
                                         timestamp,
                                         cg_instance_id=cfg.CG_INSTANCE_ID,
                                         sqlalchemy_database_uri=cfg.POSTGRES_URI,
                                         n_threads=cfg.N_THREADS)

    else:
        materialize.materialize_all_annotations(cfg.CG_TABLE_ID,
                                                dataset,
                                                annotation_type,
                                                time_stamp=timestamp,
                                                amdb_instance_id=cfg.AMDB_INSTANCE_ID,
                                                cg_instance_id=cfg.CG_INSTANCE_ID,
                                                sqlalchemy_database_uri=cfg.POSTGRES_URI,
                                                n_threads=cfg.N_THREADS)
