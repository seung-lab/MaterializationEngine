from flask import Blueprint, jsonify, abort, current_app
from materializationengine import materialize
from emannotationschemas import get_types, get_schema
import sqlalchemy
import requests
import os

__version__ = "0.0.1"
bp = Blueprint("materialize", __name__, url_prefix="/materialize")


@bp.route("/")
@bp.route("/index")
def index():
    return "Materialization Engine -- version " + __version__


@bp.route("/dataset/<dataset>")
def materialize_dataset(dataset):

    sql_uri = current_app.config['MATERIALIZATION_POSTGRES_URI']
    cg_table = current_app.config['CHUNKGRAPH_TABLE_ID']
    cg_instance_id = current_app.config['BIG_TABLE_CONFIG']['instance_id']
    amdb_instance_id = current_app.config['BIG_TABLE_CONFIG']['amdb_instance_id']
    types = get_types()

    # sort so the Reference annotations are materialized after the regular ones
    # TODO clarify if Reference of references are something we want to allow
    sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
                                                      ReferenceAnnotation))

    engine = sqlalchemy.create_engine(sql_uri)

    for type_ in sorted_types:
        materialize.materialize_all_annotations(cg_table,
                                                dataset,
                                                type_,
                                                amdb_instance_id=amdb_instance_id,
                                                cg_instance_id=cg_instance_id,
                                                sqlalchemy_database_uri=sql_uri,
                                                n_threads=4)
