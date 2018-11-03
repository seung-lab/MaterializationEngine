from flask import Blueprint, jsonify, abort, current_app, request
# from materializationengine import materialize
from emannotationschemas import get_types, get_schema
from materializationengine.models import AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema
from materializationengine.database import db

__version__ = "0.0.1"
bp = Blueprint("materialize", __name__, url_prefix="/materialize")


@bp.route("/")
@bp.route("/index")
def index():
    return "Materialization Engine -- version " + __version__

def get_datasets():
    return ["pinky100"]

@bp.route('/dataset')
def datasets():
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # print(datasets)
    # TODO wire to info service
    return jsonify(get_datasets())
    

@bp.route("/dataset/<dataset_name>", methods=["GET"])
def get_dataset_version(dataset_name):
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # print(datasets)
    if (dataset_name not in get_datasets()):
        abort(404, "dataset not valid")
        
    if (request.method == 'GET'):
        versions = (db.session.query(AnalysisVersion).all())
        schema = AnalysisVersionSchema(many=True)
        print(versions)
        results = schema.dump(versions)
        print(results)
        return jsonify(schema.dump(versions).data)

    

@bp.route("/dataset/<dataset_name>/new_version", methods=["POST"])
def materialize_dataset(dataset):

    return "This should materialize a new version eventually"
    # sql_uri = current_app.config['MATERIALIZATION_POSTGRES_URI']
    # cg_table = current_app.config['CHUNKGRAPH_TABLE_ID']
    # cg_instance_id = current_app.config['BIG_TABLE_CONFIG']['instance_id']
    # amdb_instance_id = current_app.config['BIG_TABLE_CONFIG']['amdb_instance_id']
    # types = get_types()

    # # sort so the Reference annotations are materialized after the regular ones
    # # TODO clarify if Reference of references are something we want to allow
    # sorted_types = sorted(types, lambda x: issubclass(get_schema(x),
    #                                                   ReferenceAnnotation))
    # for type_ in sorted_types:
    #     materialize.materialize_all_annotations(cg_table,
    #                                             dataset,
    #                                             type_,
    #                                             amdb_instance_id=amdb_instance_id,
    #                                             cg_instance_id=cg_instance_id,
    #                                             sqlalchemy_database_uri=sql_uri,
    #                                             n_threads=4)
