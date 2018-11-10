from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
# from materializationengine import materialize
from emannotationschemas import get_types, get_schema

from materializationengine.models import AnalysisVersion, AnalysisTable
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.database import db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import func
import pandas as pd
from materializationengine.materialize_database import db_session
from emannotationschemas.models import make_annotation_model, make_dataset_models, declare_annotation_model

__version__ = "0.0.4"
bp = Blueprint("materialize", __name__, url_prefix="/materialize")


def make_df_with_links_to_id(objects, schema, url, col):
    df = pd.DataFrame(data=schema.dump(objects, many=True).data)
    df[col] = df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(url_for(url,
                                                            id=x.id),
                                                    x[col]),
                       axis=1)
    return df


def get_datasets():
    return ["pinky100", "test_dataset"]


@bp.route("/")
@bp.route("/index")
def index():
    return render_template('datasets.html',
                           datasets=get_datasets(),
                           version=__version__)


@bp.route('/dataset/<dataset_name>')
def dataset_view(dataset_name):
    versions = AnalysisVersion.query.filter(
        AnalysisVersion.dataset == dataset_name).all()
    schema = AnalysisVersionSchema(many=True)
    df = make_df_with_links_to_id(
        versions, schema, 'materialize.version_view', 'version')

    return render_template('dataset.html',
                           dataset=dataset_name,
                           table=df.to_html(escape=False),
                           version=__version__)


@bp.route('/version/<int:id>')
def version_view(id):
    version = AnalysisVersion.query.filter(
        AnalysisVersion.id == id).first_or_404()

    tables = AnalysisTable.query.filter(
        AnalysisTable.analysisversion == version).all()

    df = make_df_with_links_to_id(tables, AnalysisTableSchema(
        many=True), 'materialize.table_view', 'id')

    return render_template('version.html',
                           dataset=version.dataset,
                           analysisversion=version.version,
                           table=df.to_html(escape=False),
                           version=__version__)


@bp.route('/table/<int:id>')
def table_view(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()

    mapping = {
        "synapse": url_for('materialize.synapse_report', id=id),
        "cell_type_local": url_for('materialize.cell_type_local_report', id=id)
    }
    if table.schema in mapping.keys():
        return redirect(mapping[table.schema])
    else:
        return redirect(url_for('materialize.generic_report',id=id))

@bp.route('/table/<int:id>/cell_type_local')
def cell_type_local_report(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    if (table.schema != 'cell_type_local'):
        abort(504, "this table is not a cell_type_local table")

    make_dataset_models(table.analysisversion.dataset, [],
                        version=table.analysisversion.version)
    CellTypeModel = make_annotation_model(table.analysisversion.dataset,
                                          table.schema,
                                          table.tablename,
                                          version=table.analysisversion.version)

    n_annotations = db_session.query(CellTypeModel).count()

    cell_type_merge_query = (db_session.query(CellTypeModel.pt_root_id,
                                              CellTypeModel.cell_type,
                                              func.count(CellTypeModel.pt_root_id).label('num_cells'))
                             .group_by(CellTypeModel.pt_root_id, CellTypeModel.cell_type)
                             .order_by('num_cells DESC')).limit(100)

    df = pd.read_sql(cell_type_merge_query.statement,
                     db.get_engine(), coerce_float=False)
    return render_template('cell_type_local.html',
                           version=__version__,
                           schema_name=table.schema,
                           table_name=table.tablename,
                           dataset=table.analysisversion.dataset,
                           table=df.to_html())


@bp.route('/table/<int:id>/synapse')
def synapse_report(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    if (table.schema != 'synapse'):
        abort(504, "this table is not a synapse table")
    make_dataset_models(table.analysisversion.dataset, [],
                        version=table.analysisversion.version)

    SynapseModel = make_annotation_model(table.analysisversion.dataset,
                                         'synapse',
                                         table.tablename,
                                         version=table.analysisversion.version)
    synapses = db_session.query(SynapseModel).count()
    n_autapses = db_session.query(SynapseModel).filter(
        SynapseModel.pre_pt_root_id == SynapseModel.post_pt_root_id).count()

    return render_template('synapses.html',
                           num_synapses=synapses,
                           num_autapses=n_autapses,
                           dataset=table.analysisversion.dataset,
                           analysisversion=table.analysisversion.version,
                           version=__version__,
                           table_name=table.tablename,
                           schema_name='synapses')


@bp.route('/table/<int:id>/generic')
def generic_report(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()

    make_dataset_models(table.analysisversion.dataset, [],
                        version=table.analysisversion.version)

    Model = make_annotation_model(table.analysisversion.dataset,
                                  table.schema,
                                  table.tablename,
                                  version=table.analysisversion.version)

    n_annotations = db_session.query(Model).count()

    return render_template('generic.html',
                           n_annotations=n_annotations,
                           dataset=table.analysisversion.dataset,
                           analysisversion=table.analysisversion.version,
                           version=__version__,
                           table_name=table.tablename,
                           schema_name=table.schema)


@bp.route('api/dataset')
def datasets():
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # print(datasets)
    # TODO wire to info service
    return jsonify(get_datasets())


@bp.route("api/dataset/<dataset_name>", methods=["GET"])
def get_dataset_version(dataset_name):
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # print(datasets)
    if (dataset_name not in get_datasets()):
        abort(404, "dataset not valid")

    if (request.method == 'GET'):
        versions = (db.session.query(AnalysisVersion).all())
        schema = AnalysisVersionSchema(many=True)
        results = schema.dump(versions)
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
