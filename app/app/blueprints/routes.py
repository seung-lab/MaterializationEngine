from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from emannotationschemas import get_types, get_schema
from emannotationschemas.models import AnalysisTable, AnalysisVersion
from app.schemas import AnalysisVersionSchema, AnalysisTableSchema, MaterializationSchema
from app.extensions import db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import func, and_, or_
import pandas as pd
from emannotationschemas.models import make_annotation_model, make_dataset_models, declare_annotation_model
import requests
import logging


__version__ = "0.2.10"

views = Blueprint("views", __name__, url_prefix='/materialize')


def get_datasets():
    url = current_app.config['INFOSERVICE_ENDPOINT'] + "/api/datasets"
    return requests.get(url).json()


@views.route("/home")
def index():
    return render_template('datasets.html',
                           datasets=get_datasets(),
                           version=__version__)

def make_df_with_links_to_id(objects, schema, url, col):
    df = pd.DataFrame(data=schema.dump(objects, many=True).data)
    df[col] = df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(url_for(url,
                                                            id=x.id),
                                                            x[col]),
                                                            axis=1)
    return df

@views.route('/test')
def test_health():
    return jsonify({"STATUS: ":"OK"}), 200

@views.route('/dataset')
def datasets():
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # TODO wire to info service
    return jsonify(get_datasets())


@views.route("/dataset/<dataset_name>", methods=["GET"])
def get_dataset_version(dataset_name):
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    if (dataset_name not in get_datasets()):
        abort(404, "dataset not valid")

    if (request.method == 'GET'):
        versions = (db.session.query(AnalysisVersion).all())
        schema = AnalysisVersionSchema(many=True)
        # results = schema.dump(versions)
        return jsonify(schema.dump(versions).data)


@views.route('/dataset/<dataset_name>')
def dataset_view(dataset_name):
    version_query = AnalysisVersion.query.filter(
        AnalysisVersion.dataset == dataset_name)
    show_all = request.args.get('all', False) is not False
    if not show_all:
        version_query = version_query.filter(AnalysisVersion.valid == True)
    versions = version_query.order_by(AnalysisVersion.version.desc()).all()

    if len(versions) > 0:
        schema = AnalysisVersionSchema(many=True)
        df = make_df_with_links_to_id(
            versions, schema, 'materialize.version_view', 'version')
        df_html_table = df.to_html(escape=False)
    else:
        df_html_table = ""

    return render_template('dataset.html',
                           dataset=dataset_name,
                           table=df_html_table,
                           version=__version__)


@views.route('/version/<int:id>')
def version_view(id):
    version = AnalysisVersion.query.filter(
        AnalysisVersion.id == id).first_or_404()

    table_query = AnalysisTable.query.filter(
        AnalysisTable.analysisversion == version)
    tables = table_query.all()

    df = make_df_with_links_to_id(tables, AnalysisTableSchema(
        many=True), 'materialize.table_view', 'id')
    df['schema']=df.schema.map(lambda x: 
                               "<a href='/schema/type/{}/view'>{}</a>".format(x,x))
    with pd.option_context('display.max_colwidth', -1):
        output_html = df.to_html(escape=False)

    return render_template('version.html',
                           dataset=version.dataset,
                           analysisversion=version.version,
                           table=output_html,
                           version=__version__)


@views.route('/table/<int:id>')
def table_view(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    mapping = {
        "synapse": url_for('materialize.synapse_report', id=id),
        "cell_type_local": url_for('materialize.cell_type_local_report', id=id)
    }
    if table.schema in mapping.keys():
        return redirect(mapping[table.schema])
    else:
        return redirect(url_for('materialize.generic_report', id=id))


@views.route('/table/<int:id>/cell_type_local')
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

    n_annotations = CellTypeModel.query.count()

    cell_type_merge_query = (db.session.query(CellTypeModel.pt_root_id,
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


@views.route('/table/<int:id>/synapse')
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
    synapses = SynapseModel.query.count()
    n_autapses = (SynapseModel.query.filter(
        SynapseModel.pre_pt_root_id == SynapseModel.post_pt_root_id)
        .filter(and_(SynapseModel.pre_pt_root_id != 0,
                     SynapseModel.post_pt_root_id != 0)).count())
    n_no_root = (SynapseModel.query
                 .filter(or_(SynapseModel.pre_pt_root_id == 0,
                              SynapseModel.post_pt_root_id == 0)).count())

    return render_template('synapses.html',
                           num_synapses=synapses,
                           num_autapses=n_autapses,
                           num_no_root=n_no_root,
                           dataset=table.analysisversion.dataset,
                           analysisversion=table.analysisversion.version,
                           version=__version__,
                           table_name=table.tablename,
                           schema_name='synapses')


@views.route('/table/<int:id>/generic')
def generic_report(id):
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    
    make_dataset_models(table.analysisversion.dataset, [],
                        version=table.analysisversion.version)

    Model = make_annotation_model(table.analysisversion.dataset,
                                  table.schema,
                                  table.tablename,
                                  version=table.analysisversion.version)

    n_annotations = Model.query.count()

    return render_template('generic.html',
                           n_annotations=n_annotations,
                           dataset=table.analysisversion.dataset,
                           analysisversion=table.analysisversion.version,
                           version=__version__,
                           table_name=table.tablename,
                           schema_name=table.schema)


