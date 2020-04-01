from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
# from materializationengine import materialize
from emannotationschemas import get_types, get_schema
from emannotationschemas.models import AnalysisTable, AnalysisVersion, format_version_db_uri
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema
from materializationengine.database import db
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import func, and_, or_
from sqlalchemy.exc import NoSuchTableError
import pandas as pd
from emannotationschemas.models import make_annotation_model, make_dataset_models, declare_annotation_model
import requests
import logging
from flask import current_app

__version__ = "0.1.1"

SQL_URI = current_app.config['MATERIALIZATION_POSTGRES_URI']


bp = Blueprint("materialize", __name__, url_prefix="/materialize")


def create_session(sql_uri: str = None):
    engine = create_engine(sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50)
    Session = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))
    session = Session()
    return session, engine


def make_df_with_links_to_id(objects, schema, url, col):
    df = pd.DataFrame(data=schema.dump(objects, many=True).data)
    df[col] = df.apply(lambda x:
                       "<a href='{}'>{}</a>".format(url_for(url,
                                                            id=x.id),
                                                    x[col]),
                       axis=1)
    return df


def get_datasets():
    url = current_app.config['INFOSERVICE_ENDPOINT'] + "/api/datasets"
    return requests.get(url).json()


@bp.route("/")
@bp.route("/index")
def index():
    return render_template('datasets.html',
                           datasets=get_datasets(),
                           version=__version__)


@bp.route('/dataset/<dataset_name>')
def dataset_view(dataset_name):
    versions = (AnalysisVersion.query.filter(
        AnalysisVersion.dataset == dataset_name).
        order_by(AnalysisVersion.version.desc()).all())

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
        return redirect(url_for('materialize.generic_report', id=id))


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


@bp.route('/table/<int:id>/generic')
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


@bp.route('/api/v2/datasets')
def datasets():
    response = db.session.query(AnalysisVersion.dataset).distinct()
    datasets = [r._asdict() for r in response]
    return jsonify(datasets)


@bp.route("/api/v2/datasets/<dataset_name>", methods=["GET"])
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

@bp.route("/api/v2/datasets/<dataset_name>/<version>", methods=["GET"])
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


@bp.route("/api/v2/datasets/<dataset_name>/<version>/<tablename>", methods=["GET"])
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
