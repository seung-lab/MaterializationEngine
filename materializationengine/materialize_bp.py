from flask import Blueprint, jsonify, abort, current_app, request, render_template, url_for, redirect
from materializationengine import materialize
from emannotationschemas import get_types, get_schema
from emannotationschemas.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import AnalysisVersionSchema, AnalysisTableSchema, IncrementalMaterializationSchema
from materializationengine.database import db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import func, and_, or_
import pandas as pd
from emannotationschemas.models import make_annotation_model, make_dataset_models, declare_annotation_model
import requests
import logging

__version__ = "0.1.2"
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


@bp.route('/version/<int:id>')
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


@bp.route('api/dataset')
def datasets():
    # datasets = (AnalysisVersion.query.filter(AnalysisVersion.dataset = 'pinky100')
    #             .first_or_404())
    # TODO wire to info service
    return jsonify(get_datasets())


@bp.route("api/dataset/<dataset_name>", methods=["GET"])
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


@bp.route("/dataset/<dataset_name>/new_version", methods=["POST"])
def materialize_dataset(dataset_name):
    if (datatset_name not in get_datasets()):
        abort(404, "Dataset name not valid")

    materialized_schema = IncrementalMaterializationSchema()
    dataset = dataset_name
    sql_uri = current_app.config['MATERIALIZATION_POSTGRES_URI']
    cg_table = current_app.config['CHUNKGRAPH_TABLE_ID']
    cg_instance_id = current_app.config['BIG_TABLE_CONFIG']['instance_id']
    amdb_instance_id = current_app.config['BIG_TABLE_CONFIG']['amdb_instance_id']

    blacklist = ["pni_synapses", "pni_synapses_i2",  "is_chandelier"]
    
    engine = create_engine(sql_uri)
    Session = sessionmaker(bind=engine)
    session = Session()
 
    analysisversion = materializationmanager.create_new_version(
        sql_uri, dataset_name, materialized_schema['time_stamp'])
    version = analysisversion.version
    base_version_number = materialized_schema['base_version']
    base_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==base_version_number).first()

    new_db_name = f'{dataset_name}_v{analysisversion.version}'
    base_db_name = f'{dataset_name}_v{base_version_number}'

    version_db_uri = format_version_db_uri(sql_uri, dataset_name, version)
    base_version_db_uri = format_version_db_uri(sql_uri, dataset_name,  base_version_number)
    current_app.logger.info(version_db_uri)
    current_app.logger.info(f'making new version {analysisversion.version} with timestamp {analysisversion.time_stamp}')
    start = time.time()
    conn = engine.connect()
    conn.execute("commit")
    conn.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '{base_db_name}';")
    conn.execute(f"create database {new_db_name} TEMPLATE {base_db_name}")
    copy_time = time.time()-start

    timings = {}
    timings['copy database'] = copy_time 

    tables = session.query(AnalysisTable).filter(
             AnalysisTable.analysisversion == base_version).all()

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            time_start = time.time()
            new_analysistable = AnalysisTable(schema=table.schema,
                                              tablename=table.tablename,
                                              valid=False,
                                              analysisversion_id=analysisversion.id)
            session.add(new_analysistable)
            session.commit()
            timings[table.tablename] = time.time() - time_start
            current_app.logger.info(table.tablename)

    for k in timings:
        current_app.logger.info("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))

    assert(base_version is not None)
    timings = {}
    
    base_version_engine = create_engine(base_version_db_uri)
    BaseVersionSession = sessionmaker(bind=base_version_engine)
    base_version_session = BaseVersionSession()
    root_model = em_models.make_cell_segment_model(dataset_name, version=analysisversion.version)

    prev_max_id = int(base_version_session.query(func.max(root_model.id).label('max_root_id')).first()[0])
    cg = chunkedgraph.ChunkedGraph(table_id=materialized_schema['cg_table_id'])
    max_root_id = materialize.find_max_root_id_before(cg,
                                                      base_version.time_stamp,
                                                      2*chunkedgraph.LOCK_EXPIRED_TIME_DELTA,
                                                      start_id=np.uint64(prev_max_id),
                                                      delta_id=100)
    max_seg_id = cg.get_segment_id(np.uint64(max_root_id))
    time_start = time.time()
    new_roots, old_roots = materialize.materialize_root_ids_delta(materialized_schema["cg_table_id"],
                                                                  dataset_name=materialized_schema["dataset_name"],
                                                                  time_stamp=analysisversion.time_stamp,
                                                                  time_stamp_base=base_version.time_stamp,
                                                                  min_root_id = max_seg_id,
                                                                  analysisversion=analysisversion,
                                                                  sqlalchemy_database_uri=version_db_uri,
                                                                  cg_instance_id=materialized_schema["cg_instance_id"],
                                                                  n_threads=materialized_schema["n_threads"])
    timings['root_ids'] = time.time()-time_start
    
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    
    anno_client = AnnotationClient(dataset_name=dataset_name)
    all_tables = anno_client.get_tables()
    missing_tables_info = [t for t in all_tables 
                           if (t['table_name'] not in [t.tablename for t in tables]) 
                           and (t['table_name']) not in blacklist]

    for table_info in missing_tables_info:
        materialize.materialize_all_annotations(materialized_schema["cg_table_id"],
                                                materialized_schema["dataset_name"],
                                                table_info['schema_name'],
                                                table_info['table_name'],
                                                analysisversion=analysisversion,
                                                time_stamp=analysisversion.time_stamp,
                                                cg_instance_id=materialized_schema["cg_instance_id"],
                                                sqlalchemy_database_uri=version_db_uri,
                                                block_size=100,
                                                n_threads=25*materialized_schema["n_threads"])
        at = AnalysisTable(schema=table_info['schema_name'],        
                           tablename=table_info['table_name'],
                           valid=True,
                           analysisversion=analysisversion)
        session.add(at)
        session.commit()
        
    
    tables = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == analysisversion).all()
    version_engine = create_engine(version_db_uri)
    VersionSession = sessionmaker(bind=version_engine)
    version_session = VersionSession()
    version_session.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analysis_user;')
    version_session.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO analysis_user;')

    for table in tables:
        if table.schema != em_models.root_model_name.lower():
            time_start = time.time()
            materialize.materialize_annotations_delta(materialized_schema["cg_table_id"],
                                                      materialized_schema["dataset_name"],
                                                      table.tablename,
                                                      table.schema,
                                                      old_roots,
                                                      analysisversion,
                                                      version_db_uri,
                                                      cg_instance_id=materialized_schema["cg_instance_id"],
                                                      n_threads=3*materialized_schema["n_threads"])
            timings[table.tablename] = time.time() - time_start
    root_model = em_models.make_cell_segment_model(dataset_name, version=analysisversion.version)
    version_session.query(root_model).filter(root_model.id.in_(old_roots.tolist())).delete(synchronize_session=False)

    version_session.commit()
    
    new_version = session.query(AnalysisVersion).filter(AnalysisVersion.version==analysisversion.version).first()
    new_version.valid = True
    session.commit()

    for k in timings:
        current_app.logger.info("%s: %.2fs = %.2fmin" % (k, timings[k], timings[k] / 60))
    return jsonify(new_version=analysisversion.version,
                   old_version=base_version_number,
                   ), 200