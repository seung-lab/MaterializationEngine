import datetime
import logging
import numpy as np
from flask import current_app
from sqlalchemy import create_engine, MetaData, text, func, and_
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import or_
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from celery.utils.log import get_task_logger
from itertools import islice, takewhile, repeat, groupby

import cloudvolume
from celery import group, chain, chord, subtask, chunks, Task
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from dynamicannotationdb.models import SegmentationMetadata
from geoalchemy2.shape import to_shape
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.extensions import create_session
from materializationengine.errors import AnnotationParseFailure, TaskFailure, WrongModelType
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.utils import make_root_id_column_name
from typing import List
from copy import deepcopy
import pandas as pd
from functools import lru_cache, partial
from operator import itemgetter
celery_logger = get_task_logger(__name__)

BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]
SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


def start_materialization(aligned_volume_name: str, pcg_table_name: str, aligned_volume_info: dict):
    """Base live materialization

    Workflow paths:
        check if supervoxel column is empty:
            if last_updated is NULL:
                -> workflow : find missing supervoxels > cloudvolume lookup supervoxels > get root ids > 
                            find missing root_ids > lookup supervoxel ids from sql > get root_ids > merge root_ids list > insert root_ids
            else:
                -> find missing supervoxels > cloudvolume lookup |
                    - > find new root_ids between time stamps  ---> merge root_ids list > upsert root_ids

    Parameters
    ----------
    aligned_volume_name : str
        [description]
    aligned_volume_info : dict
        [description]
    """

    result = get_materialization_info.s(aligned_volume_name, pcg_table_name, aligned_volume_info).delay()
    mat_info = result.get()
    for mat_metadata in mat_info:
        if mat_metadata:
            result = chunk_supervoxel_ids_task.s(mat_metadata).delay()
            supervoxel_chunks = result.get()

            process_chunks_workflow = chain(
                create_missing_segmentation_tables.s(mat_metadata),
                chord([
                    chain(
                        get_annotations_with_missing_supervoxel_ids.s(chunk),
                        get_cloudvolume_supervoxel_ids.s(mat_metadata),
                        get_root_ids.s(mat_metadata),
                        ) for chunk in supervoxel_chunks],
                        fin.si()), # return here is required for chords
                        fin.si() # final task which will process a return status/timing etc...
                    )

            process_chunks_workflow.apply_async()

class SqlAlchemyCache:

    def __init__(self):
        self._engine = None
        self._sessions = {}

    @property
    def engine(self):
        return self._engine

    def get(self, aligned_volume):
        if aligned_volume not in self._sessions:
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
            self._engine = create_engine(sql_uri, pool_recycle=3600,
                                                  pool_size=20,
                                                  max_overflow=50)
            Session = scoped_session(sessionmaker(bind=self._engine))
            self._sessions[aligned_volume] = Session
        return self._sessions[aligned_volume]

sqlalchemy_cache = SqlAlchemyCache()

@celery.task(name="process:get_materialization_info", bind=True)
def get_materialization_info(self, aligned_volume: str,
                                   pcg_table_name: str,
                                   segmentation_source: str) -> List[dict]:
    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization.

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    pcg_table_name: str
        cg_table_name
    aligned_volume_info:
        infoservice data
    Returns
    -------
    List[dict]
        list of dicts containing metadata for each table
    """
    db = get_db(aligned_volume)
    annotation_table_ids = db.get_valid_table_ids()
    metadata = []
    for annotation_table_id in annotation_table_ids:
        max_id = db.get_max_id_value(annotation_table_id)
        if max_id:
            table_name = annotation_table_id.split("__")[-1]
            segmentation_table_id = f"{annotation_table_id}__{pcg_table_name}"
            
            try:
                segmentation_metadata = db.get_segmentation_table_metadata(aligned_volume,
                                                                           table_name,
                                                                           pcg_table_name)
            except AttributeError as e:
                celery_logger.error(f"TABLE DOES NOT EXIST: {e}")
                segmentation_metadata = {'last_updated': None}
                db.cached_session.close()
            
            table_metadata = {
                'aligned_volume': str(aligned_volume),
                'schema': db.get_table_schema(aligned_volume, table_name),
                'max_id': int(max_id),
                'segmentation_table_id': segmentation_table_id,
                'annotation_table_id': annotation_table_id,
                'pcg_table_name': pcg_table_name,
                'table_name': table_name,
                'segmentation_source': segmentation_source,
                'coord_resolution': [4,4,40],
                'last_updated_time_stamp': segmentation_metadata.get('last_updated', None)
            }
            metadata.append(table_metadata.copy())
    db.cached_session.close()   
    return metadata


@celery.task(name='process:create_missing_segmentation_tables',
             bind=True)
def create_missing_segmentation_tables(self, mat_metadata: dict) -> dict:
    """Create missing segmentation tables associated with an annotation table if it 
    does not already exist.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns:
        dict: Materialization metadata
    """
    segmentation_table_id = mat_metadata.get('segmentation_table_id')
    aligned_volume = mat_metadata.get('aligned_volume')

    SegmentationModel = create_segmentation_model(mat_metadata)
 
    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.engine
    
    if not session.query(SegmentationMetadata).filter(SegmentationMetadata.table_id==segmentation_table_id).scalar():
        SegmentationModel.__table__.create(bind=engine, checkfirst=True)
        creation_time = datetime.datetime.utcnow()
        metadata_dict = {
            'annotation_table': mat_metadata.get('annotation_table_id'),
            'schema_type': mat_metadata.get('schema'),
            'table_id': segmentation_table_id,
            'valid': True,
            'created': creation_time,
            'pcg_table_name': mat_metadata.get('pcg_table_name')
        }

        seg_metadata = SegmentationMetadata(**metadata_dict)
        try:
            session.add(seg_metadata)
            session.commit()
        except Exception as e:
            celery_logger.error(f"SQL ERROR: {e}")
            session.rollback()
    else:
        session.close()
    return mat_metadata


@celery.task(name="process:chunk_supervoxel_ids_task", bind=True)
def chunk_supervoxel_ids_task(self, mat_metadata: dict, chunk_size: int = 2500) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialziation.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata
    block_size : int, optional
        [description], by default 2500

    Returns
    -------
    List[List]
        list of list containg start and end indices
    """
    AnnotationModel = create_annotation_model(mat_metadata)

    chunked_ids = chunk_ids(mat_metadata, AnnotationModel.id, chunk_size)

    return [chunk for chunk in chunked_ids]
   
def query_id_range(column, start_id: int, end_id: int):
    if end_id:
        return and_(column >= start_id, column < end_id)
    else:
        return column >= start_id

def chunk_ids(mat_metadata, model, chunk_size: int):
    aligned_volume = mat_metadata.get('aligned_volume')
    session = sqlalchemy_cache.get(aligned_volume)

    q = session.query(
        model, func.row_number().over(order_by=model).label("row_count")
    ).from_self(model)

    if chunk_size > 1:
        q = q.filter(text("row_count %% %d=1" % chunk_size))

    chunks = [id for id, in q]

    while chunks:
        chunk_start = chunks.pop(0)
        if chunks:
            chunk_end = chunks[0]
        else:
            chunk_end = None
        yield [chunk_start, chunk_end]


@celery.task(name="process:get_annotations_with_missing_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_annotations_with_missing_supervoxel_ids(self, mat_metadata: dict,
                                                      chunk: List[int]) -> dict:
    """Get list of valid annotation and their ids to lookup existing supervoxel ids. If there
    are missing supervoxels they will be set as None for cloudvolume lookup.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata
    chunk : list
        chunked range to for sql id query

    Returns
    -------
    dict
        dict of annotation and segmentation data
    """
    
    aligned_volume = mat_metadata.get("aligned_volume")
    SegmentationModel = create_segmentation_model(mat_metadata)
    AnnotationModel = create_annotation_model(mat_metadata)
    
    session = sqlalchemy_cache.get(aligned_volume)
    anno_model_cols, seg_model_cols, supervoxel_columns = get_query_columns_by_suffix(
        AnnotationModel, SegmentationModel, 'supervoxel_id')
    try:
        query = session.query(*anno_model_cols)
        chunked_id_query = query_id_range(AnnotationModel.id, chunk[0], chunk[1])
        annotation_data = [data for data in query.filter(chunked_id_query).order_by(
            AnnotationModel.id).filter(AnnotationModel.valid == True)]

        annotation_dataframe = pd.DataFrame(annotation_data, dtype=object)
        anno_ids = annotation_dataframe['id'].tolist()
        
        supervoxel_data = [data for data in session.query(*seg_model_cols).\
            filter(or_(SegmentationModel.annotation_id.in_(anno_ids)))]  
        session.close()
    except SQLAlchemyError as e:
        session.rollback()
        raise self.retry(exc=e, countdown=3)

    if not anno_ids:
        self.request.callbacks = None

    wkb_data = annotation_dataframe.loc[:, annotation_dataframe.columns.str.endswith("position")]

    annotation_dict = {}
    for column, wkb_points in wkb_data.items():
        annotation_dict[column] = [get_geom_from_wkb(wkb_point) for wkb_point in wkb_points]
    for key, value in annotation_dict.items():
        annotation_dataframe.loc[:, key] = value

    if supervoxel_data:
        segmatation_col_list = ['segmentation_id' if col == "id" else col for col in supervoxel_data[0].keys()]
        segmentation_dataframe = pd.DataFrame(supervoxel_data, columns=segmatation_col_list, dtype=object).fillna(value=np.nan)
        merged_dataframe = pd.merge(segmentation_dataframe, annotation_dataframe, how='outer', left_on='annotation_id', right_on='id')
    else:
        supervoxel_columns.extend(['annotation_id', 'segmentation_id'])
        segmentation_dataframe = pd.DataFrame(columns=supervoxel_columns, dtype=object)
        segmentation_dataframe = segmentation_dataframe.fillna(value=np.nan)
        merged_dataframe = pd.concat((segmentation_dataframe, annotation_dataframe), axis=1)
        merged_dataframe['annotation_id'] = merged_dataframe['id']
    return merged_dataframe.to_dict(orient='list')

@celery.task(name="process:get_cloudvolume_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_cloudvolume_supervoxel_ids(self, materialization_data: dict, mat_metadata: dict) -> dict:
    """Lookup missing supervoxel ids.

    Parameters
    ----------
    materialization_data : dict
        dict of annotation and segmentation data
    metadata : dict
        Materialization metadata

    Returns
    -------
    dict
        dict of annotation and with updated supervoxel id data
    """
    mat_df = pd.DataFrame(materialization_data, dtype=object)

    segmentation_source = mat_metadata.get("segmentation_source")
    coord_resolution = mat_metadata.get("coord_resolution")

    cv = cloudvolume.CloudVolume(segmentation_source, mip=0, use_https=True, bounded=False, fill_missing=True)

    position_data = mat_df.loc[:, mat_df.columns.str.endswith("position")]
    for data in mat_df.itertuples():
        for col in list(position_data):
            supervoxel_column = f"{col.rsplit('_', 1)[0]}_supervoxel_id"
            if np.isnan(getattr(data, supervoxel_column)):
                annotation_id = data.annotation_id
                pos_data = getattr(data, col)
                pos_array = np.asarray(pos_data)
                svid = np.squeeze(cv.download_point(pt=pos_array, size=1, coord_resolution=coord_resolution))
                mat_df.loc[mat_df.annotation_id == annotation_id, supervoxel_column] =  svid
    return mat_df.to_dict(orient='list')



@celery.task(name="process:get_sql_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_sql_supervoxel_ids(self, chunks: List[int], mat_metadata: dict) -> List[int]:
    """Iterates over columns with 'supervoxel_id' present in the name and
    returns supervoxel ids between start and stop ids.

    Parameters
    ----------
    chunks: dict
        name of database to target
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[int]
        list of supervoxel ids between 'start_id' and 'end_id'
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")
    session = sqlalchemy_cache.get(aligned_volume)
    try:
        columns = [column.name for column in SegmentationModel.__table__.columns]
        supervoxel_id_columns = [column for column in columns if "supervoxel_id" in column]
        supervoxel_id_data = {}
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in session.query(
                    SegmentationModel.id, getattr(SegmentationModel, supervoxel_id_column)
                ).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
        session.close()
        return supervoxel_id_data
    except Exception as e:
        raise self.retry(exc=e, countdown=3)


@celery.task(name="process:get_root_ids",
             bind=True,
             autoretry_for=(SQLAlchemyError,),
             max_retries=3)
def get_root_ids(self, materialization_data: dict, mat_metadata: dict) -> dict:
    pcg_table_name = mat_metadata.get("pcg_table_name")
    last_updated_time_stamp = mat_metadata.get("last_updated_time_stamp")
    aligned_volume = mat_metadata.get("aligned_volume")

    supervoxel_df = pd.DataFrame(materialization_data, dtype=object)
    supervoxel_df = supervoxel_df.drop(['pt_position', 'id'], 1)
    supervoxel_df = supervoxel_df.rename(columns={'segmentation_id': 'id'})

    AnnotationModel = create_annotation_model(mat_metadata)
    SegmentationModel = create_segmentation_model(mat_metadata)

    __, seg_model_cols, __ = get_query_columns_by_suffix(
        AnnotationModel, SegmentationModel, 'root_id')
    anno_ids = supervoxel_df['annotation_id'].to_list()

    # get current root ids from database
    try:
        session = sqlalchemy_cache.get(aligned_volume)
        current_root_ids = [data for data in session.query(*seg_model_cols).
                            filter(or_(SegmentationModel.annotation_id.in_(anno_ids)))]
    except SQLAlchemyError as e:
        session.rollback()
        raise self.retry(exc=e, countdown=3)
    finally:
        session.close()

    cg = ChunkedGraphGateway(pcg_table_name)

    supervoxel_col_names = list(
        supervoxel_df.loc[:, supervoxel_df.columns.str.endswith("supervoxel_id")])

    if current_root_ids:
        # merge root_id df with supervoxel df
        df = pd.DataFrame(current_root_ids, dtype=object)
        root_ids_df = pd.merge(supervoxel_df, df)

    else:
        # create empty dataframe with root_id columns
        root_id_columns = [col_name.replace('supervoxel_id', 'root_id')
                           for col_name in supervoxel_col_names if 'supervoxel_id' in col_name]
        df = pd.DataFrame(columns=root_id_columns,
                          dtype=object).fillna(value=np.nan)
        root_ids_df = pd.concat((supervoxel_df, df), axis=1)

    if not last_updated_time_stamp:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        last_updated_time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=90)

    # lookup expired roots
    old_roots, new_roots = cg.get_proofread_root_ids(
        last_updated_time_stamp, time_stamp)
    root_id_map = dict(zip(old_roots, new_roots))

    cols = [x for x in root_ids_df.columns if "root_id" in x]
    for col in cols:
        for old_root_id, new_root_id in root_id_map.items():
            root_ids_df.loc[root_ids_df[col] == old_root_id, col] = new_root_id

    # filter missing root_ids and lookup root_ids if missing
    mask = np.logical_and.reduce([root_ids_df[col].isna() for col in cols])
    missing_root_rows = root_ids_df.loc[mask]
    if not missing_root_rows.empty:
        supervoxel_data = missing_root_rows.loc[:, supervoxel_col_names]
        for col_name in supervoxel_data:
            if 'supervoxel_id' in col_name:
                root_id_name = col_name.replace('supervoxel_id', 'root_id')
                data = missing_root_rows.loc[:, col_name]
                root_id_array = np.squeeze(cg.get_roots(data.to_list()))
                root_ids_df.loc[data.index, root_id_name] = root_id_array

    return root_ids_df.to_dict(orient='records')


@celery.task(name="process:update_segmentation_table",
             bind=True)
def update_segmentation_table(self, materialization_data: dict, mat_metadata: dict) -> dict:
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    try:
        session = sqlalchemy_cache.get(aligned_volume)
        upsert(session, materialization_data, SegmentationModel)
        session.close()
    except SQLAlchemyError as e:
        raise e

@celery.task(name="process:fin", acks_late=True)
def fin(*args, **kwargs):
    return True


@celery.task(name="process:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs


def create_segmentation_model(mat_metadata):
    annotation_table_id = mat_metadata.get('annotation_table_id')
    schema_type = mat_metadata.get("schema")
    pcg_table_name = mat_metadata.get("pcg_table_name")

    SegmentationModel = em_models.make_segmentation_model(annotation_table_id, schema_type, pcg_table_name)
    return SegmentationModel

def create_annotation_model(mat_metadata):
    annotation_table_id = mat_metadata.get('annotation_table_id')
    schema_type = mat_metadata.get("schema")

    AnnotationModel = em_models.make_annotation_model(annotation_table_id, schema_type)
    return AnnotationModel

def get_geom_from_wkb(wkb):
    wkb_element = to_shape(wkb)
    if wkb_element.has_z:
        return [int(wkb_element.xy[0][0]), int(wkb_element.xy[1][0]), int(wkb_element.z)]

def get_query_columns_by_suffix(AnnotationModel, SegmentationModel, suffix):
    seg_columns = [column.name for column in SegmentationModel.__table__.columns]
    anno_columns = [column.name for column in AnnotationModel.__table__.columns]

    matched_columns = set()
    for column in seg_columns:
        prefix = (column.split("_")[0])
        for anno_col in anno_columns:
            if anno_col.startswith(prefix):
                matched_columns.add(anno_col)
    matched_columns.remove('id')

    supervoxel_columns =  [f"{col.rsplit('_', 1)[0]}_{suffix}" for col in matched_columns if col != 'annotation_id']
    # # create model columns for querying
    anno_model_cols = [getattr(AnnotationModel, name) for name in matched_columns]
    anno_model_cols.append(AnnotationModel.id)
    seg_model_cols = [getattr(SegmentationModel, name) for name in supervoxel_columns]

    # add id columns to lookup
    seg_model_cols.extend([SegmentationModel.annotation_id, SegmentationModel.id])
    return anno_model_cols, seg_model_cols, supervoxel_columns


def chunk_rows(data, chunksize: int=None):
    if chunksize:
        i = iter(data)
        generator = (list(islice(i, chunksize)) for _ in repeat(None))
    else:
        generator = iter([data])
    return takewhile(bool, generator)


def create_sql_rows(session, data_dict: dict, model):
    """Yields a dictionary if the record's id already exists, a row object 
    otherwise.
    
    TODO: strip uneeded if else statements
    """
    ids = {item[0] for item in session.query(model.id)}
    for data in data_dict:
        is_row = hasattr(data, 'to_dict')
        if is_row and data.get('id') in ids:
            yield data.to_dict(), True
        elif is_row:
            yield data, False
        elif data.get('id') in ids:
            yield data, True
        else:
            yield model(**data), False

def upsert(session, data, model, chunksize=None):
    
    for records in chunk_rows(data, chunksize):
        resources = create_sql_rows(session, records, model)
        sorted_resources = sorted(resources, key=itemgetter(1))
        for key, group in groupby(sorted_resources, itemgetter(1)):
            data = [g[0] for g in group]

            if key:
                session_upsert = partial(session.bulk_update_mappings, model)
            else:
                session_upsert = session.add_all
            try:
                session_upsert(data)
                session.commit()
            except IntegrityError:
                session.rollback()
                upsert(session, data, model)
            except Exception as e:
                session.rollback()
                num_rows = len(data)

                if num_rows > 1:
                    upsert(session, data, model, num_rows // 2)
                else:
                    celery_logger.error(e)
                    raise SQLAlchemyError
                    
