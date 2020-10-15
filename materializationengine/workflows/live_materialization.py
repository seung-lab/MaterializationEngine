import datetime
import logging
from copy import deepcopy
from functools import lru_cache, partial
from itertools import groupby, islice, repeat, takewhile
from operator import itemgetter
from typing import List

import cloudvolume
import numpy as np
import pandas as pd
from celery import Task, chain, chord, chunks, group, subtask
from celery.utils.log import get_task_logger
from flask import current_app
from sqlalchemy import MetaData, and_, create_engine, func, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import or_

from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import SegmentationMetadata
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.database import get_db, sqlalchemy_cache, create_session
from materializationengine.errors import (AnnotationParseFailure, TaskFailure,
                                          WrongModelType)
from materializationengine.shared_tasks import chunk_supervoxel_ids_task, query_id_range, fin
from materializationengine.utils import (
    make_root_id_column_name,
    create_annotation_model,
    create_segmentation_model,
    get_query_columns_by_suffix,
    get_geom_from_wkb)
from materializationengine.upsert import upsert

celery_logger = get_task_logger(__name__)

def start_materialization(aligned_volume_name: str, pcg_table_name: str, segmentation_source: dict):
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
    segmentation_source : dict
        [description]
    """

    result = get_materialization_info.s(aligned_volume_name, pcg_table_name, segmentation_source).delay()
    mat_info = result.get()

    for mat_metadata in mat_info:
        if mat_metadata:
            result = chunk_supervoxel_ids_task.s(mat_metadata).delay()
            supervoxel_chunks = result.get()

            process_chunks_workflow = chain(
                create_missing_segmentation_table.s(mat_metadata),
                chord([
                    chain(
                        get_annotations_with_missing_supervoxel_ids.s(chunk),
                        get_cloudvolume_supervoxel_ids.s(mat_metadata),
                        get_root_ids.s(mat_metadata),
                        update_segmentation_table.s(mat_metadata),
                        ) for chunk in supervoxel_chunks],
                        fin.si()), # return here is required for chords
                        update_metadata.s(mat_metadata))  # final task which will process a return status/timing etc...

            process_chunks_workflow.apply_async()
    

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
    segmentation_source:
        infoservice data
    Returns
    -------
    List[dict]
        list of dicts containing metadata for each table
    """
    db = get_db(aligned_volume)
    annotation_tables = db.get_valid_table_names()
    metadata = []
    for annotation_table in annotation_tables:
        max_id = db.get_max_id_value(annotation_table)
        if max_id:
            segmentation_table_name = build_segmentation_table_name(annotation_table, pcg_table_name)
            try:
                segmentation_metadata = db.get_segmentation_table_metadata(annotation_table,
                                                                           pcg_table_name)
            except AttributeError as e:
                celery_logger.error(f"TABLE DOES NOT EXIST: {e}")
                segmentation_metadata = {'last_updated': None}
                db.cached_session.close()

            last_updated_time_stamp = segmentation_metadata.get('last_updated', None)
            materialization_time_stamp = datetime.datetime.utcnow() 
            
            table_metadata = {
                'aligned_volume': str(aligned_volume),
                'schema': db.get_table_schema(annotation_table),
                'max_id': int(max_id),
                'segmentation_table_name': segmentation_table_name,
                'annotation_table_name': annotation_table,
                'pcg_table_name': pcg_table_name,
                'segmentation_source': segmentation_source,
                'coord_resolution': [4,4,40],
                'last_updated_time_stamp': last_updated_time_stamp,
                'materialization_time_stamp': materialization_time_stamp
            }
            metadata.append(table_metadata.copy())
    db.cached_session.close()   
    return metadata


@celery.task(name='process:create_missing_segmentation_table',
             bind=True)
def create_missing_segmentation_table(self, mat_metadata: dict) -> dict:
    """Create missing segmentation tables associated with an annotation table if it 
    does not already exist.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns:
        dict: Materialization metadata
    """
    segmentation_table_name = mat_metadata.get('segmentation_table_name')
    aligned_volume = mat_metadata.get('aligned_volume')

    SegmentationModel = create_segmentation_model(mat_metadata)
 
    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.engine
    
    if not session.query(SegmentationMetadata).filter(SegmentationMetadata.table_name==segmentation_table_name).scalar():
        SegmentationModel.__table__.create(bind=engine, checkfirst=True)
        creation_time = datetime.datetime.utcnow()
        metadata_dict = {
            'annotation_table': mat_metadata.get('annotation_table_name'),
            'schema_type': mat_metadata.get('schema'),
            'table_name': segmentation_table_name,
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
            filter(or_(SegmentationModel.id.in_(anno_ids)))]  
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
        segmatation_col_list = [col for col in supervoxel_data[0].keys()]
        segmentation_dataframe = pd.DataFrame(supervoxel_data, columns=segmatation_col_list, dtype=object).fillna(value=np.nan)
        merged_dataframe = pd.merge(segmentation_dataframe, annotation_dataframe, how='outer', left_on='id', right_on='id')
    else:
        segmentation_dataframe = pd.DataFrame(columns=supervoxel_columns, dtype=object)
        segmentation_dataframe = segmentation_dataframe.fillna(value=np.nan)
        merged_dataframe = pd.concat((segmentation_dataframe, annotation_dataframe), axis=1)
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
                pos_data = getattr(data, col)
                pos_array = np.asarray(pos_data)
                svid = np.squeeze(cv.download_point(pt=pos_array, size=1, coord_resolution=coord_resolution))
                mat_df.loc[mat_df.id == data.id, supervoxel_column] =  svid
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
    materialization_time_stamp = mat_metadata.get("materialization_time_stamp")

    supervoxel_df = pd.DataFrame(materialization_data, dtype=object)
    drop_col_names = list(
        supervoxel_df.loc[:, supervoxel_df.columns.str.endswith("position")])
    supervoxel_df = supervoxel_df.drop(drop_col_names, 1)

    AnnotationModel = create_annotation_model(mat_metadata)
    SegmentationModel = create_segmentation_model(mat_metadata)

    __, seg_model_cols, __ = get_query_columns_by_suffix(
        AnnotationModel, SegmentationModel, 'root_id')
    anno_ids = supervoxel_df['id'].to_list()

    # get current root ids from database
    try:
        session = sqlalchemy_cache.get(aligned_volume)
        current_root_ids = [data for data in session.query(*seg_model_cols).
                            filter(or_(SegmentationModel.id.in_(anno_ids)))]
    except SQLAlchemyError as e:
        session.rollback()
        current_root_ids = []
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

    cols = [x for x in root_ids_df.columns if "root_id" in x]

    updated_rows = 0

    # lookup expired roots
    if last_updated_time_stamp:
        last_updated_time_stamp = datetime.datetime.strptime(last_updated_time_stamp, '%Y-%m-%dT%H:%M:%S.%f')
        # get time stamp from 5 mins ago
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        old_roots, new_roots = cg.get_proofread_root_ids(
            last_updated_time_stamp, time_stamp)
        root_id_map = dict(zip(old_roots, new_roots))

        for col in cols:
            if not root_ids_df[root_ids_df[col].isin([old_roots])].empty:
                for old_root_id, new_root_id in root_id_map.items():
                    root_ids_df.loc[root_ids_df[col] == old_root_id, col] = new_root_id
                    updated_rows += 1

    # filter missing root_ids and lookup root_ids if missing
    mask = np.logical_and.reduce([root_ids_df[col].isna() for col in cols])
    missing_root_rows = root_ids_df.loc[mask]
    if not missing_root_rows.empty:
        supervoxel_data = missing_root_rows.loc[:, supervoxel_col_names]
        for col_name in supervoxel_data:
            if 'supervoxel_id' in col_name:
                root_id_name = col_name.replace('supervoxel_id', 'root_id')
                data = missing_root_rows.loc[:, col_name]
                root_id_array = np.squeeze(cg.get_roots(data.to_list(), time_stamp=materialization_time_stamp))
                root_ids_df.loc[data.index, root_id_name] = root_id_array
                updated_rows += len(data)

    if updated_rows == 0:
        self.request.callbacks = None
        return
        
    return root_ids_df.to_dict(orient='records')


@celery.task(name="process:update_segmentation_table",
             bind=True)
def update_segmentation_table(self, materialization_data: dict, mat_metadata: dict) -> dict:
    
    if not materialization_data:
        return None
    
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    try:
        session = sqlalchemy_cache.get(aligned_volume)
        upsert(session, materialization_data, SegmentationModel)
        upsert_time_stamp = datetime.datetime.utcnow()
        session.close()
        return {'Status': f'Upsert suceeded at {upsert_time_stamp}'}
    except SQLAlchemyError as e:
        raise e

@celery.task(name="process:update_metadata",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_metadata(self, status: dict, mat_metadata: dict):
    aligned_volume = mat_metadata['aligned_volume']
    segmentation_table_name = mat_metadata['segmentation_table_name']

    session = sqlalchemy_cache.get(aligned_volume)
    
    materialization_time_stamp = mat_metadata['materialization_time_stamp']
    last_updated_time_stamp = datetime.datetime.strptime(materialization_time_stamp, '%Y-%m-%dT%H:%M:%S.%f')

    try:
        seg_metadata = session.query(SegmentationMetadata).filter(
            SegmentationMetadata.table_name == segmentation_table_name).one()
        seg_metadata.last_updated = last_updated_time_stamp
        session.commit()
    except Exception as e:
        celery_logger.error(f"SQL ERROR: {e}")
        session.rollback()
    finally:
        session.close()


