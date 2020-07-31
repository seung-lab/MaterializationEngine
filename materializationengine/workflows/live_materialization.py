import datetime
import logging
import numpy as np
from flask import current_app
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import or_
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.automap import automap_base
from celery.utils.log import get_task_logger
import json
import cloudvolume
from celery import group, chain, chord, subtask, chunks
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.errors import AnnotationParseFailure, TaskFailure
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.utils import make_root_id_column_name
from typing import List
from copy import deepcopy
import time

celery_logger = get_task_logger(__name__)


BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ADDRESS = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]
SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]

"""
Materialization Modes

    Live segmentation:

        Iterate through an aligned_volume database:
            for each annotation table:
                get max_id
                if max_id < some threshold
                    get SegmentationModel
                    parse supervoxel columns {prefix}_supervoxel_id
                        chunk supervoxel ids for root_id lookup:
                            get all expired root_ids between now and last update per table
                            send list of ids to be query chunkedgraph for root_ids
                            return root_ids associated with supervoxel ids
                        find root_id columns associated with supervoxel id columns:
                            update or insert root_ids
                            update_last updated time_stamp in segmentation metadata
                    update root_id rows

    query db for NULL root_id rows
                {prefix}_missing_root_ids = session.query.(SegmentationModel).filter((SegmentationModel.{prefix}_root_id.is_(None)) 
                send list of ids to be query pcg for root_ids

"""


def start_materialization(aligned_volume: str):
    """Base live materialization 

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume database to target

    Returns
    -------
    [type]
        [description]
    """

    matadata_result = aligned_volume_tables_task.s(aligned_volume).delay()
    table_matadata = matadata_result.get()
    for metadata in table_matadata:
        result = chunk_supervoxel_ids_task.s(metadata).delay()
        chunks = result.get()

        supervoxel_root_id_lookup_workflow = chord(
            [update_root_ids.s(chunk, metadata) for chunk in chunks], collect.si()
        )
        supervoxel_root_id_lookup_workflow.apply_async()        

@celery.task(name="threads:collect")
def collect(*args, **kwargs):
    return True


@celery.task(name="threads:lookup_supervoxels")
def lookup_supervoxels(data, table):
    if data:
        tasks = []
        for d in data:
            celery_logger.info(d)
            tasks.append(get_supervoxel_ids_task.s(chunks=d, segmentation_data=table))
        return group(tasks)()

    else:
        raise TaskFailure("No data to process")


class SqlAlchemyTask(celery.Task):

    _models = None
    _session = None
    _engine = None

    def __call__(self, *args, **kwargs):
        aligned_volume = args[1]["aligned_volume"]
        sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
        self.sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")

        return super().__call__(*args, **kwargs)

    @property
    def engine(self):
        if self._engine is None:
            try:
                self._engine = create_engine(self.sql_uri)
                return self._engine
            except Exception as e:
                celery_logger.error(e)
        else:
            return self._engine

    @property
    def session(self):
        if self._session is None:
            try:
                self.Session = scoped_session(
                    sessionmaker(bind=self.engine)
                )
                self._session = self.Session()
                return self._session
            except Exception as e:
                celery_logger.error(e)
        else:
            return self._session

    @property
    def models(self):
        if self._models is None:
            try:
                self._models = {}
                return self._models
            except Exception as e:
                celery_logger.error(e)
        else:
            return self._models

    def after_return(self, *args, **kwargs):
        if self._session is not None:
            self.Session.remove()
        # if self._models is not None:
        #     self._models = None
        if self._engine is not None:
            self._engine.dispose()


@celery.task(bind=True, name="threads:aligned_volume_tables_task")
def aligned_volume_tables_task(self, aligned_volume: str) -> List[str]:
    """Initialize materialization by aligned volume. Iterates
    thorugh all tables and checks if they are below a table row count
    size. The list of tables are passed to workers for root_id updating
    based on supervoxel_id value and timestamp.

    Parameters
    ----------
    aligned_volume : str
        [description]

    Returns
    -------
    List[str]
        [description]
    """
    db = get_db(aligned_volume)
    segmentation_table_ids = db.get_existing_segmentation_table_ids()
    tables_to_update = []
    table_info = {}
    for seg_table_id in segmentation_table_ids:
        max_id = db._get_table_row_count(seg_table_id)
        table_name = seg_table_id.split("__")[-2]
        table_info['schema'] = db.get_table_schema(aligned_volume, table_name)
        table_info["max_id"] = int(max_id)
        table_info["segmentation_table_id"] = str(seg_table_id)
        table_info['annotation_table_id'] = f'{seg_table_id.split("__")[0]}__{aligned_volume}__{seg_table_id.split("__")[-2]}'
        table_info["aligned_volume"] = str(aligned_volume)
        table_info["pcg_table_name"] = str(seg_table_id.split("__")[-1])

        tables_to_update.append(table_info.copy())
    return tables_to_update


@celery.task(bine=True, name="threads:chunk_supervoxel_ids_task")
def chunk_supervoxel_ids_task(segmentation_data: dict, block_size: int = 2500) -> dict:
    """[summary]

    Parameters
    ----------
    segmentation_data : dict
        [description]
    block_size : int, optional
        [description], by default 2500

    Returns
    -------
    List[int]
        [description]
    """
    celery_logger.info(f"CHUNK ARGS {segmentation_data}")
    max_id = segmentation_data.get("max_id")
    n_parts = int(max(1, max_id / block_size))
    n_parts += 1
    id_chunks = np.linspace(1, max_id, num=n_parts, dtype=np.int64).tolist()
    chunk_ends = []
    for i_id_chunk in range(len(id_chunks) - 1):
        chunk_ends.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1]])
    return chunk_ends


@celery.task(
    base=SqlAlchemyTask,
    name="threads:update_root_ids",
    bind=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def update_root_ids(self, chunks: List[int], segmentation_data: dict, time_stamp = None) -> List[int]:
    """Iterates over columns with 'supervoxel_id' present in the name and
    returns supervoxel ids between start and stop ids.

    Parameters
    ----------
    segmentation_table_info: dict
        name of database to target
    start_id : int
        start index for querying
    end_id : int
        end index for querying

    Returns
    -------
    List[int]
        list of supervoxel ids between 'start_id' and 'end_id'
    """
    self.aligned_volume = segmentation_data.get("aligned_volume")
    seg_table_id = segmentation_data.get("segmentation_table_id")
    annotation_table_id = segmentation_data.get("annotation_table_id")
    schema_type = segmentation_data.get('schema')
    if seg_table_id in self.models:
        SegmentationModel = self.models[seg_table_id]
    else:
        SegmentationModel = em_models.make_segmentation_model(annotation_table_id,
                                                              schema_type,
                                                              seg_table_id.split("__")[-1])                                                   
        self.models[seg_table_id] = SegmentationModel
    try:
        logging.info(f"MODEL: {SegmentationModel}")

        columns = [column.name for column in SegmentationModel.__table__.columns]
        supervoxel_id_columns = [column for column in columns if "supervoxel_id" in column]
        logging.info(f"COLUMNS: {supervoxel_id_columns}")

        supervoxel_id_data = {}
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in self.session.query(
                    SegmentationModel.id, getattr(SegmentationModel, supervoxel_id_column)
                ).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
    except Exception as e:
        raise self.retry(exc=e, countdown=3)


    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    root_id_data = {}
    formatted_seg_data = []

    pcg_table_name = segmentation_data.get("pcg_table_name")
    # cg = ChunkedGraphGateway(pcg_table_name)

    for columns, values in supervoxel_id_data.items():
        celery_logger.info(columns)
        celery_logger.info(len(values))

        supervoxel_data = np.asarray(values, dtype=np.uint64)
        pk = supervoxel_data[:, 0].tolist()

        col = make_root_id_column_name(columns)
        celery_logger.info(col)

        # root_ids = cg.get_roots(supervoxel_data[:, 1], time_stamp=time_stamp)
        root_ids = supervoxel_data[:, 1].tolist()
        root_id_data[col] = root_ids
        celery_logger.info(root_id_data)
        # segs = [SegmentationModel(**segmentation_data)
        #                 for segmentation_data in formatted_seg_data]

    # self.session.add_all(segs)
    # self.session.commit()
    self.session.close()
    return root_id_data

@celery.task(bind=True, name="threads:get_root_ids_task")
def get_root_ids_task(
    supervoxel_id_data: List[int],
    segmentation_data: dict,
    time_stamp: datetime.datetime = None,
    pixel_ratios: List[int] = [4, 4, 40],
):
    """[summary]

    Parameters
    ----------
    supervoxel_data : dict
        name of database to target
    segmentation_data : list
        list of supervoxel ids for root_id lookup
    end_id : int
        [description]
    time_stamp : datetime.datetime
        [description]

    Returns
    -------
    [type]
        [description]
    """
    pcg_table_name = segmentation_data.get("pcg_table_name")
    # cg = ChunkedGraphGateway(pcg_table_name)

    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    root_id_data = {}
    try:
        for data in supervoxel_id_data:
            celery_logger.info(data)
            # supervoxel_data = np.asarray(supervoxels, dtype=np.uint64)
            # col = make_root_id_column_name(column_name)
            # # root_ids = cg.get_roots(supervoxel_data[:, 1], time_stamp=time_stamp)
            # root_ids = supervoxel_data
            # root_id_data[col] = root_ids
    except Exception as e:
        raise AnnotationParseFailure(e)

    # return root_id_data
    return True

@celery.task(bind=True, name="threads:update_root_ids_task")
def update_root_ids_task(
    segmentation_data: dict, start_id: int, end_id: int, time_stamp: datetime.datetime = None
):

    seg_table_id = segmentation_data.get("segmentation_table_id")
    aligned_volume = segmentation_data.get("aligned_volume")
    db = get_db(aligned_volume)
    mat_session = db.cached_session

    try:
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in mat_session.query(
                    getattr(SegmentationModel, supervoxel_id_column)
                ).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
    except Exception as e:
        raise e
