import datetime
import numpy as np
from flask import current_app
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import or_
from sqlalchemy.engine.url import make_url
from celery.utils.log import get_task_logger
import cloudvolume
from celery import group, chain, chord, subtask, chunks
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from geoalchemy2.shape import to_shape
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.errors import AnnotationParseFailure, TaskFailure
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.utils import make_root_id_column_name
from typing import List
from copy import deepcopy
import pandas as pd

celery_logger = get_task_logger(__name__)


BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]
SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


def start_materialization(aligned_volume_name: str, aligned_volume_info: dict):
    """Base live materialization

    Workflow paths:
        check if supervoxel column is empty:
            if last_updated is NULL:
                -> workflow : find missing supervoxels > cloudvolume lookup supervoxels > get root ids > 
                            find missing root_ids > lookup supervoxel ids from sql > get root_ids > merge root_ids list > insert root_ids
            else:
                -> find missing supervoxels > cloudvolume lookup |
                    - > find new root_ids between time stamps  ---> merge root_ids list > upsert root_ids

    """

    matadata_result = aligned_volume_tables_metadata.s(aligned_volume_name, aligned_volume_info).delay()
    table_matadata = matadata_result.get()
    for metadata in table_matadata:
        result = chunk_supervoxel_ids_task.s(metadata).delay()
        supervoxel_chunks = result.get()

        # process_chunks_workflow = chain(
            # collect_data.s())
        process_chunks_workflow = chain(            
            chord(
                [
                    chain(
                        get_annotations_with_missing_supervoxel_ids.s(chunk, metadata),
                        get_cloudvolume_supervoxel_ids.s(metadata),
                        get_root_ids_from_supervoxels.s(metadata),
                        collect_data.s(metadata),
                    )
                    for chunk in supervoxel_chunks
                ],
                collect_data.s(),
            ),)
            # chain(
        #         get_cloudvolume_supervoxel_ids.s(metadata),
        #         get_root_ids_from_supervoxels.s(metadata),
        #     ),
        # )
        process_chunks_workflow.apply_async()



class SqlAlchemyTask(celery.Task):

    _models = None
    _session = None
    _engine = None

    def __call__(self, *args, **kwargs):
        aligned_volume = args[1].get("aligned_volume")
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
                self.Session = scoped_session(sessionmaker(bind=self.engine))
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


@celery.task(name="threads:aligned_volume_tables_metadata", bind=True)
def aligned_volume_tables_metadata(self, aligned_volume: str,
                                         aligned_volume_info: dict) -> List[str]:
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

    table_info = {}
    tables_to_update = []
    for seg_table_id in segmentation_table_ids:
        max_id = db._get_table_row_count(seg_table_id)
        table_name = seg_table_id.split("__")[-2]
        table_info["schema"] = db.get_table_schema(aligned_volume, table_name)
        table_info["max_id"] = int(max_id)
        table_info["segmentation_table_id"] = str(seg_table_id)
        table_info["annotation_table_id"] = f'{seg_table_id.split("__")[0]}__{aligned_volume}__{seg_table_id.split("__")[-2]}'
        table_info["aligned_volume"] = str(aligned_volume)
        table_info["pcg_table_name"] = str(seg_table_id.split("__")[-1])
        table_info["segmentation_source"] = aligned_volume_info['segmentation_source']
        tables_to_update.append(table_info.copy())
    return tables_to_update


@celery.task(name="threads:chunk_supervoxel_ids_task", bind=True)
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


@celery.task(name="threads:get_annotations_with_missing_supervoxel_ids",
             base=SqlAlchemyTask,
             bind=True)
def get_annotations_with_missing_supervoxel_ids(self, chunk: List[int], 
                                                      segmentation_data: dict) -> dict:
    segmentation_table_id = segmentation_data.get("segmentation_table_id")
    annotation_table_id = segmentation_data.get("annotation_table_id")
    schema_type = segmentation_data.get("schema")

    if segmentation_table_id in self.models:
        SegmentationModel = self.models[segmentation_table_id]
    else:
        SegmentationModel = em_models.make_segmentation_model(
            annotation_table_id, schema_type, segmentation_table_id.split("__")[-1])
        self.models[segmentation_table_id] = SegmentationModel

    if annotation_table_id in self.models:
        AnnotationModel = self.models[annotation_table_id]
    else:
        AnnotationModel = em_models.make_annotation_model(annotation_table_id, schema_type)
        self.models[annotation_table_id] = AnnotationModel

    seg_columns = [column.name for column in SegmentationModel.__table__.columns]
    anno_columns = [column.name for column in AnnotationModel.__table__.columns]

    matches = set()
    for column in seg_columns:
        prefix = (column.split("_")[0])
        for anno_col in anno_columns:
            if anno_col.startswith(prefix):
                matches.add(anno_col)
    matches.remove('id')

    spatial_points = {}
    for anno_column in matches:
        column_prefix = anno_column.rsplit("_", 1)[0]
        supervoxel_column = f"{column_prefix}_supervoxel_id"
        spatial_points[anno_column] = [
            data
            for data in self.session.query(SegmentationModel.annotation_id,
                getattr(AnnotationModel, anno_column)).join(SegmentationModel).\
            filter(or_(SegmentationModel.annotation_id).between(int(chunk[0]), int(chunk[1]))).\
            filter(or_(getattr(SegmentationModel, supervoxel_column) == None)).all()
        ]
    # if there are no missing supervoxel rows, cancel the chain in the workflow
    if any([spatial_points[i] != [] for i in spatial_points]):
        self.request.chain = self.request.callbacks = None
    else:
        annotation_data = {}
        for column, wkb_points in spatial_points.items():
            points = [get_geom_from_wkb(wkb_point) for wkb_point in wkb_points]
            annotation_data[column] = points

        return annotation_data


def get_geom_from_wkb(wkb):
    wkb_element = to_shape(wkb[1])
    if wkb_element.has_z:
        return {'id': wkb[0], 'point': [int(wkb_element.xy[0][0]), int(wkb_element.xy[1][0]), int(wkb_element.z)]}


@celery.task(name="threads:get_cloudvolume_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_cloudvolume_supervoxel_ids(self, annotation_data: dict, segmentation_data: dict) -> dict:
    dict_df = pd.DataFrame({key: pd.Series(value) for key, value in annotation_data.items()})

    segmentation_source = segmentation_data.get("segmentation_source")
    cv = cloudvolume.CloudVolume(segmentation_source, mip=0)

    supervoxel_columns = {}
    supervoxel_row = {}

    for col_name in list(dict_df):
        supervoxel_data = []
        for column in dict_df.itertuples(index=False):
            row = (getattr(column, col_name))
            if pd.notna(row):
                np_row = (np.asarray((row['point']), dtype=np.uint64))
                supervoxel_id = np.squeeze(cv.download_point(np_row, size=1)) 
                supervoxel_row['id'] = row['id']
                supervoxel_row['supervoxel_id'] = int(supervoxel_id)
                supervoxel_data.append(supervoxel_row.copy())
        supervoxel_columns[col_name] = supervoxel_data
    return supervoxel_columns


@celery.task(name="threads:get_sql_supervoxel_ids",
             base=SqlAlchemyTask,
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_sql_supervoxel_ids(self, chunks: List[int], segmentation_data: dict) -> List[int]:
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
    schema_type = segmentation_data.get("schema")
    if seg_table_id in self.models:
        SegmentationModel = self.models[seg_table_id]
    else:
        SegmentationModel = em_models.make_segmentation_model(
            annotation_table_id, schema_type, seg_table_id.split("__")[-1]
        )
        self.models[seg_table_id] = SegmentationModel
    try:
        columns = [column.name for column in SegmentationModel.__table__.columns]
        supervoxel_id_columns = [column for column in columns if "supervoxel_id" in column]
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
        return supervoxel_id_data
    except Exception as e:
        raise self.retry(exc=e, countdown=3)


@celery.task(name="threads:get_root_ids_from_supervoxels",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_root_ids_from_supervoxels(self, supervoxel_id_data, segmentation_data, time_stamp=None):

    pcg_table_name = segmentation_data.get("pcg_table_name")
    cg = ChunkedGraphGateway(pcg_table_name)

    time_stamp = None
    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    frames = []
    columns = []

    for column_name, supervoxel_data in supervoxel_id_data.items():
        columns.append(column_name)
        supervoxel_df = pd.DataFrame(supervoxel_data,  columns=['id', 'supervoxel_id'])
        frames.append(supervoxel_df)
    df = pd.concat(frames, keys=columns)

    root_id_frames = []
    for column in columns:
        root_id_df = pd.DataFrame()

        col = make_root_id_column_name(column)

        column_ids = df.loc[column]['id']
        supervoxels_ids = df.loc[column]['supervoxel_id']
        supervoxels_array = np.array(supervoxels_ids.values.tolist(), dtype=np.uint64)
        root_ids = cg.get_roots(supervoxels_array, time_stamp=time_stamp)
        root_id_df['id'] = column_ids
        root_id_df[col] = root_ids.tolist()
        root_id_frames.append(root_id_df)
    data = pd.concat(root_id_frames)

    return data.apply(lambda x: [x.dropna().to_dict()], axis=1).sum()


@celery.task(name="threads:get_expired_root_ids", 
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_expired_root_ids(self, last_updated_time, segmentation_data, time_stamp=None):
    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    root_id_dict = {}
    root_column_names = []

    pcg_table_name = segmentation_data.get("pcg_table_name")
    cg = ChunkedGraphGateway(pcg_table_name)
    old_roots, new_roots = cg.get_proofread_root_ids(last_updated_time, time_stamp)
    # for columns, values in supervoxel_id_data.items():
    #     col = make_root_id_column_name(columns)
    #     root_column_names.append(col)
    #     supervoxel_data = np.asarray(values, dtype=np.uint64)
    #     pk = supervoxel_data[:, 0]

    #     root_ids = cg.get_roots(supervoxel_data[:, 1], time_stamp=time_stamp)

    #     root_id_dict["id"] = pk
    #     root_id_dict[col] = root_ids

    root_column_names.append("id")
    df = pd.DataFrame(root_id_dict)
    data = df.to_dict("records")
    return data


@celery.task(name="threads:update_root_ids",
             base=SqlAlchemyTask, bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_root_ids(self, root_id_data, segmentation_data) -> bool:
    self.aligned_volume = segmentation_data.get("aligned_volume")
    seg_table_id = segmentation_data.get("segmentation_table_id")
    annotation_table_id = segmentation_data.get("annotation_table_id")
    schema_type = segmentation_data.get("schema")
    if seg_table_id in self.models:
        SegmentationModel = self.models[seg_table_id]
    else:
        SegmentationModel = em_models.make_segmentation_model(
            annotation_table_id, schema_type, seg_table_id.split("__")[-1]
        )
        self.models[seg_table_id] = SegmentationModel
    try:
        self.session.bulk_update_mappings(SegmentationModel, root_id_data)
        self.session.commit()
    except Exception as e:
        self.cached_session.rollback()
        celery_logger.error(f"SQL Error: {e}")
    finally:
        self.session.close()
    return True


@celery.task(name="threads:update_supervoxel_rows",
             base=SqlAlchemyTask,
             bind=True,
             acks_late=True)
def update_supervoxel_rows(self, data, segmentation_data) -> bool:
    self.aligned_volume = segmentation_data.get("aligned_volume")
    seg_table_id = segmentation_data.get("segmentation_table_id")
    annotation_table_id = segmentation_data.get("annotation_table_id")
    schema_type = segmentation_data.get("schema")
    if seg_table_id in self.models:
        SegmentationModel = self.models[seg_table_id]
    else:
        SegmentationModel = em_models.make_segmentation_model(annotation_table_id,
                                                              schema_type,
                                                              seg_table_id.split("__")[-1])
        self.models[seg_table_id] = SegmentationModel

    segmentations = []
    for k, v in data.items():
        for row in v:
            row[k] = row.pop('supervoxel_id')
            segmentations.append(row)

    schema_type = self.get_schema(schema_type)

    __, flat_segmentation_schema = em_models.split_annotation_schema(schema_type)

    for segmentation in segmentations:
        flat_data = [
            data[key]
            for key, value in flat_segmentation_schema._declared_fields.items() if key in data]

    try:
        for data in flat_data:
            self.session.bulk_update_mappings(SegmentationModel, data)
            self.session.flush()
        self.session.commit()
        return True
    except Exception as e:
        self.Session.rollback()
        celery_logger.error(f"SQL ERROR {e}")
        return False
    finally:
        self.session.close()


@celery.task(name="threads:fin", acks_late=True)
def fin(*args, **kwargs):
    return True


@celery.task(name="threads:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs
