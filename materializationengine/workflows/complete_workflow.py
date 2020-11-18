
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_worker import celery
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                update_metadata,
                                                get_materialization_info)
from materializationengine.workflows.create_frozen_database import (
    create_analysis_database, create_analysis_tables, create_new_version,
    insert_annotation_data, update_analysis_metadata)
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table, get_materialization_info,
    live_update_task)
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids, update_root_ids)


@celery.task(name="process:run_complete_worflow",
             acks_late=True,
             bind=True)
def run_complete_worflow(self, datastack_info: dict):
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

    new_version_number = create_new_version(datastack_info)
    mat_info = get_materialization_info(datastack_info, new_version_number)
    database = create_analysis_database(datastack_info, new_version_number),
    materialized_tables = create_analysis_tables(datastack_info, new_version_number)
    
    for mat_metadata in mat_info:
        if mat_metadata:
            supervoxel_chunks = chunk_supervoxel_ids_task(mat_metadata)
            chunked_roots = get_expired_root_ids(mat_metadata)
            complete_workflow = chain(
                create_missing_segmentation_table.s(mat_metadata),
                chord([
                    chain(
                        live_update_task.s(chunk),
                    ) for chunk in supervoxel_chunks],
                    fin.si()),
                update_metadata.s(mat_metadata),
                chord([update_root_ids.si(root, mat_metadata)
                       for root in chunked_roots], fin.si()),
                update_metadata.si(mat_metadata),
                chord([
                    chain(insert_annotation_data.si(chunk, mat_metadata)) for chunk in supervoxel_chunks], fin.si()),
                update_analysis_metadata.si(mat_metadata))  # final task which will process a return status/timing etc...

            complete_workflow.apply_async()
