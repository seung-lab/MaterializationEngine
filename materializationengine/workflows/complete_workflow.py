import datetime
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_worker import celery
from materializationengine.shared_tasks import (chunk_supervoxel_ids_task, fin,
                                                update_metadata,
                                                get_materialization_info,
                                                final_task)
from materializationengine.workflows.create_frozen_database import (
    create_analysis_database, create_analysis_tables, create_new_version,
    copy_data_from_live_table, update_analysis_metadata, drop_indexes, add_indexes, check_tables)
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table, get_materialization_info,
    ingest_new_annotations)
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids, update_root_ids)

celery_logger = get_task_logger(__name__)


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
    materialization_time_stamp = datetime.datetime.utcnow()

    new_version_number = create_new_version(datastack_info)
    mat_info = get_materialization_info(datastack_info, new_version_number, materialization_time_stamp)


    update_live_database_tasks = []

    for mat_metadata in mat_info:
        supervoxel_chunks = chunk_supervoxel_ids_task(mat_metadata)
        chunked_roots = get_expired_root_ids(mat_metadata)
        if mat_metadata['row_count'] < 1_000_000:
            new_annotation_workflow = chain(
                create_missing_segmentation_table.si(mat_metadata),
                chord([
                    chain(
                        ingest_new_annotations.si(mat_metadata, chunk),
                    ) for chunk in supervoxel_chunks],
                    fin.si()),  # return here is required for chords
                fin.si())
        else:
            new_annotation_workflow = None
    
        update_expired_roots_workflow = chain(
            chord([
                group(update_root_ids(root_ids, mat_metadata))
                   for root_ids in chunked_roots],
                   fin.si()),
            update_metadata.si(mat_metadata),
        )

        if new_annotation_workflow is not None:
            ingest_and_freeze_workflow = chain(
                new_annotation_workflow, update_expired_roots_workflow)
            update_live_database_tasks.append(ingest_and_freeze_workflow)
        else:
            update_live_database_tasks.append(update_expired_roots_workflow)

    create_frozen_database_tasks = []

    for mat_metadata in mat_info:       
        create_frozen_database_workflow = chain(
            drop_indexes.si(mat_metadata),
            copy_data_from_live_table.si(mat_metadata),
            update_analysis_metadata.si(mat_metadata),
            add_indexes.si(mat_metadata),
            check_tables.si(mat_metadata))
        create_frozen_database_tasks.append(create_frozen_database_workflow)


    setup_versioned_database = chain(create_analysis_database.si(datastack_info, new_version_number),
                                     create_analysis_tables.si(datastack_info, new_version_number))

    final_workflow = chain(
        chord(update_live_database_tasks, fin.si()),
        setup_versioned_database,
        chord(create_frozen_database_tasks, final_task.s()),        
        )
    final_workflow.apply_async()
