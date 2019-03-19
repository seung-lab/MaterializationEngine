import pandas as pd
from emannotationschemas import models as em_models
# from emannotationschemas.base import flatten_dict
# from emannotationschemas import get_schema
# from functools import partial
# import json
import requests
import numpy as np
from pychunkedgraph.backend import chunkedgraph
from multiwrapper import multiprocessing_utils as mu
from dynamicannotationdb.annodb_meta import AnnotationMetaDB
import cloudvolume
from materializationengine import materializationmanager
from pytz import UTC
import datetime
from annotationframeworkclient.annotationengine import AnnotationClient


def _process_all_annotations_thread(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, \
        table_name, schema_name, version, \
        time_stamp, cg_table_id, \
        serialized_cg_info, serialized_mm_info, \
        serialized_cv_info, pixel_ratios = args

    anno_client = AnnotationClient(dataset_name=dataset_name)
    cg = chunkedgraph.ChunkedGraph(**serialized_cg_info)

    cv = cloudvolume.CloudVolume(**serialized_cv_info)
    mm = materializationmanager.MaterializationManager(**serialized_mm_info)

    annos_list = []
    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data = anno_client.get_annotation(table_name,
                                                     annotation_id)

        if annotation_data is None:
            continue

        deserialized_annotation = mm.deserialize_single_annotation(annotation_data,
                                                                   cg, cv,
                                                                   pixel_ratios=pixel_ratios,
                                                                   time_stamp=time_stamp)
        deserialized_annotation['id'] = int(annotation_id)
        # this is now done in deserialization
        # sv_id_to_root_id_dict = {}
        # if sv_ids is not None:
        #     for i_sv_id, sv_id in enumerate(sv_ids):
        #         print("%d / %d" % (i_sv_id + 1, len(sv_ids)), end='\r')

        #         # Read root_id from pychunkedgraph
        #         sv_id_to_root_id_dict[sv_id] = cg.get_root(sv_id)


        annos_list.append(deserialized_annotation)
        # else:

    try:
        mm.bulk_insert_annotations(annos_list)
        mm.commit_session()
    except Exception as e:
        print(e)
        print("Timestamp:", time_stamp)
        print(annos_dict)
        raise Exception(e)


def _materialize_root_ids_thread(args):
    root_ids, serialized_mm_info = args
    model = em_models.make_cell_segment_model(serialized_mm_info["dataset_name"],
                                              serialized_mm_info["version"])
    mm = materializationmanager.MaterializationManager(**serialized_mm_info,
                                                       annotation_model=model)

    annos_dict = {}
    annos_list = []
    for root_id in root_ids:
        ann = {"id": int(root_id)}
        if mm.is_sql:
            # mm.add_annotation_to_sql_database(ann)
            annos_list.append(ann)
        else:
            annos_dict[root_id] = ann

    if not mm.is_sql:
        return annos_dict
    else:
        mm.bulk_insert_annotations(annos_list)
        mm.commit_session()


# def _materialize_root_ids_inc_thread(args):
#     root_ids, serialized_mm_info = args
#     CellSegment = em_models.make_cell_segment_model(serialized_mm_info["dataset_name"],
#                                                     serialized_mm_info["version"])
#     mm = materializationmanager.MaterializationManager(**serialized_mm_info,
#                                                        annotation_model=model)

#     annos_list = []

#     root_ids = np.array(root_ids)
#     old_segments= mm.this_sqlalchemy_session.query(CellSegment).filter(CellSegment.id.in_(root_ids)).all()
#     old_ids = np.array([s.id for s in old_segments])
#     new_ids = root_ids[~np.isin(root_ids, old_ids)]

#     #for root_id in new_ids:
#     #    ann = {"id": int(root_id)}
#     #    annos_list.append(ann)

#     #mm.bulk_insert_annotations(annos_list)
#     #mm.commit_session()
#     return new_ids.tolist()


def _materialize_delta_annotation_thread(args):
    """ Helper for materialize_annotations_delta """
    (block, col, time_stamp,  mm_info, cg_info) = args
    cg = chunkedgraph.ChunkedGraph(**cg_info)
    mm = materializationmanager.MaterializationManager(**mm_info)
    annos_list = []
    for id_, sup_id in block:
        new_root = cg.get_root(sup_id, time_stamp=time_stamp)
        annos_list.append({
            'id': id_,
            col: int(new_root)
        })

    try:
        mm.bulk_update_annotations(annos_list)
        mm.commit_session()
    except Exception as e:
        print(e)
        print("Timestamp:", time_stamp)
        print(annos_list)
        raise Exception(e)


def find_max_root_id_before(cg,
                            time_stamp,
                            time_delta=datetime.timedelta(0),
                            start_id=1,
                            delta_id=100):
    if (start_id <= 1):
        return 1
    rows = cg.read_node_id_row(start_id, columns=[chunkedgraph.column_keys.Hierarchy.Child])
    cells = rows.get(chunkedgraph.column_keys.Hierarchy.Child, [])
    if len(cells) > 0:
        max_time = cells[0].timestamp
        dt = time_stamp.replace(tzinfo=UTC) - max_time - time_delta
        if (dt > datetime.timedelta(0)):
            return start_id
    seg_id = cg.get_segment_id(np.uint64(start_id))
    seg_id -= delta_id
    new_id = cg.get_node_id(seg_id, cg.root_chunk_id)
    return find_max_root_id_before(cg,
                                   time_stamp,
                                   time_delta,
                                   start_id=new_id,
                                   delta_id=delta_id)


def get_segmentation_and_scales_from_infoservice(dataset, endpoint='https://www.dynamicannotationframework.com/info'):
    url = endpoint + '/api/dataset/{}'.format(dataset)
    print(url)
    r = requests.get(url)
    assert (r.status_code == 200)
    info = r.json()

    img_cv = cloudvolume.CloudVolume(info['image_source'], mip=0)
    pcg_seg_cv = cloudvolume.CloudVolume(
        info['pychunkgraph_segmentation_source'], mip=0)
    scale_factor = img_cv.resolution / pcg_seg_cv.resolution
    pixel_ratios = tuple(scale_factor)

    return info['pychunkgraph_segmentation_source'], pixel_ratios

# def update_root_id_all_annotations(cg_table_id, dataset_name, schema_name,
#                             table_name, old_version:int, new_version:int,
#                             time_stamp=None,
#                             sqlalchemy_database_uri=None,
#                             amdb_client=None, amdb_instance_id=None,
#                             cg_client=None, cg_instance_id=None,
#                             block_size=500, n_threads=1):
#
#     OldModel = em_models.make_annotation_model(dataset_name, schema_name,
#                                                table_name, version=old_version)
#
#     NewModel = em_models.make_annotation_model(dataset_name, schema_name,
#                                                table_name, version=new_version)
#
#     max_annotation_id = OldModel.get_max_annotation_id(dataset_name,
#                                                    table_name)

    # if max_annotation_id == 0:
    #     return {}

    # cv_info = {"cloudpath": cv_path}
    # cg_info = cg.get_serialized_info()
    # amdb_info = amdb.get_serialized_info()

    # if n_threads > 1:
    #     del cg_info["credentials"]
    #     del amdb_info["credentials"]

    # n_parts = int(max(1, max_annotation_id / block_size))
    # n_parts += 1
    # # Annotation ids start at 1
    # id_chunks = np.linspace(1, max_annotation_id + 1, n_parts).astype(np.uint64)

    # multi_args = []
    # for i_id_chunk in range(len(id_chunks) - 1):
    #     multi_args.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1],
    #                        dataset_name, table_name, schema_name, version,
    #                        time_stamp,
    #                        cg_table_id, amdb_info, cg_info,
    #                        mm.get_serialized_info(), cv_info, pixel_ratios])

    # if n_threads == 1:
    #     results = mu.multiprocess_func(
    #         _process_all_annotations_thread, multi_args,
    #         n_threads=n_threads,
    #         verbose=True, debug=n_threads == 1)
    # else:
    #     results = mu.multisubprocess_func(
    #         _process_all_annotations_thread, multi_args,
    #         n_threads=n_threads, package_name="materializationengine")

    # if not mm.is_sql:
    #     # Collect the results
    #     print(results)
    #     anno_dict = {}
    #     for result in results:
    #         anno_dict.update(result)

    #     return anno_dict


def process_all_annotations(cg_table_id, dataset_name, schema_name,
                            table_name, time_stamp=None, analysisversion=None,
                            sqlalchemy_database_uri=None,
                            cg_client=None, cg_instance_id=None,
                            block_size=2500, n_threads=1):
    """ Reads data from all annotations and acquires their mapping to root_ids

    :param dataset_name: str
    :param annotation_type: str
    :param cg_table_id: str
        In future, this will be read from some config
    :param n_threads: int
    :return: dict
        annotation_id -> deserialized data
    """
    anno_client = AnnotationClient(dataset_name=dataset_name)

    if analysisversion is None:
        version = 0
        version_id = None
    else:
        version = analysisversion.version
        version_id = analysisversion.id

    if cg_client is None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id)
    elif cg_instance_id is not None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id,
                                       client=cg_client,
                                       instance_id=cg_instance_id)
    else:
        raise Exception("Missing Instance ID")

    cv_path, pixel_ratios = get_segmentation_and_scales_from_infoservice(
        dataset_name)

    mm = materializationmanager.MaterializationManager(dataset_name=dataset_name,
                                                       schema_name=schema_name,
                                                       table_name=table_name,
                                                       version=version,
                                                       version_id=version_id,
                                                       sqlalchemy_database_uri=sqlalchemy_database_uri)

    # The `max_annotation_id` sets an upper limit and can be used to iterate
    # through all smaller ids. However, there is no guarantee that any smaller
    # id exists, it is only a guarantee that no larger id exists at the time
    # the function is called.
    table_info = next(t for t in anno_client.get_tables() if t['table_name']==table_name)

    max_annotation_id = table_info['max_annotation_id']

    print("Max annotation id: %d" % max_annotation_id)

    if max_annotation_id == 0:
        return {}

    cv_mip = 0
    # if 'pni_synapses' in table_name:
    #     cv_mip = 1
    #     pixel_ratios = list(pixel_ratios)
    #     pixel_ratios[0] /= 2
    #     pixel_ratios[1] /= 2
    # else:
    #     cv_mip = 0

    cv_info = {"cloudpath": cv_path, 'mip': cv_mip}
    cg_info = cg.get_serialized_info()

    if n_threads > 1:
        del cg_info["credentials"]

    n_parts = int(max(1, max_annotation_id / block_size))
    n_parts += 1
    # Annotation ids start at 1
    id_chunks = np.linspace(1, max_annotation_id + 1, n_parts).astype(np.uint64)

    multi_args = []
    for i_id_chunk in range(len(id_chunks) - 1):
        multi_args.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1],
                           dataset_name, table_name, schema_name, version,
                           time_stamp,
                           cg_table_id, cg_info,
                           mm.get_serialized_info(), cv_info, pixel_ratios])

    if n_threads == 1:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multisubprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads, package_name="materializationengine")


def process_existing_annotations(cg_table_id,
                                 dataset_name,
                                 schema_name,
                                 table_name,
                                 time_stamp=None,
                                 time_stamp_base=None,
                                 analysisversion=None,
                                 sqlalchemy_database_uri=None,
                                 cg_client=None, cg_instance_id=None,
                                 block_size=2500, n_threads=1):
    """ update existing entries in a materialized database
    filling in empty supervoxelID's and out of date rootIDs
    for root IDs which have expired between a base timestamp
    and another timestamp.

    :param dataset_name: str
        name of dataset
    :param schema_name: str
        type of annotation
    :param cg_table_id: str
        chunk graph table
    :param table_name: str
        name of annotation table
    :param version: str
        version of table (i.e. v1)
    :param time_stamp: time.utctime
        time_stamp to lock databases to
    :param time_stamp_base: time.utctime
        time_stamp of previous materialization   
    :param sqlalchemy_database_uri:
        database connect uri (leave blank for dataframe)
    :param n_threads: int
         number of threads to use to materialize
    """

    if analysisversion is None:
        version = 0
        version_id = None
    else:
        version = analysisversion.version
        version_id = analysisversion.id

    if cg_client is None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id)
    elif cg_instance_id is not None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id,
                                       client=cg_client,
                                       instance_id=cg_instance_id)
    else:
        raise Exception("Missing Instance ID")

    cv_path, pixel_ratios = get_segmentation_and_scales_from_infoservice(
        dataset_name)

    mm = materializationmanager.MaterializationManager(dataset_name=dataset_name,
                                                       schema_name=schema_name,
                                                       table_name=table_name,
                                                       version=version,
                                                       version_id=version_id,
                                                       create_metadata=False,
                                                       sqlalchemy_database_uri=sqlalchemy_database_uri)

    # The `max_annotation_id` sets an upper limit and can be used to iterate
    # through all smaller ids. However, there is no guarantee that any smaller
    # id exists, it is only a guarantee that no larger id exists at the time
    # the function is called.
    max_annotation_id = amdb.get_max_annotation_id(dataset_name,
                                                   table_name)

    print("Max annotation id: %d" % max_annotation_id)

    if max_annotation_id == 0:
        return {}

    cv_mip = 0
    # if 'pni_synapses' in table_name:
    #     cv_mip = 1
    #     pixel_ratios = list(pixel_ratios)
    #     pixel_ratios[0] /= 2
    #     pixel_ratios[1] /= 2
    # else:
    #     cv_mip = 0

    cv_info = {"cloudpath": cv_path, 'mip': cv_mip}
    cg_info = cg.get_serialized_info()

    if n_threads > 1:
        del cg_info["credentials"]
        del amdb_info["credentials"]

    n_parts = int(max(1, max_annotation_id / block_size))
    n_parts += 1
    # Annotation ids start at 1
    id_chunks = np.linspace(1, max_annotation_id + 1, n_parts).astype(np.uint64)

    multi_args = []
    for i_id_chunk in range(len(id_chunks) - 1):
        multi_args.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1],
                           dataset_name, table_name, schema_name, version,
                           time_stamp,
                           cg_table_id, amdb_info, cg_info,
                           mm.get_serialized_info(), cv_info, pixel_ratios])

    print(multi_args)

    if n_threads == 1:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multisubprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads, package_name="materializationengine")

    if mm.is_sql:
        mm.add_to_analysis_table()
    else:
        # Collect the results
        print(results)
        anno_dict = {}
        for result in results:
            anno_dict.update(result)

        return anno_dict


def materialize_root_ids_delta(cg_table_id, 
                               dataset_name,
                               time_stamp,
                               time_stamp_base,
                               min_root_id =1,
                               analysisversion=None,
                               sqlalchemy_database_uri=None,
                               cg_client=None,
                               cg_instance_id=None,
                               n_threads=1):

    if cg_client is None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id)
    elif cg_instance_id is not None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id,
                                       client=cg_client,
                                       instance_id=cg_instance_id)
    else:
        raise Exception("Missing Instance ID")

    if analysisversion is None:
        version = 0
        version_id = None
    else:
        version = analysisversion.version
        version_id = analysisversion.id

    new_root_ids, expired_root_ids = cg.get_delta_roots(time_stamp_base,
                                                    time_stamp,
                                                    min_seg_id=min_root_id,
                                                    n_threads=n_threads)

    model = em_models.make_cell_segment_model(dataset_name, version=version)
    mm = materializationmanager.MaterializationManager(
        dataset_name=dataset_name, schema_name=em_models.root_model_name.lower(),
        table_name=em_models.root_model_name.lower(),
        version=version,
        version_id=version_id,
        annotation_model=model,
        sqlalchemy_database_uri=sqlalchemy_database_uri)

    existing_new_cellsegments = mm.this_sqlalchemy_session.query(model).filter(model.id.in_(new_root_ids.tolist())).all()
    print(f'found {len(new_root_ids)} new root_ids, \
    {len(expired_root_ids)} expired, \
    {len(existing_new_cellsegments)} of new ones already in cellsegments')

    existing_new_root_ids = np.array([cs.id for cs in existing_new_cellsegments], dtype=np.uint64)
    filtered_new_root_ids = new_root_ids[~np.isin(new_root_ids, existing_new_root_ids)]

    # DEPRECATED OLD SLOW WAY OF DETERMINING ROOT IDS
    # root_ids = np.array(cg.get_latest_roots(time_stamp=time_stamp,
    #                                n_threads=n_threads))
    # old_root_ids = np.array(cg.get_latest_roots(time_stamp=time_stamp_base,
    #                                n_threads=n_threads))
    # slow_expired_root_ids = old_root_ids[~np.isin(old_root_ids, root_ids)]
    # slow_new_root_ids = root_ids[~np.isin(root_ids, old_root_ids)]

    print(f'Inserting {len(filtered_new_root_ids)} root_ids into cellsegment table')
    if len(filtered_new_root_ids) > 0:
        root_id_blocks = np.array_split(filtered_new_root_ids, n_threads*3)
        multi_args = []

        for root_id_block in root_id_blocks:
            multi_args.append([root_id_block, mm.get_serialized_info()])

        if n_threads == 1:
            results = mu.multiprocess_func(
                _materialize_root_ids_thread, multi_args,
                n_threads=n_threads,
                verbose=True, debug=n_threads == 1)
        else:
            results = mu.multisubprocess_func(
                _materialize_root_ids_thread, multi_args,
                n_threads=n_threads, package_name="materializationengine")

    return new_root_ids, expired_root_ids


def materialize_annotations_delta(
    cg_table_id, dataset_name,
    table_name, schema_name,
    old_roots,
    analysisversion,
    sqlalchemy_database_uri=None, cg_client=None,
    cg_instance_id=None, n_threads=1):

    if cg_client is None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id)
    elif cg_instance_id is not None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id,
                                       client=cg_client,
                                       instance_id=cg_instance_id)
    cg_info = cg.get_serialized_info()
    if n_threads > 1:
        del cg_info["credentials"]
    model = em_models.make_annotation_model(dataset_name,
                                            schema_name,
                                            table_name,
                                            version=analysisversion.version)

    mm = materializationmanager.MaterializationManager(
        dataset_name=dataset_name, schema_name=schema_name,
        table_name=table_name,
        version=analysisversion.version,
        version_id=analysisversion.id,
        annotation_model=model,
        sqlalchemy_database_uri=sqlalchemy_database_uri)

    root_columns = [ col for col in model.__table__.columns.keys() if col.endswith('root_id')]
    
    base_query = mm.this_sqlalchemy_session.query(model)
    for col in root_columns:
        sup_col = col.replace('root_id', 'supervoxel_id')
        col_query = base_query.filter(model.__dict__[col].in_(old_roots.tolist()))
        col_query_missing = base_query.filter(model.__dict__[col] == None)
        need_update_count = col_query.count()
        print(table_name, col, need_update_count, col_query_missing.count())
        if need_update_count > 0:
            col_query = base_query.filter(model.__dict__[col].in_(old_roots.tolist()))
            col_query_missing = base_query.filter(model.__dict__[col] == None)
            id_sup_ids = []
            for obj in col_query.all():
                sup_id = int(obj.__dict__[sup_col])
                id_sup_ids.append((int(obj.id), int(sup_id)))
                # new_root_id = cg.get_root(sup_id, analysisversion.time_stamp)
                # obj.__dict__[col]=new_root_id
            n_blocks = np.min((len(id_sup_ids), n_threads))
            blocks = np.array_split(np.array(id_sup_ids, dtype=np.uint64), n_blocks)
            multi_args = []
            for block in blocks:
                multi_args.append((block.tolist(),
                                  col,
                                  analysisversion.time_stamp,
                                  mm.get_serialized_info(),
                                  cg_info))

            if n_threads == 1:
                results = mu.multiprocess_func(
                    _materialize_delta_annotation_thread, multi_args,
                    n_threads=n_threads,
                    verbose=True, debug=n_threads == 1)
            else:
                results = mu.multisubprocess_func(
                    _materialize_delta_annotation_thread, multi_args,
                    n_threads=n_threads, package_name="materializationengine")

        # version_session.commit()
    

def materialize_root_ids(cg_table_id, dataset_name,
                         time_stamp,
                         analysisversion=None,
                         sqlalchemy_database_uri=None, cg_client=None,
                         cg_instance_id=None, n_threads=1):

    if cg_client is None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id)
    elif cg_instance_id is not None:
        cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id,
                                       client=cg_client,
                                       instance_id=cg_instance_id)
    else:
        raise Exception("Missing Instance ID")

    if analysisversion is None:
        version = 0
        version_id = None
    else:
        version = analysisversion.version
        version_id = analysisversion.id

    model = em_models.make_cell_segment_model(dataset_name, version=version)
    mm = materializationmanager.MaterializationManager(
        dataset_name=dataset_name, schema_name=em_models.root_model_name.lower(),
        table_name=em_models.root_model_name.lower(),
        version=version,
        version_id=version_id,
        annotation_model=model,
        sqlalchemy_database_uri=sqlalchemy_database_uri)

    mm.bulk_insert_annotations([{"id": 0}])
    mm.commit_session()
    # if mm.is_sql:
    #     mm._drop_table()
    #     print("Dropped table")

    root_ids = cg.get_latest_roots(time_stamp=time_stamp,
                                   n_threads=n_threads)

    print("len(root_ids)", len(root_ids))

    root_id_blocks = np.array_split(root_ids, n_threads*3)
    multi_args = []

    for root_id_block in root_id_blocks:
        multi_args.append([root_id_block, mm.get_serialized_info()])

    if n_threads == 1:
        results = mu.multiprocess_func(
            _materialize_root_ids_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multisubprocess_func(
            _materialize_root_ids_thread, multi_args,
            n_threads=n_threads, package_name="materializationengine")

    # mm.add_to_analysis_table()
   

def materialize_all_annotations(cg_table_id,
                                dataset_name,
                                schema_name,
                                table_name,
                                analysisversion: None,
                                time_stamp=None,
                                sqlalchemy_database_uri=None,
                                amdb_client=None,
                                amdb_instance_id=None,
                                cg_client=None,
                                cg_instance_id=None,
                                block_size=2500,
                                n_threads=1):
    """ Create a materialized pandas data frame
    of an annotation type

    :param dataset_name: str
        name of dataset
    :param schema_name: str
        type of annotation
    :param cg_table_id: str
        chunk graph table
    :param table_name: str
        name of annotation table
    :param version: str
        version of table (i.e. v1)
    :param time_stamp: time.utctime
        time_stamp to lock databases to
    :param sqlalchemy_database_uri:
        database connect uri (leave blank for dataframe)
    :param block_size:
        number of annotations to process in a block
    :param n_threads: int
         number of threads to use to materialize
    """

    process_all_annotations(cg_table_id,
                            dataset_name=dataset_name,
                            schema_name=schema_name,
                            table_name=table_name,
                            analysisversion=analysisversion,
                            time_stamp=time_stamp,
                            sqlalchemy_database_uri=sqlalchemy_database_uri,
                            cg_client=cg_client,
                            cg_instance_id=cg_instance_id,
                            block_size=block_size,
                            n_threads=n_threads)


