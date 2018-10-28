import pandas as pd
from emannotationschemas.models import root_model_name, make_cell_segment_model
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
from . import materializationmanager


def _process_all_annotations_thread(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, \
        table_name, schema_name, version, \
        time_stamp, cg_table_id, serialized_amdb_info, \
        serialized_cg_info, serialized_mm_info, \
        serialized_cv_info, pixel_ratios = args

    amdb = AnnotationMetaDB(**serialized_amdb_info)

    cg = chunkedgraph.ChunkedGraph(**serialized_cg_info)

    cv = cloudvolume.CloudVolume(**serialized_cv_info)
    mm = materializationmanager.MaterializationManager(**serialized_mm_info)

    annos_dict = {}

    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data_b, bsps = amdb.get_annotation(
            dataset_name, table_name, annotation_id, time_stamp=time_stamp)

        if annotation_data_b is None:
            continue

        deserialized_annotation = mm.deserialize_single_annotation(annotation_data_b,
                                                                   cg,
                                                                   cv,
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

        if mm.is_sql:
            mm.add_annotation_to_sql_database(deserialized_annotation)
        # else:
        annos_dict[annotation_id] = deserialized_annotation

    if not mm.is_sql:
        return annos_dict
    else:
        try:
            mm.commit_session()
        except Exception as e:
            print("Timestamp:", time_stamp)
            print(annos_dict)
            raise Exception(e)

def _materialize_root_ids_thread(args):
    root_ids, serialized_mm_info = args
    model = make_cell_segment_model(serialized_mm_info["dataset_name"],
                                    serialized_mm_info["version"])
    mm = materializationmanager.MaterializationManager(**serialized_mm_info,
                                                       annotation_model=model)

    annos_dict = {}
    for root_id in root_ids:
        ann = {"id": int(root_id)}
        if mm.is_sql:
            mm.add_annotation_to_sql_database(ann)
        else:
            annos_dict[root_id] = ann

    if not mm.is_sql:
        return annos_dict
    else:
        mm.commit_session()
        

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


def process_all_annotations(cg_table_id, dataset_name, schema_name,
                            table_name, time_stamp=None, version:  int=1,
                            sqlalchemy_database_uri=None,
                            amdb_client=None, amdb_instance_id=None,
                            cg_client=None, cg_instance_id=None,
                            block_size=500, n_threads=1):
    """ Reads data from all annotations and acquires their mapping to root_ids

    :param dataset_name: str
    :param annotation_type: str
    :param cg_table_id: str
        In future, this will be read from some config
    :param n_threads: int
    :return: dict
        annotation_id -> deserialized data
    """
    if amdb_client is None:
        amdb = AnnotationMetaDB()
    elif amdb_instance_id is not None:
        amdb = AnnotationMetaDB(client=amdb_client,
                                instance_id=amdb_instance_id)
    else:
        raise Exception("Missing Instance ID for AnnotationMetaDB")

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
                                                       sqlalchemy_database_uri=sqlalchemy_database_uri)

    # The `max_annotation_id` sets an upper limit and can be used to iterate
    # through all smaller ids. However, there is no guarantee that any smaller
    # id exists, it is only a guarantee that no larger id exists at the time
    # the function is called.
    max_annotation_id = amdb.get_max_annotation_id(dataset_name,
                                                   table_name)

    if max_annotation_id == 0:
        return {}

    cv_info = {"cloudpath": cv_path}
    cg_info = cg.get_serialized_info()
    amdb_info = amdb.get_serialized_info()

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

    if n_threads == 1:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multisubprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads, package_name="materializationengine")

    if not mm.is_sql:
        # Collect the results
        print(results)
        anno_dict = {}
        for result in results:
            anno_dict.update(result)

        return anno_dict


def materialize_root_ids(cg_table_id, dataset_name,
                         version,
                         time_stamp,
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

    root_ids = cg.get_latest_roots(time_stamp=time_stamp, n_threads=n_threads)

    model = make_cell_segment_model(dataset_name, version=version)
    mm = materializationmanager.MaterializationManager(
        dataset_name=dataset_name, schema_name=root_model_name.lower(),
        table_name=root_model_name.lower(),
        version=version,
        annotation_model=model,
        sqlalchemy_database_uri=sqlalchemy_database_uri)

    # if mm.is_sql:
    #     mm._drop_table()
    #     print("Dropped table")

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

    if not mm.is_sql:
        # Collect the results
        anno_dict = {}
        for result in results:
            anno_dict.update(result)

        df = pd.DataFrame.from_dict(anno_dict, orient="index")
        return df


def materialize_all_annotations(cg_table_id,
                                dataset_name,
                                schema_name,
                                table_name,
                                version:  int=1,
                                time_stamp=None,
                                sqlalchemy_database_uri=None,
                                amdb_client=None,
                                amdb_instance_id=None,
                                cg_client=None,
                                cg_instance_id=None,
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
    :param n_threads: int
         number of threads to use to materialize
    """

    anno_dict = process_all_annotations(cg_table_id,
                                        dataset_name=dataset_name,
                                        schema_name=schema_name,
                                        table_name=table_name,
                                        version=version,
                                        time_stamp=time_stamp,
                                        sqlalchemy_database_uri=sqlalchemy_database_uri,
                                        amdb_client=amdb_client,
                                        amdb_instance_id=amdb_instance_id,
                                        cg_client=cg_client,
                                        cg_instance_id=cg_instance_id,
                                        n_threads=n_threads)

    if anno_dict is not None:
        df = pd.DataFrame.from_dict(anno_dict, orient="index")
        return df
