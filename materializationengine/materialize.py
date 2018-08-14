import pandas as pd
from emannotationschemas.models import make_all_models, Base
from emannotationschemas.base import flatten_dict
from emannotationschemas import get_schema
from functools import partial
import json
import numpy as np

from pandas.io.json import json_normalize


from pychunkedgraph.backend import chunkedgraph, multiprocessing_utils as mu
from dynamicannotationdb import annodb

from . import materializationmanager


def _process_all_annotations_thread(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, annotation_type, cg_table_id, \
        serialized_amdb_info, serialized_cg_info, serialized_mm_info = args

    amdb = annodb.AnnotationMetaDB(**serialized_amdb_info)

    cg = chunkedgraph.ChunkedGraph(table_id=cg_table_id, **serialized_amdb_info)

    mm = materializationmanager.MaterializationManager(**serialized_mm_info)

    annos_dict = {}

    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data_b, sv_ids = amdb.get_annotation(dataset_name, annotation_type, annotation_id)

        if annotation_data_b is None:
            continue

        sv_id_to_root_id_dict = {}
        if sv_ids is not None:
            for i_sv_id, sv_id in enumerate(sv_ids):
                print("%d / %d" % (i_sv_id + 1, len(sv_ids)), end='\r')

                # Read root_id from pychunkedgraph
                sv_id_to_root_id_dict[sv_id] = cg.get_root(sv_id)

        deserialized_annotation = mm.deserialize_single_annotation(
            annotation_data_b, sv_id_to_root_id_dict)

        if mm.is_sql:
            mm.add_annotation_to_sql_database(deserialized_annotation)
        else:
            annos_dict[annotation_id] = deserialized_annotation
    if not mm.is_sql:
        return annos_dict


def process_all_annotations(cg_table_id, dataset_name, annotation_type,
                            sqlalchemy_database_uri=None,
                            amdb_client=None, amdb_instance_id=None,
                            cg_client=None, cg_instance_id=None, n_threads=1):
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
        amdb = annodb.AnnotationMetaDB()
    elif amdb_instance_id is not None:
        amdb = annodb.AnnotationMetaDB(client=amdb_client,
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

    mm = materializationmanager.MaterializationManager(dataset_name=dataset_name,
                                                       annotation_type=annotation_type,
                                                       sqlalchemy_database_uri=sqlalchemy_database_uri)

    # The `max_annotation_id` sets an upper limit and can be used to iterate
    # through all smaller ids. However, there is no guarantee that any smaller
    # id exists, it is only a guarantee that no larger id exists at the time
    # the function is called.
    max_annotation_id = amdb.get_max_annotation_id(dataset_name,
                                                   annotation_type)

    if max_annotation_id == 0:
        return {}

    cg_info = cg.get_serialized_info()
    amdb_info = amdb.get_serialized_info()

    if n_threads > 1:
        del cg_info["credentials"]
        del amdb_info["credentials"]

    # Annotation ids start at 1
    id_chunks = np.linspace(1, max_annotation_id + 1,
                            min([n_threads * 3, max_annotation_id]) + 1).astype(np.uint64)
    multi_args = []
    for i_id_chunk in range(len(id_chunks) - 1):
        multi_args.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1],
                           dataset_name, annotation_type, cg_table_id,
                           amdb_info,
                           cg_info,
                           mm.get_serialized_info()])

    if n_threads == 1:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads)

    if not mm.is_sql:
        # Collect the results
        anno_dict = {}
        for result in results:
            anno_dict.update(result)

        return anno_dict


def materialize_all_annotations(cg_table_id,
                                dataset_name,
                                annotation_type,
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
    :param annotation_type: str
        type of annotation
    :param cg_table_id: str
        chunk graph table
    :param n_threads: int
         number of threads to use to materialize
    """

    anno_dict = process_all_annotations(cg_table_id,
                                        dataset_name=dataset_name,
                                        annotation_type=annotation_type,
                                        sqlalchemy_database_uri=sqlalchemy_database_uri,
                                        amdb_client=amdb_client,
                                        amdb_instance_id=amdb_instance_id,
                                        cg_client=cg_client,
                                        cg_instance_id=cg_instance_id,
                                        n_threads=n_threads)

    if anno_dict is not None:
        df = pd.DataFrame.from_dict(anno_dict, orient="index")
        return df
