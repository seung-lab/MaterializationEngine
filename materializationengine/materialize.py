import pandas as pd
from emannotationschemas import get_schema
from functools import partial
import json
import numpy as np

from pandas.io.json import json_normalize


from pychunkedgraph.backend import chunkedgraph, multiprocessing_utils as mu
from dynamicannotationdb import annodb


class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass


def materialize_bsp(root_id_d, item):
    """ Function to fill in root_id of BoundSpatialPoint fields
    using the dictionary provided. Will alter item in place.
    Meant to be passed to mm.Schema as a context variable ('bsp_fn')

    :param root_id_d: dict
        dictionary to look up root ids from supervoxel id
    :param item: dict
        deserialized boundspatialpoint to process
    :return: None
        will edit item in place
    """

    try:
        item['root_id'] = root_id_d[item['supervoxel_id']]
    except KeyError:
        msg = "cannot process {}, root_id not found in {}"
        raise RootIDNotFoundException(msg.format(item, root_id_d))


def get_schema_in_context(annotation_type, root_id_d):
    """Get an instantiation of the schema for this annotation_type
    with the proper context for materialization.

    :param annotation_type: str
        type of annotation
    :param root_id_d:
        dictionary to lookup root_ids from supervoxel_ids
    :return: mm.Schema
        a schema ready to validate/load/dump json
    """

    Schema = get_schema(annotation_type)
    myp = partial(materialize_bsp, root_id_d)
    schema = Schema(context={'bsp_fn': myp})
    return schema


def flatten_ann(ann, seperator="_"):
    """Flatten an annotation dictionary to a set of key/value pairs

    :param ann: dict
        dictionary to flatten
    :param seperator: str
        seperator to use when unnested dictionary (default="_")
    :return: dict
        a flattened form of the dictionary
    """
    # TODO implement this flattening
    return ann


def materialize_annoation_as_dictionary(oid,
                                        blob,
                                        root_id_dict,
                                        annotation_type):
    schema = get_schema_in_context(annotation_type, root_id_dict)
    result = schema.load(json.loads(blob))

    if len(result.errors) > 0:
        msg = "ann {} does not meet schema of {}. Errors ({})"
        raise AnnotationParseFailure(msg.format(blob,
                                                annotation_type,
                                                result.errors))
    anno = json_normalize(result.data)
    anno_dict = dict(zip(anno.keys(), anno.values[0]))

    return anno_dict


def _process_all_annotations_thread(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, annotation_type, cg_table_id = args

    amdb = annodb.AnnotationMetaDB()
    cg = chunkedgraph.ChunkedGraph(cg_table_id)

    anno_dict = {}

    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data_b, sv_ids = amdb.get_annotation(annotation_id)

        if annotation_data_b is None:
            continue

        sv_id_to_root_id_dict = {}
        for sv_id in sv_ids:

            # Read root_id from pychunkedgraph
            sv_id_to_root_id_dict[sv_id] = cg.get_root(sv_id)

        anno_dict[annotation_id] = materialize_annoation_as_dictionary(
            annotation_id, annotation_data_b, sv_id_to_root_id_dict,
            annotation_type)

    return anno_dict


def process_all_annotations(dataset_name, annotation_type, cg_table_id,
                            n_threads=1):
    """ Reads data from all annotations and acquires their mapping to root_ids

    :param dataset_name: str
    :param annotation_type: str
    :param cg_table_id: str
        In future, this will be read from some config
    :param n_threads: int
    :return: dict
        annotation_id -> deserialized data
    """
    amdb = annodb.AnnotationMetaDB()

    # The `max_annotation_id` sets an upper limit and can be used to iterate
    # through all smaller ids. However, there is no guarantee that any smaller
    # id exists, it is only a guarantee that no larger id exists at the time
    # the function is called.
    max_annotation_id = amdb.get_max_annotation_id(dataset_name,
                                                   annotation_type)

    # Annotation ids start at 1
    id_chunks = np.linspace(1, max_annotation_id + 1, n_threads * 3 + 1)

    multi_args = []
    for i_id_chunk in range(len(id_chunks) - 1):
        multi_args.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1],
                           dataset_name, annotation_type, cg_table_id])

    if n_threads == 1:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads,
            verbose=True, debug=n_threads == 1)
    else:
        results = mu.multiprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads)

    # Collect the results
    anno_dict = {}
    for result in results:
        anno_dict.update(result)

    return anno_dict


def materialize_all_annotations(dataset_name,
                                annotation_type,
                                cg_table_id,
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

    anno_dict = process_all_annotations(dataset_name,
                                        annotation_type,
                                        cg_table_id,
                                        n_threads=n_threads)

    df = pd.DataFrame.from_dict(anno_dict, orient="index")

    return df
