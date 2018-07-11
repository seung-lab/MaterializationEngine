from materializationengine.datacollector import process_all_annotations
import pandas as pd
from emannotationschemas import get_schema
from functools import partial
import json


class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass


def materialize_bsp(root_id_d, item):
    '''function to fill in root_id of BoundSpatialPoint fields
    using the dictionary provided. Will alter item in place.
    Meant to be passed to mm.Schema as a context variable ('bsp_fn')

    :param root_id_d: dict
        dictionary to look up root ids from supervoxel id
    :param item: dict
        deserialized boundspatialpoint to process
    :return: None
        will edit item in place
    '''
    try:
        item['root_id'] = root_id_d[item['supervoxel_id']]
    except KeyError:
        msg = "cannot process {}, root_id not found in {}"
        raise RootIDNotFoundException(msg.format(item, root_id_d))


def get_schema_in_context(annotation_type, root_id_d):
    '''get an instantiation of the schema for this annotation_type
    with the proper context for materialization.

    :param annotation_type: str
        type of annotation
    :param root_id_d:
        dictionary to lookup root_ids from supervoxel_ids
    :return: mm.Schema
        a schema ready to validate/load/dump json
    '''
    Schema = get_schema(annotation_type)
    myp = partial(materialize_bsp, root_id_d)
    schema = Schema(context={'bsp_fn': myp})
    return schema


def flatten_ann(ann, seperator="_"):
    '''flatten an annotation dictionary to a set of key/value pairs

    :param ann: dict
        dictionary to flatten
    :param seperator: str
        seperator to use when unnested dictionary (default="_")
    :return: dict
        a flattened form of the dictionary
    '''
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

    ann = result.data
    ann_flat = flatten_ann(ann)
    ann_flat['oid'] = oid

    return ann_flat


def materialize_all_annotations(dataset_name,
                                annotation_type,
                                cg_table_id,
                                n_threads=1):
    '''create a materialized pandas data frame
    of an annotation type

    :param dataset_name: str
        name of dataset
    :param annotation_type: str
        type of annotation
    :param cg_table_id: str
        chunk graph table
    :param n_threads: int
         number of threads to use to materialize
    '''
    ann_list = process_all_annotations(dataset_name,
                                       annotation_type,
                                       cg_table_id,
                                       n_threads=n_threads)

    ds = []
    for oid, blob, root_id_dict in ann_list:
        d = materialize_annoation_as_dictionary(oid,
                                                blob,
                                                root_id_dict,
                                                annotation_type)
        ds.append(d)
    df = pd.DataFrame(ds, index='oid')

    return df
