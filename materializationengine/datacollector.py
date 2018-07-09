import numpy as np

from pychunkedgraph.backend import chunkedgraph, multiprocessing_utils as mu
from dynamicannotationdb import annodb


def _process_all_annotations_thread(args):
    """ Helper for process_all_annotations """
    anno_id_start, anno_id_end, dataset_name, annotation_type, cg_table_id = args

    amdb = annodb.AnnotationMetaDB()
    cg = chunkedgraph.ChunkedGraph(cg_table_id)

    combined_annotations = []

    for annotation_id in range(anno_id_start, anno_id_end):
        # Read annoation data from dynamicannotationdb
        annotation_data, sv_ids = amdb.get_annotation(annotation_id)

        if annotation_data is None:
            continue

        sv_id_to_root_id_dict = {}
        for sv_id in sv_ids:

            # Read root_id from pychunkedgraph
            sv_id_to_root_id_dict[sv_id] = cg.get_root(sv_id)

        combined_annotations.append([annotation_id, annotation_data,
                                     sv_id_to_root_id_dict])


def process_all_annotations(dataset_name, annotation_type, cg_table_id,
                            n_threads=1):
    """ Reads data from all annotations and acquires their mapping to root_ids

    :param dataset_name: str
    :param annotation_type: str
    :param cg_table_id: str
        In future, this will be read from some config
    :param n_threads: int
    :return: list
        [[annotation_id, data blob, dict(svid->root_id)], ...]
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
        results = mu.multisubprocess_func(
            _process_all_annotations_thread, multi_args,
            n_threads=n_threads)

    # Collect the results
    combined_annotations = []
    for result in results:
        combined_annotations.extend(result)

    return combined_annotations