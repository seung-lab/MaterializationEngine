import pandas as pd
import os
import numpy as np
import time

from annotationengine import annotation, annotationclient

HOME = os.path.expanduser("~")


def load_synapses(path=HOME + "/Downloads/pinky40_run2_remapped.df"):
    """ Cheap test scenario using real synapses """

    df = pd.read_csv(path)

    locs = np.array(df[["presyn_x", "centroid_x", "postsyn_x"]])
    data = np.array(df)

    mask = ~np.any(np.isnan(locs), axis=1)

    df = df[mask]

    df['pre_pt.position'] = list(np.array(df[['presyn_x', 'presyn_y', 'presyn_z']], dtype=np.int))
    df['ctr_pt.position'] = list(np.array(df[['centroid_x', 'centroid_y', 'centroid_z']], dtype=np.int))
    df['post_pt.position'] = list(np.array(df[['postsyn_x', 'postsyn_y', 'postsyn_z']], dtype=np.int))

    return df


def insert_synapses(syn_df, dataset_name='pinky40', annotation_type="synapse"):
    ac = annotationclient.AnnotationClient("https://35.196.20.36:4001")
    ac.bulk_import_df(dataset_name, annotation_type, syn_df)


if __name__ == "__main__":

    print("LOADING synapses")

    time_start = time.time()
    syn_df = load_synapses()
    print("Time for loading: %.2fmin" % ((time.time() - time_start) / 60))

    time_start = time.time()
    insert_synapses(syn_df)
    print("Time for inserting: %.2fmin" % ((time.time() - time_start) / 60))

