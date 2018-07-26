import pandas as pd
import os
import numpy as np

# from annotationengine import annotation, annotationclient

HOME = os.path.expanduser("~")


def load_synapses(path=HOME + "/Downloads/pinky40_run2_remapped.df"):
    """ Cheap test scenario using real synapses """

    df = pd.read_csv(path)

    sv_ids = np.array(df[["presyn_segid", "postsyn_segid"]])
    data = np.array(df)

    mask = ~np.any(np.isnan(sv_ids), axis=1)
    df = df[mask]

    df['pre_pt.position'] = list(np.array(df[['presyn_x', 'presyn_y', 'presyn_z']], dtype=np.int))
    df['ctr_pt.position'] = list(np.array(df[['centroid_x', 'centroid_y', 'centroid_z']], dtype=np.int))
    df['post_pt.position'] = list(np.array(df[['postsyn_x', 'postsyn_y', 'postsyn_z']], dtype=np.int))

    return df


def insert_synapses(syn_df, dataset_name='pinky', annotation_type="synapse"):
    ac = annotationclient.AnnotationClient("http://104.196.202.90:4000")
    ac.bulk_import_df(dataset_name, annotation_type, syn_df)


if __name__ == "__main__":

    print("LOADING synapses")
    syn_df = load_synapses()

    syn_df = syn_df[:10]
