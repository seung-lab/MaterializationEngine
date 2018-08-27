import logging
import os


class BaseConfig(object):
    DEBUG = False
    TESTING = False
    HOME = os.path.expanduser("~")
    LOGGING_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOGGING_LOCATION = HOME + '/me_log/bookshelf.log'
    LOGGING_LEVEL = logging.DEBUG
    CG_TABLE_ID = "pinky40_fanout2_v7"
    DATASET_NAME = "pinky40"
    AMDB_INSTANCE_ID = "pychunkedgraph"
    CG_INSTANCE_ID = "pychunkedgraph"
    N_THREADS = 20
    POSTGRES_URI = os.environ("MATERIALIZATION_POSTGRES_URI", None)
    if not os.path.exists(os.path.dirname(LOGGING_LOCATION)):
        os.makedirs(os.path.dirname(LOGGING_LOCATION))
