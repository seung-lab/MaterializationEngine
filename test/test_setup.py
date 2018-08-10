import pytest
from dynamicannotationdb.annodb import AnnotationMetaDB
from emannotationschemas import get_types
from pychunkedgraph.backend import chunkedgraph
import subprocess
import os
from google.auth import credentials
from google.cloud import bigtable, exceptions
from time import sleep
from signal import SIGTERM
import grpc


class DoNothingCreds(credentials.Credentials):
    def refresh(self, request):
        pass


@pytest.fixture(scope='session', autouse=True)
def bigtable_client(request):
    # setup Emulator
    bigtables_emulator = subprocess.Popen(["gcloud",
                                           "beta",
                                           "emulators",
                                           "bigtable",
                                           "start"],
                                          preexec_fn=os.setsid,
                                          stdout=subprocess.PIPE)

    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    startup_msg = "Waiting for BigTables Emulator to start up at {}..."
    print(startup_msg.format(os.environ["BIGTABLE_EMULATOR_HOST"]))
    c = bigtable.Client(project='emulated', credentials=DoNothingCreds(),
                        admin=True)
    retries = 5
    while retries > 0:
        try:
            c.list_instances()
        except exceptions._Rendezvous as e:
            # Good error - means emulator is up!
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                print(" Ready!")
                break
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                sleep(1)
            retries -= 1
            print(".")
    if retries == 0:
        print("\nCouldn't start Bigtable Emulator."
              " Make sure it is setup correctly.")
        exit(1)
    yield c
    # setup Emulator-Finalizer

    def fin():
        os.killpg(os.getpgid(bigtables_emulator.pid), SIGTERM)
        bigtables_emulator.wait()

    request.addfinalizer(fin)


@pytest.fixture(scope='session')
def annodb(bigtable_client):
    db = AnnotationMetaDB(client=bigtable_client, instance_id="test_instance")

    yield db


@pytest.fixture(scope='session')
def test_annon_dataset(annodb):
    amdb = annodb

    dataset_name = 'test_dataset'
    types = get_types()
    for type_ in types:
        amdb.create_table(dataset_name, type_)
    yield amdb, dataset_name


@pytest.fixture(scope='session')
def chunkgraph_tuple(bigtable_client, fan_out=2, n_layers=4):
    cg_table_id = "test_cg"
    graph = chunkedgraph.ChunkedGraph(cg_table_id,
                                      client=bigtable_client,
                                      instance_id="test_instance",
                                      is_new=True, fan_out=fan_out,
                                      n_layers=n_layers, cv_path="", chunk_size=(64, 64, 64))

    yield graph, cg_table_id
    graph.table.delete()

    print("\n\nTABLE DELETED")
