from emannotationschemas import mesh_models, models
from sqlalchemy import create_engine

from materializationengine import materializationmanager


def materialize_mesh_labels(dataset_name, table_name, columns,
                            sqlalchemy_database_uri, analysisversion):
    #TODO: Analysis version?!
    if analysisversion is None:
        version = 0
        version_id = None
    else:
        version = analysisversion.version
        version_id = analysisversion.id

    mm = materializationmanager.MaterializationManager(
        dataset_name=dataset_name,
        table_name=table_name,
        version=version,
        version_id=version_id,
        columns=columns,
        sqlalchemy_database_uri=sqlalchemy_database_uri)
