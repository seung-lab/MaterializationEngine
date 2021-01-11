from re import S
from materializationengine.models import AnalysisVersion, AnalysisTable, AnalysisMetadata
from .conftest import DATASTACK, TEST_TIMESTAMP, MAT_VERSION, ALIGNED_VOLUME, SCHEMA, ANNOTATION_TABLE_NAME
import datetime

EXPIRES_TIMESTAMP = TEST_TIMESTAMP + datetime.timedelta(days=5)


def test_analysis_version():
    analysisversion = AnalysisVersion(datastack=DATASTACK,
                                      time_stamp=TEST_TIMESTAMP,
                                      version=MAT_VERSION,
                                      valid=False,
                                      expires_on=EXPIRES_TIMESTAMP)
    assert analysisversion.datastack == DATASTACK
    assert analysisversion.time_stamp == TEST_TIMESTAMP
    assert analysisversion.version == MAT_VERSION
    assert analysisversion.valid == False
    assert analysisversion.expires_on == EXPIRES_TIMESTAMP


def test_analysis_table():
    analysis_table = AnalysisTable(aligned_volume=ALIGNED_VOLUME,
                                   schema=SCHEMA,
                                   table_name=ANNOTATION_TABLE_NAME,
                                   valid=True,
                                   created=TEST_TIMESTAMP,
                                   analysisversion_id=1)
    assert analysis_table.aligned_volume == ALIGNED_VOLUME



def test_analysis_metadata():
    analysis_metadata = AnalysisMetadata(schema=SCHEMA,
                                        table_name=ANNOTATION_TABLE_NAME,
                                        valid=True,
                                        created=TEST_TIMESTAMP,
                                        last_updated=None)
    assert analysis_metadata.schema == SCHEMA
    assert analysis_metadata.table_name == ANNOTATION_TABLE_NAME
    assert analysis_metadata.valid == True
    assert analysis_metadata.created == TEST_TIMESTAMP
    assert analysis_metadata.last_updated == None
