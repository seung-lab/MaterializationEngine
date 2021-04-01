from materializationengine.workflows.bulk_upload import get_gcs_file_info
from unittest import mock
import logging
import datetime


@mock.patch("materializationengine.workflows.bulk_upload.gcsfs", autospec=True)
def test_get_gcs_file_info(mock_gcsfs, bulk_upload_metadata):
    mock_gcsfs.return_value.GCSFileSystem.return_value = "/test_data"
    upload_timestamp = datetime.datetime.utcnow()
    gcs_info = get_gcs_file_info(upload_timestamp, bulk_upload_metadata)
    logging.info(gcs_info)
    assert False


