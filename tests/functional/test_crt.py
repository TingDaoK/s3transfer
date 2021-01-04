import mock
import unittest
import tempfile
import os

from awscrt.s3 import S3Client, S3RequestType
from botocore.session import Session

from s3transfer.crt import CRTTransferManager


class TestCRTTransferManager(unittest.TestCase):
    def setUp(self):
        self.s3_crt_client = mock.Mock(S3Client)
        self.session = Session()
        self.transfer_manager = CRTTransferManager(
            crt_s3_client=self.s3_crt_client, session=self.session)
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'

        with open(self.filename, 'wb') as f:
            f.write(self.content)

    def test_upload(self):
        future = self.transfer_manager.upload(
            self.bucket, self.key, self.filename, {}, [])
        future.result()
        self.s3_crt_client.make_request.assert_called_with(
            file=self.filename, type=S3RequestType.PUT_OBJECT)

    def test_download(self):
        future = self.transfer_manager.download(
            self.bucket, self.key, self.filename, {}, [])
        future.result()
        self.s3_crt_client.make_request.assert_called_with(
            file=self.filename, type=S3RequestType.GET_OBJECT)

    def test_delete(self):
        future = self.transfer_manager.delete(
            self.bucket, self.key, {}, [])
        future.result()
        self.s3_crt_client.make_request.assert_called_with(
            type=S3RequestType.DEFAULT)
