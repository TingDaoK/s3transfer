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
        self.region = self.session.get_config_variable("region")
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'
        self.expected_path = "/" + self.bucket + "/" + self.key
        self.expected_host = "s3.%s.amazonaws.com" % (self.region)

        with open(self.filename, 'wb') as f:
            f.write(self.content)

    def test_upload(self):
        future = self.transfer_manager.upload(
            self.bucket, self.key, self.filename, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertEqual(callargs_kwargs["file"], self.filename)
        self.assertEqual(callargs_kwargs["type"], S3RequestType.PUT_OBJECT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_download(self):
        future = self.transfer_manager.download(
            self.bucket, self.key, self.filename, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertEqual(callargs_kwargs["file"], self.filename)
        self.assertEqual(callargs_kwargs["type"], S3RequestType.GET_OBJECT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_delete(self):
        future = self.transfer_manager.delete(
            self.bucket, self.key, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertIsNone(callargs_kwargs["file"])
        self.assertEqual(callargs_kwargs["type"], S3RequestType.DEFAULT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
