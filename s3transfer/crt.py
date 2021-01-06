import logging
import os
from io import BytesIO

import botocore.awsrequest
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit
import awscrt.http
from awscrt.s3 import S3Client, S3RequestType
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
from awscrt.auth import AwsCredentialsProvider

from s3transfer.futures import BaseTransferFuture, BaseTransferMeta
from s3transfer.utils import CallArgs
from s3transfer.utils import get_callbacks

logger = logging.getLogger(__name__)


class CRTUtil(object):
    '''
    Utilities related to CRT.
    '''
    def crt_request_from_aws_request(aws_request):
        url_parts = urlsplit(aws_request.url)
        crt_path = url_parts.path
        if url_parts.query:
            crt_path = '%s?%s' % (crt_path, url_parts.query)
        headers_list = []
        for name, value in aws_request.headers.items():
            if isinstance(value, str):
                headers_list.append((name, value))
            else:
                headers_list.append((name, str(value, 'utf-8')))

        crt_headers = awscrt.http.HttpHeaders(headers_list)
        # CRT requires body (if it exists) to be an I/O stream.
        crt_body_stream = None
        if aws_request.body:
            if hasattr(aws_request.body, 'seek'):
                crt_body_stream = aws_request.body
            else:
                crt_body_stream = BytesIO(aws_request.body)

        crt_request = awscrt.http.HttpRequest(
            method=aws_request.method,
            path=crt_path,
            headers=crt_headers,
            body_stream=crt_body_stream)
        return crt_request


class CRTTransferMeta(BaseTransferMeta):
    """Holds metadata about the CRTTransferFuture"""

    def __init__(self, transfer_id=None, call_args=None):
        self._transfer_id = transfer_id
        self._call_args = call_args
        self._user_context = {}
        self._size = 0

    @property
    def call_args(self):
        return self._call_args

    @property
    def transfer_id(self):
        return self._transfer_id

    @property
    def user_context(self):
        return self._user_context

    @property
    def size(self):
        return self._size

    def provide_transfer_size(self, size):
        """A method to provide the size of a transfer request

        By providing this value, the TransferManager will not try to
        call HeadObject or use the use OS to determine the size of the
        transfer.
        """
        self._size = size


class CRTTransferFuture(BaseTransferFuture):
    def __init__(self, s3_request=None, meta=None):
        """The future associated to a submitted transfer request via CRT S3 client

        :type s3_request: S3Request
        :param s3_request: The s3_request, the CRT s3 request handles cancel
            and the finish future.

        :type meta: CRTTransferMeta
        :param meta: The metadata associated to the request. This object
            is visible to the requester.
        """
        self._s3_request = s3_request
        self._crt_future = None
        if s3_request:
            self._crt_future = self._s3_request.finished_future
        self._meta = meta
        if meta is None:
            self._meta = CRTTransferMeta()

    @property
    def meta(self):
        return self._meta

    def set_s3_request(self, s3_request):
        self._s3_request = s3_request
        self._crt_future = self._s3_request.finished_future

    def done(self):
        return self._crt_future.done()

    def result(self):
        try:
            if self._s3_request:
                result = self._crt_future.result()
                self._s3_request = None
                return result
            return
        except KeyboardInterrupt as e:
            self.cancel()
            raise e

    def cancel(self):
        # TODO support cancel correctly for error handling
        raise NotImplementedError('cancel')


class CRTTransferConfig(object):
    def __init__(self,
                 max_bandwidth=None,
                 multipart_chunksize=0,
                 max_request_processes=0):
        """Configuration for the ProcessPoolDownloader
        :param max_bandwidth: The maximum bandwidth of the transfer manager
            will take.
        :param multipart_chunksize: The chunk size of each ranged download.
        :param max_request_processes: The maximum number of processes that
            will be making S3 API transfer-related requests at a time.
        """
        self.max_bandwidth = max_bandwidth
        self.multipart_chunksize = multipart_chunksize
        self.max_request_processes = max_request_processes


class CRTS3ClientFactory:
    def create_client(self, region, transfer_config=None,
                      botocore_credential_provider=None):
        event_loop_group = EventLoopGroup(
            transfer_config.max_request_processes)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(
            event_loop_group, host_resolver)
        provider = AwsCredentialsProvider.new_python(
            botocore_credential_provider)
        target_gbps = 0
        if transfer_config.max_bandwidth:
            # Translate bytes to gigabits
            target_gbps = transfer_config.max_bandwidth * \
                8 / (1000 * 1000 * 1000)

        return S3Client(
            bootstrap=bootstrap,
            region=region,
            credential_provider=provider,
            part_size=transfer_config.multipart_chunksize,
            throughput_target_gbps=target_gbps)


class FakeRawResponse(BytesIO):
    def stream(self, amt=1024, decode_content=None):
        while True:
            chunk = self.read(amt)
            if not chunk:
                break
            yield chunk


class CrtCredentialProviderWrapper():
    """
    Provides the credential for CRT.
    CRT will invoke get_credential method and
    expected a dictionary return value back.
    """

    def __init__(self, session=None):
        self._session = session

    def get_credential(self):
        credentials = self._session.get_credentials().get_frozen_credentials()

        return {
            "AccessKeyId": credentials.access_key,
            "SecretAccessKey": credentials.secret_key,
            "SessionToken": credentials.token
        }


class S3ClientArgsCreator:
    def __init__(self, session):
        # Turn off the signing process and depends on the crt client to sign
        # the request
        client_config = Config(signature_version=UNSIGNED)
        self._client = session.create_client(
            's3', config=client_config)  # Initialize client

        self._client.meta.events.register(
            'request-created.s3.*', self._capture_http_request)
        self._client.meta.events.register(
            'after-call.s3.*',
            self._change_response_to_serialized_http_request)
        self._client.meta.events.register(
            'before-send.s3.*', self._make_fake_http_response)
        # initialize client with removed HTTP layer here

    def _capture_http_request(self, request, **kwargs):
        request.context['http_request'] = request

    def _change_response_to_serialized_http_request(
            self, context, parsed, **kwargs):
        request = context['http_request']
        parsed['HTTPRequest'] = request.prepare()

    def _make_fake_http_response(self, request, **kwargs):
        return botocore.awsrequest.AWSResponse(
            None,
            200,
            {},
            FakeRawResponse(b""),
        )

    def _get_crt_callback(self, future, callback_type):

        def invoke_subscriber_callbacks(*args, **kwargs):
            for callback in get_callbacks(future, callback_type):
                if callback_type == "progress":
                    callback(bytes_transferred=args[0])
                else:
                    callback(*args, **kwargs)

        return invoke_subscriber_callbacks

    def _get_botocore_request(self, request_type, call_args):
        if not call_args.extra_args:
            call_args.extra_args = {}
        if request_type == 'get_object':
            serialized_request = self._client.get_object(
                Bucket=call_args.bucket, Key=call_args.key,
                **call_args.extra_args)["HTTPRequest"]
        elif request_type == 'put_object':
            # Set the body stream later
            serialized_request = self._client.put_object(
                Bucket=call_args.bucket, Key=call_args.key,
                **call_args.extra_args)["HTTPRequest"]
        elif request_type == 'delete_object':
            serialized_request = self._client.delete_object(
                Bucket=call_args.bucket, Key=call_args.key,
                **call_args.extra_args)["HTTPRequest"]
        else:
            serialized_request = self._client.copy_object(
                CopySource=call_args.copy_source,
                Bucket=call_args.bucket, Key=call_args.key,
                **call_args.extra_args)["HTTPRequest"]
        return serialized_request

    def get_make_request_args(self, request_type, call_args, future):
        serialized_request = self._get_botocore_request(
            request_type, call_args)
        crt_request = CRTUtil.crt_request_from_aws_request(
            serialized_request)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(serialized_request.url)
            crt_request.headers.set("host", url_parts.netloc)
        on_queued = self._get_crt_callback(future, 'queued')
        on_queued()

        file_path = None
        if request_type == 'get_object':
            s3_meta_request_type = S3RequestType.GET_OBJECT
            file_path = call_args.fileobj
        elif request_type == 'put_object':
            s3_meta_request_type = S3RequestType.PUT_OBJECT
            file_path = call_args.fileobj
        else:
            s3_meta_request_type = S3RequestType.DEFAULT

        if s3_meta_request_type == S3RequestType.PUT_OBJECT:
            file_stats = os.stat(file_path)
            data_len = file_stats.st_size
            crt_request.headers.set("Content-Length", str(data_len))
            content_type = "application/octet-stream"
            if 'ContentType' in call_args.extra_args:
                content_type = call_args.extra_args['ContentType']
            crt_request.headers.set(
                "Content-Type", content_type)
        return {
            'request': crt_request,
            'type': s3_meta_request_type,
            'file': file_path,
            'on_done': self._get_crt_callback(future, 'done'),
            'on_progress': self._get_crt_callback(future, 'progress')
        }


class CRTTransferManager(object):
    """
    Transfer manager based on CRT s3 client.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, *args):
        # Wait until all the transfer done, even if some fails and clean up all
        # the underlying resource
        try:
            for future in self._futures:
                future.result()
        except Exception:
            pass
        shutdown_event = self._crt_s3_client.shutdown_event
        del self._crt_s3_client
        if not shutdown_event.wait(1):
            # TODO throw an exception instead
            print("something fucked up")

    def __init__(self, session=None, config=None, crt_s3_client=None):
        """
        session: botocore.session
        config: CRTTransferConfig
        """
        self._crt_credential_provider = CrtCredentialProviderWrapper(session)
        if crt_s3_client is None:
            crt_s3_client = self._create_crt_s3_client(session, config)
        self._crt_s3_client = crt_s3_client
        self._submitter = CRTSubmitter()

        self._s3_args_creator = S3ClientArgsCreator(session)
        self._futures = []

    def _create_crt_s3_client(self, session, transfer_config):
        client_factory = CRTS3ClientFactory()
        return client_factory.create_client(
            region=session.get_config_variable("region"),
            transfer_config=transfer_config,
            botocore_credential_provider=self._crt_credential_provider
        )

    def download(self, bucket, key, fileobj, extra_args=None,
                 subscribers=None):
        callargs = CallArgs(
            bucket=bucket, key=key, fileobj=fileobj,
            extra_args=extra_args, subscribers=subscribers)
        return self._submit_transfer("get_object", callargs)

    def upload(self, bucket, key, fileobj, extra_args=None,
               subscribers=None):
        callargs = CallArgs(
            bucket=bucket, key=key, fileobj=fileobj,
            extra_args=extra_args, subscribers=subscribers)
        return self._submit_transfer("put_object", callargs)

    def delete(self, bucket, key, extra_args=None,
               subscribers=None):
        callargs = CallArgs(
            bucket=bucket, key=key, extra_args=extra_args,
            subscribers=subscribers)
        return self._submit_transfer("delete_object", callargs)

    def _submit_transfer(self, request_type, call_args):
        future = CRTTransferFuture(None, CRTTransferMeta(call_args=call_args))
        crt_s3_request = self._submitter.submit(
            request_type, call_args, future, self._crt_s3_client, self._s3_args_creator)
        future.set_s3_request(crt_s3_request)
        self._futures.append(future)
        return future


class CRTSubmitter(object):
    # using botocore client
    def __init__(self):
        self._executor = CRTExecutor()

    def submit(self, request_type, call_args, future,
               crt_s3_client, s3_args_creator):
        crt_callargs = s3_args_creator.get_make_request_args(
            request_type, call_args, future)
        return self._executor.submit(crt_s3_client, crt_callargs)


class CRTExecutor(object):
    def __init__(self):
        pass

    def submit(self, crt_s3_client, crt_callargs):
        return crt_s3_client.make_request(**crt_callargs)
