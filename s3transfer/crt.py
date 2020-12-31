import logging
import os
from io import BytesIO

import botocore.awsrequest
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit, six
import awscrt.http
from awscrt.s3 import S3Client, S3RequestType
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup,
from awscrt.auth import AwsCredentialsProvider

from s3transfer.futures import CRTTransferFuture, TransferMeta
from s3transfer.utils import CallArgs

logger = logging.getLogger(__name__)


class CrtUtil(object):
    '''
    Utilities related to CRT.
    '''
    def crt_request_from_aws_request(aws_request):
        url_parts = urlsplit(aws_request.url)
        if isinstance(aws_request, botocore.awsrequest.AWSPreparedRequest):
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
        else:
            crt_path = url_parts.path if url_parts.path else '/'
            if aws_request.params:
                array = []
                for (param, value) in aws_request.params.items():
                    value = str(value)
                    array.append('%s=%s' % (param, value))
                crt_path = crt_path + '?' + '&'.join(array)
            elif url_parts.query:
                crt_path = '%s?%s' % (crt_path, url_parts.query)
            crt_headers = awscrt.http.HttpHeaders(aws_request.headers.items())
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


class CrtSubscribersManager(object):
    """
    A simple wrapper to handle the subscriber for CRT
    """

    def __init__(self, subscribers=None, future=None):
        self._subscribers = subscribers
        self._future = future
        self._on_queued_callbacks = self._get_callbacks("queued")
        self._on_progress_callbacks = self._get_callbacks("progress")
        self._on_done_callbacks = self._get_callbacks("done")

    def _get_callbacks(self, callback_type):
        callbacks = []
        for subscriber in self._subscribers:
            callback_name = 'on_' + callback_type
            if hasattr(subscriber, callback_name):
                callbacks.append(getattr(subscriber, callback_name))
        return callbacks

    def on_queued(self):
        # On_queued seems not being useful for CRT.
        for callback in self._on_queued_callbacks:
            callback(self._future)

    def on_progress(self, bytes_transferred):
        for callback in self._on_progress_callbacks:
            callback(self._future, bytes_transferred)

    def on_done(self):
        for callback in self._on_done_callbacks:
            callback(self._future)


class CRTTransferManager(object):
    """
    Transfer manager based on CRT s3 client.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, *args):
        for i in self.futures:
            i.result()

    def __init__(self, session, config):
        self.config = config
        self._submitter = CRTSubmitter(
            session, config, session.get_config_variable("region"))

        self.futures = []

    @property
    def client(self):
        return self._submitter.client

    def download(self, bucket, key, fileobj, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=fileobj,
                            extra_args=extra_args, subscribers=subscribers, request_type="get_object")
        return self._submit_transfer(callargs)

    def upload(self, bucket, key, fileobj, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=fileobj,
                            extra_args=extra_args, subscribers=subscribers, request_type="put_object")
        return self._submit_transfer(callargs)

    def delete(self, bucket, key, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key,
                            extra_args=extra_args, subscribers=subscribers, request_type="delete_object")
        return self._submit_transfer(callargs)

    def _submit_transfer(self, call_args):
        future = self._submitter.submit(call_args)
        self.futures.append(future)
        return future


class FakeRawResponse(six.BytesIO):
    def stream(self, amt=1024, decode_content=None):
        while True:
            chunk = self.read(amt)
            if not chunk:
                break
            yield chunk


class CRTSubmitter(object):
    # using botocore client
    def __init__(self, session, config, region):
        # Turn off the signing process and depends on the crt client to sign the request
        client_config = Config(signature_version=UNSIGNED)
        self._client = session.create_client(
            's3', config=client_config)  # Initialize client

        self._client.meta.events.register(
            'request-created.s3.*', self._capture_http_request)
        self._client.meta.events.register(
            'after-call.s3.*', self._change_response_to_serialized_http_request)
        self._client.meta.events.register(
            'before-send.s3.*', self._make_fake_http_response)
        self._executor = CRTExecutor(config, session, region)

    def _capture_http_request(self, request, **kwargs):
        request.context['http_request'] = request

    def _change_response_to_serialized_http_request(self, context, parsed, **kwargs):
        request = context['http_request']
        parsed['HTTPRequest'] = request.prepare()

    def _make_fake_http_response(self, request, **kwargs):
        return botocore.awsrequest.AWSResponse(
            None,
            200,
            {},
            FakeRawResponse(b""),
        )

    @property
    def client(self):
        return self._client

    def submit(self, call_args):
        if call_args.request_type == 'get_object':
            serialized_request = self._client.get_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'put_object':
            # Set the body stream later
            serialized_request = self._client.put_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'delete_object':
            serialized_request = self._client.delete_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'copy_object':
            serialized_request = self._client.copy_object(
                CopySource=call_args.copy_source, Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        return self._executor.submit(serialized_request, call_args)


class CrtCredentialProviderWrapper():
    """
    Provides the credential for CRT.
    CRT will invoke get_credential method and expected a dictionary return value back.
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


class CRTExecutor(object):
    def __init__(self, configs=None, session=None, region=None):
        # initialize crt client in here
        event_loop_group = EventLoopGroup(configs.max_request_concurrency)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)
        credential_provider = AwsCredentialsProvider.new_python(
            CrtCredentialProviderWrapper(session))

        # # if max_bandwidth not set, we will target 100 Gbps, which means as much as possible.
        target_gbps = 0
        if configs.max_bandwidth:
            # Translate bytes to gigabits
            target_gbps = configs.max_bandwidth*8/(1000*1000)

        self._crt_client = S3Client(
            bootstrap=bootstrap,
            region=region,
            credential_provider=credential_provider,
            part_size=configs.multipart_chunksize,
            throughput_target_gbps=target_gbps)

    def submit(self, serialized_http_requests, call_args):
        logger.debug(serialized_http_requests)
        crt_request = CrtUtil.crt_request_from_aws_request(
            serialized_http_requests)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(serialized_http_requests.url)
            crt_request.headers.set("host", url_parts.netloc)
        future = CRTTransferFuture(None, TransferMeta(call_args))
        future.subscriber_manager.on_queued()

        if call_args.request_type == 'get_object':
            type = S3RequestType.GET_OBJECT
        elif call_args.request_type == 'put_object':
            type = S3RequestType.PUT_OBJECT
        else:
            type = S3RequestType.DEFAULT

        if type == S3RequestType.PUT_OBJECT:
            file_stats = os.stat(call_args.fileobj)
            data_len = file_stats.st_size
            crt_request.headers.set("Content-Length", str(data_len))
            content_type = "text/plain"
            if 'ContentType' in call_args.extra_args:
                content_type = call_args.extra_args['ContentType']
            crt_request.headers.set(
                "Content-Type", content_type)

        s3_request = self._crt_client.make_request(request=crt_request,
                                                   type=type,
                                                   file=call_args.fileobj,
                                                   on_done=future.on_done,
                                                   on_progress=future.on_progress)
        future.set_s3_request(s3_request)
        return future
