import io
import json

from googleapiclient import model, http
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import httplib2
from googleservices.utils import get_google_credentials
from googleservices.errors import GoogleCloudError

__author__ = 'krakover'

class GoogleCloudStorageModel(model.JsonModel):
    """Adds optional global parameters to all requests."""

    def __init__(self, trace=None, **kwargs):
        super(GoogleCloudStorageModel, self).__init__(**kwargs)
        self.trace = trace

    def request(self, headers, path_params, query_params, body_value):
        """Updates outgoing request."""
        if 'trace' not in query_params and self.trace:
            query_params['trace'] = self.trace

        return super(GoogleCloudStorageModel, self).request(headers, path_params, query_params, body_value)


# pylint: disable=E1002
class GoogleCloudStorageHttp(http.HttpRequest):
    """Converts errors into CloudStorage errors."""

    def __init__(self, http_model, *args, **kwargs):
        super(GoogleCloudStorageHttp, self).__init__(*args, **kwargs)
        self._model = http_model

    @staticmethod
    def factory(storage_model):
        """Returns a function that creates a CloudStorageHttp with the given model."""
        def _create_cloudstorage_http_request(*args, **kwargs):
            captured_model = storage_model
            return GoogleCloudStorageHttp(captured_model, *args, **kwargs)

        return _create_cloudstorage_http_request

    def execute(self, **kwargs):
        try:
            return super(GoogleCloudStorageHttp, self).execute(**kwargs)
        except HttpError, e:
            self._model._log_response(e.resp, e.content)

            if e.resp.get('content-type', '').startswith('application/json'):
                result = json.loads(e.content)
                error = result.get('error', {}).get('errors', [{}])[0]
                raise GoogleCloudError.create(error, result, [])
            else:
                raise GoogleCloudError(
                    ('Could not connect with Google Cloud Storage server.\n'
                     'Http response status: %s\n'
                     'Http response content:\n%s') % (e.resp.get('status', '(unexpected)'), e.content))


class GoogleCloudStorageClient(object):
    def __init__(self, use_jwt_credentials_auth=False, jwt_account_name='', jwt_key_func=None, oauth_credentails_file=None, trace=None):
        """
        :param trace: A value to add to all outgoing requests
        :return:
        """
        super(GoogleCloudStorageClient, self).__init__()
        self.trace = trace
        self.use_jwt_credentials_auth = use_jwt_credentials_auth
        self.jwt_account_name = jwt_account_name
        self.jwt_key_func = jwt_key_func
        self.oauth_credentails_file = oauth_credentails_file

        self._credentials = None

    def read_file(self, bucket_name, file_name):
        return self.objects().get_media(bucket=bucket_name, object=file_name).execute()

    def read_file_metadata(self, bucket_name, file_name):
        return self.objects().get(bucket=bucket_name, object=file_name).execute()

    def write_file(self, bucket_name, file_name, content, content_type):
        media = MediaIoBaseUpload(io.BytesIO(content), content_type)
        response = self.objects().insert(bucket=bucket_name, name=file_name, media_body=media).execute()

        return response

    def bucketAccessControls(self):
        """Returns the bucketAccessControls Resource."""
        return self.api_client.bucketAccessControls()

    def buckets(self):
        """Returns the buckets Resource."""
        return self.api_client.buckets()

    def channels(self):
        """Returns the channels Resource."""
        return self.api_client.channels()

    def defaultObjectAccessControls(self):
        """Returns the defaultObjectAccessControls Resource."""
        return self.api_client.defaultObjectAccessControls()

    def objects(self):
        """Returns the objects Resource."""
        return self.api_client.objects()

    @property
    def credentials(self):
        if not self._credentials:
            self._credentials = get_google_credentials(self.use_jwt_credentials_auth, self.jwt_account_name, self.jwt_key_func, self.oauth_credentails_file)
        return self._credentials

    def get_http_for_request(self):
        _http = httplib2.Http()
        _http = self.credentials.authorize(_http)
        self.credentials.refresh(_http)

        return _http

    @property
    def api_client(self):
        _http = self.get_http_for_request()
        cloudstorage_model = GoogleCloudStorageModel(trace=self.trace)
        cloudstorage_http = GoogleCloudStorageHttp.factory(cloudstorage_model)

        return build("storage", "v1", http=_http, model=cloudstorage_model, requestBuilder=cloudstorage_http)
