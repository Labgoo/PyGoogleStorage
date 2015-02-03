import io
import json
import os
import logging
from googleapiclient import model, http
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import httplib2
from oauth2client import gce
from oauth2client.appengine import AppAssertionCredentials
from oauth2client.file import Storage
from errors import GoogleCloudStorageAuthorizationError, GoogleCloudStorageError, GoogleCloudStorageNotFoundError

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
    """Converts errors into BigQuery errors."""

    def __init__(self, http_model, *args, **kwargs):
        super(GoogleCloudStorageHttp, self).__init__(*args, **kwargs)
        self._model = http_model

    @staticmethod
    def factory(model):
        """Returns a function that creates a BigQueryHttp with the given model."""
        def _create_bigquery_http_request(*args, **kwargs):
            captured_model = model
            return GoogleCloudStorageHttp(captured_model, *args, **kwargs)

        return _create_bigquery_http_request

    def execute(self, **kwargs):
        try:
            return super(GoogleCloudStorageHttp, self).execute(**kwargs)
        except HttpError, e:
            # TODO(user): Remove this when apiclient supports logging of error responses.
            self._model._log_response(e.resp, e.content)

            if e.resp.get('content-type', '').startswith('application/json'):
                result = json.loads(e.content)
                error = result.get('error', {}).get('errors', [{}])[0]
                raise GoogleCloudStorageError.create(error, result, [])
            else:
                if e.resp.reason == 'Not Found':
                    raise GoogleCloudStorageNotFoundError(e.resp.reason)
                if e.resp.status == 404:
                    raise GoogleCloudStorageNotFoundError(e.resp.reason)
                if e.resp.reason == 'Forbidden':
                    raise GoogleCloudStorageAuthorizationError(e.resp.reason)
                if e.resp.status == 403:
                    raise GoogleCloudStorageAuthorizationError(e.resp.reason)
                raise GoogleCloudStorageError(
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
        if self._credentials:
            return self._credentials

        if self.use_jwt_credentials_auth:  # Local debugging using pem file
            scope = 'https://www.googleapis.com/auth/bigquery'
            from oauth2client.client import SignedJwtAssertionCredentials
            credentials = SignedJwtAssertionCredentials(self.jwt_account_name, self.jwt_key_func(), scope=scope)
            logging.info("Using Standard jwt authentication")
            self._credentials = self._credentials
            return credentials
        elif self.is_in_appengine():  # App engine
            scope = 'https://www.googleapis.com/auth/bigquery'
            credentials = AppAssertionCredentials(scope=scope)
            logging.info("Using Standard appengine authentication")
            self._credentials = self._credentials
            return credentials
        elif self.oauth_credentails_file:  # Local oauth token
            storage = Storage(self.oauth_credentails_file)
            credentials = storage.get()
            if not credentials:
                raise GoogleCloudStorageAuthorizationError('No credential file present')
            logging.info("Using Standard OAuth authentication")
            self._credentials = self._credentials
            return credentials
        elif self.is_in_gce_machine():  # GCE authorization
            credentials = gce.AppAssertionCredentials('')
            logging.info("Using GCE authentication")
            self._credentials = self._credentials
            return credentials
        raise GoogleCloudStorageAuthorizationError('No Credentials provided')

    def get_http_for_request(self):
        http = httplib2.Http()
        http = self.credentials.authorize(http)
        self.credentials.refresh(http)

        return http

    @staticmethod
    def is_in_appengine():
        'SERVER_SOFTWARE' in os.environ and os.environ['SERVER_SOFTWARE'].startswith('Google App Engine/')

    @staticmethod
    def is_in_gce_machine():
        try:
            metadata_uri = 'http://metadata.google.internal'
            http = httplib2.Http()
            http.request(metadata_uri, method='GET')
            return True
        except httplib2.ServerNotFoundError:
            return False

    @property
    def api_client(self):
        http = self.get_http_for_request()
        cloudstorage_model = GoogleCloudStorageModel(trace=self.trace)
        cloudstorage_http = GoogleCloudStorageHttp.factory(cloudstorage_model)

        return build("storage", "v1", http=http, model=cloudstorage_model, requestBuilder=cloudstorage_http)
