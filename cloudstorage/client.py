import io
import os
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import httplib2
from oauth2client import gce
from oauth2client.appengine import AppAssertionCredentials
from oauth2client.file import Storage
from errors import GoogleCloudStorageAuthorizationError

__author__ = 'krakover'


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

    def get_http_for_request(self):
        if self.use_jwt_credentials_auth:  # Local debugging using pem file
            scope = 'https://www.googleapis.com/auth/bigquery'
            from oauth2client.client import SignedJwtAssertionCredentials
            credentials = SignedJwtAssertionCredentials(self.jwt_account_name, self.jwt_key_func(), scope=scope)
            logging.info("Using Standard jwt authentication")
            return credentials.authorize(httplib2.Http())

        elif self.is_in_appengine():  # App engine
            from google.appengine.api import memcache
            scope = 'https://www.googleapis.com/auth/bigquery'
            credentials = AppAssertionCredentials(scope=scope)
            logging.info("Using Standard appengine authentication")
            return credentials.authorize(httplib2.Http(memcache))

        elif self.oauth_credentails_file:  # Local oauth token
            http = httplib2.Http()
            storage = Storage(self.oauth_credentails_file)
            credentials = storage.get()
            if not credentials:
                raise EnvironmentError('No credential file present')
            http = credentials.authorize(http)
            credentials.refresh(http)
            logging.info("Using Standard OAuth authentication")
            return http

        elif self.is_in_gce_machine():  # GCE authorization
            http = httplib2.Http()
            credentials = gce.AppAssertionCredentials('')
            http = credentials.authorize(http)
            credentials.refresh(http)
            logging.info("Using GCE authentication")
            return http

        raise GoogleCloudStorageAuthorizationError()

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
        return build("storage", "v1", http=http)
