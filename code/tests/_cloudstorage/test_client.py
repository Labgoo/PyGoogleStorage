from unittest import TestCase

from mock import patch

from code.cloudstorage.client import GoogleCloudStorageClient


__author__ = 'krakover'


class GoogleCloudStorageClientTest(TestCase):

    def testIsOnGCE(self):
        self.assertFalse(GoogleCloudStorageClient().is_in_gce_machine())

    def testIsOnAppengine(self):
        self.assertFalse(GoogleCloudStorageClient().is_in_appengine())
