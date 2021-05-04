import os

from datetime import datetime
from contextlib import suppress

from rest_framework.exceptions import ValidationError

from src.helpers.attachment import AttachmentHelper
from src.services.abstract import AbstractServiceProvider

from config.settings import (SERVICE_ATTACHMENT_HOST, SERVICE_ATTACHMENT_SECRET_TOKEN, SERVICE_SSO_SECRET_KEY)


# Create your services here.

class AttachmentService(AbstractServiceProvider):
    URL_POST_FILE = 'file/upload/'

    def __init__(self):
        super().__init__()

        self.host = SERVICE_ATTACHMENT_HOST
        self.key = SERVICE_ATTACHMENT_SECRET_TOKEN
        self.secret = SERVICE_SSO_SECRET_KEY
        self.headers = {}

        # Authorize
        self.auth()

    def set_key(self, key: str = ''):
        self.key = key
        self.auth()
        return self

    @staticmethod
    def delete_file(path):
        os.remove(path)

    def upload_bulk(self, **kwargs):
        """
        Upload bulk files
        :param kwargs:
        :return:
        """
        files = kwargs.get('files', [])
        key = kwargs.get('key')
        typed = kwargs.get('type', AttachmentHelper.TYPE_OTHER)
        entities = kwargs.get('entities', False)

        if not files:
            return

        if key:
            self.set_key(key=key)

        elements = []
        for file in files:
            elements.append(self.upload(**{'files': file, 'key': key, 'type': typed, 'entities': entities}))

        return elements

    def upload(self, **kwargs):
        """
        Upload single files
        :param kwargs:
        :return:
        """
        files = kwargs.get('files')
        key = kwargs.get('key')
        typed = kwargs.get('type', AttachmentHelper.TYPE_OTHER)
        entities = kwargs.get('entities', False)
        tuples = kwargs.get('tuples', False)

        # Files not uploaded , skip
        if not files:
            return

        # Key specified update authorization
        if key:
            self.set_key(key=key)

        if isinstance(files, list) and len(files):
            return self.upload_bulk(**kwargs)

        # Remove content type for files
        self.headers.pop('content-type', None)
        files = {'file': files}
        params = {'url': self.URL_POST_FILE, 'files': files}

        with suppress(ValidationError):
            response = self.make_request(method='post', **params)
            response.update({'type': typed})
            # Saving
            element = self._save(response=response)
            return element if not entities else self.indexed(element, index=0, retrieve=True)

        return None if not tuples else None, False

    def upload_path(self, **kwargs):
        """
        Upload file on path
        :param kwargs:
        :return:
        """

        # Files not uploaded , skip
        if not (path := kwargs.get('path')):
            return

        if not os.path.exists(path):
            return

        with open(path, 'rb') as file:
            self.system()

            # Remove content type for files
            self.headers.pop('content-type', None)
            files = {'file': file}
            params = {'url': self.URL_POST_FILE, 'files': files}
            response = self.make_request(method='post', **params)
            response.update({'type': kwargs.get('type', AttachmentHelper.TYPE_OTHER)})

        # Remove local
        if kwargs.get('delete', True, ):
            os.remove(path)

        return self._save(response=response)

    @staticmethod
    def _save(**kwargs):
        response = kwargs.get('response', {})
        # Object saving
        attributes = response.get('attributes', {})
        output = {
            'url': response.get('url', ''),
            'file_name': response.get('file_hash', ''),
            'name': response.get('name', ''),
            'size': response.get('size', 0),
            'timestamp': response.get('created_at', datetime.now()),
            'edited_timestamp': response.get('created_at', datetime.now()),
            'width': attributes.get('width', 0),
            'height': attributes.get('height', 0),
            'mime_type': response.get('content_type', ''),
            'data': response,
            'type': response.get('type')
        }
        return output

    def auth(self):
        self.headers['Authorization'] = f'Bearer {self.key}'
        return self

    def system(self):
        self.headers['Authorization'] = f'Bearer {self.secret}'
        return self

    @staticmethod
    def indexed(elements=None, index: int = 0, retrieve: bool = False):
        """
        Get and / or retrieve element at index
        :param elements:
        :param index:
        :param retrieve:
        :return:
        """
        if elements is None:
            elements = []

        condition = index < len(elements)

        if condition and retrieve:
            return elements[index]

        return condition
