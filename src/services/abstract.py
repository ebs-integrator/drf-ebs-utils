import requests

from django.utils.translation import ugettext_lazy as _

from rest_framework.exceptions import ValidationError
from rest_framework.status import (HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_201_CREATED)

from abc import ABC, abstractmethod
from contextlib import suppress

from simplejson import JSONDecodeError


class AbstractServiceProvider(ABC):
    default_headers = {
        'content-type': 'application/json',
        'charset': 'UTF-8',
        'connection': 'keep-alive',
        'accept': '*/*'
    }

    def __init__(self):
        self.host = None
        self.key = None
        self.username = None
        self.password = None
        self.token = None
        self.agent = None
        self.soap = None
        self.secret = None
        self.headers = self.default_headers

        # Auth service
        self.auth()

    @abstractmethod
    def auth(self) -> dict:
        """
            Base authentication.
            :param self: Class instance.
            :return: dict response data.
        """
        ...

    def create_request(self, method: str, **kwargs):
        """
        Make a request but return response instance
        :param method:
        :param kwargs:
        :return:
        """
        url = kwargs.pop('url')
        params = {'url': f'{self.host}{url}', 'headers': self.headers, 'allow_redirects': True}
        params.update(kwargs)

        # Where requests is a package, method are 'get' or 'post'
        response = getattr(requests, method)(**params)

        if response.status_code not in [HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_201_CREATED]:
            detail = {}
            validation = ValidationError(detail=detail, code=response.status_code)

            with suppress(JSONDecodeError):
                validation.detail.update(**response.json())
                raise validation

            validation.detail = {
                'code': response.status_code,
                'detail': _('Remote provider failure.'),
                'content_type': response.headers['content-type'],
                'content': response.content,
            }

            raise validation

        return response

    def make_request(self, method: str, **kwargs):
        """
        Make a request but return json
        :param method:
        :param kwargs:
        :return:
        """
        response = self.create_request(method, **kwargs)
        return response.json()
