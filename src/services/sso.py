import json

from abc import ABC

from src.services.abstract import AbstractServiceProvider

from config.settings import (SERVICE_SSO_SERVICE_TOKEN, SERVICE_SSO_SECRET_TOKEN, SERVICE_SSO_DOMAIN)


# Create your services here.

class SSOService(AbstractServiceProvider, ABC):
    URL_POST_REGISTER = 'authentication/register'
    URL_POST_CONFIRM = 'users/confirm'
    URL_POST_LOGIN = 'authentication/login'
    URL_PATCH_RESEND = 'users/resend-confirmation'
    URL_PATCH_PASSWORD = 'users/set-password'
    URL_POST_RESTORE_PASSWORD = 'authentication/restore-request'
    URL_POST_RESTORE_CONFIRM_PASSWORD = 'authentication/restore-confirm'
    URL_POST_REFRESH = 'authentication/refresh'

    def __init__(self):
        super().__init__()

        self.host = SERVICE_SSO_DOMAIN
        self.key = SERVICE_SSO_SECRET_TOKEN
        self.token = SERVICE_SSO_SERVICE_TOKEN

        # Authorize
        self.auth()

    def profile(self, **kwargs):
        """
        Profile user
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        params = {'url': self.URL_POST_LOGIN, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def login(self, **kwargs):
        """
        Register user
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_LOGIN, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def set_password(self, **kwargs):
        """
        Set user password
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})
        headers = kwargs.get('headers', {})
        authorization = headers.get('Authorization', '')

        data = json.dumps(payload)
        self.headers.update({'Authorization': authorization})
        params = {'url': self.URL_PATCH_PASSWORD, 'data': data}
        response = self.make_request(method='patch', **params)

        return response

    def reset_password(self, **kwargs):
        """
        Reset user password
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_RESTORE_PASSWORD, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def reset_password_confirm(self, **kwargs):
        """
        Reset user password confirm
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_RESTORE_CONFIRM_PASSWORD, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def refresh(self, **kwargs):
        """
        Refresh user
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_REFRESH, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def register(self, **kwargs):
        """
        Register user
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_REGISTER, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def confirm(self, **kwargs):
        """
        Confirm user
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_POST_CONFIRM, 'data': data}
        response = self.make_request(method='post', **params)

        return response

    def resend(self, **kwargs):
        """
        Resend user confirmation
        :param kwargs:
        :return:
        """
        payload = kwargs.get('data', {})

        data = json.dumps(payload)
        self.headers.pop('Authorization', None)
        params = {'url': self.URL_PATCH_RESEND, 'data': data}
        response = self.make_request(method='patch', **params)

        return response

    def auth(self):
        self.headers['Service-Token'] = f'{self.token}'
        self.headers['Secret-Token'] = f'{self.key}'
        return self
