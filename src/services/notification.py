import json

from rest_framework.exceptions import ValidationError
from datetime import datetime
from contextlib import suppress
from src.helpers.notification import NotificationHelper
from src.services.abstract import AbstractServiceProvider
from config.settings import (SERVICE_NOTIFICATION_HOST, SERVICE_NOTIFICATION_SECRET_KEY)


# Create your services here.


class NotificationService(AbstractServiceProvider):
    URL_POST_WAITING = 'notification/waiting/'
    URL_POST_WAITING_BULK = 'notification/waiting/bulk/'

    def __init__(self):
        super().__init__()

        self.host = SERVICE_NOTIFICATION_HOST
        self.key = SERVICE_NOTIFICATION_SECRET_KEY
        self.headers = self.default_headers

        # Authorize
        self.auth()

    @staticmethod
    def _set_method_payload(**kwargs):
        method = kwargs.get('method', 'email')
        recipient = kwargs.get('recipient', '')
        sender = kwargs.get('sender', '')
        subject = str(kwargs.get('subject', ''))
        body = kwargs.get('body', '')
        internal = kwargs.get('internal', False)

        # Compose payload
        payload = {
            "is_internal_recipient": internal,
            "recipient": recipient,
            "delivery_method": method,
            "message_subject": subject,
            "message_body": str(body)
        }

        if sender != '':
            payload.update({'sender': sender})

        return payload

    def send(self, **kwargs):
        """
        Send single notification message
        :param kwargs:
        :return:
        """
        # forming payload
        dump = kwargs.get('dump', False)
        payload = self._set_method_payload(**kwargs)

        # forming metadata for db
        if metadata := kwargs.get('metadata', {}):
            metadata['title'] = str(metadata.get('title'))
            metadata['description'] = str(metadata.get('description'))

        if dump or not payload.get('recipient'):
            return payload

        data = json.dumps(payload)
        params = {'url': self.URL_POST_WAITING, 'data': data}
        with suppress(ValidationError):
            response = self.make_request(method='post', **params)
            return self._save(response=response, metadata=metadata)

        return

    def bulk_send(self, notifications: list = None):
        """
        Send multiple notification message
        :param notifications:
        :return:
        """
        if not notifications:
            return []

        data = json.dumps({'notifications': notifications})
        params = {'url': self.URL_POST_WAITING_BULK, 'data': data}

        with suppress(ValidationError):
            response = self.make_request(method='post', **params)

            # Saving
            elements = []
            notifications = response.get('notifications', [])
            for notification in notifications:
                elements.append(self._save(response=notification))

            return elements

        return

    @staticmethod
    def _save(**kwargs):
        response = kwargs.get('response', {})
        delivery_method = response.get('delivery_method', 'email').upper()
        metadata = kwargs.get('metadata', {})

        # Object saving
        output = {
            'title': response.get('message_subject', ''),
            'description': response.get('message_body', ''),
            'read': response.get('is_read', False),
            'timestamp': response.get('created_at', datetime.now()),
            'delivery_method': delivery_method,
            'edited_timestamp': response.get('modified_at', datetime.now()),
            'recipient': response.get('recipient', ''),
            'sender': response.get('sender', ''),
            'code_name': response.get('id', ''),
            'type': response.get('delivery_method', 'email').upper(),
            'data': response,
            'status': NotificationHelper.STATUS_NOTIFIED,
            'metadata': metadata
        }

        return output

    def auth(self):
        self.headers['Authorization'] = f'Bearer {self.key}'
        return self
