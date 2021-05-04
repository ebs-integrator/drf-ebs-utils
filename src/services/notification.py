import json

from functools import cached_property

from typing import Union

from django.db.models import Q
from django.template.loader import render_to_string
from django.utils import translation
from rest_framework.exceptions import ValidationError

from datetime import datetime

from contextlib import suppress

from applications.notifications.helpers import NotificationHelper
from applications.settings.helpers import SettingHelper
from applications.settings.models import Setting
from applications.users.models import User
from applications.users.helpers import RoleHelper

from applications.commons.services.request import AbstractServiceProvider
from applications.commons.services.storage import StorageService

from applications.notifications.serializers.notification import NotificationSerializer
from applications.notifications.models import Notification

from applications.nomenclatures.models import NomenclatureAlertAddress

from config.settings import SERVICE_NOTIFICATION_HOST, SERVICE_NOTIFICATION_SECRET_KEY


# Create your services here.


class NotificationService(StorageService, AbstractServiceProvider):
    URL_POST_WAITING = 'notification/waiting/'
    URL_POST_WAITING_BULK = 'notification/waiting/bulk/'

    def __init__(self):
        super().__init__()

        self.host = SERVICE_NOTIFICATION_HOST
        self.key = SERVICE_NOTIFICATION_SECRET_KEY
        self.headers = self.default_headers

        self.model = Notification
        self.get_serializer_class = NotificationSerializer
        self.create_serializer_class = NotificationSerializer
        self.unique_identifier = 'code_name'
        self.unique_identifiers = ['code_name', 'type', 'target_id']

        # settings
        self.settings_model = Setting
        self.settings_helper = SettingHelper

        # Authorize
        self.auth()

        # Locale
        self.LOCALE = translation.get_language()

    @staticmethod
    def get_template(path: str = 'mail/global', payload=None):
        if payload is None:
            payload = {}

        return render_to_string(f'{path}.html', payload)

    def get_model(self):
        return self.model

    def _set_method_payload(self, **kwargs):
        method = kwargs.get('method', 'email')
        recipient = kwargs.get('recipient', '')
        sender = kwargs.get('sender', '')
        subject = str(kwargs.get('subject', ''))
        body = kwargs.get('body', '')
        internal = kwargs.get('internal', False)

        template = kwargs.get('template', {})
        text = template.get('text', '')
        html = template.get('html', {})
        path = html.get('path', '') or 'mail/global'
        html_payload = html.get('payload', {})

        if html_payload:
            body = self.get_template(path, html_payload)
        elif text:
            body = text

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
        # set locale
        if recipient := kwargs.get('recipient', ''):
            # set initial locale
            self.LOCALE = translation.get_language()

            # set user locale
            self.set_locale(value=recipient)

        # forming payload
        dump = kwargs.get('dump', False)
        payload = self._set_method_payload(**kwargs)

        # forming metadata for db
        if metadata := kwargs.get('metadata', {}):
            metadata['title'] = str(metadata.get('title'))
            metadata['description'] = str(metadata.get('description'))

        # restore payload
        if recipient:
            self.set_initial_locale()

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

    def _save(self, **kwargs):
        response = kwargs.get('response', {})
        delivery_method = response.get('delivery_method', 'email').upper()
        metadata = kwargs.get('metadata', {})

        # Object saving
        output = {
            'title': response.get('message_subject', ''),
            'description': response.get('message_body', ''),
            'read': response.get('is_read', False),
            'timestamp': response.get('created_at', datetime.now()),
            'edited_timestamp': response.get('modified_at', datetime.now()),
            'target_id': self._get_user_by(delivery_method, response.get('recipient', '')),
            'user_id': self._get_user_by(delivery_method, response.get('sender', '')),
            'code_name': response.get('id', ''),
            'type': response.get('delivery_method', 'email').upper(),
            'data': response,
            'status': NotificationHelper.STATUS_NOTIFIED,
            'metadata': metadata
        }

        return self.upsert(data=output)

    def _get_user_by(self, method: str, data: str) -> Union[int, None]:
        """
        Get user pk by key
        :param method: Method to check
        :param data: Email or Phone
        :return:
        """
        if method not in [self.model.TYPE_SMS, self.model.TYPE_EMAIL]:
            return None

        key = 'email'
        if method == self.model.TYPE_SMS:
            key = 'phone'

        queryset = User.objects.filter(**{key: data})
        return queryset.values_list("id", flat=True).first()

    @cached_property
    def alerts(self) -> Union[list, None]:
        """
        Get alert addresses
        :return:
        """
        return NomenclatureAlertAddress.objects.all()

    @staticmethod
    def alerts_roles(roles=None) -> Union[list, None]:
        """
        Get alert addresses by roles
        :return:
        """
        if not roles:
            roles = [RoleHelper.SY_CREDIT_OFFICER_SUPERIOR]

        return User.objects.filter(groups__name__in=roles)

    def alerting_roles(self, **payload) -> Union[list, None]:
        """
        Send notification payloads to alert addresses by roles
        :return:
        """
        alerts = self.alerts_roles(payload.get('roles', []))

        if not alerts:
            return

        # Send notifications
        for alert in alerts:
            payload.update({'recipient': alert.email})
            self.send(**payload)

        return

    def alerting(self, **payload) -> Union[list, None]:
        """
        Send notification payloads to alert addresses
        :return:
        """
        alerts = self.alerts

        if not alerts:
            return

        # Send notifications
        for alert in alerts:
            email = alert.user.email if alert.user else alert.email
            payload.update({'recipient': email})
            self.send(**payload)

        return

    def alerting_bulk(self, notifications):
        """
        Send notification payloads to alert addresses
        :return:
        """
        alerts = self.alerts

        if not alerts:
            return

        # Send notifications
        for alert in alerts:
            email = alert.user.email if alert.user else alert.email

            for notification in notifications:
                notification.update({'recipient': email})

            self.bulk_send(notifications)

        return

    def auth(self):
        self.headers['Authorization'] = f'Bearer {self.key}'
        return self

    @staticmethod
    def set_locale(user: User = None, value: str = None):
        # get user
        if not user and value:
            user = User.objects.filter(Q(email=value) | Q(phone=value)).first()

        # validate
        if not isinstance(user, User):
            return

        # get locale
        if not (locale := getattr(user, 'locale', None)):
            return

        # set locale
        translation.activate(locale)

    def set_initial_locale(self):
        if self.LOCALE == translation.get_language():
            return
        translation.activate(self.LOCALE)
