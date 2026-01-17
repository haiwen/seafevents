import json
import hmac
import logging

from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, DateTime, Text, text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT

from seafevents.db import Base

logger = logging.getLogger(__name__)

PENDING = 0
FAILURE = 3


class Webhooks(Base):
    """
    webhooks model
    """
    __tablename__ = 'webhooks'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    repo_id = mapped_column(String(36), nullable=False, index=True)
    url = mapped_column(String(2000), nullable=False)
    settings = mapped_column(Text)
    creator = mapped_column(String(255), nullable=False)
    created_at = mapped_column(DateTime, server_default=text('current_timestamp(6)'))
    is_valid = mapped_column(TINYINT, default=1)

    @property
    def hook_settings(self):
        try:
            hook_settings = json.loads(self.settings)
        except (Exception, ):
            return {}
        return hook_settings

    def is_event_trigger(self, event):
        hook_settings = self.hook_settings
        if not hook_settings:
            return False
        events = hook_settings.get('events', [])
        if event in events:
            return True

    def gen_request_body(self, event):
        """
        must return dict
        """
        if event.get('event') == 'repo-update':
            return {'event': 'repo-update', 'data': event.get('data')}
        return {}

    def gen_request_headers(self, request_body):
        """
        must return dict
        """
        hook_settings = self.hook_settings
        if not hook_settings:
            return {}
        secret = hook_settings.get('secret')
        if not secret:
            return {}

        msg = json.dumps(request_body)
        signature = 'sha256=' + hmac.new(
            secret.encode('utf8'), msg.encode('utf8'), digestmod='sha256').hexdigest()
        return {'X-Seafile-Signature': signature}


class WebhookJobs(Base):
    """
    webhook_jobs model
    """
    __tablename__ = 'webhook_jobs'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    webhook_id = mapped_column(INTEGER(unsigned=True), index=True, nullable=False)
    created_at = mapped_column(DateTime, server_default=text('current_timestamp(6)'))
    trigger_at = mapped_column(DateTime)
    status = mapped_column(TINYINT, default=0, index=True)
    url = mapped_column(String(2000), nullable=False)
    request_headers = mapped_column(Text)
    request_body = mapped_column(Text)
    response_status = mapped_column(Integer)
    response_body = mapped_column(Text)

    def __init__(self, webhook_id, created_at, trigger_at, status, url, request_headers,
                 request_body, response_status, response_body):
        super().__init__()
        self.webhook_id = webhook_id
        self.created_at = created_at
        self.trigger_at = trigger_at
        self.status = status
        self.url = url
        self.request_headers = json.dumps(request_headers) if isinstance(request_headers, dict) else request_headers
        self.request_body = json.dumps(request_body) if isinstance(request_body, dict) else request_body
        self.response_status = response_status
        self.response_body = json.dumps(response_body) if isinstance(response_body, dict) else response_body
