"""Public-only webhook delivery, with DNS results pinned for each request."""

import ipaddress
import socket
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.exceptions import ConnectTimeoutError


class InvalidWebhookURL(ValueError):
    pass


def resolve_webhook_url(url):
    # Keep URL validation in sync with seahub.utils.webhook.
    if not isinstance(url, str) or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in url):
        raise InvalidWebhookURL('Invalid webhook URL.')
    try:
        if '\\' in url:
            raise ValueError()
        parts = urlsplit(url)
        if (parts.scheme not in ('http', 'https') or not parts.hostname or
                parts.username is not None or parts.password is not None or parts.fragment or
                parts.port == 0):
            raise ValueError()
        # Use the same normalization as the HTTP client (including IDNA).
        prepared = requests.Request('POST', url).prepare()
        parts = urlsplit(prepared.url)
        host = parts.hostname
        if '%' in host:  # Disallow IPv6 scope identifiers and escaped hostnames.
            raise ValueError()
        try:
            socket.inet_aton(host)
        except OSError:
            pass
        else:
            # Refuse non-canonical numeric hosts; resolver interpretations of
            # octal/hexadecimal and shortened IPv4 differ across platforms.
            ipaddress.IPv4Address(host)
        port = parts.port if parts.port is not None else (443 if parts.scheme == 'https' else 80)
        if port == 0:
            raise ValueError()
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        if not addresses:
            raise ValueError()
        for family, _, _, _, address in addresses:
            ip = ipaddress.ip_address(address[0])
            if (family not in (socket.AF_INET, socket.AF_INET6) or
                    not ip.is_global or ip.is_multicast or ip.is_reserved or
                    ip.is_loopback or ip.is_link_local or ip.is_unspecified):
                raise ValueError()
            if isinstance(ip, ipaddress.IPv6Address):
                # Reject transition/translation addresses as well as site-local IPv6.
                if (ip.is_site_local or ip.ipv4_mapped or ip.sixtofour or ip.teredo or
                        ip in ipaddress.ip_network('64:ff9b::/96') or
                        ip in ipaddress.ip_network('64:ff9b:1::/48')):
                    raise ValueError()
        return prepared.url, parts, tuple(dict.fromkeys(a[4][0] for a in addresses)), port
    except (ValueError, OSError, requests.RequestException) as e:
        raise InvalidWebhookURL('Webhook URL must resolve only to public IP addresses.') from e


class _PinnedConnectionMixin:
    def __init__(self, *args, pinned_addresses, **kwargs):
        self.pinned_addresses = pinned_addresses
        super().__init__(*args, **kwargs)

    def _new_conn(self):
        # Fall back only while opening the TCP socket, before sending any POST
        # bytes. Retrying a whole request could duplicate webhook delivery.
        original_host = self._dns_host
        try:
            for index, address in enumerate(self.pinned_addresses):
                self._dns_host = address
                try:
                    return super()._new_conn()
                except ConnectTimeoutError:
                    # Includes NewConnectionError (e.g. connection refused).
                    if index == len(self.pinned_addresses) - 1:
                        raise
        finally:
            self._dns_host = original_host


class _PinnedHTTPConnection(_PinnedConnectionMixin, HTTPConnection):
    pass


class _PinnedHTTPSConnection(_PinnedConnectionMixin, HTTPSConnection):
    pass


class _PinnedHTTPPool(HTTPConnectionPool):
    ConnectionCls = _PinnedHTTPConnection


class _PinnedHTTPSPool(HTTPSConnectionPool):
    ConnectionCls = _PinnedHTTPSConnection


class _PinnedAdapter(HTTPAdapter):
    def __init__(self, parts, addresses, port):
        super().__init__()
        if parts.scheme == 'https':
            # Connect to the checked IP, retaining the original TLS SNI and
            # certificate hostname verification. The request URL retains Host.
            self.pool = _PinnedHTTPSPool(
                addresses[0], port, pinned_addresses=addresses, server_hostname=parts.hostname,
                assert_hostname=parts.hostname)
        else:
            self.pool = _PinnedHTTPPool(addresses[0], port, pinned_addresses=addresses)

    def get_connection(self, url, proxies=None):
        return self.pool

    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        return self.pool

    def close(self):
        self.pool.close()
        super().close()


def post_webhook(url, body, headers, timeout=30):
    url, parts, addresses, port = resolve_webhook_url(url)
    with requests.Session() as session:
        # Environment proxies could resolve the hostname again or bypass the
        # checked destination. Webhook delivery always uses a direct connection.
        session.trust_env = False
        session.mount(parts.scheme + '://', _PinnedAdapter(parts, addresses, port))
        # Explicit Host is needed because the connection pool uses the pinned IP.
        headers = dict(headers or {})
        headers['Host'] = parts.netloc
        return session.post(url, json=body, headers=headers, timeout=timeout,
                            allow_redirects=False)
