import importlib.util
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import requests
from urllib3.exceptions import ReadTimeoutError


spec = importlib.util.spec_from_file_location(
    'webhook_http', Path(__file__).resolve().parents[2] / 'webhook' / 'http.py')
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)


class WebhookTransportTest(unittest.TestCase):
    def setUp(self):
        self.received = []
        received = self.received

        class Receiver(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle(self):
                try:
                    super().handle()
                except ConnectionResetError:
                    pass  # TLS clients may reject the test certificate.

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                received.append((self.path, self.headers, json.loads(body)))
                if self.path.startswith('/redirect/'):
                    self.send_response(int(self.path.rsplit('/', 1)[1]))
                    self.send_header('Location', 'http://127.0.0.1/forbidden')
                else:
                    self.send_response(204)
                self.end_headers()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
        self.addCleanup(self.server.server_close)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.attempts = []
        self.lookups = []

    def deliver(self, path='/', scheme='http', host='hooks.example', fail_first=False,
                first_ip='8.8.8.8', connect_error=ConnectionRefusedError):
        real_dns = socket.getaddrinfo
        real_connect = socket.socket.connect
        self.attempts.clear()
        self.lookups.clear()

        def resolve(name, port, *args, **kwargs):
            self.lookups.append(name)
            if name == host:
                # A second hostname lookup would rebind to loopback.
                ips = (first_ip, '1.1.1.1') if self.lookups.count(host) == 1 else ('127.0.0.1',)
                return [(socket.AF_INET6 if ':' in ip else socket.AF_INET,
                         socket.SOCK_STREAM, 6, '', (ip, port)) for ip in ips]
            self.assertIn(name, (first_ip, '1.1.1.1'))
            return real_dns(name, port, *args, **kwargs)

        def connect(sock, address):
            self.attempts.append(address[0])
            if fail_first and address[0] == first_ip:
                raise connect_error('controlled first-address failure')
            return real_connect(sock, self.server.server_address)

        with patch.object(socket, 'getaddrinfo', side_effect=resolve), \
                patch.object(socket.socket, 'connect', connect), \
                patch.dict(os.environ, {'HTTP_PROXY': 'http://127.0.0.1:1',
                                        'HTTPS_PROXY': 'http://127.0.0.1:1',
                                        'NO_PROXY': '', 'no_proxy': ''}):
            return transport.post_webhook(
                scheme + '://' + host + path, {'repo_id': 'test'},
                {'X-Seafile-Signature': 'test-signature'}, timeout=1)

    def test_connection_failure_falls_back_without_resolving_hostname_again(self):
        self.assertEqual(self.deliver(fail_first=True).status_code, 204)
        self.assertEqual(self.attempts, ['8.8.8.8', '1.1.1.1'])
        self.assertEqual(self.lookups.count('hooks.example'), 1)
        self.assertEqual(len(self.received), 1)
        _, headers, body = self.received[0]
        self.assertEqual(headers['Host'], 'hooks.example')
        self.assertEqual(headers['X-Seafile-Signature'], 'test-signature')
        self.assertEqual(body, {'repo_id': 'test'})

    def test_read_failure_does_not_try_second_address(self):
        # Response headers/read failures happen after TCP connect, outside the
        # connection fallback; never resend a potentially delivered POST.
        with patch.object(transport._PinnedHTTPConnection, 'getresponse',
                          side_effect=ReadTimeoutError(None, '/', 'controlled read timeout')):
            with self.assertRaises(requests.exceptions.ReadTimeout):
                self.deliver()
        self.assertEqual(self.attempts, ['8.8.8.8'])

    def test_connect_timeout_and_unreachable_ipv6_fall_back(self):
        for first_ip, error in (('8.8.8.8', socket.timeout),
                                ('2606:4700:4700::1111', OSError)):
            with self.subTest(first_ip=first_ip):
                response = self.deliver(fail_first=True, first_ip=first_ip, connect_error=error)
                self.assertEqual(response.status_code, 204)
                self.assertEqual(self.attempts, [first_ip, '1.1.1.1'])
                self.assertEqual(self.lookups.count('hooks.example'), 1)

    def test_redirects_are_not_followed(self):
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                self.received.clear()
                response = self.deliver('/redirect/%s' % status)
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.history, [])
                self.assertEqual(len(self.received), 1)

    def test_private_and_mixed_dns_answers_never_connect(self):
        for ips in (('127.0.0.1',), ('169.254.169.254',), ('8.8.8.8', '10.0.0.1')):
            answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, 80)) for ip in ips]
            with self.subTest(ips=ips), patch.object(socket, 'getaddrinfo', return_value=answers), \
                    patch.object(socket.socket, 'connect') as connect:
                with self.assertRaises(transport.InvalidWebhookURL):
                    transport.post_webhook('http://hooks.example/', {}, {})
                connect.assert_not_called()

    def test_https_fallback_preserves_sni_and_certificate_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key = str(Path(directory) / 'cert.pem'), str(Path(directory) / 'key.pem')
            subprocess.run([
                'openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', key, '-out', cert, '-days', '1', '-subj', '/CN=hooks.example',
                '-addext', 'subjectAltName=DNS:hooks.example',
            ], check=True, capture_output=True)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            names = []
            context.set_servername_callback(lambda sock, name, ctx: names.append(name))
            self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
            with patch('requests.adapters.DEFAULT_CA_BUNDLE_PATH', cert):
                self.assertEqual(self.deliver(scheme='https', fail_first=True).status_code, 204)
                self.assertEqual(self.attempts, ['8.8.8.8', '1.1.1.1'])
                self.assertEqual(names, ['hooks.example'])
                with self.assertRaises(requests.exceptions.SSLError):
                    self.deliver(scheme='https', host='wrong.example')
                self.assertEqual(self.attempts, ['8.8.8.8'])


if __name__ == '__main__':
    unittest.main()
