#coding: utf-8

import ccnet
import datetime

class NoConnectionError(Exception):
    """Indicates we can't connect to daemon""" 
    pass

class MessageReceiver(object):
    """A message receiver has a dedicated ccnet client for it."""
    def __init__(self, ccnet_conf_dir, msg_type):
        self.ccnet_conf_dir = ccnet_conf_dir 
        self.msg_type = msg_type
        self.client = self.prepare_ccnet_client()

    def prepare_ccnet_client(self):
        client = ccnet.Client(self.ccnet_conf_dir)
        ret = client.connect_daemon()
        client.prepare_recv_message(self.msg_type)

        return client

    def __str__(self):
        return "<message receiver: conf dir = %s, msg_type = %s>" \
            % (self.ccnet_conf_dir, self.msg_type)

    def is_connected(self):
        if self.client and self.client.is_connected():
            return True

        return False

    def get_message(self):
        """Block waiting for a message of the type specified when init this
        receiver.

        """
        msg = self.client.receive_message()
        if msg:
            return msg
        else:
            if not self.client.is_connected():
                raise NoConnectionError
            else:
                return None

    def reconnect(self):
        if self.is_connected():
            return
        self.client = self.prepare_ccnet_client()
