#coding: utf-8

import ccnet
import datetime

class NoConnectionError(Exception):
    """Indicates we can't connect to daemon""" 
    pass

class Message(object):
    """Ccnet message"""
    def __init__(self, mtype, body, ctime):
        self.mtype = mtype
        self.body = body
        self.ctime = datetime.datetime.fromtimestamp(float(ctime))

    def __str__(self):
        return "<Message(type='%s', body='%s')>" % (self.mtype, self.body)

class MessageReceiver(object):
    """A message receiver has a dedicated ccnet client for it."""
    def __init__(self, ccnet_conf_dir, msg_type):
        self.ccnet_conf_dir = ccnet_conf_dir 
        self.msg_type = msg_type
        self.client = self.prepare_ccnet_client()

    def prepare_ccnet_client(self):
        client = ccnet.Client()

        if client.load_confdir(self.ccnet_conf_dir) < 0:
            raise RuntimeError("%s: failed to load config dir" % (self,))
            
        ret = client.connect_daemon(ccnet.CLIENT_SYNC)
        if ret < 0:
            raise NoConnectionError("%s: can't connect to daemon: %s" % (self, str(ret)))
        
        if client.prepare_recv_message(self.msg_type) < 0:
            raise RuntimeError("%s: failed to prepare receive message" % (self,))

        return client

    def __str__(self):
        return "<message receiver: conf dir = %s, msg_type = %s>" \
            % (self.ccnet_conf_dir, self.msg_type)

    def is_connected(self):
        if self.client and self.client.is_connected():
            return True

        return False

    def get_message(self):
        """Block waiting for a message of the type specied when init this
        receiver.

        """
        msg = self.client.receive_message()
        if msg:
            # <app, body, ctime>
            return Message(msg[0], msg[1], msg[2])
        else:
            if not self.client.is_connected():
                raise NoConnectionError
            else:
                return None

    def reconnect(self):
        if self.is_connected():
            return
        self.client = self.prepare_ccnet_client()
