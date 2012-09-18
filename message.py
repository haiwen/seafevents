#coding: utf-8

import ccnet
import datetime

__all__ = [
    "MessageReceiver"
]

class Message(object):
    """Ccnet message"""
    def __init__(self, mtype, body, ctime):
        self.mtype = mtype
        self.body = body
        self.ctime = datetime.datetime.fromtimestamp(float(ctime))

class MessageReceiver(object):
    """A message receiver has a dedicated ccnet client for it."""
    def __init__(self, ccnet_conf_dir, msg_type):
        self.conf_dir = ccnet_conf_dir 
        self.msg_type = msg_type
        self.client = ccnet.Client()

        if self.client.load_confdir(ccnet_conf_dir) < 0:
            raise RuntimeError("%s: failed to load config dir" % (self,))
            
        ret = self.client.connect_daemon(ccnet.CLIENT_SYNC)
        if ret < 0:
            raise RuntimeError("%s: can't connect to daemon: %s" % (self, str(ret)))
        
        if self.client.prepare_recv_message(msg_type) < 0:
            raise RuntimeError("%s: failed to prepare receive message" % (self,))

    def __str__(self):
        return "<message receiver: conf dir = %s, msg_type = %s>" \
            % (self.conf_dir, self.msg_type)

    def get_message(self):
        """Block waiting for a message of the type specied when init this
        receiver.

        """
        msg = self.client.receive_message()
        if msg:
            # type, body, ctime
            return Message(msg[0], msg[1], msg[2])
        else:
            return None
