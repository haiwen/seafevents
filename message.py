#coding: utf-8

import ccnet

class MessageReceiver(object):
    '''A message receiver has a dedicated ccnet client for it.'''
    def __init__(self, ccnet_conf_dir, msg_type):
        self.ccnet_conf_dir = ccnet_conf_dir 
        self.msg_type = msg_type
        self._client = self.prepare_ccnet_client()

    def prepare_ccnet_client(self):
        client = ccnet.Client(self.ccnet_conf_dir)
        client.connect_daemon()
        client.prepare_recv_message(self.msg_type)

        return client

    def __str__(self):
        return '<message receiver: conf dir = %s, msg_type = %s>' \
            % (self.ccnet_conf_dir, self.msg_type)

    def get_message(self):
        '''Block waiting for a message '''
        return self._client.receive_message()

    def reconnect(self):
        self._client = self.prepare_ccnet_client()
