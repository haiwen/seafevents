import signal
import os
import libevent

from seafevents.utils import do_exit

class SignalHandler(object):
    def __init__(self, ev_base):
        self._evbase = ev_base
        self._sighandlers = {}
        self._register_signal_handlers()

    def _register(self, signum, callback):
        sig = libevent.Signal(self._evbase, signal.SIGTERM, callback)
        sig.add()               # pylint: disable=E1101

        # We have to keep a reference to the Signal object (an libevent.Event
        # object, indeed), otherwise when the signal event would be
        # unregistered when the object is destructed
        self._sighandlers[signum] = sig

    def _register_signal_handlers(self):
        '''We have to register the signal handers through libevent, instead of
        the signal module, because the signal handlers register through the
        signal module would not be called when python is executing c extension
        code.

        '''
        self._register(signal.SIGINT, sigint_handler)
        self._register(signal.SIGTERM, sigint_handler)
        if hasattr(signal, 'SIGCHLD'):
            self._register(signal.SIGCHLD, sigchild_handler)

def sigint_handler(*args):
    dummy = args
    do_exit(0)

def sigchild_handler(*args):
    dummy = args
    try:
        os.wait3(os.WNOHANG)
    except:
        pass
