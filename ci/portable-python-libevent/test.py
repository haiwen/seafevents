import libevent
import ccnet

from ccnet.async import Timer

class T(Timer):
    def __init__(self, *args, **kwargs):
        Timer.__init__(self, *args, **kwargs)
        self.n = 0
    def callback(self):
        print 'Hello, n = %s' % self.n
        self.n += 1

base = libevent.Base()

t = T(base, 1)
base.loop()