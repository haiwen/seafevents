def __bootstrap__():
    global __bootstrap__, __loader__, __file__
    import sys, os, pkg_resources, imp, platform
    if os.name == 'nt':
        __file__ = '_libevent.pyd'
    else:
        if 'arm' in platform.machine():
            machine = 'pi'
        elif 'x86_64' in platform.machine():
            machine = 'x86_64'
        else:
            machine = 'i386'
        __file__ = pkg_resources.resource_filename(__name__,'libevent-py%s-%s-%s.so' % (sys.version[:3],sys.platform,machine))
    __loader__ = None; del __bootstrap__, __loader__
    imp.load_dynamic(__name__,__file__)

__bootstrap__()
