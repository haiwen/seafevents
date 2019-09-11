import os

from seafevents.utils import do_exit


def sigint_handler(*args):
    dummy = args
    do_exit(0)


def sigchild_handler(*args):
    dummy = args
    try:
        os.wait3(os.WNOHANG)
    except:
        pass
