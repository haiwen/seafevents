import os
import signal

from seafevents.utils import do_exit, run


def sigint_handler(*args):
    dummy = args
    do_exit(0)


def sigchild_handler(*args):
    dummy = args
    try:
        os.wait3(os.WNOHANG)
    except:
        pass


def kill_face_cluster():
    cmd = [
        'pkill', '-f', 'seafevents.face_recognition.face_cluster'
    ]
    run(cmd)


def signal_term_handler(signal, frame):
    kill_face_cluster()
    os._exit(0)


def set_signal():
    # TODO: look like python will add signal to queue when cpu exec c extension code,
    # and will call signal callback method after cpu exec python code
    # ref: https://docs.python.org/2/library/signal.html
    signal.signal(signal.SIGTERM, signal_term_handler)
