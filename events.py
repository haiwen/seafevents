#!/usr/bin/env python
#coding: utf-8

import argparse
import os
import sys
import time
import atexit
import signal

from ccnet import NetworkError

from message import MessageReceiver
from db import init_db_session_class
from handler import handle_message

import logging

def parse_args():
    parser = argparse.ArgumentParser(
         description='seafile events recorder')

    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('w'),
        help='log file')

    parser.add_argument(
        '-c',
        '--ccnet-conf-dir',
        default='~/.ccnet',
        help='ccnet server config directory')

    parser.add_argument(
        '--config_file',
        default=os.path.join(os.getcwd(), 'events.conf'),
        help='ccnet server config directory')

    parser.add_argument(
        '--loglevel',
        default='debug',
    )

    parser.add_argument(
        '-P',
        '--pidfile',
    )

    return parser.parse_args()

def init_logging(args):
    """Configure logging module"""
    level = args.loglevel

    if level == 'debug':
        level = logging.DEBUG
    elif level == 'info':
        level = logging.INFO
    else:
        level = logging.WARNING

    kw = {
        'format': '[%(asctime)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': level,
        'stream': args.logfile
    }

    logging.basicConfig(**kw)

def do_exit(retcode):
    logging.info('Quit')
    sys.exit(retcode)

def do_reconnect(receiver):
    '''Reconnect to ccnet server when server restarts'''
    while True:
        try:
            logging.info('%s: try to reconnect to daemon', receiver)
            receiver.reconnect()
        except NetworkError:
            time.sleep(2)
        else:
            logging.info('%s: Reconnected to daemon', receiver)
            break

def create_receiver(args):
    try:
        receiver = MessageReceiver(args.ccnet_conf_dir, 'seaf_server.event')
    except NetworkError:
        logging.warning("can't connect to ccnet daemon. Now quit")
        sys.exit(1)

    return receiver


def write_pidfile(pidfile):
    pid = os.getpid()
    with open(pidfile, 'w') as fp:
        fp.write(str(pid))

    def remove_pidfile():
        '''Remove the pidfile when exit'''
        logging.info('remove pidfile %s' % pidfile)
        try:
            os.remove(pidfile)
        except:
            pass

    atexit.register(remove_pidfile)

def sighandler(signum, frame):
    """Kill itself when Ctrl-C, for development"""
    os.kill(os.getpid(), signal.SIGKILL)

def main():
    args = parse_args()
    init_logging(args)
    ev_receiver = create_receiver(args)
    Session = init_db_session_class(args.config_file)
    session = Session()

    signal.signal (signal.SIGINT, sighandler)

    if args.pidfile:
        write_pidfile(args.pidfile)
    logging.info('starts to read message')
    while True:
        try:
            msg = ev_receiver.get_message()
        except NetworkError:
            logging.warning('connection to daemon is lost')
            do_reconnect(ev_receiver)
            continue
        else:
            handle_message(session, msg)

    do_exit(1)

if __name__ ==  '__main__':
    main()
