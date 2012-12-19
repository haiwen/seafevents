#!/usr/bin/env python
#coding: utf-8

import argparse
import os
import sys
import time

from message import MessageReceiver, NoConnectionError
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
        "format": '[%(asctime)s] %(message)s',
        "datefmt": '%m/%d/%Y %H:%M:%S',
        "level": level,
        "stream": args.logfile
    }

    logging.basicConfig(**kw)
    
def do_exit(retcode):
    logging.info("Quit")
    sys.exit(retcode)

def do_reconnect(receiver):
    while True:
        try:
            logging.info("%s: try to reconnect to daemon", receiver)
            receiver.reconnect()
        except NoConnectionError:
            logging.info("%s: failed to reconnect to daemon", receiver)
            time.sleep(2)
        else:
            logging.info("%s: Reconnected to daemon", receiver)
            break

def create_receiver(args):
    try:
        receiver = MessageReceiver(args.ccnet_conf_dir, "seaf_server.event")
    except NoConnectionError:
        logging.warning("Can't connect to ccnet daemon. Now quit")
        sys.exit(1)

    return receiver

def main():
    args = parse_args()
    init_logging(args)
    ev_receiver = create_receiver(args)
    Session = init_db_session_class(args.config_file)
    session = Session()

    logging.info("Starts to read message") 
    while True:
        try:
            msg = ev_receiver.get_message()
        except NoConnectionError:
            logging.warning("Connection to daemon is lost")
            do_reconnect(ev_receiver)
            continue
        except:
            logging.exception("error when read message")
            if not ev_receiver.is_connected():
                do_reconnect(ev_receiver)
            continue

        if not msg:
            logging.warning("failed to read message")
            continue

        handle_message(session, msg)

    do_exit(0)

if __name__ ==  '__main__':
    main()
