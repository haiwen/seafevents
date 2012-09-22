#!/usr/bin/env python
#coding: utf-8

import argparse
import os
import sys
import logging
import time

import message
from db import init_db_session, RepoUpdateEvent

__all__ = []

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
        default='debug'
    )

    return parser.parse_args()

def init_logging(args):
    level = args.loglevel
    
    if level == 'debug':
        level = logging.DEBUG
    elif level == 'info':
        level = logging.INFO
    else:
        level = logging.WARNING
        
    logging.basicConfig(
        format= '[%(asctime)s] %(message)s',
        datefmt= '%m/%d/%Y %H:%M:%S',
        level= level,
        stream= args.logfile
    )
    
def do_exit(retcode):
    logging.info("Quit")
    sys.exit(retcode)

def do_reconnect(receiver):
    while True:
        try:
            logging.info("%s: try to reconnect to daemon", receiver)
            receiver.reconnect()
        except message.NoConnectionError:
            logging.info("%s: failed to reconnect to daemon", receiver)
            time.sleep(2)
        else:
            logging.info("%s: Reconnected to daemon", receiver)
            break

def create_receiver(args):
    try:
        receiver = message.MessageReceiver(args.ccnet_conf_dir, "seaf_server.event")
    except message.NoConnectionError:
        logging.warning("Can't connect to ccnet daemon. Now quit")
        sys.exit(1)

    return receiver
        

def main():
    args = parse_args()
    init_logging(args)
    ev_receiver = create_receiver(args)
    session = init_db_session(args.config_file)

    logging.info("Starts to read message") 
    while True:
        try:
            msg = ev_receiver.get_message()
        except message.NoConnectionError:
            logging.warning("Connection to daemon is lost")
            do_reconnect(ev_receiver)
            continue

        if not msg:
            logging.warning("failed to read message")
            continue

        elements = msg.body.split('\t')
        if len(elements) != 3:
            logging.warning("got bad message: %s", elements)
            continue

        repo_id = elements[1]
        commit_id = elements[2]

        event = RepoUpdateEvent(repo_id, commit_id, msg.ctime)

        logging.debug("get an event: %s", event)

        session.add(event)
        session.commit()

    do_exit(0)

if __name__ ==  '__main__':
    main()
