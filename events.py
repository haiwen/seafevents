#!/usr/bin/env python
#coding: utf-8

import argparse
import os
import sys
import logging

from message import MessageReceiver
from db import init_db_session, RepoUpdateEvent

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
        default='info'
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

def main():
    args = parse_args()
    ev_receiver = MessageReceiver(args.ccnet_conf_dir, "seaf_server.event")
    session = init_db_session(args)

    init_logging(args)

    logging.info("Starts to read message") 
    while True:
        msg = ev_receiver.get_message()
        if not msg:
            break

        elements = msg.body.split()
        if len(elements) != 3:
            logging.warning("bad message: %s", elements)
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
