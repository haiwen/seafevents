#!/usr/bin/env python
#coding: utf-8

import gevent
from gevent import monkey
monkey.patch_all()

import argparse
import ConfigParser
import os
import sys
import time
import atexit
import signal
import logging

import ccnet

from db import init_db_session_class
from handler import handle_message
from index import index_files

def parse_args():
    parser = argparse.ArgumentParser(
         description='seafile events recorder')

    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='log file')

    parser.add_argument(
        '--config-file',
        default=os.path.join(os.getcwd(), 'events.conf'),
        help='ccnet server config directory')

    parser.add_argument(
        '--loglevel',
        default='debug',
    )

    parser.add_argument(
        '-P',
        '--pidfile',
        help='the location of the pidfile'
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

def do_exit(code):
    logging.info('exit with code %s', code)
    sys.exit(code)

def parse_interval(interval):
    unit = 1
    if interval.endswith('s'):
        pass
    elif interval.endswith('m'):
        unit *= 60
    elif interval.endswith('h'):
        unit *= 60 * 60
    elif interval.endswith('d'):
        unit *= 60 * 60 * 24
    else:
        logging.critical('invalid index interval "%s"' % interval)
        do_exit(1)

    return int(interval.rstrip('smhd')) * unit

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

def sigint_handler(*args):
    dummy = args
    do_exit(0)

def sigchild_handler(*args):
    dummy = args
    try:
        os.wait3(os.WNOHANG)
    except:
        logging.exception('Error in sigchild_handler:')

def start_mq_client(ccnet_session, dbsession):
    def msg_cb(msg):
        handle_message(dbsession, msg)

    mq = 'seaf_server.event'
    mqclient = ccnet_session.create_master_processor('mq-client')
    mqclient.set_callback(msg_cb)
    mqclient.start(mq)
    logging.info('listen to mq: %s', mq)

def get_seafes_conf(config):
    '''Parse search related options from events.conf'''
    section_name = 'INDEX FILES'
    key_seafesdir = 'seafesdir'
    key_index_logfile = 'logfile'
    key_index_interval = 'interval'
    key_index_office_pdf = 'index_office_pdf'

    d = {}
    if not config.has_section(section_name):
        return d

    def get_option_from_conf_or_env (key, env_key, default=None):
        '''Get option value from events.conf. If not specified in events.conf,
        check the environment variable.

        '''
        try:
            value = config.get(section_name, key)
        except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
            value = os.environ.get(env_key.upper(), default)

        return value

    # [ seafesdir ]
    seafesdir = get_option_from_conf_or_env(key_seafesdir, 'SEAFES_DIR', None)
    if not seafesdir:
        raise RuntimeError('seafesdir is not set')
    if not os.path.exists(seafesdir):
        logging.critical('seafesdir %s does not exist' % seafesdir)
        do_exit(1)

    # [ index logfile ]

    # default index file is 'index.log' in the seafes dir
    default_index_logfile = os.path.join(seafesdir, 'index.log')
    index_logfile = get_option_from_conf_or_env (key_index_logfile,
                                                 'SEAFES_INDEX_LOGFILE',
                                                 default=default_index_logfile)

    # [ index interval ]
    interval = config.get(section_name, key_index_interval).lower()
    val = parse_interval(interval)
    if val < 0:
        logging.critical('invalid index interval %s' % val)
        do_exit(1)
    elif val < 60:
        logging.warning('index interval too short')

    # [ index office/pdf files  ]
    index_office_pdf = False
    try:
        index_office_pdf = config.get(section_name, key_index_office_pdf)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        pass
    else:
        index_office_pdf = index_office_pdf.lower()
        if index_office_pdf == 'true' or index_office_pdf == '1':
            index_office_pdf = True

    logging.info('seafes dir: %s', seafesdir)
    logging.info('seafes logfile: %s', index_logfile)
    logging.info('seafes index interval: %s', interval)
    logging.info('seafes index office/pdf: %s', index_office_pdf)

    d['interval'] = val
    d['seafesdir'] = seafesdir
    d['index_office_pdf'] = index_office_pdf
    d['logfile'] = index_logfile

    return d

def get_config(config_file):
    config = ConfigParser.ConfigParser()
    try:
        config.read(config_file)
    except Exception, e:
        logging.critical('failed to read config file %s', e)
        do_exit(1)

    return config

def start_ccnet_session(ccnet_dir, dbsession):
    ccnet_session = ccnet.AsyncClient(ccnet_dir)
    connect_interval = 2
    while True:
        logging.info('connecting to ccnet server')
        try:
            ccnet_session.connect_daemon()
            break
        except ccnet.NetworkError:
            time.sleep(connect_interval)

    logging.info('connected to ccnet server')

    start_mq_client(ccnet_session, dbsession)
    return ccnet_session

def get_db_session(config):
    DBSessionClass = init_db_session_class(config)
    dbsession = DBSessionClass()
    logging.info('connected to database')
    return dbsession

def get_ccnet_dir():
    try:
        return os.environ['CCNET_CONF_DIR']
    except KeyError:
        raise RuntimeError('ccnet config dir is not set')

def main():
    args = parse_args()
    init_logging(args)
    config = get_config(args.config_file)
    seafes_conf = get_seafes_conf(config)
    dbsession = get_db_session(args.config_file)

    ccnet_dir = get_ccnet_dir()

    gevent.signal(signal.SIGINT, sigint_handler)
    gevent.signal(signal.SIGCHLD, sigchild_handler)

    if args.pidfile:
        write_pidfile(args.pidfile)

    if seafes_conf:
        gevent.spawn(index_files, seafes_conf)

    ccnet_session = start_ccnet_session(ccnet_dir, dbsession)
    while True:
        try:
            ccnet_session.main_loop()
        except ccnet.NetworkError:
            # auto reconnect
            logging.warning('connection to ccnet server is lost')
            ccnet_session = start_ccnet_session(ccnet_dir, dbsession)
        except Exception, e:
            logging.exception(str(e))
            do_exit(0)

if __name__ ==  '__main__':
    main()
