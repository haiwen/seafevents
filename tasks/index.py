#coding: UTF-8

import os
import logging
import gevent

from .utils import get_python_executable, run

def update_file_index(seafesdir, logfile):
    '''Invoking the update_repos.py, log to ./index.log'''
    assert os.path.exists(seafesdir)
    script_name = 'update_repos.py'
    script_path = os.path.join(seafesdir, script_name)
    loglevel = 'debug'
    # python update_repos.py --logfile ./index.log --loglevel debug update
    cmd = [
        get_python_executable(),
        script_path,
        '--logfile', logfile,
        '--loglevel', loglevel,
        'update',
    ]
    run(cmd, cwd=seafesdir)

def index_files(conf):
    logging.info('periodic file indexer is started, interval = %s sec, seafesdir = %s',
                 conf['interval'], conf['seafesdir'])
    interval = conf['interval']
    seafesdir = conf['seafesdir']
    logfile = conf['logfile']
    while True:
        gevent.sleep(interval)
        logging.info('starts to index files')
        try:
            update_file_index(seafesdir, logfile)
        except Exception:
            logging.exception('error when index files:')
