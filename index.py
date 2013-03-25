#coding: UTF-8

import os
import sys
import time
import subprocess
import logging

def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False):
    '''Run a program and wait it to finish, and return its exit code. The
    standard output of this program is supressed.

    '''
    with open(os.devnull, 'w') as devnull:
        if suppress_stdout:
            stdout = devnull
        else:
            stdout = sys.stdout

        if suppress_stderr:
            stderr = devnull
        else:
            stderr = sys.stderr

        subprocess.Popen(argv,
                         cwd=cwd,
                         stdout=stdout,
                         stderr=stderr,
                         env=env)

def update_file_index(seafesdir):
    '''Invoking the update_repos.py, log to ./index.log'''
    assert os.path.exists(seafesdir)
    script_name = 'update_repos.py'
    script_path = os.path.join(seafesdir, script_name)
    loglevel = 'debug'
    logfile = os.path.join(os.path.dirname(__file__), 'index.log')
    # python update_repos.py --logfile ./index.log --loglevel debug update
    cmd = [
        sys.executable, script_path,
        '--logfile', logfile,
        '--loglevel', loglevel,
        'update',
    ]
    run(cmd, cwd=seafesdir)

def index_files(conf):
    logging.info('periodic file indexer is started, interval = %s sec, seafesdir = %s',
                 conf['interval'], conf['seafesdir'])
    if not conf:
        return
    interval = conf['interval']
    seafesdir = conf['seafesdir']
    while True:
        time.sleep(interval)
        logging.info('starts to index files')
        try:
            update_file_index(seafesdir)
        except Exception, e:
            logging.exception('error when index files: %s' % e)
