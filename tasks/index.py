#coding: UTF-8

import os
import logging

from .utils import get_python_executable, run

def update_file_index(seafesdir, index_office_pdf, logfile):
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

    def get_env():
        env = dict(os.environ)
        if index_office_pdf:
            env['SEAFES_INDEX_OFFICE_PDF'] = 'true'

        return env

    env = get_env()
    run(cmd, cwd=seafesdir, env=env)

def index_files(conf):
    seafesdir = conf['seafesdir']
    logfile = conf['logfile']
    index_office_pdf = conf['index_office_pdf']
    logging.info('starts to index files')
    try:
        update_file_index(seafesdir, index_office_pdf, logfile)
    except Exception:
        logging.exception('error when index files:')
