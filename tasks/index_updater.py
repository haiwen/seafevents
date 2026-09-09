# coding: UTF-8

import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run, parse_bool, parse_interval, get_opt_from_conf_or_env
from seafevents.app.config import ENABLE_SEARCH, IS_PRO_VERSION, SEARCH_ENGINE

__all__ = [
    'IndexUpdater',
]


class IndexUpdater(object):
    def __init__(self, config):
        self._enabled = False

        self._seafesdir = None
        self._interval = None
        self._enable_full_text_search = None
        self._logfile = None
        self._loglevel = None
        self._es_host = None
        self._es_port = None

        self._timer = None

        self._parse_config(config)

    def _parse_config(self, config):
        """Parse index update related parts of events.conf"""
        section_name = 'INDEX FILES'
        key_seafesdir = 'seafesdir'
        key_logfile = 'logfile'
        key_loglevel = 'loglevel'
        key_index_interval = 'interval'
        key_enable_full_text_search = 'enable_full_text_search'
        key_es_host = 'es_host'
        key_es_port = 'es_port'

        default_index_interval = 30 * 60 # 30 min

        if not config.has_section(section_name):
            return

        is_pro_version = IS_PRO_VERSION
        enabled = ENABLE_SEARCH and SEARCH_ENGINE == 'elasticsearch'
        logging.debug('seafes enabled: %s', enabled)

        if not enabled or not is_pro_version:
            return

        self._enabled = True

        # [ seafesdir ]
        seafesdir = get_opt_from_conf_or_env(config, section_name, key_seafesdir, 'SEAFES_DIR', None)
        if not seafesdir:
            logging.critical('seafesdir is not set')
            raise RuntimeError('seafesdir is not set')
        if not os.path.exists(seafesdir):
            logging.critical('seafesdir %s does not exist' % seafesdir)
            raise RuntimeError('seafesdir is not set')

        # [ index logfile ]

        # default index file is 'index.log' in SEAFEVENTS_LOG_DIR
        default_logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'index.log')
        logfile = get_opt_from_conf_or_env (config, section_name,
                                            key_logfile,
                                            'SEAFES_LOGFILE',
                                            default=default_logfile)

        default_loglevel = 'warning'
        loglevel = get_opt_from_conf_or_env(config, section_name, key_loglevel, default=default_loglevel)

        # [ index interval ]
        interval = get_opt_from_conf_or_env(config, section_name, key_index_interval,
                                            default=default_index_interval)
        interval = parse_interval(interval, default_index_interval)

        # [ index office/pdf files  ]
        enable_full_text_search = parse_bool(
            get_opt_from_conf_or_env(
                config, section_name, key_enable_full_text_search, 'ENABLE_FULL_TEXT_SEARCH', default=True
            )
        )

        # [ es host/port ]
        es_host = get_opt_from_conf_or_env(config, section_name, key_es_host, 'ELASTICSEARCH_HOST')
        es_port = get_opt_from_conf_or_env(config, section_name, key_es_port, 'ELASTICSEARCH_PORT')
        if es_port:
            try:
                es_port = int(es_port)
            except ValueError:
                logging.warning('invalid es_port "%s"' % es_port)
                es_host = None
                es_port = None

        logging.debug('seafes dir: %s', seafesdir)
        logging.debug('seafes logfile: %s', logfile)
        logging.debug('seafes index interval: %s sec', interval)
        logging.debug('seafes full-text searching: %s', enable_full_text_search)

        if es_host:
            logging.debug('elasticsearch host: %s', es_host)
            logging.debug('elasticsearch port: %s', es_port)

        self._seafesdir = seafesdir
        self._interval = interval
        self._enable_full_text_search = enable_full_text_search
        self._logfile = os.path.abspath(logfile)
        self._loglevel = loglevel
        self._es_host = es_host
        self._es_port = es_port

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start index updater: it is not enabled!')
            return

        logging.info('search indexer is started, interval = %s sec', self._interval)
        IndexUpdateTimer(
            self._interval, self._seafesdir, self._enable_full_text_search,
            self._logfile, self._loglevel, self._es_host, self._es_port
        ).start()

    def is_enabled(self):
        return self._enabled


class IndexUpdateTimer(Thread):

    def __init__(self, interval, seafesdir, enable_full_text_search, logfile, loglevel, es_host, es_port):
        Thread.__init__(self)
        self._interval = interval
        self._seafesdir = seafesdir
        self._enable_full_text_search = enable_full_text_search
        self._logfile = logfile
        self._loglevel = loglevel
        self._es_host = es_host
        self._es_port = es_port
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to index files')
                try:
                    assert os.path.exists(self._seafesdir)
                    cmd = [
                        get_python_executable(),
                        '-m', 'seafes.indexes.repo_file.index_local',
                        '--logfile', self._logfile,
                        '--loglevel', self._loglevel,
                        'update',
                    ]

                    env = dict(os.environ)
                    run(cmd, cwd=self._seafesdir, env=env)
                except Exception as e:
                    logging.exception('error when index files: %s', e)

    def cancel(self):
        self.finished.set()
