# coding: UTF-8

import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run, parse_bool, parse_interval, get_opt_from_conf_or_env
from seafevents.app.config import IS_PRO_VERSION

__all__ = [
    'ESWikiIndexUpdater',
]


class ESWikiIndexUpdater(object):
    def __init__(self, config):
        self._enabled = False

        self._seafesdir = None
        self._interval = None
        self._logfile = None
        self._loglevel = None
        self._es_host = None
        self._es_port = None

        self._parse_config(config)

    def _parse_config(self, config):
        """Parse index update related parts of events.conf"""
        section_name = 'INDEX FILES'
        key_enabled = 'enabled'
        key_seafesdir = 'seafesdir'
        key_logfile = 'wiki_logfile'
        key_loglevel = 'loglevel'
        key_index_interval = 'interval'
        key_es_host = 'es_host'
        key_es_port = 'es_port'

        default_index_interval = 30 * 60 # 30 min

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        is_pro_version = IS_PRO_VERSION
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

        # default index file is 'index_wiki.log' in SEAFEVENTS_LOG_DIR
        default_logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'index_wiki.log')
        logfile = get_opt_from_conf_or_env (config, section_name,
                                            key_logfile,
                                            'SEAFES_WIKI_LOGFILE',
                                            default=default_logfile)

        default_loglevel = 'warning'
        loglevel = get_opt_from_conf_or_env(config, section_name, key_loglevel, default=default_loglevel)

        # [ index interval ]
        interval = get_opt_from_conf_or_env(config, section_name, key_index_interval,
                                            default=default_index_interval)
        interval = parse_interval(interval, default_index_interval)

        # [ es host/port  ]
        es_host = None
        es_port = None
        if config.has_option(section_name, key_es_host) and config.has_option(section_name, key_es_port):
            host = config.get(section_name, key_es_host).lower()
            port = config.get(section_name, key_es_port).lower()
            try:
                port = int(port.lower())
            except ValueError:
                logging.warning('invalid es_port "%s"' % port)
            else:
                es_host = host
                es_port = port

        logging.debug('seafes dir: %s', seafesdir)
        logging.debug('seafes logfile: %s', logfile)
        logging.debug('seafes index interval: %s sec', interval)

        if es_host:
            logging.debug('elasticsearch host: %s', es_host)
            logging.debug('elasticsearch port: %s', es_port)

        self._seafesdir = seafesdir
        self._interval = interval
        self._logfile = os.path.abspath(logfile)
        self._loglevel = loglevel
        self._es_host = es_host
        self._es_port = es_port

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start wiki index updater: it is not enabled!')
            return

        logging.info('search wiki indexer is started, interval = %s sec', self._interval)
        WikiIndexUpdateTimer(
            self._interval, self._seafesdir,
            self._logfile, self._loglevel, self._es_host, self._es_port
        ).start()

    def is_enabled(self):
        return self._enabled


class WikiIndexUpdateTimer(Thread):

    def __init__(self, interval, seafesdir, logfile, loglevel, es_host, es_port):
        Thread.__init__(self)
        self._interval = interval
        self._seafesdir = seafesdir
        self._logfile = logfile
        self._loglevel = loglevel
        self._es_host = es_host
        self._es_port = es_port
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to index wiki files')
                try:
                    assert os.path.exists(self._seafesdir)
                    cmd = [
                        get_python_executable(),
                        '-m', 'seafes.indexes.wiki.index_wiki_local',
                        '--logfile', self._logfile,
                        '--loglevel', self._loglevel,
                        'update',
                    ]

                    env = dict(os.environ)
                    if self._es_host:
                        env['SEAFES_ES_HOST'] = self._es_host
                        env['SEAFES_ES_PORT'] = str(self._es_port)

                    run(cmd, cwd=self._seafesdir, env=env)
                except Exception as e:
                    logging.exception('error when index wiki files: %s', e)

    def cancel(self):
        self.finished.set()
