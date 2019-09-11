#!/usr/bin/env python
# coding: utf-8
import argparse
import configparser
import os

from seafevents import is_audit_enabled
from seafevents.db import create_db_tables
from seafevents.utils import write_pidfile, get_config
from seafevents.app.log import LogConfigurator
from seafevents.app.app import App
from seafevents.app.mq_handler import init_message_handlers


class AppArgParser(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='seafevents main program')

        self._add_args()

    def parse_args(self):
        return self._parser.parse_args()

    def _add_args(self):
        self._parser.add_argument(
            '--logfile',
            help='log file')

        self._parser.add_argument(
            '--config-file',
            default=os.path.join(os.getcwd(), 'events.conf'),
            help='seafevents config file')

        self._parser.add_argument(
            '--loglevel',
            default='info',
        )

        self._parser.add_argument(
            '-P',
            '--pidfile',
            help='the location of the pidfile'
        )

        self._parser.add_argument(
            '-R',
            '--reconnect',
            action='store_true',
            help='try to reconnect to daemon when disconnected'
        )


def is_cluster_enabled():
    cfg = configparser.ConfigParser()
    if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
        confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']
    else:
        confdir = os.environ['SEAFILE_CONF_DIR']
    conf = os.path.join(confdir, 'seafile.conf')
    cfg.read(conf)
    if cfg.has_option('cluster', 'enabled'):
        return cfg.getboolean('cluster', 'enabled')
    else:
        return False


def is_syslog_enabled(config):
    if config.has_option('Syslog', 'enabled'):
        try:
            return config.getboolean('Syslog', 'enabled')
        except ValueError:
            return False
    return False


def main(background_tasks_only=False):
    args = AppArgParser().parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile) # pylint: disable=W0612
    if args.logfile:
        logdir = os.path.dirname(os.path.realpath(args.logfile))
        os.environ['SEAFEVENTS_LOG_DIR'] = logdir

    os.environ['EVENTS_CONFIG_FILE'] = os.path.expanduser(args.config_file)

    if args.pidfile:
        write_pidfile(args.pidfile)

    create_db_tables()
    config = get_config(args.config_file)
    enable_audit = is_audit_enabled(config)
    init_message_handlers(enable_audit)

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()

    events_handler_enabled = True
    background_tasks_enabled = True

    if background_tasks_only:
        events_handler_enabled = False
        background_tasks_enabled = True
    elif is_cluster_enabled():
        events_handler_enabled = True
        background_tasks_enabled = False

    app = App(args, events_handler_enabled=events_handler_enabled,
              background_tasks_enabled=background_tasks_enabled)

    app.serve_forever()


def run_background_tasks():
    main(background_tasks_only=True)


if __name__ == '__main__':
    main()
