# -*- coding: utf-8 -*-
import os
import logging
import argparse

from seafevents.db import create_db_tables, prepare_db_tables
from seafevents.utils import write_pidfile
from seafevents.app.log import LogConfigurator
from seafevents.app.app import App
from seafevents.app.config import get_config, is_cluster_enabled, is_syslog_enabled


def main(background_tasks_only=False):
    parser = argparse.ArgumentParser(description='seafevents main program')
    parser.add_argument('--config-file', default=os.path.join(os.getcwd(), 'events.conf'), help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('-P', '--pidfile', help='the location of the pidfile')
    args = parser.parse_args()

    if args.logfile:
        logdir = os.path.dirname(os.path.realpath(args.logfile))
        os.environ['SEAFEVENTS_LOG_DIR'] = logdir

    if args.pidfile:
        write_pidfile(args.pidfile)

    ccnet_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('CCNET_CONF_DIR')
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    if not ccnet_conf_dir or not seafile_conf_dir:
        logging.error('Environment variable ccnet_conf_dir or seafile_conf_dir is not define')
        raise RuntimeError('Environment variable ccnet_conf_dir and seafile_conf_dir is not define')

    os.environ['EVENTS_CONFIG_FILE'] = os.path.expanduser(args.config_file)
    ccnet_conf_path = os.path.join(ccnet_conf_dir, 'ccnet.conf')
    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    os.environ['DJANGO_SETTINGS_MODULE'] = 'seahub.settings'  # set env for repo monitor cache

    ccnet_config = get_config(ccnet_conf_path)
    seafile_config = get_config(seafile_conf_path)
    config = get_config(args.config_file)
    try:
        create_db_tables(config)
        prepare_db_tables(seafile_config)
    except Exception as e:
        logging.error('Failed create tables, error: %s' % e)
        raise RuntimeError('Failed create tables, error: %s' % e)

    app_logger = LogConfigurator(args.loglevel, args.logfile)
    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()

    foreground_tasks_enabled = True
    background_tasks_enabled = True

    if background_tasks_only:
        foreground_tasks_enabled = False
        background_tasks_enabled = True
    elif is_cluster_enabled(seafile_config):
        foreground_tasks_enabled = True
        background_tasks_enabled = False

    from gevent import monkey; monkey.patch_all()

    app = App(config, ccnet_config, seafile_config, foreground_tasks_enabled=foreground_tasks_enabled,
              background_tasks_enabled=background_tasks_enabled)

    app.serve_forever()


def run_background_tasks():
    main(background_tasks_only=True)


if __name__ == '__main__':

    main()
