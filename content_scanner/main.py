# coding: utf-8
import argparse
import os

from seafevents.app.config import get_config
from .content_scan import ContentScan


class AppArgParser(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='content-scan program')

        self._add_args()

    def parse_args(self):
        return self._parser.parse_args()

    def _add_args(self):
        self._parser.add_argument(
            '--logfile',
            help='log file')

        self._parser.add_argument(
            '--config-file',
            default=os.path.join(os.getcwd(), 'seafevents.conf'),
            help='content scan config file')

        self._parser.add_argument(
            '--loglevel',
            default='info',
        )


def main():
    args = AppArgParser().parse_args()
    config = get_config(args.config_file)
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    seafile_config = get_config(seafile_conf_path)

    content_scanner = ContentScan(config, seafile_config)
    content_scanner.start()


if __name__ == '__main__':
    main()
