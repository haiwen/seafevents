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
    content_scanner = ContentScan(config)
    content_scanner.start()


if __name__ == '__main__':
    main()
