#!/usr/bin/env python
#coding: utf-8

import argparse
import configparser
import os
import logging
from seafevents.app.log import LogConfigurator
from .config import appconfig, load_config
from .models import ContentScanResult, ContentScanRecord
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
    app_logger = LogConfigurator(args.loglevel, args.logfile)
    try:
        load_config(args.config_file)
    except Exception as e:
        logging.error('Error loading content-scan config: %s' % e)
        raise RuntimeError("Error loading content-scan config: %s" % e)

    content_scanner = ContentScan()
    content_scanner.start()

if __name__ == '__main__':
    main()
