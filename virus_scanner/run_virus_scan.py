#!/usr/bin/env python
#coding: utf-8

import os
import sys
import logging
import argparse
from ConfigParser import ConfigParser
from scan_settings import Settings
from virus_scan import VirusScan

if __name__ == "__main__":
    kw = {
        'format': '[%(asctime)s] [%(levelname)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': logging.DEBUG,
        'stream': sys.stdout
    }
    logging.basicConfig(**kw)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file',
                        default=os.path.join(os.path.abspath('..'), 'events.conf'),
                        help='seafevents config file')
    args = parser.parse_args()

    setting = Settings(args.config_file)
    if setting.is_enabled():
        VirusScan(setting).start()
    else:
        logging.info('Virus scan is disabled.')
