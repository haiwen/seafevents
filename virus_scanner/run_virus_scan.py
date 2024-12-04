# coding: utf-8

import os
import sys
import logging
import argparse

from seafevents.db import prepare_db_tables
from seafevents.virus_scanner.scan_settings import Settings
from seafevents.virus_scanner.virus_scan import VirusScan

from seafevents.app.config import get_config

if __name__ == "__main__":
    kw = {
        'format': '[seafevents] [%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(funcName)s %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S',
        'level': logging.INFO,
        'stream': sys.stdout
    }
    logging.basicConfig(**kw)

    from seafevents.virus_scanner.scan_settings import logger
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file',
                        default=os.path.join(os.path.abspath('..'), 'events.conf'),
                        help='seafevents config file')
    args = parser.parse_args()

    config = get_config(args.config_file)
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    seafile_config = get_config(seafile_conf_path)

    setting = Settings(config, seafile_config)
    if setting.is_enabled():
        prepare_db_tables(seafile_config)
        VirusScan(setting).start()
    else:
        logger.info('Virus scan is disabled.')
