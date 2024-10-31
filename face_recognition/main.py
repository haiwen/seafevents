# coding: utf-8
import argparse
import os

from seafevents.app.config import get_config
from seafevents.face_recognition.face_cluster_updater import RepoFaceClusterUpdater
from seafevents.app.log import LogConfigurator


class AppArgParser(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='face cluster program')

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
            help='face cluster config file')

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
    LogConfigurator(args.loglevel, args.logfile)

    face_cluster_updater = RepoFaceClusterUpdater(config, seafile_config)
    face_cluster_updater.start()


if __name__ == '__main__':
    main()
