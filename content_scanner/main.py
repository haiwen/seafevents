# coding: utf-8
# argparse 是 Python 的一个内置库（不是第三方库），用于解析命令行参数
# 你可以定义命令行参数、选项和子命令，并且可以自动生成帮助信息和使用文档
import argparse
import os

from seafevents.app.config import get_config
from .content_scan import ContentScan

# AppArgParser 用于解析内容扫描程序的命令行参数
class AppArgParser(object):

    # 初始化AppArgParser对象，创建一个argparse.ArgumentParser实例，并添加参数到其中(内容扫描程序)
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='content-scan program')

        self._add_args()

    # 解析命令行参数，并返回解析后的参数
    def parse_args(self):
        return self._parser.parse_args()

    # 向 argparse.ArgumentParser 实例添加特定的参数。
    def _add_args(self):
        # 它添加了--logfile、--config-file和--loglevel参数。
        # --logfile参数指定日志文件
        # --config-file参数指定内容扫描配置文件的路径
        # --loglevel参数指定日志级别（默认为“info”）
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
    # 转换命令行参数
    args = AppArgParser().parse_args()
    # 获取配置
    config = get_config(args.config_file)
    # 获取 seafile 配置
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    seafile_config = get_config(seafile_conf_path)

    # 启动内容扫描
    content_scanner = ContentScan(config, seafile_config)
    content_scanner.start()


if __name__ == '__main__':
    main()
