import os
import sys
import re
import logging
import tempfile
import ConfigParser
from pysearpc import searpc_server
from ccnet.async import RpcServerProc

from .task_manager import task_manager
from .rpc import OfficeConverterRpcClient, OFFICE_RPC_SERVICE_NAME
from .doctypes import DOC_TYPES, PPT_TYPES, EXCEL_TYPES
from seafevents.utils import has_office_tools
from seafevents.utils.config import parse_max_size, parse_max_pages, parse_workers, parse_bool

__all__ = [
    'office_converter',
    'OfficeConverterRpcClient',
]

FILE_ID_PATTERN = re.compile(r'^[0-9a-f]{40}$')
def _valid_file_id(file_id):
    if not isinstance(file_id, basestring):
        return False
    return FILE_ID_PATTERN.match(str(file_id)) is not None

class OfficeConverter(object):
    supported_doctypes = DOC_TYPES + PPT_TYPES + EXCEL_TYPES + ('pdf', )

    def __init__(self, conf):
        self._enabled = conf['enabled']

        if self._enabled:
            self._outputdir = conf['outputdir']
            self._num_workers = conf['workers']
            self._max_size = conf['max_size']
            self._max_pages = conf['max_pages']

    def add_task(self, file_id, doctype, url):
        if doctype not in self.supported_doctypes:
            raise Exception('doctype "%s" is not supported' % doctype)

        if not _valid_file_id(file_id):
            raise Exception('invalid file id')

        return task_manager.add_task(file_id, doctype, url)

    def query_convert_status(self, file_id, page):
        if not _valid_file_id(file_id):
            raise Exception('invalid file id')

        return task_manager.query_task_status(file_id, page)

    def register_rpc(self, ccnet_client):
        '''Register office rpc service'''
        searpc_server.create_service(OFFICE_RPC_SERVICE_NAME)
        ccnet_client.register_service(OFFICE_RPC_SERVICE_NAME,
                                      'basic',
                                      RpcServerProc)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        self.query_convert_status)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        self.add_task)

    def start(self):
        pdf_dir = os.path.join(self._outputdir, 'pdf')
        html_dir = os.path.join(self._outputdir, 'html')
        task_manager.init(num_workers=self._num_workers,
                          pdf_dir=pdf_dir,
                          html_dir=html_dir,
                          max_pages=self._max_pages)
        task_manager.run()

        logging.info('office converter started')

    def stop(self):
        task_manager.stop()

    def is_enabled(self):
        if not has_office_tools():
            return False
        return self._enabled
