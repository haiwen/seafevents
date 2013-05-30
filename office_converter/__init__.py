import os
import sys

from .task_manager import task_manager
from .rpc import OfficeConverterRpcClient, OFFICE_RPC_SERVICE_NAME

__all__ = [
    'office_converter',
    'OfficeConverterRpcClient',
]

class OfficeConverter(object):
    def __init__(self):
        pass

    def add_task(self, file_id, doctype, url):
        supported_doctypes = ('pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx')

        if doctype not in supported_doctypes:
            raise Exception('doctype "%s" is not supported' % doctype)

        if len(file_id) != 40:
            raise Exception('invalid file id')

        return task_manager.add_task(file_id, doctype, url)

    def query_convert_status(self, file_id):
        if len(file_id) != 40:
            raise Exception('invalid file id')

        return task_manager.query_task_status(file_id)

    def query_file_pages(self, file_id):
        if len(file_id) != 40:
            raise Exception('invalid file id')

        return task_manager.query_file_pages(file_id)

    def start(self, conf):
        self.conf = conf

        num_workers = conf['workers']
        pdf_dir = os.path.join(conf['outputdir'], 'pdf')
        html_dir = os.path.join(conf['outputdir'], 'html')

        task_manager.init(num_workers=num_workers, pdf_dir=pdf_dir, html_dir=html_dir)
        task_manager.run()

    def stop(self):
        task_manager.stop()

office_converter = OfficeConverter()
