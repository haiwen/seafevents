#coding: utf-8

import os
import Queue
import tempfile
import threading
import urllib2
import logging
import atexit
import json

from .convert import Convertor, ConvertorFatalError
from .doctypes import EXCEL_TYPES

__all__ = ["task_manager"]

def _checkdir_with_mkdir(dname):
    if os.path.exists(dname):
        if not os.path.isdir(dname):
            raise RuntimeError("%s exists, but not a directory" % dname)

        if not os.access(dname, os.R_OK | os.W_OK):
            raise RuntimeError("Access to %s denied" % dname)
    else:
        os.makedirs(dname)


class ConvertTask(object):
    """A convert task is the representation of a convert request. A task is in
    one of these status:

    - QUEUED:  waiting to be converted
    - PROCESSING: being fetched or converted
    - DONE: succefully converted to pdf
    - ERROR: error in fetching or converting

    """
    def __init__(self, file_id, doctype, url, pdf_dir, html_dir):
        self.url = url
        self.doctype = doctype
        self.file_id = file_id

        self._status = 'QUEUED'
        self.error = None

        # fetched office document
        self.document = None
        # pdf output
        self.pdf = os.path.join(pdf_dir, file_id)
        # html output, each page of the document is converted to a separate
        # html file, and displayed in iframes on seahub
        self.htmldir = os.path.join(html_dir, file_id)

        self.pdf_info = {}
        self.last_processed_page = 0

    def __str__(self):
        return "<type: %s, id: %s>" % (self.doctype, self.file_id)

    def page_status(self, page_number):
        if self.status == 'ERROR':
            return 'ERROR'
        else:
            return 'DONE' if page_number <= self.last_processed_page \
                else 'PROCESSING'

    def get_status(self):
        return self._status

    def set_status(self, status):
        assert status in ('QUEUED', 'PROCESSING', 'DONE', 'ERROR')

        # Remove temporary file when done or error
        if status == 'ERROR' or status == 'DONE':
            fn = ''
            if self.doctype == 'pdf' and self.pdf:
                fn = self.pdf
            elif self.document:
                fn = self.document

            if fn and os.path.exists(fn):
                logging.debug("removing temporary document %s", fn)
                try:
                    os.remove(fn)
                except OSError, e:
                    logging.warning('failed to remove temporary document %s: %s', fn, e)

        self._status = status

    status = property(get_status, set_status, None, "status of this task")


class Worker(threading.Thread):
    """Worker thread for task manager. A worker thread has a dedicated
    libreoffice connected to it.

    """
    should_exit = False

    def __init__(self, tasks_queue, index, **kwargs):
        threading.Thread.__init__(self, **kwargs)

        self._tasks_queue = tasks_queue

        self._index = index

    def _convert_to_pdf(self, task):
        """Use libreoffice API to convert document to pdf"""
        convertor = task_manager.convertor

        logging.debug('start to convert task %s', task)

        success = False
        _checkdir_with_mkdir(os.path.dirname(task.pdf))
        try:
            success = convertor.convert_to_pdf(task.document, task.pdf)
        except ConvertorFatalError:
            return False

        if success:
            logging.debug("succefully converted %s to pdf", task)
        else:
            logging.warning("failed to convert %s to pdf", task)

        return success

    def _convert_pdf_to_html(self, task):
        """Use pdf2htmlEX to convert pdf to html"""
        _checkdir_with_mkdir(task.htmldir)
        def progress_callback(page, pdf_info):
            task.last_processed_page = page
            task.pdf_info = pdf_info
        try:
            task_manager.convertor.pdf_to_html2(
                task.pdf, task.htmldir, task_manager.max_pages, progress_callback)
        except Exception:
            logging.exception('failed to convert %s to html', task)
            task.status = 'ERROR'
            task.error = 'failed to convert document'
        else:
            logging.debug('successfully convert %s to html', task)
            task.status = 'DONE'

    def _convert_excel_to_html(self, task):
        '''Use libreoffice to convert excel to html'''
        _checkdir_with_mkdir(task.htmldir)
        if not task_manager.convertor.excel_to_html(
                task.document, os.path.join(task.htmldir, 'index.html')):
            logging.warning('failed to convert %s from excel to html', task)
            task.status = 'ERROR'
            task.error = 'failed to convert excel to html'
            return False
        else:
            logging.debug('successfully convert excel %s to html', task)
            task.status = 'DONE'
            return True

    def write_content_to_tmp(self, task):
        '''write the document/pdf content to a temporary file'''
        content = task.content
        try:
            suffix = "." + task.doctype
            fd, tmpfile = tempfile.mkstemp(suffix=suffix)
            os.close(fd)

            with open(tmpfile, 'wb') as fp:
                fp.write(content)
        except Exception, e:
            logging.warning('failed to write fetched document for task %s: %s', task, str(e))
            task.status = 'ERROR'
            task.error = 'failed to write fetched document to temporary file'
            return False
        else:
            if task.doctype == 'pdf':
                task.pdf = tmpfile
            else:
                task.document = tmpfile
            return True

    def _fetch_document_or_pdf(self, task):
        """Fetch the document or pdf of a convert task from its url, and write it to
        a temporary file.

        """
        logging.debug('start to fetch task %s', task)
        try:
            file_response = urllib2.urlopen(task.url)
            content = file_response.read()
        except Exception as e:
            logging.warning('failed to fetch document of task %s (%s): %s', task, task.url, e)
            task.status = 'ERROR'
            task.error = 'failed to fetch document'
            return False
        else:
            task.content = content
            return True

    def _handle_task(self, task):
        """
                    libreoffice
        Excel  ===============>  html
                         libreoffice           pdf2htmlEX
        Document file  ===============>  pdf ==============> html

                pdf2htmlEX
        PDF   ==============> html
        """
        task.status = 'PROCESSING'

        success = self._fetch_document_or_pdf(task)
        if not success:
            return

        success = self.write_content_to_tmp(task)
        if not success:
            return

        if task.doctype in EXCEL_TYPES:
            success = self._convert_excel_to_html(task)
            if not success:
                task.status = 'ERROR'
                task.error = 'failed to convert excel to document'
            return

        if task.doctype != 'pdf':
            success = self._convert_to_pdf(task)
            if not success:
                task.status = 'ERROR'
                task.error = 'failed to convert document'
                return

        self._convert_pdf_to_html(task)

    def run(self):
        """Repeatedly get task from tasks queue and process it."""
        while True:
            try:
                task = self._tasks_queue.get(timeout=1)
            except Queue.Empty:
                continue

            self._handle_task(task)


class TaskManager(object):
    """Task manager schedules the processing of convert tasks. A task comes
    from a http convert request, which contains a url of the location of the
    document to convert. The handling of a task consists of these steps:

    - fetch the document
    - write the fetched content to a temporary file
    - convert the document

    After the document is successfully convertd, the path of the output main html file
    would be "<html-dir>/<file_id>/index.html". For example, if the html dir is /var/html/, and
    the file_id of the document is 'aaa-bbb-ccc', the final pdf would be
    /var/html/aaa-bbb-ccc/index.html

    """
    def __init__(self):
        # (file id, task) map
        self._tasks_map = {}
        self._tasks_map_lock = threading.Lock()

        # tasks queue
        self._tasks_queue = Queue.Queue()
        self._workers = []

        # Things to be initialized in self.init()
        self.pdf_dir = None
        self.html_dir = None
        self.max_pages = 50

        self._num_workers = 2

        self.convertor = Convertor()

    def init(self, num_workers=2, max_pages=50, pdf_dir='/tmp/seafile-pdf-dir', html_dir='/tmp/seafile-html-dir'):
        self._set_pdf_dir(pdf_dir)
        self._set_html_dir(html_dir)

        self._num_workers = num_workers
        self.max_pages = max_pages

    def _set_pdf_dir(self, pdf_dir):
        """Init the directory to store converted pdf"""
        _checkdir_with_mkdir(pdf_dir)
        self.pdf_dir = pdf_dir

    def _set_html_dir(self, html_dir):
        _checkdir_with_mkdir(html_dir)
        self.html_dir = html_dir

    def _task_file_exists(self, file_id, doctype=None):
        '''Test whether the file has already been converted'''
        file_html_dir = os.path.join(self.html_dir, file_id)
        if doctype not in EXCEL_TYPES:
            done_file = os.path.join(file_html_dir, 'done')
        else:
            done_file = os.path.join(file_html_dir, 'index.html')

        return os.path.exists(done_file)

    def add_task(self, file_id, doctype, url):
        """Create a convert task and dipatch it to worker threads"""
        with self._tasks_map_lock:
            if file_id in self._tasks_map:
                task = self._tasks_map[file_id]
                if task.status != 'ERROR' and task.status != 'DONE':
                    # If there is already a convert task in progress, don't create a
                    # new one.
                    return

            if not self._task_file_exists(file_id, doctype):
                task = ConvertTask(file_id, doctype, url, self.pdf_dir, self.html_dir)
                self._tasks_map[file_id] = task
                self._tasks_queue.put(task)

    def _get_pdf_info(self, file_id):
        info_file = os.path.join(self.html_dir, file_id, 'info.json')
        if not os.path.exists(info_file):
            return None

        with open(info_file, 'r') as fp:
            return json.load(fp)

    def query_task_status_excel(self, file_id):
        ret = {}
        with self._tasks_map_lock:
            if file_id in self._tasks_map:
                task = self._tasks_map[file_id]
                if task.status == 'ERROR':
                    ret['status'] = 'ERROR'
                    ret['error'] = task.error
                elif task.status in ('QUEUED', 'PROCESSING'):
                    ret['status'] = task.status
                else:
                    if self._task_file_exists(file_id, 'xls'):
                        ret['status'] = 'DONE'
                    else:
                        ret['status'] = 'ERROR'
                        ret['error'] = 'invalid file id'
        return ret

    def query_task_status(self, file_id, page):
        if page == 0:
            return self.query_task_status_excel(file_id)
        ret = {}
        with self._tasks_map_lock:
            if file_id in self._tasks_map:
                task = self._tasks_map[file_id]
                if task.status == 'ERROR':
                    ret['status'] = 'ERROR'
                    ret['error'] = task.error
                else:
                    ret['status'] = task.page_status(page)
                    ret['info'] = task.pdf_info
                    ret['info']['processed_pages'] = task.last_processed_page
            else:
                if self._task_file_exists(file_id):
                    ret['status'] = 'DONE'
                    ret['info'] = self._get_pdf_info(file_id)
                    ret['info']['processed_pages'] = ret['info']['final_pages']
                    return ret
                else:
                    ret['status'] = 'ERROR'
                    ret['error'] = 'invalid file id'
        return ret

    def run(self):
        assert self._tasks_map is not None
        assert self._tasks_map_lock is not None
        assert self._tasks_queue is not None
        assert self.pdf_dir is not None
        assert self.html_dir is not None

        atexit.register(self.stop)

        self.convertor.start()

        for i in range(self._num_workers):
            t = Worker(self._tasks_queue, i)
            t.setDaemon(True)
            t.start()
            self._workers.append(t)

    def stop(self):
        logging.info('stop libreoffice...')
        self.convertor.stop()

task_manager = TaskManager()
