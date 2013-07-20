#coding: utf-8

import os
import Queue
import tempfile
import threading
import urllib2
import logging
import shutil
import glob
import atexit

from .convert import Convertor, pdf_to_html
from .convert import ConvertorInitError, ConvertorFatalError

__all__ = ["task_manager"]

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
        # html output
        self.html = os.path.join(html_dir, file_id, 'index.html')

    def __str__(self):
        return "<type: %s, id: %s>" % (self.doctype, self.file_id)

    def get_status(self):
        return self._status

    def set_status(self, status):
        assert (status in ('QUEUED', 'PROCESSING', 'DONE', 'ERROR'))

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

        # Used to generate the unique pipe name for libreoffice process to
        # listen on.
        self._index = index

        self._convertor = None
        self._create_convertor()

    def _create_convertor(self):
        """Create a convertor at start or when the current one is dead."""
        pipe = "pipe%d" % self._index
        try:
            self._convertor = Convertor(pipe)
        except ConvertorInitError:
            logging.critical("failed to start convertor %s", pipe)

    def _convert_to_pdf(self, task):
        """Use libreoffice API to convert document to pdf"""
        if self._convertor is None:
            self._create_convertor()
            if self._convertor is None:
                # create convertor failed
                task.status = 'ERROR'
                task.error = 'internal server error'
                return False

        logging.debug('start to convert task %s', task)

        success = False
        try:
            success = self._convertor.do_convert(task.document, task.doctype, task.pdf)
        except ConvertorFatalError:
            self._convertor = None

        if success:
            logging.debug("succefully converted %s to pdf", task)
        else:
            logging.warning("failed to converted %s to pdf", task)

        return success

    def _convert_to_html(self, task):
        """Use pdf2htmlEX to convert pdf to html"""
        if pdf_to_html(task.pdf, task.html, task_manager.max_pages) != 0:
            logging.warning("failed to convert %s to html", task)
            task.status = 'ERROR'
            task.error = 'failed to convert document'
        else:
            logging.debug("successfully convert %s to html", task)
            task.status = 'DONE'

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
            logging.warning('failed to fetch document of task %s: %s', task, e)
            task.status = 'ERROR'
            task.error = 'failed to fetch document'
            return False
        else:
            task.content = content
            return True

    def _handle_task(self, task):
        """
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

        if task.doctype != 'pdf':
            success = self._convert_to_pdf(task)
            if not success:
                task.status = 'ERROR'
                task.error = 'failed to convert document'
                return

        self._convert_to_html(task)

    def run(self):
        """Repeatedly get task from tasks queue and process it."""
        while True:
            try:
                task = self._tasks_queue.get(timeout=1)
            except Queue.Empty:
                if self.should_exit:
                    if self._convertor:
                        self._convertor.stop()
                    break
                else:
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

    def init(self, num_workers=2, max_pages=50, pdf_dir='/tmp/seafile-pdf-dir', html_dir='/tmp/seafile-html-dir'):
        self._set_pdf_dir(pdf_dir)
        self._set_html_dir(html_dir)

        self._num_workers = num_workers
        self.max_pages = max_pages

    def _checkdir_with_mkdir(self, dname):
        if os.path.exists(dname):
            if not os.path.isdir(dname):
                raise RuntimeError("%s exists, but not a directory" % dname)

            if not os.access(dname, os.R_OK | os.W_OK):
                raise RuntimeError("Access to %s denied" % dname)
        else:
            os.mkdir(dname)

    def _set_pdf_dir(self, pdf_dir):
        """Init the directory to store converted pdf"""
        self._checkdir_with_mkdir(pdf_dir)
        self.pdf_dir = pdf_dir

    def _set_html_dir(self, html_dir):
        self._checkdir_with_mkdir(html_dir)
        self.html_dir = html_dir

    def _task_file_exists(self, file_id):
        '''Test whether the file has already been converted'''
        file_html_dir = os.path.join(self.html_dir, file_id)
        index_html = os.path.join(file_html_dir, 'index.html')

        if os.path.exists(file_html_dir):
            if os.path.exists(index_html):
                return True
            else:
                # The dir <html_dir>/<file_id>/ exists, but
                # <html_dir>/<file_id>/index.html does not exist. Something
                # wrong must have happened. In this case, we remove the
                # file_html_dir to return to a clean state
                shutil.rmtree(file_html_dir)
                return False
        else:
            return False

    def add_task(self, file_id, doctype, url):
        """Create a convert task and dipatch it to worker threads"""
        ret = {}
        if self._task_file_exists(file_id):
            ret['exists'] = True
            return ret
        else:
            ret['exists'] = False

        with self._tasks_map_lock:
            if self._tasks_map.has_key(file_id):
                task = self._tasks_map[file_id]
                if task.status != 'ERROR':
                    # If there is already a convert task in progress, don't create a
                    # new one.
                    return ret

            task = ConvertTask(file_id, doctype, url, self.pdf_dir, self.html_dir)
            self._tasks_map[file_id] = task

        self._tasks_queue.put(task)

        return ret

    def query_task_status(self, file_id):
        ret = {}
        with self._tasks_map_lock:
            if not self._tasks_map.has_key(file_id):
                ret['error'] = 'invalid file id'
            else:
                task = self._tasks_map[file_id]

                if task.status == 'ERROR':
                    ret['error'] = task.error

                elif task.status == 'DONE':
                    if not self._task_file_exists(file_id):
                        # The file has been converted, but the converted files
                        # has been deleted for some reason. In this case we
                        # restart the task
                        task = ConvertTask(task.file_id, task.doctype, task.url,
                                           self.pdf_dir, self.html_dir)
                        self._tasks_map[file_id] = task
                        self._tasks_queue.put(task)

                ret['status'] = task.status

        return ret

    def query_file_pages(self, file_id):
        '''Query how many pages a file has'''
        ret = {}
        file_html_dir = os.path.join(self.html_dir, file_id)
        if not os.path.exists(file_html_dir):
            ret['error'] = 'the file is not converted yet'
            return ret

        page_pattern = os.path.join(self.html_dir, file_id, '*.page')
        try:
            pages = glob.glob(page_pattern)
        except Exception, e:
            ret['error'] = str(e)
            return ret

        ret['count'] = len(pages)

        return ret

    def run(self):
        assert self._tasks_map is not None
        assert self._tasks_map_lock is not None
        assert self._tasks_queue is not None
        assert self.pdf_dir is not None
        assert self.html_dir is not None

        atexit.register(self.stop)

        for i in range(self._num_workers):
            t = Worker(self._tasks_queue, i)
            t.setDaemon(True)
            t.start()
            self._workers.append(t)

    def stop(self):
        '''Set the flag for the worker threads to exit'''
        if not self._workers:
            return

        Worker.should_exit = True
        logging.info('waiting for worker threads to exit...')
        for t in self._workers:
            t.join()
        logging.info('worker threads now exited')

task_manager = TaskManager()
