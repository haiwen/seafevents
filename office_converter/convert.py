# coding: utf-8

import os
import time
import subprocess
import threading
import re
import logging

from ..utils import get_python_executable, run, run_and_wait, find_in_path

__all__ = [
    "Convertor",
    "ConvertorFatalError",
]

def _check_output(*popenargs, **kwargs):
    r"""Run command with arguments and return its output as a byte string.

    Backported from Python 2.7 as it's implemented as pure python on stdlib.

    >>> check_output(['/usr/bin/python', '--version'])
    Python 2.6.2
    """
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, _ = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output.decode('utf-8')


class ConvertorFatalError(Exception):
    """Fatal error when converting. Typically it means the libreoffice process
    is dead.

    """
    pass

def is_python3():
    libreoffice_exe = find_in_path('libreoffice')
    if not libreoffice_exe:
        return False
    try:
        output = _check_output('libreoffice --version', shell=True)
    except subprocess.CalledProcessError:
        return False
    else:
        m = re.match(r'LibreOffice (\d)\.(\d)', output)
        if not m:
            return False
        major, minor = list(map(int, m.groups()))
        if (major == 4 and minor >= 2) or major > 4:
            return True

    return False

class Convertor(object):
    def __init__(self):
        self.unoconv_py = os.path.join(os.path.dirname(__file__), 'unoconv.py')
        self.cwd = os.path.dirname(__file__)
        self.pipe = 'seafilepipe'
        self.proc = None
        self.lock = threading.Lock()
        self._python = None

    def get_uno_python(self):
        if not self._python:
            if is_python3():
                py3 = find_in_path('python3')
                if py3:
                    logging.info('unoconv process will use python 3')
                    self._python = py3

            self._python = self._python or get_python_executable()

        return self._python

    def start(self):
        args = [
            get_python_executable(),
            self.unoconv_py,
            '-vvv',
            '--pipe',
            self.pipe,
            '-l',
        ]

        self.proc = run(args, cwd=self.cwd)

        time.sleep(3)
        exists_args = ["ps", "-ef"]
        result = run(exists_args, output=subprocess.PIPE)
        rows = result.stdout.read().decode('utf-8')
        if self.unoconv_py in rows:
            logging.info('unoconv process already start.')
        else:
            logging.warning('Failed to running unoconv process.')

    def stop(self):
        if self.proc:
            try:
                self.proc.terminate()
            except:
                pass

    def convert_to_pdf(self, doc_path, pdf_path):
        '''This method is thread-safe'''
        if self.proc.poll() != None:
            return self.convert_to_pdf_fallback(doc_path, pdf_path)

        args = [
            get_python_executable(),
            self.unoconv_py,
            '-vvv',
            '--pipe',
            self.pipe,
            '-f', 'pdf',
            '-o',
            pdf_path,
            doc_path,
        ]

        try:
            _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logging.warning('error when invoking libreoffice: %s', e.output)
            return False
        else:
            return True

    def excel_to_html(self, doc_path, html_path):
        if self.proc.poll() != None:
            return self.excel_to_html_fallback(doc_path, html_path)

        args = [
            get_python_executable(),
            self.unoconv_py,
            '-vvv',
            '-d', 'spreadsheet',
            '-f', 'html',
            '--pipe',
            self.pipe,
            '-o',
            html_path,
            doc_path,
        ]

        try:
            _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logging.warning('error when invoking libreoffice: %s', e.output)
            return False
        else:
            improve_table_border(html_path)
            return True

    def excel_to_html_fallback(self, doc_path, html_path):
        args = [
            get_python_executable(),
            self.unoconv_py,
            '-vvv',
            '-d', 'spreadsheet',
            '-f', 'html',
            '-o',
            html_path,
            doc_path,
        ]

        with self.lock:
            try:
                _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                logging.warning('error when invoking libreoffice: %s', e.output)
                return False
            else:
                improve_table_border(html_path)
                return True

    def convert_to_pdf_fallback(self, doc_path, pdf_path):
        '''When the unoconv listener is dead for some reason, we fallback to
        start a new libreoffce instance for each request. A lock must be used
        since there can only be one libreoffice instance running at a time.

        '''
        args = [
            get_python_executable(),
            self.unoconv_py,
            '-vvv',
            '-f', 'pdf',
            '-o',
            pdf_path,
            doc_path,
        ]

        with self.lock:
            try:
                _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                logging.warning('error when invoking libreoffice: %s', e.output)
                return False
            else:
                return True

def change_html_dir_perms(path):
    '''The default permission set by pdf2htmlEX is 700, we need to set it to 770'''
    args = [
        'chmod',
        '-R',
        '770',
        path,
    ]
    return run_and_wait(args)

pattern = re.compile('<TABLE(.*)BORDER="0">')
def improve_table_border(path):
    with open(path, 'r') as fp:
        content = fp.read()
    content = re.sub(pattern, r'<TABLE\1BORDER="1" style="border-collapse: collapse;">', content)
    with open(path, 'w') as fp:
        fp.write(content)

_PAGES_LINE_RE = re.compile(r'^Pages:\s+(\d+)$')
_PAGES_SIZE_RE = re.compile(r'^Page size:\s+([\d\.]+)[\sx]+([\d\.]+)\s+pts.*$')
_PAGES_ROTATION_RE = re.compile(r'^Page rot:\s+(\d+)$')
