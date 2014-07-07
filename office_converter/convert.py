# coding: utf-8

import time
import os
import sys
import subprocess
import tempfile
import shutil
import threading
import re
import logging

from .doctypes import DOC_TYPES, PPT_TYPES, EXCEL_TYPES

from ..utils import get_python_executable, run, run_and_wait, find_in_path

__all__ = [
    "Convertor",
    "ConvertorFatalError",
]

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
        output = subprocess.check_output('libreoffice --version', shell=True)
    except subprocess.CalledProcessError:
        return False
    else:
        m = re.match(r'LibreOffice (\d)\.(\d)', output)
        if not m:
            return False
        major, minor = map(int, m.groups())
        if major == 4 and minor >= 2:
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
            self.get_uno_python(),
            self.unoconv_py,
            '-vvv',
            '--pipe',
            self.pipe,
            '-l',
        ]

        self.proc = run(args, cwd=self.cwd)

        retcode = self.proc.poll()
        if retcode != None:
            logging.warning('unoconv process exited with code %s' % retcode)

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
            self.get_uno_python(),
            self.unoconv_py,
            '-vvv',
            '--pipe',
            self.pipe,
            '-o',
            pdf_path,
            doc_path,
        ]

        if run_and_wait(args, cwd=self.cwd) != 0:
            return False
        else:
            return True

    def excel_to_html(self, doc_path, html_path):
        if self.proc.poll() != None:
            return self.excel_to_html_fallback(doc_path, html_path)

        args = [
            self.get_uno_python(),
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

        if run_and_wait(args, cwd=self.cwd) != 0:
            return False
        else:
            improve_table_border(html_path)
            return True

    def excel_to_html_fallback(self, doc_path, html_path):
        args = [
            self.get_uno_python(),
            self.unoconv_py,
            '-vvv',
            '-d', 'spreadsheet',
            '-f', 'html',
            '-o',
            html_path,
            doc_path,
        ]

        if run_and_wait(args, cwd=self.cwd) != 0:
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
            self.get_uno_python(),
            self.unoconv_py,
            '-vvv',
            '-o',
            pdf_path,
            doc_path,
        ]

        with self.lock:
            if run_and_wait(args, cwd=self.cwd) != 0:
                return False
            else:
                return True

    def pdf_to_html(self, pdf, html, pages):
        html_dir = os.path.dirname(html)
        html_name = os.path.basename(html)

        try:
            tmpdir = tempfile.mkdtemp()
        except Exception, e:
            logging.warning('failed to make temp dir: %s' % e)
            return -1

        pdf2htmlEX_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                           'pdf2htmlEX')
        args = [
            'pdf2htmlEX',
            '--tounicode', '1',
            '--data-dir', pdf2htmlEX_data_dir, # split pages for dynamic loading
            '--dest-dir', tmpdir,              # out put dir
            '--no-drm', '1',                   # ignore DRM protect
            '--split-pages', '1',              # split pages for dynamic loading
            '--embed-css', '0',                # do not embed css
            '--embed-outline', '0',            # do not embed outline
            '--css-filename', 'file.css',      # css file name
            '--outline-filename', 'file.outline', # outline file name
            '--page-filename', '%d.page',         # outline file name
            '--last-page', str(pages),            # max page range
            '--fit-width', '850',                 # page width
            pdf,                                  # src file
            html_name,                            # output main html file name
        ]

        def get_env():
            '''Setup env for pdf2htmlEX'''
            env = dict(os.environ)
            try:
                env['LD_LIBRARY_PATH'] = env['SEAFILE_LD_LIBRARY_PATH']
                env['FONTCONFIG_PATH'] = '/etc/fonts'
            except KeyError:
                pass

            return env

        env = get_env()

        try:
            proc = subprocess.Popen(args, stdout=sys.stdout, env=env, stderr=sys.stderr)
            retcode = proc.wait()
        except Exception, e:
            # Error happened when invoking the subprocess. We remove the tmpdir
            # and exit
            logging.warning("failed to invoke pdf2htmlEX: %s", e)
            shutil.rmtree(tmpdir)
            return -1
        else:
            if retcode == 0:
                # Successful
                shutil.move(tmpdir, html_dir)
                if change_html_dir_perms(html_dir) != 0:
                    return -1
                else:
                    return 0
            else:
                # Unsuccessful
                logging.warning("pdf2htmlEX failed with code %d", retcode)
                shutil.rmtree(tmpdir)
                return -1


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
