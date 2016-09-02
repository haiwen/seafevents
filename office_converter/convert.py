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
import json

from .doctypes import DOC_TYPES, PPT_TYPES, EXCEL_TYPES

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
    return output

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
        major, minor = map(int, m.groups())
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
            '-f', 'pdf',
            '-o',
            pdf_path,
            doc_path,
        ]

        try:
            _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, e:
            logging.warning('error when invoking libreoffice: %s', e.output)
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

        try:
            _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, e:
            logging.warning('error when invoking libreoffice: %s', e.output)
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

        with self.lock:
            try:
                _check_output(args, cwd=self.cwd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError, e:
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
            self.get_uno_python(),
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
            except subprocess.CalledProcessError, e:
                logging.warning('error when invoking libreoffice: %s', e.output)
                return False
            else:
                return True

    def _run_pdf2htmlEX(self, args):
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
        subprocess.check_call(args, stdout=sys.stdout, env=env, stderr=sys.stderr)

    def _convert_one_pdf_page(self, pdf, htmldir, page_number):
        page_file_name = '%s.page' % page_number
        dst_page = os.path.join(htmldir, page_file_name)

        if os.path.exists(dst_page):
            return

        try:
            tmpdir = tempfile.mkdtemp()
        except Exception, e:
            logging.warning('failed to create temp dir: %s' % e)
            return -1
        src_page = os.path.join(tmpdir, page_file_name)

        pdf2htmlEX_data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'pdf2htmlEX')

        args = [
            'pdf2htmlEX',
            '--tounicode', '1',
            '--data-dir', pdf2htmlEX_data_dir,
            '--dest-dir', tmpdir,
            '--no-drm', '1',
            '--split-pages', '0',
            '--embed-outline', '0',
            '--process-outline', '0',
            '--first-page', str(page_number),
            '--last-page', str(page_number),
            '--fit-width', '850',
            # disable "@media print" in css
            '--printing', '0',
            pdf, page_file_name,
        ]

        try:
            self._run_pdf2htmlEX(args)
            shutil.move(src_page, dst_page)
        finally:
            shutil.rmtree(tmpdir)

    def _get_pdf_info_json(self, pdf):
        """
        The output of pdfinfo is like:
        vagrant@seafile-dev:/vagrant/src/seafevents$ pdfinfo /vagrant/tmp/devops.pdf
        Tagged:         no
        Form:           none
        Pages:          3
        Encrypted:      no
        Page size:      612 x 792 pts (letter)
        Page rot:       0
        File size:      292995 bytes
        Optimized:      yes
        PDF version:    1.4
        """
        output = _check_output(['pdfinfo', pdf])
        info = {}
        rotated = False
        for line in output.splitlines():
            line = line.strip()
            m = _PAGES_LINE_RE.match(line)
            if m:
                pages = int(m.group(1))
                info['pages'] = pages
                continue
            m = _PAGES_ROTATION_RE.match(line)
            if m:
                rotation = int(m.group(1))
                rotated = rotation in (90, 270)
                continue
            m = _PAGES_SIZE_RE.match(line)
            if m:
                w, h = map(float, m.groups())
                info['page_width'] = w
                info['page_height'] = h
        if 'pages' in info and 'page_width' in info:
            if rotated:
                info['page_width'], info['page_height'] = info['page_height'], info['page_width']
            return info
        else:
            raise Exception('failed to parse pdf information')

    def pdf_to_html2(self, pdf, htmldir, pages, progress_callback):
        if not os.path.exists(htmldir):
            os.mkdir(htmldir)
        info_json_file = os.path.join(htmldir, 'info.json')
        done_file = os.path.join(htmldir, 'done')

        pdf_info = self._get_pdf_info_json(pdf)
        pages = min(pages, pdf_info['pages'])
        pdf_info['final_pages'] = pages
        with open(info_json_file, 'w') as fp:
            json.dump(pdf_info, fp)
        progress_callback(0, pdf_info)

        for page in xrange(1, pages + 1):
            self._convert_one_pdf_page(pdf, htmldir, page)
            progress_callback(page, pdf_info)

        with open(done_file, 'w') as fp:
            # touch the done file
            pass

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

_PAGES_LINE_RE = re.compile(r'^Pages:\s+(\d+)$')
_PAGES_SIZE_RE = re.compile(r'^Page size:\s+([\d\.]+)[\sx]+([\d\.]+)\s+pts.*$')
_PAGES_ROTATION_RE = re.compile(r'^Page rot:\s+(\d+)$')
