# coding: utf-8

import time
import os
import sys
import subprocess
import tempfile
import shutil
import uno, unohelper
import logging

from com.sun.star.beans import PropertyValue
from com.sun.star.connection import NoConnectException
from com.sun.star.document.UpdateDocMode import QUIET_UPDATE
from com.sun.star.lang import DisposedException, IllegalArgumentException
from com.sun.star.io import IOException, XOutputStream
from com.sun.star.script import CannotConvertException
from com.sun.star.uno import Exception as UnoException
from com.sun.star.uno import RuntimeException

__all__ = [
    "Convertor",
    "ConvertorInitError",
    "ConvertorFatalError",
    "pdf_to_html",
]

class ConvertorInitError(Exception):
    """Failed to initialize a convertor."""
    pass

class ConvertorFatalError(Exception):
    """Fatal error when converting. Typically it means the libreoffice process
    is dead.

    """
    pass

def UnoProps(**args):
    props = []
    for key in args:
        prop = PropertyValue()
        prop.Name = key
        prop.Value = args[key]
        props.append(prop)
    return tuple(props)

class OutputStream(unohelper.Base, XOutputStream):
    def __init__(self):
        self.closed = 0

    def closeOutput(self):
        self.closed = 1

    def writeBytes(self, seq):
        sys.stdout.write(seq.value)

    def flush(self):
        pass

def get_filter_by_doctype(doctype):
    """Return the libreoffice filter name for the given document type."""
    if doctype == 'doc' or doctype == 'docx':
        return 'writer_pdf_Export'
    elif doctype == 'ppt' or doctype == 'pptx':
        return 'impress_pdf_Export'
    elif doctype == 'xls' or doctype == 'xlsx':
        return 'calc_pdf_Export'
    else:
        logging.warning('invalid doctype %s', doctype)
        raise RuntimeError("invalid doctype %s" % doctype)

class Convertor(object):
    """A convertor wraps a connection to a libreoffice process. On init, it
    spawns a libreoffice process and connects to it. It calls libreoffice API
    to convert the documents.

    """
    def __init__(self, pipe_name):
        self._pipe = pipe_name
        self._office_proc = None
        self._desktop = None
        self._home_dir = None

        try:
            self._start_office()
            self._connect_office()
        except Exception, e:
            logging.warning('failed to init convertor(%s): %s', pipe_name, str(e))
            raise ConvertorInitError()

    def _start_office(self):
        """Spawn the libreoffice process, make it listen on the named pipe."""

        # Don't supply "--headless" in args, just use '--invisible', otherwise
        # libreoffice can't handle font embeding correctly

        args = ['soffice', '--invisible', '--nocrashreport', '--nodefault', '--nologo', '--nofirststartwizard', '--norestore','--accept=pipe,name=%s;urp' % self._pipe]

        # To run multiple libreoffice process simultaneously, each process
        # must has its own HOME env variable.
        self._home_dir = tempfile.mkdtemp()

        env = dict(os.environ)
        env['HOME'] = self._home_dir
        logging.info('Starting a libreoffice process')

        self._office_proc = subprocess.Popen(args, env=env)

    def _connect_office(self):
        """Connect to libreoffice process through uno api. If any error occurs
        here, the libreoffice process would be terminated before any exception
        is raised.

        """
        context = uno.getComponentContext()
        svcmgr = context.ServiceManager
        resolver = svcmgr.createInstanceWithContext("com.sun.star.bridge.UnoUrlResolver", context)
        uno_url = "pipe,name=%s;urp;StarOffice.ComponentContext" % self._pipe

        unocontext = None
        tried = 0
        timeout = 12
        try:
            while tried < timeout:
                # check if libreoffice is still alive
                retcode = self._office_proc.poll()
                if retcode != None:
                    raise RuntimeError('libreoffice exited abormally with code %d', retcode)

                try:
                    unocontext = resolver.resolve("uno:%s" % uno_url)
                except NoConnectException:
                    time.sleep(0.5)
                    tried += 0.5
                except:
                    raise
                else:
                    break

            if not unocontext:
                # failed to connect to libreoffice
                raise RuntimeError('failed to connect to libreoffice')
        except:
            self._finalize()
            raise

        unosvcmgr = unocontext.ServiceManager
        logging.info('a convertor successfully connected to libreoffice')
        self._desktop = unosvcmgr.createInstanceWithContext("com.sun.star.frame.Desktop", unocontext)

    def _terminate_office(self):
        """Stop the spawned libreoffice process when this convertor exit."""
        if self._desktop:
            try:
                self._desktop.terminate()
            except:
                pass
            else:
                self._office_proc = None

        if self._office_proc:
            try:
                self._office_proc.terminate()
            except:
                pass

            self._office_proc.poll()

    def _finalize(self):
        """Clean up work"""
        self._terminate_office()
        if self._home_dir:
            try:
                os.rmdir(self._home_dir)
            except OSError:
                pass

    def stop(self):
        self._finalize()

    @staticmethod
    def _prepare_doucment(document):
        ### Update document links
        try:
            document.updateLinks()
        except AttributeError:
            # the document doesn't implement the XLinkUpdate interface
            pass

        ### Update document indexes
        try:
            document.refresh()
            indexes = document.getDocumentIndexes()
        except AttributeError:
            # the document doesn't implement the XRefreshable and/or
            # XDocumentIndexesSupplier interfaces
            pass
        else:
            for i in range(0, indexes.getCount()):
                indexes.getByIndex(i).update()

    def _convert(self, file_path, doctype, pdf):
        inputprops = UnoProps(Hidden=True, ReadOnly=True, UpdateDocMode=QUIET_UPDATE, FilterOptions="")
        inputurl = unohelper.systemPathToFileUrl(file_path)
        document = self._desktop.loadComponentFromURL(inputurl , "_blank", 0, inputprops)

        if not document:
            raise UnoException("The document '%s' could not be opened." % inputurl, None)

        self._prepare_doucment(document)

        outputurl = unohelper.systemPathToFileUrl(pdf)
        filter_name = get_filter_by_doctype(doctype)
        outputprops = UnoProps(FilterName=filter_name, OutputStream=OutputStream(), Overwrite=True, FilterData=())

        try:
            document.storeToURL(outputurl, tuple(outputprops))
        except IOException, e:
            raise UnoException("Unable to store document to %s with properties %s. Exception: %s" % (outputurl, outputprops, e), None)

        document.dispose()
        document.close(True)

        return True

    def do_convert(self, file_path, doctype, pdf):
        """Convert the supplied document to pdf format. Return True if
        conversion is successful. On error, ConvertorFatalError is raised if
        the connected libreoffice process is dead. On other errors, False is
        returned.

        """
        try:
            return self._convert(file_path, doctype, pdf)
        except DisposedException, e:
            # TODO: Can we reconnect to office here, instead of terminate it?
            self._finalize()
            raise ConvertorFatalError()
        except Exception, e:
            logging.warning('conversion failed: %s', str(e))
            if self._office_proc.poll() != None:
                logging.warning('A libreoffice process is dead')
                self._finalize()
                raise ConvertorFatalError()
            else:
                return False

def pdf_to_html(pdf, html):
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
        '--data-dir', pdf2htmlEX_data_dir, # split pages for dynamic loading
        '--dest-dir', tmpdir,              # out put dir
        '--no-drm', '1',                   # ignore DRM protect
        '--split-pages', '1',              # split pages for dynamic loading
        '--embed-css', '0',                # do not embed css
        '--embed-outline', '0',            # do not embed outline
        '--css-filename', 'file.css',      # css file name
        '--outline-filename', 'file.outline', # outline file name
        '--page-filename', '%d.page',         # outline file name
        pdf,                                  # src file
        html_name,                            # output main html file name
    ]

    try:
        proc = subprocess.Popen(args, stdout=sys.stdout, stderr=sys.stderr)
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
            return 0
        else:
            # Unsuccessful
            logging.warning("pdf2htmlEX failed with code %d", retcode)
            shutil.rmtree(tmpdir)
            return -1
