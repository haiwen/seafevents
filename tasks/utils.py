import os
import sys
import subprocess

def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None

def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python2.7',
        'python27',
        'python2.6',
        'python26',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path

pyexec = None
def get_python_executable():
    '''Find a suitable python executable'''
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec

def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False):
    '''Run a program and wait it to finish, and return its exit code. The
    standard output of this program is supressed.

    '''
    with open(os.devnull, 'w') as devnull:
        if suppress_stdout:
            stdout = devnull
        else:
            stdout = sys.stdout

        if suppress_stderr:
            stderr = devnull
        else:
            stderr = sys.stderr

        subprocess.Popen(argv,
                         cwd=cwd,
                         stdout=stdout,
                         stderr=stderr,
                         env=env)
