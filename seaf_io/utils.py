from datetime import datetime

class TaskError(Exception):
    def __init__(self, msg, response_code=None):
        self.msg = msg
        self.response_code = response_code


class Task:

    def __init__(self, task_id, func, args):
        self.id = task_id
        self.func = func
        self.args = args

        self.status = 'init'

        self.started_at = None
        self.finished_at = None

        self.result = None
        self.error = None

    def run(self):
        self.status = 'running'
        self.started_at = datetime.now()
        return self.func(*self.args)

    def set_result(self, result):
        self.result = result
        self.status = 'success'
        self.finished_at = datetime.now()

    def set_error(self, error: TaskError):
        self.error = error
        self.status = 'error'
        self.finished_at = datetime.now()

    def is_finished(self):
        return self.status in ['error', 'success']

    def get_cost_time(self):
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).seconds
        return None

    def get_info(self):
        return f'{self.id}-{self.func}-{self.status}'

    def __str__(self):
        return f'<Task {self.id} {self.func.__name__} {self.status}>'


def height_transfer(base_row_height='default'):
    # convert pixel of seatable height to excel height
    # the default unit of height in excel is 24 pixel, which is 14.4 pound
    height_dict = {
        'default': 1,
        'double':  2,
        'triple': 3,
        'quadruple': 4
    }
    row_height_mul = (height_dict.get(base_row_height, 1))
    return round((32 * row_height_mul * 14.4 ) / 24, 2)


def width_transfer(pixel):
    # convert pixel of seatable to excel width
    # the default width of excel is 8.38 (width of "0" in font size of 11) which is 72px
    return round((pixel * 8.38) / 72, 2)


def is_int_str(v):
    if '.' not in v:
        return True
    return False


def gen_decimal_format(num):
    if is_int_str(num):
        return '0'

    decimal_cnt = len(str(num).split('.')[1])
    if decimal_cnt > 8:
        decimal_cnt = 8
    return '0.' + '0' * decimal_cnt
