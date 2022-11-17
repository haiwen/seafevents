# coding: utf-8

from threading import Thread
import queue
import logging
from .ali_scan import AliScanner

class Worker(Thread):
    def __init__(self, platform, key, key_id, region, do_work, task_queue):
        Thread.__init__(self)
        self.do_work = do_work
        self.task_queue = task_queue
        self.client = None
        if platform == 'ali':
            self.client = AliScanner(key, key_id, region)
        else:
            logging.error('Unknown platform: %s', platform)

    def run(self):
        while True:
            try:
                task = self.task_queue.get()
                if task is None:
                    break
                self.do_work(task, self.client)
            except Exception as e:
                logging.warning('Failed to execute task: %s' % e)
            finally:
                self.task_queue.task_done()

class ThreadPool(object):
    def __init__(self, platform, key, key_id, region, do_work, nworker=10):
        self.platform = platform
        self.key = key
        self.key_id = key_id
        self.region = region
        self.do_work = do_work
        self.nworker = nworker
        self.task_queue = queue.Queue()

    def start(self):
        for i in range(self.nworker):
            Worker(self.platform, self.key, self.key_id, self.region, self.do_work, self.task_queue).start()

    def put_task(self, task):
        self.task_queue.put(task)

    def join(self, stop=False):
        self.task_queue.join()
        # notify all thread to stop
        if stop:
            for i in range(self.nworker):
                self.task_queue.put(None)
