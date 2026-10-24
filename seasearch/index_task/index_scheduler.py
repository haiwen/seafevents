import heapq
import time
from collections import OrderedDict
from itertools import count
from threading import Condition


class IndexTask(object):
    def __init__(self, now):
        self.first_seen = now
        self.due = now
        self.content = False
        self.metadata = False
        self.row_ids = set()
        self.cleanup = set()
        self.failures = 0
        self.retry_at = 0
        self.version = 0
        self.realtime = False

    def merge(self, other):
        self.content |= other.content
        self.metadata |= other.metadata
        self.row_ids.update(other.row_ids)
        self.cleanup.update(other.cleanup)
        self.realtime |= other.realtime


class IndexScheduler(object):
    """One pending snapshot per repository; executing snapshots are kept separate."""
    def __init__(self, debounce=2, max_wait=10, throttle=10, realtime_quota=5):
        self.debounce = debounce
        self.max_wait = max_wait
        self.pending = {}
        self.running = {}
        self.heaps = {True: [], False: []}
        self.throttle = throttle
        self.next_allowed = OrderedDict()
        self.realtime_quota = realtime_quota
        self.realtime_streak = 0
        self.sequence = count(1)
        self.condition = Condition()
        self.stopped = False

    def submit(self, repo_id, content=False, metadata=False, row_ids=(), cleanup=(), immediate=False,
               background=False):
        with self.condition:
            now = time.monotonic()
            self._expire_cooldowns(now)
            task = self.pending.setdefault(repo_id, IndexTask(now))
            task.content |= content
            task.metadata |= metadata
            task.row_ids.update(row_ids)
            task.cleanup.update(cleanup)
            task.realtime |= not background
            task.due = max(task.retry_at, self.next_allowed.get(repo_id, 0),
                           min(now + (0 if immediate else self.debounce),
                                             task.first_seen + self.max_wait))
            self._schedule(repo_id, task)

    def _expire_cooldowns(self, now):
        # Starts are recorded in monotonic order; retain only unexpired cooldowns.
        while self.next_allowed:
            repo_id, deadline = next(iter(self.next_allowed.items()))
            if deadline > now:
                break
            del self.next_allowed[repo_id]

    def _schedule(self, repo_id, task):
        """Called with the condition held; invalidate older heap entries lazily."""
        task.version = next(self.sequence)
        if repo_id not in self.running:
            heapq.heappush(self.heaps[task.realtime], (task.due, task.version, repo_id))
        # Frequent edits must not grow stale heap entries without bound.
        if sum(map(len, self.heaps.values())) > max(64, 2 * len(self.pending)):
            self.heaps = {True: [], False: []}
            for repo_id, task in self.pending.items():
                if repo_id not in self.running:
                    self.heaps[task.realtime].append((task.due, task.version, repo_id))
            for heap in self.heaps.values():
                heapq.heapify(heap)
        self.condition.notify_all()

    def _peek(self, realtime):
        heap = self.heaps[realtime]
        while heap:
            _, version, repo_id = heap[0]
            task = self.pending.get(repo_id)
            if task is not None and task.version == version and repo_id not in self.running:
                return task
            heapq.heappop(heap)
        return None

    def take(self):
        with self.condition:
            while not self.stopped:
                if not self.pending:
                    self.condition.wait()
                    continue
                now = time.monotonic()
                self._expire_cooldowns(now)
                realtime, background = self._peek(True), self._peek(False)
                if realtime is None and background is None:
                    # Pending tasks may all belong to repositories currently running.
                    self.condition.wait()
                    continue
                ready_rt = realtime is not None and realtime.due <= now
                ready_bg = background is not None and background.due <= now
                if not ready_rt and not ready_bg:
                    deadline = min(task.due for task in (realtime, background) if task is not None)
                    self.condition.wait(deadline - now)
                    continue
                priority = ready_rt and (not ready_bg or self.realtime_streak < self.realtime_quota)
                _, _, repo_id = heapq.heappop(self.heaps[priority])
                task = self.pending[repo_id]
                del self.pending[repo_id]
                self.running[repo_id] = task
                self.next_allowed[repo_id] = now + self.throttle
                self.next_allowed.move_to_end(repo_id)
                self.realtime_streak = min(self.realtime_streak + 1, self.realtime_quota) if priority else 0
                if not self.pending:
                    for heap in self.heaps.values():
                        heap.clear()
                return repo_id, task
            return None

    def retry(self, repo_id, task):
        with self.condition:
            newer = self.pending.pop(repo_id, None)
            if newer:
                task.merge(newer)
            task.failures += 1
            task.retry_at = time.monotonic() + min(2 ** min(task.failures - 1, 6), 60)
            task.due = max(task.retry_at, self.next_allowed.get(repo_id, 0))
            self.pending[repo_id] = task
            self._schedule(repo_id, task)

    def finish(self, repo_id, task):
        with self.condition:
            # Identity protects against a completed/retried snapshot releasing a newer lease.
            if self.running.get(repo_id) is not task:
                return
            del self.running[repo_id]
            pending = self.pending.get(repo_id)
            if pending is not None:
                self._schedule(repo_id, pending)
            self.condition.notify_all()

    def stop(self):
        with self.condition:
            self.stopped = True
            self.condition.notify_all()
