"""Isolated unit tests: python3 -m unittest discover -s tests/seasearch -v.

Load the small modules/classes without importing the application's DB/RPC bootstrap.
The code under test is compiled directly from its source files.
"""
import ast
import importlib.util
import json
import os
import time
from pathlib import Path
from threading import Event, Thread, Lock, local, current_thread
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]


def load_class(path, name, namespace):
    namespace.setdefault('logger', Mock())
    namespace.setdefault('time', time)
    tree = ast.parse((ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, 'exec'), namespace)
    return namespace[name]


spec = importlib.util.spec_from_file_location(
    'index_scheduler', ROOT / 'seasearch/index_task/index_scheduler.py')
scheduler_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduler_module)
IndexScheduler = scheduler_module.IndexScheduler


class SchedulerTests(unittest.TestCase):
    def test_running_repo_does_not_block_other_repo_or_run_twice(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        scheduler.submit('A', content=True)
        _, first = scheduler.take()
        for _ in range(100):  # Exercise heap compaction while A is reserved.
            scheduler.submit('A', row_ids=['row'])
        scheduler.submit('B', content=True)
        repo, other = scheduler.take()
        self.assertEqual(repo, 'B')
        scheduler.finish(repo, other)
        entered, received = Event(), Event()
        result = []

        def consume():
            entered.set()
            result.append(scheduler.take())
            received.set()

        thread = Thread(target=consume, daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertFalse(received.wait(0.05))
            scheduler.finish('A', first)
            self.assertTrue(received.wait(1))
            self.assertEqual(result[0][0], 'A')
            self.assertEqual(result[0][1].row_ids, {'row'})
            # A stale finalizer cannot release the new snapshot's reservation.
            scheduler.finish('A', first)
            self.assertIs(scheduler.running['A'], result[0][1])
            scheduler.finish(*result[0])
            self.assertFalse(scheduler.running)
        finally:
            scheduler.stop()
            thread.join(1)

    def test_retry_releases_only_after_finalizer_and_merges_new_events(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('A', content=True)
            _, first = scheduler.take()
            scheduler.submit('A', row_ids=['row'])
            scheduler.retry('A', first)
            self.assertIs(scheduler.running['A'], first)
            scheduler.finish('A', first)
            clock.return_value = 101
            _, retry = scheduler.take()
            self.assertEqual(retry.row_ids, {'row'})
            self.assertEqual(retry.failures, 1)
            scheduler.finish('A', retry)

    def test_stop_wakes_worker_waiting_for_running_repo(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        scheduler.submit('A', content=True)
        scheduler.take()
        scheduler.submit('A', content=True)
        result = []
        thread = Thread(target=lambda: result.append(scheduler.take()), daemon=True)
        thread.start()
        scheduler.stop()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [None])

    def test_throttle_preserves_trailing_update_and_expires_cooldown(self):
        scheduler = IndexScheduler(debounce=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('repo', content=True)
            _, first = scheduler.take()
            clock.return_value = 103
            scheduler.submit('repo', content=True)
            clock.return_value = 106
            scheduler.submit('repo', row_ids=['row'])
            self.assertEqual(scheduler.pending['repo'].due, 110)
            scheduler.finish('repo', first)
            clock.return_value = 110
            _, task = scheduler.take()
            self.assertTrue(task.content)
            self.assertEqual(task.row_ids, {'row'})
            self.assertFalse(scheduler.pending)  # No unconditional trailing/no-op task.
            clock.return_value = 131
            scheduler.submit('other', content=True)
            self.assertNotIn('repo', scheduler.next_allowed)

    def test_retry_and_compensation_cannot_bypass_throttle(self):
        scheduler = IndexScheduler(debounce=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('repo', content=True)
            _, task = scheduler.take()
            scheduler.retry('repo', task)
            scheduler.finish('repo', task)
            scheduler.submit('repo', metadata=True, immediate=True, background=True)
            self.assertEqual(scheduler.pending['repo'].due, 110)
            self.assertTrue(scheduler.pending['repo'].realtime)
            clock.return_value = 110
            self.assertEqual(scheduler.take()[0], 'repo')

    def test_realtime_priority_and_background_fairness(self):
        scheduler = IndexScheduler(debounce=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100):
            for i in range(3):
                scheduler.submit('bg' + str(i), content=True, immediate=True, background=True)
            for i in range(12):
                scheduler.submit('rt' + str(i), content=True)
            order = [scheduler.take()[0] for _ in range(15)]
        self.assertEqual(order[:5], ['rt' + str(i) for i in range(5)])
        self.assertEqual(order[5], 'bg0')
        self.assertEqual(order[11], 'bg1')
        self.assertEqual(order[-1], 'bg2')

    def test_background_promotion_invalidates_old_heap_entry(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100):
            scheduler.submit('promoted', content=True, background=True)
            scheduler.submit('background', content=True, background=True)
            scheduler.submit('promoted', row_ids=['row'])
            self.assertEqual(scheduler.take()[0], 'promoted')
            self.assertEqual(scheduler.take()[0], 'background')
            self.assertFalse(scheduler.pending)

    def test_rescheduling_ignores_stale_entries_and_preserves_due_order(self):
        scheduler = IndexScheduler(throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('edited', content=True)  # due 102
            scheduler.submit('other', content=True)   # due 102
            clock.return_value = 101
            scheduler.submit('edited', row_ids=['row'])  # due 103
            clock.return_value = 104
            self.assertEqual(scheduler.take()[0], 'other')
            repo_id, task = scheduler.take()
            self.assertEqual(repo_id, 'edited')
            # Reuse the same repo ID; stale entries must never match a new task.
            scheduler.finish('edited', task)
            scheduler.submit('edited', content=True, immediate=True)
            self.assertEqual(scheduler.take()[0], 'edited')
            self.assertFalse(any(scheduler.heaps.values()))

    def test_hot_repository_does_not_accumulate_unbounded_heap_entries(self):
        scheduler = IndexScheduler()
        with patch.object(scheduler_module.time, 'monotonic', return_value=100):
            for _ in range(10000):
                scheduler.submit('hot', content=True)
        self.assertLessEqual(sum(map(len, scheduler.heaps.values())), 64)
        self.assertEqual(len(scheduler.pending), 1)

    def test_compensation_batch_drains_without_scanning_pending(self):
        class NoEnumerationDict(dict):
            def items(self):
                raise AssertionError('take must not enumerate pending tasks')

        scheduler = IndexScheduler()
        with patch.object(scheduler_module.time, 'monotonic', return_value=100):
            for i in range(1000):
                scheduler.submit(str(i), content=True, immediate=True)
            scheduler.pending = NoEnumerationDict(scheduler.pending)
            self.assertEqual({scheduler.take()[0] for _ in range(1000)},
                             {str(i) for i in range(1000)})

    def test_continuous_events_have_maximum_delay(self):
        scheduler = IndexScheduler()
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('repo', content=True)
            for second in range(101, 115):
                clock.return_value = second
                scheduler.submit('repo', row_ids=['row'])
            self.assertEqual(len(scheduler.pending), 1)
            self.assertEqual(scheduler.pending['repo'].due, 110)
            repo, task = scheduler.take()
            self.assertEqual(repo, 'repo')
            self.assertTrue(task.content)
            self.assertEqual(task.row_ids, {'row'})

    def test_events_during_execution_are_not_lost(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        scheduler.submit('repo', content=True)
        _, snapshot = scheduler.take()
        scheduler.submit('repo', row_ids=['new-row'])
        self.assertTrue(snapshot.content)
        self.assertEqual(snapshot.row_ids, set())
        scheduler.finish('repo', snapshot)
        self.assertEqual(scheduler.take()[1].row_ids, {'new-row'})

    def test_retry_merges_new_events_without_bypassing_backoff(self):
        scheduler = IndexScheduler(debounce=0, throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('repo', content=True)
            _, snapshot = scheduler.take()
            scheduler.submit('repo', row_ids=['row'])
            scheduler.retry('repo', snapshot)
            scheduler.finish('repo', snapshot)
            scheduler.submit('repo', metadata=True, immediate=True)
            self.assertEqual(scheduler.pending['repo'].due, 101)
            clock.return_value = 101
            _, task = scheduler.take()
            self.assertTrue(task.content and task.metadata)
            self.assertEqual(task.row_ids, {'row'})
            task.failures = 100
            scheduler.retry('repo', task)
            self.assertEqual(task.due, 161)


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.repo_data = Mock()
        self.ns = {
            'IS_PRO_VERSION': True, 'ENABLE_SEARCH': True, 'SEARCH_ENGINE': 'seasearch',
            'Event': Event, 'IndexScheduler': IndexScheduler, 'os': os, 'json': json,
            'Lock': Lock, 'local': local,
            'time': time, 'current_thread': current_thread,
            'logger': Mock(), 'repo_data': self.repo_data, 'IndexManager': Mock(),
            'REPO_TYPE_WIKI': 'wiki',
            'REDIS_HOST': 'redis', 'REDIS_PORT': 6379, 'REDIS_PASSWORD': 'password',
            'get_mq': Mock(return_value=Mock()),
            'urlsplit': urlsplit,
            'get_opt_from_conf_or_env': Mock(side_effect=lambda config, section, key, *args, **kw: {
                'seasearch_url': 'https://search.example.com', 'seasearch_token': 'test-token',
                'wiki_file_size_limit': 10,
            }.get(key, kw.get('default'))),
        }
        cls = load_class('seasearch/index_task/seasearch_index_manager.py',
                         'SeasearchIndexManger', self.ns)
        with patch.dict(os.environ, {'SEASEARCH_INDEX_INTERVAL': '60'}):
            self.manager = cls(Mock())
        self.manager.ensure_indexes = Mock()
        self.manager.index_manager = Mock()
        self.manager.indexes = {'file': Mock(), 'wiki': Mock()}
        self.manager.status_indexes = {'file': Mock(), 'wiki': Mock()}

    def test_interval_environment_only_and_invalid_fallback(self):
        self.assertEqual(self.manager.interval, 60)
        cls = self.ns['SeasearchIndexManger']
        for value in ['invalid', '0', '-10']:
            with patch.dict(os.environ, {'SEASEARCH_INDEX_INTERVAL': value}):
                self.assertEqual(cls(Mock()).interval, 1800)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(cls(Mock()).interval, 1800)

    def test_mq_is_initialized_as_instance_attribute(self):
        self.ns['get_mq'].assert_called_once_with('redis', 6379, 'password')
        self.assertIs(self.manager.mq, self.ns['get_mq'].return_value)

    def test_worker_count_environment_and_start(self):
        cls = self.ns['SeasearchIndexManger']
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(cls(Mock()).worker_count, 1)
        for value in ['0', '-2', 'bad']:
            with patch.dict(os.environ, {'SEASEARCH_INDEX_WORKERS': value}):
                self.assertEqual(cls(Mock()).worker_count, 1)
        with patch.dict(os.environ, {'SEASEARCH_INDEX_WORKERS': '3'}):
            manager = cls(Mock())
        self.ns['Thread'] = Mock()
        manager.start()
        names = [call.kwargs['name'] for call in self.ns['Thread'].call_args_list]
        self.assertEqual(names, ['seasearch_events', 'seasearch_compensation',
                                 'seasearch_index_worker_0', 'seasearch_index_worker_1',
                                 'seasearch_index_worker_2'])
        manager.start()
        self.assertEqual(self.ns['Thread'].call_count, 5)

    def test_worker_objects_are_thread_local(self):
        self.ns['IndexManager'].side_effect = lambda: Mock()
        self.repo_data.get_repo_index_info.return_value = ('head', None, False)
        task = scheduler_module.IndexTask(0)
        task.content = True
        original = self.manager.index_manager
        results = []

        def ensure(kind):
            self.manager.indexes[kind] = Mock()
            self.manager.status_indexes[kind] = Mock()

        self.manager.ensure_indexes.side_effect = ensure

        def run():
            self.assertIsNone(self.manager.index_manager)
            self.assertEqual(self.manager.indexes, {})
            self.manager.process_task('repo', task)
            results.append(self.manager.index_manager)
            self.manager.indexes['file'] = Mock()

        for _ in range(2):
            thread = Thread(target=run)
            thread.start()
            thread.join(1)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(results), 2)
        self.assertIsNot(results[0], results[1])
        self.assertIs(self.manager.index_manager, original)

    def test_worker_finally_releases_reservation_on_failure(self):
        self.manager.scheduler = IndexScheduler(debounce=0, throttle=0)
        self.manager.scheduler.submit('A', content=True)

        def fail(repo_id, task):
            self.manager.finished.set()
            raise RuntimeError('failed')

        self.manager.process_task = fail
        self.manager.run_worker()
        self.assertFalse(self.manager.scheduler.running)
        self.assertEqual(self.manager.scheduler.pending['A'].failures, 1)

    def test_worker_logs_queue_wait_and_execution_duration(self):
        self.manager.scheduler = IndexScheduler(debounce=0, throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            self.manager.scheduler.submit('A', content=True)
            clock.return_value = 105

            def process(repo_id, task):
                clock.return_value = 108
                self.manager.finished.set()

            self.manager.process_task = process
            self.manager.run_worker()
        logs = self.ns['logger'].info.call_args_list
        start = next(call.args for call in logs if 'task started:' in call.args[0])
        end = next(call.args for call in logs if 'task ended:' in call.args[0])
        self.assertEqual(start[1], 'A')
        self.assertEqual(start[-1], 5)
        self.assertEqual(end[-2:], ('completed', 3))
        self.assertFalse(self.manager.scheduler.running)

    def test_restart_scan_recreates_background_tasks_without_new_events(self):
        # Fresh scheduler has none of the old process's events, throttle or retry state.
        self.manager.scheduler = IndexScheduler(debounce=0)
        self.repo_data.get_repo_id_commit_id.side_effect = [
            [('file', 'C3', None), ('wiki', 'W3', 'wiki')], []]
        self.repo_data.get_virtual_repo_in_repos.return_value = []
        self.ns.update({
            'REPO_STATUS_FILE_INDEX_NAME': 'files', 'WIKI_STATUS_INDEX_NAME': 'wikis',
            'RepoStatusIndex': Mock(), 'WikiStatusIndex': Mock(), 'SeaSearchAPI': Mock(),
        })
        self.ns['SeaSearchAPI'].return_value.check_index_mapping.return_value = {'is_exist': False}
        self.manager.finished.wait = Mock(side_effect=lambda interval: self.manager.finished.set())
        self.manager.run_compensation()
        self.assertEqual(set(self.manager.scheduler.pending), {'file', 'wiki'})
        file_task, wiki_task = (self.manager.scheduler.pending[key] for key in ('file', 'wiki'))
        self.assertTrue(file_task.content and file_task.metadata)
        self.assertTrue(wiki_task.content)
        self.assertFalse(wiki_task.metadata or file_task.realtime or wiki_task.realtime)
        self.manager.finished.wait.assert_called_once_with(60)

    def test_scan_only_submits_actual_changes_and_deleted_candidates(self):
        statuses = [
            {'repo_id': name, 'commit_id': 'head', 'updatingto': None, 'metadata_updated_time': 100}
            for name in ['unchanged', 'description', 'changed', 'recovering', 'lost-index', 'deleted']]
        statuses[2]['commit_id'] = 'old'
        statuses[3]['updatingto'] = 'unfinished'
        file_status, wiki_status, api = Mock(), Mock(), Mock()
        file_status.get_all_repos_from_index.return_value = statuses
        wiki_status.get_all_repos_from_index.return_value = [
            {'repo_id': 'wiki', 'commit_id': 'W1', 'updatingto': None}]
        api.check_index_mapping.side_effect = lambda name: {'is_exist': name != 'file_lost-index'}
        self.ns.update({
            'REPO_STATUS_FILE_INDEX_NAME': 'files', 'WIKI_STATUS_INDEX_NAME': 'wikis',
            'REPO_FILE_INDEX_PREFIX': 'file_', 'WIKI_INDEX_PREFIX': 'wiki_',
            'RepoStatusIndex': Mock(return_value=file_status),
            'WikiStatusIndex': Mock(return_value=wiki_status), 'SeaSearchAPI': Mock(return_value=api),
        })
        self.ns['IndexManager'].return_value.has_description_changes.side_effect = lambda repo, stamp: repo == 'description'
        self.repo_data.get_virtual_repo_in_repos.return_value = [('virtual',)]
        self.repo_data.get_repo_id_commit_id.side_effect = [[
            (name, 'head', None) for name in ['unchanged', 'description', 'changed', 'recovering',
                                           'lost-index', 'new', 'virtual']]
            + [('wiki', 'W1', 'wiki')], []]
        self.manager.scan_repositories()
        pending = self.manager.scheduler.pending
        self.assertEqual(set(pending), {'description', 'changed', 'recovering', 'lost-index', 'new', 'deleted'})
        self.assertFalse(pending['description'].content)
        self.assertTrue(pending['description'].metadata)
        for name in ['changed', 'recovering', 'lost-index', 'new']:
            self.assertTrue(pending[name].content and pending[name].metadata)
        self.assertEqual(pending['deleted'].cleanup, {'file'})
        self.assertTrue(all(not task.realtime for task in pending.values()))
        self.ns['IndexManager'].return_value.has_description_changes.assert_any_call('unchanged', 100)

    def test_failed_scan_does_not_schedule_cleanup(self):
        api, statuses = Mock(), Mock()
        api.check_index_mapping.return_value = {'is_exist': True}
        statuses.get_all_repos_from_index.return_value = [
            {'repo_id': 'deleted', 'commit_id': 'old', 'updatingto': None}]
        self.ns.update({
            'REPO_STATUS_FILE_INDEX_NAME': 'files', 'WIKI_STATUS_INDEX_NAME': 'wikis',
            'RepoStatusIndex': Mock(return_value=statuses), 'WikiStatusIndex': Mock(return_value=statuses),
            'SeaSearchAPI': Mock(return_value=api),
        })
        self.repo_data.get_repo_id_commit_id.side_effect = RuntimeError('database unavailable')
        with self.assertRaises(RuntimeError):
            self.manager.scan_repositories()
        self.assertFalse(self.manager.scheduler.pending)

    def test_invalid_shared_config_disables_start_without_retry_or_network(self):
        cls = self.ns['SeasearchIndexManger']
        for url, token in [(None, 'token'), ('', 'token'), ('ftp://host', 'token'),
                           ('https://', 'token'), ('http://host:99999', 'token'),
                           ('https://host', None), ('https://host', '  ')]:
            with self.subTest(url=url, token=token):
                self.ns['get_opt_from_conf_or_env'] = lambda config, section, key, *a, **kw: {
                    'seasearch_url': url, 'seasearch_token': token,
                }.get(key, kw.get('default'))
                manager = cls(Mock())
                self.assertFalse(manager.enabled)
                manager.start()  # No Thread/network dependencies are provided.
                self.assertFalse(manager.threads)
                self.assertFalse(manager.scheduler.pending)

    def test_invalid_wiki_config_only_disables_wiki(self):
        cls = self.ns['SeasearchIndexManger']
        self.ns['get_opt_from_conf_or_env'] = lambda config, section, key, *a, **kw: {
            'seasearch_url': 'https://host', 'seasearch_token': 'token',
            'wiki_file_size_limit': 'invalid',
        }.get(key, kw.get('default'))
        manager = cls(Mock())
        self.assertTrue(manager.enabled)
        self.assertEqual(manager.disabled_kinds, {'wiki'})
        self.repo_data.get_repo_index_info.return_value = ('head', 'wiki', False)
        task = scheduler_module.IndexTask(0)
        task.content = True
        manager.process_task('repo', task)
        self.assertIsNone(manager.index_manager)

    def test_missing_head_is_skipped_without_initializing_index(self):
        self.repo_data.get_repo_index_info.return_value = (None, None, False)
        task = scheduler_module.IndexTask(0)
        task.content = True
        self.manager.index_manager = None
        self.manager.process_task('repo', task)
        self.manager.ensure_indexes.assert_not_called()
        self.ns['IndexManager'].assert_not_called()
        self.ns['logger'].warning.assert_called_once()
        # Cleanup-only candidates of existing repositories need neither a head nor a warning.
        self.ns['logger'].reset_mock()
        task.content = False
        task.cleanup.add('file')
        self.manager.process_task('repo', task)
        self.ns['logger'].warning.assert_not_called()

    def test_network_initialization_failure_still_retries(self):
        self.manager.scheduler = IndexScheduler(debounce=0)
        self.manager.scheduler.submit('repo', content=True)
        self.repo_data.get_repo_index_info.return_value = ('head', None, False)
        self.manager.ensure_indexes.side_effect = ConnectionError('temporarily unavailable')
        original_retry = self.manager.scheduler.retry

        def retry_and_stop(repo_id, task):
            original_retry(repo_id, task)
            self.manager.finished.set()

        self.manager.scheduler.retry = retry_and_stop
        self.manager.run_worker()
        self.assertEqual(self.manager.scheduler.pending['repo'].failures, 1)

    def test_description_message_filters_and_deduplicates(self):
        repo_id = 'a' * 36
        self.manager.handle_message(json.dumps({
            'msg_type': 'metadata-rows-updated', 'repo_id': repo_id, 'rows': [
                {'row_id': 'r1', 'updated_columns': ['_description']},
                {'row_id': 'r1', 'updated_columns': ['_description', '_size']},
                {'row_id': 'r2', 'updated_columns': ['_size']}, None,
            ]}))
        self.assertEqual(self.manager.scheduler.pending[repo_id].row_ids, {'r1'})

    def test_idle_subscription_recreated_and_message_resets_timer(self):
        first, second, mq = MagicMock(), MagicMock(), Mock()
        first.__enter__.return_value = first
        second.__enter__.return_value = second
        first.get_message.side_effect = [None, {'data': 'event'}, None, None]

        def stop_reading(**kwargs):
            self.manager.finished.set()
            return None

        second.get_message.side_effect = stop_reading
        mq.pubsub.side_effect = [first, second]
        self.manager.mq = mq
        self.ns.update({
            'REDIS_HOST': 'redis', 'REDIS_PORT': 6379, 'REDIS_PASSWORD': 'password',
            'get_mq': Mock(return_value=mq),
            # subscribe=0, empty=299, message=300, empty=599, idle=600,
            # resubscribe=600, final empty=600.
            'time': SimpleNamespace(monotonic=Mock(side_effect=[0, 299, 300, 599, 600, 600, 600])),
        })
        self.manager.handle_message = Mock()
        self.manager.handle_events()
        self.ns['get_mq'].assert_not_called()
        first.subscribe.assert_called_once_with('repo_update', 'metadata_update_channel')
        second.subscribe.assert_called_once_with('repo_update', 'metadata_update_channel')
        self.assertEqual(first.get_message.call_count, 4)
        first.__exit__.assert_called_once()
        second.__exit__.assert_called_once()
        self.manager.handle_message.assert_called_once_with('event')
        self.ns['logger'].info.assert_called_once()
        self.ns['logger'].exception.assert_not_called()

    def test_initial_mq_failure_is_retried(self):
        mq, subscriber = Mock(), MagicMock()
        subscriber.__enter__.return_value = subscriber
        mq.pubsub.return_value = subscriber
        subscriber.get_message.side_effect = lambda **kwargs: self.manager.finished.set()
        self.manager.mq = None
        self.ns.update({
            'REDIS_HOST': 'redis', 'REDIS_PORT': 6379, 'REDIS_PASSWORD': '',
            'get_mq': Mock(side_effect=[None, mq]),
            'time': SimpleNamespace(monotonic=lambda: 0),
        })
        self.manager.finished.wait = Mock()
        self.manager.handle_events()
        self.assertEqual(self.ns['get_mq'].call_count, 2)
        self.manager.finished.wait.assert_called_once_with(1)
        subscriber.subscribe.assert_called_once_with('repo_update', 'metadata_update_channel')

    def test_subscription_exception_closes_and_retries(self):
        first, second, mq = MagicMock(), MagicMock(), Mock()
        first.__enter__.return_value = first
        second.__enter__.return_value = second
        first.get_message.side_effect = ConnectionError('connection dropped')
        second.get_message.side_effect = lambda **kwargs: self.manager.finished.set()
        mq.pubsub.side_effect = [first, second]
        self.manager.mq = mq
        self.ns.update({
            'REDIS_HOST': 'redis', 'REDIS_PORT': 6379, 'REDIS_PASSWORD': '',
            'get_mq': Mock(return_value=mq),
            'time': SimpleNamespace(monotonic=lambda: 0),
        })
        self.manager.finished.wait = Mock()
        self.manager.handle_events()
        first.__exit__.assert_called_once()
        second.subscribe.assert_called_once_with('repo_update', 'metadata_update_channel')
        self.manager.finished.wait.assert_called_once_with(1)
        self.ns['logger'].exception.assert_called_once()

    def test_latest_head_and_wiki_routing(self):
        scheduler = IndexScheduler(debounce=0)
        scheduler.submit('repo', content=True)
        _, task = scheduler.take()
        self.repo_data.get_repo_index_info.return_value = ('latest', 'wiki', False)
        self.manager.process_task('repo', task)
        self.manager.index_manager.update_wiki_index.assert_called_once_with(
            'repo', 'latest', self.manager.indexes['wiki'], self.manager.status_indexes['wiki'])
        self.manager.index_manager.update_library_file_index.assert_not_called()

    def test_content_before_description_and_compensation(self):
        scheduler = IndexScheduler(debounce=0)
        scheduler.submit('repo', content=True, metadata=True, row_ids=['r1'])
        _, task = scheduler.take()
        self.repo_data.get_repo_index_info.return_value = ('latest', None, False)
        self.manager.process_task('repo', task)
        calls = self.manager.index_manager.method_calls
        self.assertEqual([call[0] for call in calls], [
            'update_library_file_index', 'update_library_file_descriptions',
            'compensate_library_descriptions'])

    def test_cleanup_rechecks_existence(self):
        task = scheduler_module.IndexTask(0)
        task.cleanup.add('file')
        self.repo_data.get_repo_index_info.return_value = ('head', None, False)
        self.manager.process_task('repo', task)
        self.manager.index_manager.delete_repo_file_index.assert_not_called()
        self.repo_data.get_repo_index_info.return_value = None
        self.manager.process_task('repo', task)
        self.manager.index_manager.delete_repo_file_index.assert_called_once()

    def test_failed_initialization_does_not_disable_other_kind(self):
        del self.manager.ensure_indexes
        self.manager.indexes = {}
        self.manager.status_indexes = {}
        self.ns.update({
            'get_opt_from_conf_or_env': lambda config, section, key, *args, **kw: kw.get('default', key),
            'SeaSearchAPI': Mock(), 'RepoStatusIndex': Mock(), 'RepoFileIndex': Mock(),
            'WikiStatusIndex': Mock(side_effect=RuntimeError('Wiki unavailable')),
            'WikiIndex': Mock(), 'SHARD_NUM': 1,
            'REPO_STATUS_FILE_INDEX_NAME': 'files', 'WIKI_STATUS_INDEX_NAME': 'wikis',
        })
        with self.assertRaises(RuntimeError):
            self.manager.ensure_indexes('wiki')
        self.manager.ensure_indexes('file')
        self.assertIn('file', self.manager.indexes)
        self.assertNotIn('wiki', self.manager.indexes)


class RebuildRecoveryTests(unittest.TestCase):
    def exercise_rebuild(self, kind, failure=None, head_after_restart='A'):
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: False,
            'REPO_FILE_INDEX_PREFIX': 'file_', 'WIKI_INDEX_PREFIX': 'wiki_',
            'ZERO_OBJ_ID': 'zero', 'RequestException': ConnectionError, 'logger': Mock(),
        })
        state = {'commit': 'A', 'target': None, 'watermark': 100}
        exists = [False]
        documents = {}
        history = []
        index, statuses = Mock(), Mock()
        index.check_index.side_effect = lambda name: exists[0]

        def read_status(repo_id):
            history.append('read')
            if failure == 'read':
                raise RuntimeError('status read failed')
            # Return a snapshot, like a real HTTP GET, rather than a live view.
            target = state['target']
            return SimpleNamespace(from_commit=state['commit'], to_commit=target,
                                   metadata_updated_time=state['watermark'],
                                   need_recovery=lambda: target is not None)

        def begin(repo_id, old, new, *watermark):
            history.append(('begin', old, new))
            if failure == 'begin':
                raise RuntimeError('status persistence failed')
            state.update(commit=old, target=new)
            if watermark:
                state['watermark'] = watermark[0]

        def create(name):
            history.append('create')
            self.assertEqual(state['commit'], 'zero')
            self.assertEqual(state['target'], 'A')
            if failure == 'before_create':
                raise SystemExit('restart after persisting rebuild intent')
            exists[0] = True
            if failure == 'after_create':
                raise SystemExit('restart immediately after creating empty content index')
            if failure == 'create_response':
                raise ConnectionError('creation succeeded but response lost')

        def update(name, repo_id, old, new, *args):
            if old == 'zero':
                documents['unchanged-file'] = 'body'
            documents['latest'] = new

        def finish(repo_id, new, *watermark):
            state.update(commit=new, target=None)
            if watermark:
                state['watermark'] = watermark[0]

        statuses.get_repo_status_by_id.side_effect = read_status
        statuses.begin_update_repo.side_effect = begin
        statuses.finish_update_repo.side_effect = finish
        index.create_index_if_missing.side_effect = create
        index.update.side_effect = update

        def invoke():
            manager = cls.__new__(cls)
            manager.session, manager.metadata_server_api = Mock(), Mock()
            if kind == 'file':
                return manager.update_library_file_index(
                    'repo', 'A', index, statuses, None, update_metadata=False)
            return manager.update_wiki_index('repo', 'A', index, statuses)

        if failure in ('before_create', 'after_create'):
            with self.assertRaises(SystemExit):
                invoke()
        elif failure == 'create_response':
            with self.assertRaises(ConnectionError):
                invoke()
        elif failure in ('read', 'begin'):
            self.assertFalse(invoke())
            index.create_index_if_missing.assert_not_called()
            self.assertFalse(exists[0])
            self.assertEqual(state['commit'], 'A')
            return
        else:
            self.assertTrue(invoke())
            self.assertEqual(history[:3], ['read', ('begin', 'zero', 'A'), 'create'])
            self.assertEqual(documents['unchanged-file'], 'body')
            self.assertIsNone(state['target'])
            if kind == 'file':
                self.assertIsNone(state['watermark'])
            return

        self.assertEqual(state['commit'], 'zero')
        self.assertEqual(state['target'], 'A')
        if kind == 'file':
            self.assertIsNone(state['watermark'])
        # A new process reads the same persisted status and the existing empty index.
        had_empty_index = exists[0]
        failure = None
        index.reset_mock()
        index.create_index_if_missing.side_effect = lambda name: exists.__setitem__(0, True)
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        if kind == 'file':
            result = manager.update_library_file_index(
                'repo', head_after_restart, index, statuses, None, update_metadata=False)
        else:
            result = manager.update_wiki_index('repo', head_after_restart, index, statuses)
        self.assertTrue(result)
        self.assertEqual(index.update.call_args_list[0].args[2], 'zero')
        self.assertEqual(documents['unchanged-file'], 'body')
        self.assertEqual(documents['latest'], head_after_restart)
        self.assertEqual(state['commit'], head_after_restart)
        self.assertIsNone(state['target'])
        if had_empty_index and head_after_restart == 'B':
            # Existing empty index must replay ZERO->A before advancing to newer head B.
            self.assertEqual([call.args[2:4] for call in index.update.call_args_list],
                             [('zero', 'A'), ('A', 'B')])

    def test_creation_interruption_rebuilds_even_when_head_is_unchanged(self):
        for kind in ('file', 'wiki'):
            for failure in ('before_create', 'after_create', 'create_response'):
                with self.subTest(kind=kind, failure=failure):
                    self.exercise_rebuild(kind, failure)

    def test_creation_interruption_recovers_then_catches_up_to_new_head(self):
        for kind in ('file', 'wiki'):
            with self.subTest(kind=kind):
                self.exercise_rebuild(kind, 'after_create', head_after_restart='B')

    def test_failed_status_io_never_creates_an_empty_index(self):
        for kind in ('file', 'wiki'):
            for failure in ('read', 'begin'):
                with self.subTest(kind=kind, failure=failure):
                    self.exercise_rebuild(kind, failure)

    def test_normal_missing_index_rebuild_persists_intent_before_creation(self):
        for kind in ('file', 'wiki'):
            with self.subTest(kind=kind):
                self.exercise_rebuild(kind)


class DescriptionTests(unittest.TestCase):
    def test_realtime_description_missing_then_visible_retries_with_row_ids(self):
        rows = [{'_id': 'row', '_parent_dir': '/', '_name': 'a.txt', '_description': 'new'}]
        manager_cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: True,
            'REPO_FILE_INDEX_PREFIX': 'file_', 'get_metadata_by_row_ids': Mock(return_value=rows),
        })
        index_cls = load_class('seasearch/index_store/repo_file_index.py', 'RepoFileIndex', {
            'os': os, 'md5': lambda path: path, 'is_sys_dir_or_file': lambda path: False,
        })
        manager = manager_cls.__new__(manager_cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        index = index_cls.__new__(index_cls)
        index.seasearch_api = Mock()
        index.check_index = Mock(return_value=True)
        index.seasearch_api.get_document_by_id.side_effect = [
            {'error': 'not found'},
            {'_source': {'path': '/a.txt', 'content': 'body', 'description': 'old'}},
        ]
        scheduler = IndexScheduler(debounce=0, throttle=0)
        with patch.object(scheduler_module.time, 'monotonic', return_value=100) as clock:
            scheduler.submit('repo', row_ids=['row'])
            repo_id, task = scheduler.take()
            with self.assertRaisesRegex(RuntimeError, 'not yet indexed'):
                manager.update_library_file_descriptions(repo_id, task.row_ids, index)
            scheduler.retry(repo_id, task)
            scheduler.finish(repo_id, task)
            self.assertEqual(scheduler.pending['repo'].row_ids, {'row'})
            self.assertEqual(scheduler.pending['repo'].failures, 1)
            index.seasearch_api.update_document_by_id.assert_not_called()
            clock.return_value = 101
            repo_id, task = scheduler.take()
            manager.update_library_file_descriptions(repo_id, task.row_ids, index)
            scheduler.finish(repo_id, task)
        index.seasearch_api.update_document_by_id.assert_called_once_with(
            'file_repo', '/a.txt', {'path': '/a.txt', 'content': 'body', 'description': 'new'})
        self.assertFalse(scheduler.pending or scheduler.running)

    def test_description_index_missing_raises_but_deleted_rows_are_skipped(self):
        enabled, query = Mock(return_value=True), Mock(return_value=[])
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': enabled, 'get_metadata_by_row_ids': query,
            'REPO_FILE_INDEX_PREFIX': 'file_',
        })
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        index = Mock()
        index.check_index.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'File index not yet available'):
            manager.update_library_file_descriptions('repo', ['row'], index)
        query.assert_not_called()
        index.check_index.return_value = True
        index.update_descriptions.return_value = True
        manager.update_library_file_descriptions('repo', ['deleted-row'], index)
        index.update_descriptions.assert_called_once_with('file_repo', [])
        enabled.return_value = False
        index.reset_mock()
        manager.update_library_file_descriptions('repo', ['row'], index)
        index.check_index.assert_not_called()

    def test_description_probe_is_read_only_and_limits_results(self):
        enabled = Mock(return_value=True)
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': enabled, 'timestamp_to_isoformat_timestr': str,
            'METADATA_TABLE': SimpleNamespace(name='Table1'),
        })
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        manager.metadata_server_api.query_rows.return_value = {'results': []}
        self.assertFalse(manager.has_description_changes('repo', 100))
        sql = manager.metadata_server_api.query_rows.call_args.args[1]
        self.assertIn("_mtime` >= '99.0'", sql)
        self.assertTrue(sql.endswith('LIMIT 1'))
        manager.metadata_server_api.query_rows.return_value = {'results': [{'_id': 'row'}]}
        self.assertTrue(manager.has_description_changes('repo', 100))
        enabled.return_value = False
        manager.metadata_server_api.reset_mock()
        self.assertFalse(manager.has_description_changes('repo', 100))
        manager.metadata_server_api.query_rows.assert_not_called()

    def test_restart_replays_partial_wiki_before_latest_head(self):
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'WIKI_INDEX_PREFIX': 'wiki_', 'ZERO_OBJ_ID': 'zero',
            'RequestException': ConnectionError, 'logger': Mock(),
        })
        state = {'commit': 'W1', 'target': None}
        statuses, index = Mock(), Mock()
        statuses.get_repo_status_by_id.side_effect = lambda repo: SimpleNamespace(
            from_commit=state['commit'], to_commit=state['target'],
            need_recovery=lambda: state['target'] is not None)

        def begin(repo, old, new):
            state.update(commit=old, target=new)

        def finish(repo, new):
            state.update(commit=new, target=None)

        statuses.begin_update_repo.side_effect = begin
        statuses.finish_update_repo.side_effect = finish
        index.update.side_effect = SystemExit('process terminated')
        with self.assertRaises(SystemExit):
            cls.__new__(cls).update_wiki_index('repo', 'W2', index, statuses)
        self.assertEqual(state, {'commit': 'W1', 'target': 'W2'})
        index.reset_mock()
        index.update.side_effect = None
        self.assertTrue(cls.__new__(cls).update_wiki_index('repo', 'W3', index, statuses))
        self.assertEqual([call.args[2:4] for call in index.update.call_args_list],
                         [('W1', 'W2'), ('W2', 'W3')])
        self.assertEqual(state, {'commit': 'W3', 'target': None})

    def test_restart_repeats_description_range_when_watermark_write_interrupted(self):
        query = Mock(return_value=[{'_id': 'row'}])
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: True,
            'time': SimpleNamespace(time=lambda: 200),
            'timestamp_to_isoformat_timestr': str, 'query_metadata_rows': query,
            'METADATA_TABLE': SimpleNamespace(name='Table1'), 'REPO_FILE_INDEX_PREFIX': 'file_',
        })
        index, statuses = Mock(), Mock()
        statuses.get_repo_status_by_id.return_value = SimpleNamespace(
            metadata_updated_time=100, from_commit='head', to_commit=None)
        statuses.add_repo_status.side_effect = SystemExit('killed before watermark persistence')
        first = cls.__new__(cls)
        first.session, first.metadata_server_api = Mock(), Mock()
        with self.assertRaises(SystemExit):
            first.compensate_library_descriptions('repo', index, statuses)
        statuses.add_repo_status.side_effect = None
        restarted = cls.__new__(cls)
        restarted.session, restarted.metadata_server_api = Mock(), Mock()
        restarted.compensate_library_descriptions('repo', index, statuses)
        self.assertEqual(query.call_args_list[0].args[2], query.call_args_list[1].args[2])
        self.assertEqual(index.update_descriptions.call_count, 2)

    def test_unchanged_description_never_writes_document(self):
        cls = load_class('seasearch/index_store/repo_file_index.py', 'RepoFileIndex', {
            'os': os, 'md5': lambda path: path, 'is_sys_dir_or_file': lambda path: False,
        })
        index = cls.__new__(cls)
        index.seasearch_api = Mock()
        for old, new in [('same', 'same'), ('', None), (None, '')]:
            index.seasearch_api.get_document_by_id.return_value = {
                '_source': {'path': '/a.txt', 'description': old, 'content': 'body'}}
            self.assertTrue(index.update_descriptions('file_repo', [{
                '_parent_dir': '/', '_name': 'a.txt', '_description': new}]))
        index.seasearch_api.update_document_by_id.assert_not_called()

    def test_restart_replays_partial_content_before_latest_head(self):
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: False,
            'REPO_FILE_INDEX_PREFIX': 'file_', 'ZERO_OBJ_ID': 'zero',
            'RequestException': ConnectionError, 'logger': Mock(),
        })
        state = {'commit': 'C1', 'target': None}
        statuses, index = Mock(), Mock()
        statuses.get_repo_status_by_id.side_effect = lambda repo: SimpleNamespace(
            from_commit=state['commit'], to_commit=state['target'], metadata_updated_time=100,
            need_recovery=lambda: state['target'] is not None)

        def begin(repo, old, new, watermark):
            state.update(commit=old, target=new)

        def finish(repo, new, watermark):
            state.update(commit=new, target=None)

        statuses.begin_update_repo.side_effect = begin
        statuses.finish_update_repo.side_effect = finish
        first = cls.__new__(cls)
        first.session, first.metadata_server_api = Mock(), Mock()
        index.update.side_effect = SystemExit('process terminated during bulk write')
        with self.assertRaises(SystemExit):
            first.update_library_file_index('repo', 'C2', index, statuses, None, update_metadata=False)
        self.assertEqual(state, {'commit': 'C1', 'target': 'C2'})
        # Fresh manager/process, same external status storage; head advanced while stopped.
        restarted = cls.__new__(cls)
        restarted.session, restarted.metadata_server_api = Mock(), Mock()
        index.reset_mock()
        index.update.side_effect = None
        self.assertTrue(restarted.update_library_file_index(
            'repo', 'C3', index, statuses, None, update_metadata=False))
        self.assertEqual([call.args[2:4] for call in index.update.call_args_list],
                         [('C1', 'C2'), ('C2', 'C3')])
        self.assertEqual(state, {'commit': 'C3', 'target': None})

    def test_failed_diff_never_finishes_commit(self):
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: False,
            'REPO_FILE_INDEX_PREFIX': 'file_', 'ZERO_OBJ_ID': 'zero',
            'RequestException': ConnectionError, 'logger': Mock(),
        })
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        index, statuses = Mock(), Mock()
        statuses.get_repo_status_by_id.return_value = SimpleNamespace(
            from_commit='C1', to_commit=None, metadata_updated_time=100,
            need_recovery=lambda: False)
        index.update.side_effect = RuntimeError('diff failed')
        self.assertFalse(manager.update_library_file_index(
            'repo', 'C2', index, statuses, None, update_metadata=False))
        statuses.finish_update_repo.assert_not_called()

    def test_equal_head_with_recovery_is_not_skipped(self):
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: False,
            'REPO_FILE_INDEX_PREFIX': 'file_', 'ZERO_OBJ_ID': 'zero',
            'RequestException': ConnectionError, 'logger': Mock(),
        })
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        index, statuses = Mock(), Mock()
        statuses.get_repo_status_by_id.return_value = SimpleNamespace(
            from_commit='C1', to_commit='C2', metadata_updated_time=100,
            need_recovery=lambda: True)
        self.assertTrue(manager.update_library_file_index(
            'repo', 'C1', index, statuses, None, update_metadata=False))
        self.assertEqual(index.update.call_count, 2)
        self.assertEqual(index.update.call_args.args[2:4], ('C2', 'C1'))
        statuses.finish_update_repo.assert_called_once_with('repo', 'C1', 100)

    def test_preserves_content_and_clears_description(self):
        cls = load_class('seasearch/index_store/repo_file_index.py', 'RepoFileIndex', {
            'os': os, 'md5': lambda path: path, 'is_sys_dir_or_file': lambda path: False,
        })
        index = cls.__new__(cls)
        index.seasearch_api = Mock()
        source = {'path': '/a.txt', 'content': 'new-content', 'size': 100, 'description': 'old'}
        index.seasearch_api.get_document_by_id.return_value = {'_source': source}
        for description in ['new', '', None]:
            self.assertTrue(index.update_descriptions('file_repo', [{
                '_parent_dir': '/', '_name': 'a.txt', '_description': description,
                '_obj_id': 'old-object', '_size': 1}]))
            written = index.seasearch_api.update_document_by_id.call_args.args[2]
            self.assertEqual(written, {**source, 'description': description or ''})
        self.assertEqual(source['description'], 'old')

    def test_failed_description_pass_does_not_advance_watermark(self):
        query = Mock(return_value=[{'_id': 'row'}])
        cls = load_class('seasearch/index_store/index_manager.py', 'IndexManager', {
            'need_index_metadata_info': lambda *args: True,
            'time': SimpleNamespace(time=lambda: 200),
            'timestamp_to_isoformat_timestr': str, 'query_metadata_rows': query,
            'METADATA_TABLE': SimpleNamespace(name='Table1'), 'REPO_FILE_INDEX_PREFIX': 'file_',
        })
        manager = cls.__new__(cls)
        manager.session, manager.metadata_server_api = Mock(), Mock()
        index, statuses = Mock(), Mock()
        statuses.get_repo_status_by_id.return_value = SimpleNamespace(
            metadata_updated_time=100, from_commit='head', to_commit=None)
        index.update_descriptions.return_value = False
        with self.assertRaises(RuntimeError):
            manager.compensate_library_descriptions('repo', index, statuses)
        statuses.add_repo_status.assert_not_called()
        index.update_descriptions.return_value = True
        manager.compensate_library_descriptions('repo', index, statuses)
        statuses.add_repo_status.assert_called_once_with('repo', 'head', None, 200)
        self.assertIn("_mtime` >= '99.0'", query.call_args.args[2])


if __name__ == '__main__':
    unittest.main()
