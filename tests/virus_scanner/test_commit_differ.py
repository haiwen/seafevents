import importlib.util
import sys
import types
from pathlib import Path

import pytest


class FakeDirent(object):
    def __init__(self, name, obj_id, entry_type, size=0):
        self.name = name
        self.id = obj_id
        self.type = entry_type
        self.size = size


class FakeSeafDir(object):
    def __init__(self, entries):
        self.entries = {entry.name: entry for entry in entries}

    def get_files_list(self):
        return [entry for entry in self.entries.values()
                if entry.type == 'file']

    def get_subdirs_list(self):
        return [entry for entry in self.entries.values()
                if entry.type == 'dir']

    def lookup_dent(self, name):
        return self.entries.get(name)

    def remove_entry(self, name):
        self.entries.pop(name, None)


class FakeFsManager(object):
    def __init__(self):
        self.directories = {}
        self.loaded_ids = []

    def add_dir(self, obj_id, *entries):
        self.directories[obj_id] = entries

    def load_seafdir(self, repo_id, version, obj_id):
        self.loaded_ids.append(obj_id)
        return FakeSeafDir(self.directories[obj_id])


@pytest.fixture(scope='module')
def commit_differ_module():
    fs_manager = FakeFsManager()
    seafobj = types.ModuleType('seafobj')
    seafobj.fs_mgr = fs_manager

    module_path = (Path(__file__).resolve().parents[2] /
                   'virus_scanner' / 'commit_differ.py')
    spec = importlib.util.spec_from_file_location(
        'virus_scanner_commit_differ_for_test', module_path)
    module = importlib.util.module_from_spec(spec)

    original_seafobj = sys.modules.get('seafobj')
    sys.modules['seafobj'] = seafobj
    try:
        spec.loader.exec_module(module)
    finally:
        if original_seafobj is None:
            sys.modules.pop('seafobj', None)
        else:
            sys.modules['seafobj'] = original_seafobj

    module.fake_fs_manager = fs_manager
    return module


@pytest.fixture
def fs_manager(commit_differ_module):
    manager = commit_differ_module.fake_fs_manager
    manager.directories.clear()
    manager.loaded_ids.clear()
    return manager


def file_entry(name, obj_id, size=1):
    return FakeDirent(name, obj_id, 'file', size)


def dir_entry(name, obj_id):
    return FakeDirent(name, obj_id, 'dir')


def test_rescans_deleted_virus_when_root_and_file_ids_are_unchanged(
        commit_differ_module, fs_manager):
    fs_manager.add_dir('root-id', file_entry('eicar.com.txt', 'virus-id', 68))

    differ = commit_differ_module.CommitDiffer(
        'repo-id', 1, 'root-id', 'root-id',
        virus_files={('eicar.com.txt', 'virus-id')},
    )

    assert differ.diff() == [('/eicar.com.txt', 'virus-id', 68)]


def test_follows_only_unchanged_directories_containing_deleted_virus_path(
        commit_differ_module, fs_manager):
    fs_manager.add_dir(
        'root-id',
        dir_entry('target', 'target-dir-id'),
        dir_entry('unrelated', 'unrelated-dir-id'),
    )
    fs_manager.add_dir('target-dir-id', dir_entry('nested', 'nested-dir-id'))
    fs_manager.add_dir(
        'nested-dir-id', file_entry('eicar.com.txt', 'virus-id', 68))
    fs_manager.add_dir(
        'unrelated-dir-id', file_entry('other.txt', 'other-file-id', 10))

    differ = commit_differ_module.CommitDiffer(
        'repo-id', 1, 'root-id', 'root-id',
        virus_files={('/target/nested/eicar.com.txt', 'virus-id')},
    )

    assert differ.diff() == [
        ('/target/nested/eicar.com.txt', 'virus-id', 68),
    ]
    assert 'unrelated-dir-id' not in fs_manager.loaded_ids


def test_does_not_rescan_same_file_id_at_a_different_path(
        commit_differ_module, fs_manager):
    fs_manager.add_dir('root-id', file_entry('copy.txt', 'virus-id', 68))

    differ = commit_differ_module.CommitDiffer(
        'repo-id', 1, 'root-id', 'root-id',
        virus_files={('/original.txt', 'virus-id')},
    )

    assert differ.diff() == []


def test_normal_content_change_is_still_scanned(
        commit_differ_module, fs_manager):
    fs_manager.add_dir('old-root', file_entry('document.txt', 'old-id', 10))
    fs_manager.add_dir('new-root', file_entry('document.txt', 'new-id', 20))

    differ = commit_differ_module.CommitDiffer(
        'repo-id', 1, 'old-root', 'new-root')

    assert differ.diff() == [('/document.txt', 'new-id', 20)]
