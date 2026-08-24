import importlib.util
import logging
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture(scope='module')
def virus_scan_module():
    package_name = 'virus_scanner_scan_for_test'
    package_dir = Path(__file__).resolve().parents[2] / 'virus_scanner'
    package = types.ModuleType(package_name)
    package.__path__ = [str(package_dir)]

    fake_seafobj = types.ModuleType('seafobj')
    fake_seafobj.fs_mgr = types.SimpleNamespace()
    fake_seafobj.block_mgr = types.SimpleNamespace()
    fake_seafobj.commit_mgr = types.SimpleNamespace()
    fake_seaserv = types.ModuleType('seaserv')
    fake_seaserv.seafile_api = types.SimpleNamespace()

    fake_db_oper = types.ModuleType(package_name + '.db_oper')
    fake_db_oper.DBOper = object
    fake_commit_differ = types.ModuleType(package_name + '.commit_differ')
    fake_commit_differ.CommitDiffer = object
    fake_thread_pool = types.ModuleType(package_name + '.thread_pool')
    fake_thread_pool.ThreadPool = object
    fake_scan_settings = types.ModuleType(package_name + '.scan_settings')
    fake_scan_settings.logger = logging.getLogger('virus_scan_test')

    fake_seafevents = types.ModuleType('seafevents')
    fake_seafevents.__path__ = []
    fake_utils = types.ModuleType('seafevents.utils')
    fake_utils.get_python_executable = lambda: 'python'
    fake_app = types.ModuleType('seafevents.app')
    fake_app.__path__ = []
    fake_config = types.ModuleType('seafevents.app.config')
    fake_config.SEAHUB_DIR = '/tmp'

    module_names = {
        package_name: package,
        package_name + '.db_oper': fake_db_oper,
        package_name + '.commit_differ': fake_commit_differ,
        package_name + '.thread_pool': fake_thread_pool,
        package_name + '.scan_settings': fake_scan_settings,
        'seafobj': fake_seafobj,
        'seaserv': fake_seaserv,
        'seafevents': fake_seafevents,
        'seafevents.utils': fake_utils,
        'seafevents.app': fake_app,
        'seafevents.app.config': fake_config,
    }
    saved = {name: sys.modules.get(name) for name in module_names}
    sys.modules.update(module_names)

    try:
        path = package_dir / 'virus_scan.py'
        spec = importlib.util.spec_from_file_location(
            package_name + '.virus_scan', path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original

    return module


def test_records_deleted_virus_reuploaded_unchanged_after_commit_diff(
        virus_scan_module, monkeypatch):
    module = virus_scan_module
    repo_id = 'repo-id'
    scan_commit_id = 'scan-commit-id'
    head_commit_id = 'head-commit-id'
    file_id = 'virus-file-id'
    repo = types.SimpleNamespace(id=repo_id, version=2)

    get_file_id = Mock(return_value=file_id)
    commits = {
        scan_commit_id: types.SimpleNamespace(
            root_id='scan-root-id', ctime=1787533200),
        head_commit_id: types.SimpleNamespace(
            root_id='head-root-id', ctime=1787533800),
    }
    get_repo = Mock(return_value=repo)
    get_commit = Mock(
        side_effect=lambda actual_repo_id, version, commit_id: commits[commit_id])
    monkeypatch.setattr(
        module, 'seafile_api',
        types.SimpleNamespace(
            get_repo=get_repo,
            get_commit=get_commit,
            get_file_id_by_commit_and_path=get_file_id,
        ),
        raising=False,
    )

    differ = Mock()
    differ.diff.return_value = []
    commit_differ = Mock(return_value=differ)
    monkeypatch.setattr(module, 'CommitDiffer', commit_differ)
    root_ids = {
        scan_commit_id: 'scan-root-id',
        head_commit_id: 'head-root-id',
    }
    get_commit_root_id = Mock(
        side_effect=lambda actual_repo_id, version, commit_id: root_ids[commit_id])
    monkeypatch.setattr(
        module, 'commit_mgr',
        types.SimpleNamespace(get_commit_root_id=get_commit_root_id))

    db_oper = types.SimpleNamespace(
        get_deleted_virus_files=Mock(return_value=[
            ('/nested/eicar.com.txt', file_id, 'Eicar-Signature'),
        ]),
        add_virus_record=Mock(return_value=0),
        update_vscan_record=Mock(),
    )
    scanner = module.VirusScan.__new__(module.VirusScan)
    scanner.db_oper = db_oper
    scanner.settings = types.SimpleNamespace(
        enable_send_mail=False,
        scan_cmd='clamdscan',
    )

    scanner.scan_virus(module.ScanTask(
        repo_id, head_commit_id, scan_commit_id))

    get_repo.assert_called_once_with(repo_id)
    assert get_commit_root_id.call_args_list == [
        ((repo_id, 1, scan_commit_id),),
        ((repo_id, 1, head_commit_id),),
    ]
    assert get_commit.call_args_list == [
        ((repo_id, 2, scan_commit_id),),
        ((repo_id, 2, head_commit_id),),
    ]
    commit_differ.assert_called_once_with(
        repo_id, 1, 'scan-root-id', 'head-root-id')
    db_oper.get_deleted_virus_files.assert_called_once_with(
        repo_id,
        datetime(2026, 8, 24, 1, 0, 0),
        datetime(2026, 8, 24, 1, 10, 0),
    )
    get_file_id.assert_called_once_with(
        repo_id, head_commit_id, '/nested/eicar.com.txt')
    db_oper.add_virus_record.assert_called_once_with([
        (repo_id, head_commit_id, '/nested/eicar.com.txt',
         file_id, 'Eicar-Signature'),
    ])
    db_oper.update_vscan_record.assert_called_once_with(repo_id, head_commit_id)
