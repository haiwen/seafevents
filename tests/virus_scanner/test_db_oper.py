import importlib.util
import logging
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, sessionmaker


@pytest.fixture(scope='module')
def db_modules():
    class TestBase(DeclarativeBase):
        pass

    package_name = 'virus_scanner_for_test'
    package_dir = Path(__file__).resolve().parents[2] / 'virus_scanner'

    package = types.ModuleType(package_name)
    package.__path__ = [str(package_dir)]

    fake_seafevents = types.ModuleType('seafevents')
    fake_seafevents.__path__ = []
    fake_db = types.ModuleType('seafevents.db')
    fake_db.Base = TestBase
    fake_db.SeafBase = types.SimpleNamespace(classes=types.SimpleNamespace())

    fake_scan_settings = types.ModuleType(package_name + '.scan_settings')
    fake_scan_settings.logger = logging.getLogger('virus_scan_test')

    saved_modules = {
        name: sys.modules.get(name)
        for name in ('seafevents', 'seafevents.db')
    }
    sys.modules[package_name] = package
    sys.modules[package_name + '.scan_settings'] = fake_scan_settings
    sys.modules['seafevents'] = fake_seafevents
    sys.modules['seafevents.db'] = fake_db

    try:
        models = load_module(package_name + '.models', package_dir / 'models.py')
        db_oper = load_module(package_name + '.db_oper', package_dir / 'db_oper.py')
    finally:
        for name, original in saved_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original

    return models, db_oper, TestBase


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def db_oper(db_modules):
    models, db_oper_module, base = db_modules
    engine = create_engine('sqlite:///:memory:')
    base.metadata.create_all(engine)
    session_cls = sessionmaker(bind=engine)
    settings = types.SimpleNamespace(
        session_cls=session_cls,
        seaf_session_cls=None,
    )
    return models, db_oper_module, db_oper_module.DBOper(settings), session_cls


def test_persists_file_id_and_marks_actual_delete_time(db_oper):
    models, db_oper_module, oper, session_cls = db_oper

    assert oper.add_virus_record([
        ('repo-id', 'commit-id', '/eicar.com.txt', 'virus-id', 'Eicar-Signature'),
    ]) == 0

    with session_cls() as session:
        stored = session.scalars(select(models.VirusFile)).one()
        assert stored.file_id == 'virus-id'
        assert stored.deleted_at is None
        assert db_oper_module.delete_virus_file(session, stored.vid) == 0

    with session_cls() as session:
        stored = session.scalars(select(models.VirusFile)).one()
        assert stored.has_deleted is True
        assert isinstance(stored.deleted_at, datetime)
        assert stored.deleted_at.microsecond == 0


def test_returns_only_deleted_files_in_commit_time_window(db_oper):
    models, _, oper, session_cls = db_oper
    window_start = datetime(2026, 8, 24, 1, 0, 0)
    window_end = datetime(2026, 8, 24, 1, 10, 0)

    with session_cls() as session:
        session.add_all([
            models.VirusFile(
                repo_id='repo-id', commit_id='commit-id',
                file_path='/match.txt', file_id='match-id',
                has_deleted=True, has_ignored=False,
                virus_signature='Match',
                deleted_at=datetime(2026, 8, 24, 1, 5, 0),
            ),
            models.VirusFile(
                repo_id='repo-id', commit_id='commit-id',
                file_path='/boundary.txt', file_id='boundary-id',
                has_deleted=True, has_ignored=False,
                virus_signature='Boundary',
                deleted_at=window_end,
            ),
            models.VirusFile(
                repo_id='repo-id', commit_id='commit-id',
                file_path='/too-old.txt', file_id='old-id',
                has_deleted=True, has_ignored=False,
                deleted_at=datetime(2026, 8, 24, 0, 59, 59),
            ),
            models.VirusFile(
                repo_id='repo-id', commit_id='commit-id',
                file_path='/not-deleted.txt', file_id='active-id',
                has_deleted=False, has_ignored=False,
                deleted_at=None,
            ),
            models.VirusFile(
                repo_id='other-repo', commit_id='commit-id',
                file_path='/other.txt', file_id='other-id',
                has_deleted=True, has_ignored=False,
                deleted_at=datetime(2026, 8, 24, 1, 5, 0),
            ),
        ])
        session.commit()

    records = oper.get_deleted_virus_files(
        'repo-id', window_start, window_end)

    assert records == [
        ('/boundary.txt', 'boundary-id', 'Boundary'),
        ('/match.txt', 'match-id', 'Match'),
    ]
