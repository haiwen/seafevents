import importlib.util
import logging
import sys
import types
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


def test_persists_file_id_and_returns_only_deleted_virus_files(db_modules):
    models, db_oper, base = db_modules
    engine = create_engine('sqlite:///:memory:')
    base.metadata.create_all(engine)
    session_cls = sessionmaker(bind=engine)
    settings = types.SimpleNamespace(
        session_cls=session_cls,
        seaf_session_cls=None,
    )
    oper = db_oper.DBOper(settings)

    assert oper.add_virus_record([
        ('repo-id', 'commit-id', '/eicar.com.txt', 'virus-id', 'Eicar-Signature'),
    ]) == 0

    with session_cls() as session:
        stored = session.scalars(select(models.VirusFile)).one()
        assert stored.file_id == 'virus-id'
        stored.has_deleted = True
        session.add(models.VirusFile(
            repo_id='repo-id',
            commit_id='old-commit-id',
            file_path='/legacy-virus.txt',
            file_id=None,
            has_deleted=True,
            has_ignored=False,
        ))
        session.add(models.VirusFile(
            repo_id='other-repo-id',
            commit_id='other-commit-id',
            file_path='/eicar.com.txt',
            file_id='other-virus-id',
            has_deleted=True,
            has_ignored=False,
        ))
        session.commit()

    assert oper.get_deleted_virus_files('repo-id') == {
        ('/eicar.com.txt', 'virus-id'),
    }
