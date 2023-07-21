# coding: utf-8
from sqlalchemy import or_, and_, select, update

from .models import VirusScanRecord, VirusFile
from .scan_settings import logger
from seafevents.db import SeafBase


class DBOper(object):
    def __init__(self, settings):
        self.edb_session = settings.session_cls
        self.seafdb_session = settings.seaf_session_cls

    def get_repo_list(self):
        session = self.seafdb_session()
        repo_list = []
        try:
            repo = SeafBase.classes.Repo
            branch = SeafBase.classes.Branch
            virtual_repo = SeafBase.classes.VirtualRepo

            # select r.repo_id, b.commit_id from Repo r, Branch b
            # where r.repo_id = b.repo_id and b.name = "master"
            # and r.repo_id not in (select repo_id from VirtualRepo)
            stmt = select(repo.repo_id, branch.commit_id).where(
                repo.repo_id == branch.repo_id, branch.name == 'master',
                repo.repo_id.not_in(select(virtual_repo.repo_id)))

            rows = session.execute(stmt).unique().all()
            for row in rows:
                repo_id, commit_id = row
                scan_commit_id = self.get_scan_commit_id(repo_id)
                repo_list.append((repo_id, commit_id, scan_commit_id))
        except Exception as e:
            logger.error('Failed to fetch repo list from db: %s.', e)
            repo_list = None
        finally:
            session.close()

        return repo_list

    def get_scan_commit_id(self, repo_id):
        session = self.edb_session()
        try:
            stmt = select(VirusScanRecord).where(VirusScanRecord.repo_id == repo_id).limit(1)
            r = session.scalars(stmt).first()
            scan_commit_id = r.scan_commit_id if r else None
            return scan_commit_id
        except Exception as e:
            logger.error(e)
        finally:
            session.close()

    def update_vscan_record(self, repo_id, scan_commit_id):
        session = self.edb_session()
        try:
            stmt = select(VirusScanRecord).where(VirusScanRecord.repo_id == repo_id).limit(1)
            r = session.scalars(stmt).first()
            if not r:
                vrecord = VirusScanRecord(repo_id, scan_commit_id)
                session.add(vrecord)
            else:
                stmt = update(VirusScanRecord).where(VirusScanRecord.repo_id == repo_id).\
                    values(scan_commit_id=scan_commit_id)
                session.execute(stmt)

            session.commit()
        except Exception as e:
            logger.warning('Failed to update virus scan record from db: %s.', e)
        finally:
            session.close()

    def add_virus_record(self, records):
        session = self.edb_session()
        try:
            session.add_all(VirusFile(repo_id, commit_id, file_path, 0, 0)
                            for repo_id, commit_id, file_path in records)
            session.commit()
            return 0
        except Exception as e:
            logger.warning('Failed to add virus records to db: %s.', e)
            return -1
        finally:
            session.close()


def get_virus_files(session, repo_id, has_handled, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    if has_handled not in (True, False, None):
        logger.error('has_handled must be True or False or None')
        raise RuntimeError('has_handled must be True or False or None')

    try:
        stmt = select(VirusFile)
        if repo_id:
            stmt = stmt.where(VirusFile.repo_id == repo_id)
        if has_handled is not None:
            if has_handled:
                stmt = stmt.where(or_(VirusFile.has_deleted == 1, VirusFile.has_ignored == 1))
            else:
                stmt = stmt.where(and_(VirusFile.has_deleted == 0, VirusFile.has_ignored == 0))
        stmt = stmt.slice(start, start+limit)
        return session.scalars(stmt).all()
    except Exception as e:
        logger.warning('Failed to get virus files from db: %s.', e)
        return None


def delete_virus_file(session, vid):
    try:
        stmt = update(VirusFile).where(VirusFile.vid == vid).values(has_deleted=1)
        session.execute(stmt)
        session.commit()
        return 0
    except Exception as e:
        logger.warning('Failed to delete virus file: %s.', e)
        return -1


def operate_virus_file(session, vid, ignore):
    try:
        stmt = update(VirusFile).where(VirusFile.vid == vid).values(has_ignored=ignore)
        session.execute(stmt)
        session.commit()
        return 0
    except Exception as e:
        logger.warning('Failed to operate virus file: %s.', e)
        return -1


def get_virus_file_by_vid(session, vid):
    try:
        stmt = select(VirusFile).where(VirusFile.vid == vid).limit(1)
        return session.scalars(stmt).first()
    except Exception as e:
        logger.warning('Failed to get virus file by vid: %s.', e)
        return None
