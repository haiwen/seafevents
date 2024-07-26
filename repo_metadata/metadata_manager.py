# coding: UTF-8

import logging
from datetime import datetime
from sqlalchemy import text

from seafobj.exceptions import GetObjectError
from seafobj import CommitDiffer, commit_mgr, fs_mgr
from seafevents.repo_data import repo_data


logger = logging.getLogger(__name__)

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'


def get_diff_files(repo_id, old_commit_id, new_commit_id):
    if old_commit_id == new_commit_id:
        return

    old_root = None
    if old_commit_id:
        try:
            old_commit = commit_mgr.load_commit(repo_id, 0, old_commit_id)
            old_root = old_commit.root_id
        except GetObjectError as e:
            logger.debug(e)
            old_root = None

    try:
        new_commit = commit_mgr.load_commit(repo_id, 0, new_commit_id)
    except GetObjectError as e:
        # new commit should exists in the obj store
        logger.warning(e)
        return

    new_root = new_commit.root_id
    version = new_commit.get_version()

    if old_root == new_root:
        return

    old_root = old_root if old_root else ZERO_OBJ_ID

    differ = CommitDiffer(repo_id, version, old_root, new_root, False, False)

    return differ.diff()


class MetadataManager(object):

    def __init__(self, session, repo_metadata):
        self.session = session
        self.repo_metadata = repo_metadata

    def update_metadata_index(self, repo_id, old_commit_id, new_commit_id):
        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, renamed_files, moved_files, \
        renamed_dirs, moved_dirs = get_diff_files(repo_id, old_commit_id, new_commit_id)

        self.repo_metadata.update(repo_id, added_files, deleted_files, added_dirs, deleted_dirs, modified_files,
                                  renamed_files, moved_files, renamed_dirs, moved_dirs, new_commit_id)

    def recovery(self, repo_id, from_commit, to_commit):
        logger.warning('%s: metadata in recovery', repo_id)
        self.update_metadata_index(repo_id, from_commit, to_commit)

        self.finish_update_metadata(repo_id, to_commit)

    def update_metadata(self, repo_id, latest_commit_id):
        with self.session() as session:
            sql = "SELECT enabled, from_commit, to_commit FROM repo_metadata WHERE repo_id='%s'" % repo_id

            record = session.execute(text(sql)).fetchone()

        if not record or not record[0]:
            return

        from_commit = record[1]
        to_commit = record[2]

        if to_commit:
            self.recovery(repo_id, from_commit, to_commit)
            from_commit = to_commit

        if latest_commit_id != from_commit:
            logger.info('Updating repo %s' % repo_id)
            logger.debug('latest_commit_id: %s, from_commit: %s' %
                         (latest_commit_id, from_commit))

            self.begin_update_metadata(repo_id, from_commit, latest_commit_id)
            self.update_metadata_index(repo_id, from_commit, latest_commit_id)
            self.finish_update_metadata(repo_id, latest_commit_id)
        else:
            logger.debug('Repo %s already uptdate', repo_id)

    def begin_update_metadata(self, repo_id, old_commit_id, new_commit_id):
        with self.session() as session:
            session.execute(
                text('update repo_metadata set from_commit=:from_commit, to_commit=:to_commit where repo_id=:repo_id'),
                {'from_commit': old_commit_id, 'to_commit': new_commit_id, 'repo_id': repo_id})
            session.commit()

    def finish_update_metadata(self, repo_id, new_commit_id):
        with self.session() as session:
            session.execute(
                text('update repo_metadata set from_commit=:from_commit, to_commit=:to_commit where repo_id=:repo_id'),
                {'from_commit': new_commit_id, 'to_commit': '', 'repo_id': repo_id})
            session.commit()

    def delete_metadata(self, repo_id):
        self.repo_metadata.delete_base(repo_id)

    def begin_create_metadata(self, repo_id, commit_id, new_commit_id):
        with self.session() as session:
            sql = "SELECT enabled, from_commit, to_commit FROM repo_metadata WHERE repo_id='%s'" % repo_id
            record = session.execute(text(sql)).fetchone()
        if not record:
            with self.session() as session:
                session.execute(
                    text("""
                    INSERT INTO repo_metadata (`repo_id`, `enabled`, `from_commit`, `to_commit`, `modified_time`, `created_time`) VALUES (:repo_id, :enabled, :from_commit, :to_commit, :modified_time, :created_time)
                    """),
                    {'from_commit': commit_id, 'to_commit': new_commit_id, 'enabled': 1, 'repo_id': repo_id,
                     'modified_time': datetime.utcnow(), 'created_time': datetime.utcnow()})
                session.commit()
        else:
            with self.session() as session:
                session.execute(
                    text(
                        'update repo_metadata set from_commit=:from_commit, to_commit=:to_commit, enabled=:enabled where repo_id=:repo_id'),
                    {'from_commit': commit_id, 'to_commit': new_commit_id, 'repo_id': repo_id, 'enabled': 1})
                session.commit()
