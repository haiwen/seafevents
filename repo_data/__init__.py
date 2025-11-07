import os
import logging
from sqlalchemy.sql import text

from seafevents.seasearch.utils.constants import REPO_TYPE_WIKI
from seafevents.db import init_db_session_class

logger = logging.getLogger(__name__)


class RepoData(object):
    def __init__(self):
        self._db_session_class = init_db_session_class(db='seafile')

    def to_dict(self, result_proxy):
        res = []
        for i in result_proxy.mappings():
            res.append(i)
        return res

    def _get_repo_id_commit_id(self, start, count):
        session = self._db_session_class()
        name = 'master'
        try:
            cmd = f"""
                SELECT RepoInfo.repo_id, Branch.commit_id, RepoInfo.type
                FROM RepoInfo
                INNER JOIN Branch ON RepoInfo.repo_id = Branch.repo_id
                WHERE Branch.name = '{name}'
                LIMIT {start}, {count};
            """
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_wiki_repo_id_commit_id(self, start, count):
        session = self._db_session_class()
        name = 'master'
        repo_type = REPO_TYPE_WIKI
        try:
            cmd = f"""
                SELECT RepoInfo.repo_id, Branch.commit_id, RepoInfo.type
                FROM RepoInfo
                INNER JOIN Branch ON RepoInfo.repo_id = Branch.repo_id
                WHERE Branch.name = '{name}'
                  AND RepoInfo.type = '{repo_type}'
                LIMIT {start}, {count};
            """
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_all_trash_repo_list(self):
        session = self._db_session_class()
        try:
            cmd = """SELECT repo_id FROM RepoTrash"""
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_all_repo_list(self):
        session = self._db_session_class()
        try:
            cmd = """SELECT repo_id FROM Repo"""
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_repo_head_commit(self, repo_id):
        session = self._db_session_class()
        try:
            cmd = f"""
                SELECT b.commit_id
                FROM Branch as b
                INNER JOIN Repo as r ON b.repo_id = r.repo_id
                WHERE b.repo_id = '{repo_id}'
            """
            res = session.execute(text(cmd)).fetchone()
            return res[0] if res else None
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_repo_name_mtime_size(self, repo_id):
        session = self._db_session_class()
        try:
            cmd = f"""
                SELECT RepoInfo.name, RepoInfo.update_time, "SIZE"
                FROM RepoInfo
                INNER JOIN RepoSize ON RepoInfo.repo_id = RepoSize.repo_id
                WHERE RepoInfo.repo_id = '{repo_id}'
            """
            res = session.execute(text(cmd))
            return self.to_dict(res)
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_virtual_repo_in_repos(self, repo_ids):
        session = self._db_session_class()
        if not repo_ids:
            return []
        try:
            formatted_ids = ", ".join("'{}'".format(id) for id in repo_ids)
            cmd = """SELECT repo_id from VirtualRepo WHERE repo_id IN ({})""".format(formatted_ids)
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def _get_mtime_by_repo_ids(self, repo_ids):
        session = self._db_session_class()
        if not repo_ids:
            return []
        try:
            if len(repo_ids) == 1:
                cmd = """SELECT repo_id, update_time FROM RepoInfo WHERE repo_id = '%s'""" % repo_ids[0]
            else:
                cmd = """SELECT repo_id, update_time FROM RepoInfo WHERE repo_id IN {}""".format(tuple(repo_ids))
            res = session.execute(text(cmd)).fetchall()
            return res
        except Exception as e:
            raise e
        finally:
            session.close()

    def get_repo_name_mtime_size(self, repo_id):
        try:
            return self._get_repo_name_mtime_size(repo_id)
        except Exception as e:
            logger.error(e)
            return self._get_repo_name_mtime_size(repo_id)

    def get_all_repo_list(self):
        try:
            return self._get_all_repo_list()
        except Exception as e:
            logger.error(e)
            return self._get_all_repo_list()

    def get_all_trash_repo_list(self):
        try:
            return self._get_all_trash_repo_list()
        except Exception as e:
            logger.error(e)
            return self._get_all_trash_repo_list()

    def get_repo_id_commit_id(self, start, count):
        try:
            return self._get_repo_id_commit_id(start, count)
        except Exception as e:
            logger.error(e)
            return self._get_repo_id_commit_id(start, count)

    def get_wiki_repo_id_commit_id(self, start, count):
        try:
            return self._get_wiki_repo_id_commit_id(start, count)
        except Exception as e:
            logger.error(e)
            return self._get_wiki_repo_id_commit_id(start, count)

    def get_repo_head_commit(self, repo_id):
        try:
            return self._get_repo_head_commit(repo_id)
        except Exception as e:
            logger.error(e)
            return self._get_repo_head_commit(repo_id)

    def get_virtual_repo_in_repos(self, repo_ids):
        try:
            return self._get_virtual_repo_in_repos(repo_ids)
        except Exception as e:
            logger.error(e)
            return self._get_virtual_repo_in_repos(repo_ids)

    def get_mtime_by_repo_ids(self, repo_ids):
        try:
            return self._get_mtime_by_repo_ids(repo_ids)
        except Exception as e:
            logger.error(e)
            return self._get_mtime_by_repo_ids(repo_ids)

repo_data = RepoData()
