import logging
from sqlalchemy import text
from seafevents.db import init_db_session_class


class SeahubDB(object):

    def __init__(self):
        self.session = init_db_session_class(db='seahub')()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        except Exception as e:
            logging.error(f"Error during session cleanup: {e}")
            self.session.rollback()
        finally:
            if self.session:
                self.session.close()

    def get_wps_file_version(self, repo_id, file_path):
        sql = "SELECT version FROM wps_file_version WHERE repo_id = :repo_id AND path = :path"
        result = self.session.execute(text(sql), {'repo_id': repo_id, 'path': file_path})
        rows = result.fetchone()
        if not rows:
            return None
        return rows[0]

    def set_wps_file_version(self, repo_id, file_path):
        version = self.get_wps_file_version(repo_id, file_path) or 0
        version += 1

        try:
            if version == 1:
                sql = "INSERT INTO wps_file_version (repo_id, path, version) VALUES (:repo_id, :path, :version)"
            else:
                sql = "UPDATE wps_file_version SET version = :version WHERE repo_id = :repo_id AND path = :path"

            self.session.execute(text(sql), {'repo_id': repo_id, 'path': file_path, 'version': version})

            logging.info(f"Set wps file version: repo_id={repo_id}, path={file_path}, version={version}")
            return version
        except Exception as e:
            self.session.rollback()
            logging.error(f"Failed to set wps file version: {e}")
            raise
