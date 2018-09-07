import os
import logging
import time
from datetime import timedelta
from datetime import datetime
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy import desc
from MySQLdb.cursors import SSDictCursor

from seafobj import commit_mgr, CommitDiffer
from models import FileTypeStat

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

def iterate_query(sql, cursor, arraysize=1):
    cursor.execute(sql)
    while True:
        rows = cursor.fetchmany(arraysize)
        if not rows:
            break
        for row in rows:
            yield row

class FileTypesCounter(object):
    def __init__(self, settings):
        self.settings = settings
        if not settings.type_list and not settings.count_all_file_types:
            return

        self.edb_session = scoped_session(settings.session_cls)
        settings.init_seafile_db()
        self.cursor = settings.sdb_conn.cursor(cursorclass=SSDictCursor)

    def start_count(self):
        if not self.settings.type_list and not self.settings.count_all_file_types:
            return

        time_start = time.time()
        logging.info("Start counting file types..")

        self.type_list = self.settings.type_list
        self.count_all_file_types = self.settings.count_all_file_types
        try:
            sql = '''SELECT b.repo_id, b.commit_id FROM Branch b
                     LEFT JOIN VirtualRepo v
                     ON b.repo_id=v.repo_id
                     WHERE name = \'master\' AND v.repo_id IS NULL'''
            results = iterate_query(sql, self.cursor, 1000)
        except Exception as e:
            logging.warning('Failed to get repo_id, commit_id from seafile db: %s', e)
            self.cursor.close()
            return

        now = datetime.utcnow()

        for result in results:
            repo_id = result['repo_id']
            commit_id = result['commit_id']
            try:
                q = self.edb_session.query(FileTypeStat.commit_id,
                                           FileTypeStat.file_type, FileTypeStat.file_count).filter(
                                           FileTypeStat.repo_id==repo_id).order_by(desc(FileTypeStat.timestamp))
                rows = q.all()
                if rows:
                    last_commit_id = rows[0][0]
                else:
                    last_commit_id = None

                last_root_id = ZERO_OBJ_ID

                # no update
                if last_commit_id == commit_id:
                    continue

                cur_commit = commit_mgr.load_commit(repo_id, 1, commit_id)
                if not cur_commit:
                    logging.warning('FileTypesCounter: failed to load commit %s for repo %.8s.', commit_id, repo_id)
                    continue
                if last_commit_id:
                    last_commit = commit_mgr.load_commit(repo_id, 1, last_commit_id)
                    if not last_commit:
                        logging.warning('FileTypesCounter: failed to load commit %s for repo %.8s.', last_commit_id, repo_id)
                        continue
                    last_root_id = last_commit.root_id

                differ = CommitDiffer(repo_id, cur_commit.version, last_root_id, cur_commit.root_id)
                added_files, deleted_files, added_dirs, deleted_dirs, modified_files,\
                    renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff_to_unicode()

                delta_files = {}

                for f in added_files:
                    if '.' in f.path:
                        suffix = f.path.split('.')[-1]
                    else:
                        suffix = ''
                    if self.count_all_file_types:
                        if not delta_files.has_key(suffix):
                            delta_files[suffix] = 0
                        delta_files[suffix] += 1
                    else:
                        if suffix in self.type_list:
                            if not delta_files.has_key(suffix):
                                delta_files[suffix] = 0
                            delta_files[suffix] += 1

                for f in deleted_files:
                    if '.' in f.path:
                        suffix = f.path.split('.')[-1]
                    else:
                        suffix = ''
                    if self.count_all_file_types:
                        if not delta_files.has_key(suffix):
                            delta_files[suffix] = 0
                        delta_files[suffix] -= 1
                    else:
                        if suffix in self.type_list:
                            if not delta_files.has_key(suffix):
                                delta_files[suffix] = 0
                            delta_files[suffix] -= 1

                # skip if this commit doesn't contain specified types.
                need_update = False
                for f_type in delta_files:
                    if delta_files[f_type] != 0:
                        need_update = True
                        break
                if not need_update:
                    continue

                # update the types that exist in db records
                for row in rows:
                    file_type = row[1]
                    file_count = row[2]
                    if file_type in delta_files:
                        file_count += delta_files[file_type]
                        del delta_files[file_type]

                    self.edb_session.query(FileTypeStat).filter(FileTypeStat.repo_id==repo_id,
                                                                FileTypeStat.file_type==file_type).update(
                                                                {"commit_id": commit_id, "file_count": file_count,
                                                                 "timestamp": now})
                # new types
                for file_type in delta_files:
                    if delta_files[file_type] == 0:
                        continue
                    record = FileTypeStat(repo_id, now, commit_id, file_type, delta_files[file_type])
                    self.edb_session.add(record)
            except Exception as e:
                self.edb_session.remove()
                self.cursor.close()
                logging.warning('query error : %s.', e)
                return

        try:
            self.edb_session.commit()
            logging.info("Finish counting file types, total time: %s seconds.", str(time.time() - time_start))
        except Exception as e:
            logging.warning('Failed to commit db.')
        finally:
            self.edb_session.remove()
            self.cursor.close()
