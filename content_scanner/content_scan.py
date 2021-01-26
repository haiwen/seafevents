#coding: utf-8
import logging
import time
import json
from datetime import datetime
from os.path import splitext
from seafobj import CommitDiffer, commit_mgr, fs_mgr
from .config import appconfig
from .thread_pool import ThreadPool
from .models import ContentScanRecord, ContentScanResult
from seafevents.db import SeafBase

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

class ScanTask(object):
    def __init__(self, repo_id, last_commit_id, new_commit_id):
        self.repo_id = repo_id
        self.last_commit_id = last_commit_id
        self.new_commit_id = new_commit_id

## Get the head_commit_id list from seafile-db.
## Compare each head_commit_id with the last scanned commit_id,
## if they're not equal, do diff and content scan.
class ContentScan(object):
    def __init__(self):
        self.thread_pool = ThreadPool(self.diff_and_scan_content, appconfig.thread_num)
        self.thread_pool.start()

    def start(self):
        try:
            self.do_scan_task()
        except Exception as e:
            logging.warning('Error: %s', e)

    def do_scan_task(self):
        logging.info("Start scan task..")
        time_start = time.time()

        dt = datetime.utcnow()
        dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        self.dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

        edb_session = appconfig.session_cls()
        seafdb_session = appconfig.seaf_session_cls()

        # Get repo list from seafile-db
        Branch = SeafBase.classes.Branch
        VirtualRepo= SeafBase.classes.VirtualRepo
        q = seafdb_session.query(Branch.repo_id, Branch.commit_id)
        q = q.outerjoin(VirtualRepo, Branch.repo_id==VirtualRepo.repo_id)
        q = q.filter(VirtualRepo.repo_id == None)
        results = q.all()
        for row in results:
            repo_id = row.repo_id
            new_commit_id = row.commit_id
            last_commit_id = None
            q = edb_session.query(ContentScanRecord.commit_id)
            q = q.filter(ContentScanRecord.repo_id==repo_id)
            result = q.first()
            if result:
                last_commit_id = result[0]

            self.put_task(repo_id, last_commit_id, new_commit_id)

        # Remove deleted repo's record after all threads finished
        self.thread_pool.join()
        q = edb_session.query(ContentScanRecord)
        q = q.filter(ContentScanRecord.timestamp != self.dt)
        q.delete()
        q = edb_session.query(ContentScanResult)
        subqurey = edb_session.query(ContentScanRecord.repo_id)
        q = q.filter(ContentScanResult.repo_id.notin_(subqurey))
        # need fetch subqurey
        q.delete(synchronize_session='fetch')
        edb_session.commit()

        edb_session.close()
        seafdb_session.close()
        logging.info('Finish scan task, total time: %s seconds\n', str(time.time() - time_start))

        self.thread_pool.join(stop=True)

    def diff_and_scan_content(self, task, client):
        repo_id = task.repo_id
        last_commit_id = task.last_commit_id
        new_commit_id = task.new_commit_id
        edb_session = appconfig.session_cls()

        # repo not changed, update timestamp
        if last_commit_id == new_commit_id:
            q = edb_session.query(ContentScanRecord)
            q = q.filter(ContentScanRecord.repo_id==repo_id,
                         ContentScanRecord.commit_id==last_commit_id)
            q.update({"timestamp": self.dt})
            edb_session.commit()
            edb_session.close()
            return

        # diff
        version = 1
        new_commit = commit_mgr.load_commit(repo_id, version, new_commit_id)
        if new_commit is None:
            version = 0
            new_commit = commit_mgr.load_commit(repo_id, version, new_commit_id)
        if not new_commit:
            logging.warning('Failed to load commit %s/%s', repo_id, new_commit_id)
            edb_session.close()
            return
        last_commit = None
        if last_commit_id:
            last_commit = commit_mgr.load_commit(repo_id, version, last_commit_id)
            if not last_commit:
                logging.warning('Failed to load commit %s/%s', repo_id, last_commit_id)
                edb_session.close()
                return
        new_root_id = new_commit.root_id
        last_root_id = last_commit.root_id if last_commit else ZERO_OBJ_ID

        differ = CommitDiffer(repo_id, version, last_root_id, new_root_id,
                              True, False)
        added_files, deleted_files, added_dirs, deleted_dirs, modified_files,\
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

        # Handle renamed, moved and deleted files.
        q = edb_session.query(ContentScanResult).filter(ContentScanResult.repo_id==repo_id)
        results = q.all()
        if results:
            path_pairs_to_rename = []
            paths_to_delete = []
            # renamed dirs
            for r_dir in renamed_dirs:
                r_path = r_dir.path + '/'
                l = len(r_path)
                for row in results:
                    if r_path == row.path[:l]:
                        new_path = r_dir.new_path + '/' + row.path[l:]
                        path_pairs_to_rename.append((row.path, new_path))
            # moved dirs
            for m_dir in moved_dirs:
                m_path = m_dir.path + '/'
                l = len(m_path)
                for row in results:
                    if m_path == row.path[:l]:
                        new_path = m_dir.new_path + '/' + row.path[l:]
                        path_pairs_to_rename.append((row.path, new_path))
            # renamed files
            for r_file in renamed_files:
                r_path = r_file.path
                for row in results:
                    if r_path == row.path:
                        new_path = r_file.new_path
                        path_pairs_to_rename.append((row.path, new_path))
            # moved files
            for m_file in moved_files:
                m_path = m_file.path
                for row in results:
                    if m_path == row.path:
                        new_path = m_file.new_path
                        path_pairs_to_rename.append((row.path, new_path))

            for old_path, new_path in path_pairs_to_rename:
                q = edb_session.query(ContentScanResult)
                q = q.filter(ContentScanResult.repo_id==repo_id, ContentScanResult.path==old_path)
                q = q.update({"path": new_path})

            # deleted files
            for d_file in deleted_files:
                d_path = d_file.path
                for row in results:
                    if d_path == row.path:
                        paths_to_delete.append(row.path)
            # We will scan modified_files and re-record later,
            # so delete previous records now
            for m_file in modified_files:
                m_path = m_file.path
                for row in results:
                    if m_path == row.path:
                        paths_to_delete.append(row.path)

            for path in paths_to_delete:
                q = edb_session.query(ContentScanResult)
                q = q.filter(ContentScanResult.repo_id==repo_id, ContentScanResult.path==path)
                q.delete()

            edb_session.commit()

        # scan added_files and modified_files by third-party API.
        files_to_scan = []
        files_to_scan.extend(added_files)
        files_to_scan.extend(modified_files)
        a_count = 0
        scan_results = []
        for f in files_to_scan:
            if not self.should_scan_file (f.path, f.size):
                continue
            seafile_obj = fs_mgr.load_seafile(repo_id, 1, f.obj_id)
            content = seafile_obj.get_content()
            if not content:
                continue
            result = client.scan(content)
            if result and isinstance(result, dict):
                item = {"path": f.path, "detail": result}
                scan_results.append(item)
            else:
                logging.warning('Failed to scan %s:%s', repo_id, f.path)

        for item in scan_results:
            detail = json.dumps(item["detail"])
            new_record = ContentScanResult(repo_id, item["path"], appconfig.platform, detail)
            edb_session.add(new_record)
            a_count += 1
        if a_count >= 1:
            logging.info('Found %d new illegal files.', a_count)

        # Update ContentScanRecord
        if last_commit_id:
            q = edb_session.query(ContentScanRecord).filter(ContentScanRecord.repo_id==repo_id)
            q.update({"commit_id": new_commit_id, "timestamp": self.dt})
        else:
            new_record = ContentScanRecord(repo_id, new_commit_id, self.dt)
            edb_session.add(new_record)

        edb_session.commit()
        edb_session.close()

    def put_task(self, repo_id, last_commit_id, new_commit_id):
        task = ScanTask(repo_id, last_commit_id, new_commit_id)
        self.thread_pool.put_task(task)

    def should_scan_file(self, fpath, fsize):
        if fsize > appconfig.size_limit:
            return False

        filename, suffix = splitext(fpath)
        if suffix[1:] not in appconfig.suffix_list:
            return False

        return True

