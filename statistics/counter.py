import os
import logging
import hashlib
import time
from ConfigParser import ConfigParser
from datetime import timedelta
from datetime import datetime
from sqlalchemy import func, desc
from models import FileOpsStat, TotalStorageStat, UserTraffic, SysTraffic,\
                   MonthlyUserTraffic, MonthlySysTraffic, FileTypeStat, HistoryTotalStorageStat
from seafevents.events.models import FileUpdate
from seafevents.events.models import FileAudit
from seafevents.app.config import appconfig
from seafevents.db import SeafBase
from db import get_org_id
from seafobj import commit_mgr, CommitDiffer
from seafobj.objstore_factory import SeafObjStoreFactory
from seafobj.exceptions import GetObjectError

# This is a throwaway variable to deal with a python bug
throwaway = datetime.strptime('20110101','%Y%m%d')

login_records = {}
traffic_info = {}

ZERO_OBJ_ID = '0000000000000000000000000000000000000000'

def update_hash_record(session, login_name, login_time):
    if not appconfig.enable_statistics:
        return
    time_str = login_time.strftime('%Y-%m-%d 01:01:01')
    time_by_day = datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    md5_key = hashlib.md5((login_name + time_str).encode('utf-8')).hexdigest()
    login_records[md5_key] = (login_name, time_by_day)

def save_traffic_info(session, timestamp, user_name, repo_id, oper, size):
    if not appconfig.enable_statistics:
        return
    org_id = get_org_id(repo_id)
    time_str = timestamp.strftime('%Y-%m-%d')
    if not traffic_info.has_key(time_str):
        traffic_info[time_str] = {}
    if not traffic_info[time_str].has_key((org_id, user_name, oper)):
        traffic_info[time_str][(org_id, user_name, oper)] = size
    else:
        traffic_info[time_str][(org_id, user_name, oper)] += size

class FileOpsCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()

    def start_count(self):
        logging.info('Start counting file operations..')
        time_start = time.time()
        added = 0
        deleted = 0
        visited = 0

        dt = datetime.utcnow()
        delta = timedelta(hours=1)
        _start = (dt - delta)

        start = _start.strftime('%Y-%m-%d %H:00:00')
        end = _start.strftime('%Y-%m-%d %H:59:59')

        s_timestamp = datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
        e_timestamp = datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
        try:
            q = self.edb_session.query(FileOpsStat.timestamp).filter(
                                       FileOpsStat.timestamp==s_timestamp)
            if q.first():
                self.edb_session.close()
                return

            q = self.edb_session.query(FileUpdate.timestamp, FileUpdate.file_oper).filter(
                                       FileUpdate.timestamp.between(
                                       s_timestamp, e_timestamp))
        except Exception as e:
            self.edb_session.close()
            logging.warning('FileOpsCounter query error : %s.', e)
            return

        rows = q.all()
        for row in rows:
            if 'Added' in row.file_oper:
                added += 1
            elif 'Deleted' in row.file_oper or 'Removed' in row.file_oper:
                deleted += 1
        try:
            q = self.edb_session.query(func.count(FileAudit.eid)).filter(
                                       FileAudit.timestamp.between(
                                       s_timestamp, e_timestamp))
        except Exception as e:
            self.edb_session.close()
            logging.warning('FileOpsCounter query error : %s.', e)
            return
    
        visited = q.first()[0]

        if added==0 and deleted==0 and visited ==0:
            self.edb_session.close()
            return

        if added:
            new_record = FileOpsStat(s_timestamp, 'Added', added)
            self.edb_session.add(new_record)
        if deleted:
            new_record = FileOpsStat(s_timestamp, 'Deleted', deleted)
            self.edb_session.add(new_record)
        if visited:
            new_record = FileOpsStat(s_timestamp, 'Visited', visited)
            self.edb_session.add(new_record)

        logging.info('Finish counting file operations in %s seconds, %d added, %d deleted, %d visited',
                     str(time.time() - time_start), added, deleted, visited)

        self.edb_session.commit()
        self.edb_session.close()

class TotalStorageCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()
        self.seafdb_session = appconfig.seaf_session_cls()

    def start_count(self):
        logging.info('Start counting total storage..')
        time_start = time.time()
        try:
            RepoSize = SeafBase.classes.reposize
            VirtualRepo= SeafBase.classes.virtualrepo

            q = self.seafdb_session.query(func.sum(RepoSize.size).label("size")).outerjoin(VirtualRepo,\
                                          RepoSize.repo_id==VirtualRepo.repo_id).filter(VirtualRepo.repo_id == None)
            size = q.first()[0]
        except Exception as e:
            self.seafdb_session.close()
            logging.warning('Failed to get total storage occupation')
            return

        dt = datetime.utcnow()
        _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
        timestamp = datetime.strptime(_timestamp,'%Y-%m-%d %H:%M:%S')

        try:
            q = self.edb_session.query(TotalStorageStat).filter(TotalStorageStat.timestamp==timestamp)
        except Exception as e:
            self.seafdb_session.close()
            self.edb_session.close()
            logging.warning('TotalStorageCounter query error : %s.', e)

        try:
            r = q.first()
            if not r:
                newrecord = TotalStorageStat(timestamp, size)
                self.edb_session.add(newrecord)
                self.edb_session.commit()
                logging.info('Finish counting total storage in %s seconds.', str(time.time() - time_start))
        except Exception as e:
            logging.warning('Failed to add record to TotalStorageStat: %s.', e)

        self.seafdb_session.close()
        self.edb_session.close()

class TrafficInfoCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()

    def start_count(self):
        time_start = time.time()
        logging.info('Start counting traffic info..')

        dt = datetime.utcnow()
        delta = timedelta(days=1)
        yesterday = (dt - delta).date()
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        today = dt.date()
        today_str = today.strftime('%Y-%m-%d')
        local_traffic_info = traffic_info.copy()
        traffic_info.clear()

        if local_traffic_info.has_key(yesterday_str):
            s_time = time.time()
            self.update_record(local_traffic_info, yesterday, yesterday_str)
            logging.info('Traffic Counter: %d items has been recorded on %s, time: %s seconds.' %\
                        (len(local_traffic_info[yesterday_str]), yesterday_str, str(time.time() - s_time)))

        if local_traffic_info.has_key(today_str):
            s_time = time.time()
            self.update_record(local_traffic_info, today, today_str)
            logging.info('Traffic Counter: %d items has been updated on %s, time: %s seconds.' %\
                        (len(local_traffic_info[today_str]), today_str, str(time.time() - s_time)))

        try:
            self.edb_session.commit()
        except Exception as e:
            logging.warning('Failed to update traffic info: %s.', e)
        finally:
            logging.info('Traffic counter finished, total time: %s seconds.' %\
                        (str(time.time() - time_start)))
            self.edb_session.close()
            del local_traffic_info

    def update_record(self, local_traffic_info, date, date_str):
        # org_delta format: org_delta[(org_id, oper)] = size
        # Calculate each org traffic into org_delta, then update SysTraffic.
        org_delta = {}

        trans_count = 0
        # Update UserTraffic
        for row in local_traffic_info[date_str]:
            trans_count += 1
            org_id = row[0]
            user = row[1]
            oper = row[2]
            size = local_traffic_info[date_str][row]
            if not org_delta.has_key((org_id, oper)):
                org_delta[(org_id, oper)] = size
            else:
                org_delta[(org_id, oper)] += size

            try:
                q = self.edb_session.query(UserTraffic.size).filter(
                                           UserTraffic.timestamp==date,
                                           UserTraffic.user==user,
                                           UserTraffic.org_id==org_id,
                                           UserTraffic.op_type==oper)
                result = q.first()
                if result:
                    size_in_db = result[0]
                    self.edb_session.query(UserTraffic).filter(UserTraffic.timestamp==date,
                                                               UserTraffic.user==user,
                                                               UserTraffic.org_id==org_id,
                                                               UserTraffic.op_type==oper).update(
                                                               {"size": size + size_in_db})
                else:
                    new_record = UserTraffic(user, date, oper, size, org_id)
                    self.edb_session.add(new_record)

                # commit every 100 items.
                if trans_count >= 100:
                    self.edb_session.commit()
                    trans_count = 0
            except Exception as e:
                logging.warning('Failed to update traffic info: %s.', e)
                return

        # Update SysTraffic
        for row in org_delta:
            org_id = row[0]
            oper = row[1]
            size = org_delta[row]
            try:
                q = self.edb_session.query(SysTraffic.size).filter(
                                           SysTraffic.timestamp==date,
                                           SysTraffic.org_id==org_id,
                                           SysTraffic.op_type==oper)
                result = q.first()
                if result:
                    size_in_db = result[0]
                    self.edb_session.query(SysTraffic).filter(SysTraffic.timestamp==date,
                                                              SysTraffic.org_id==org_id,
                                                              SysTraffic.op_type==oper).update(
                                                              {"size": size + size_in_db})
                else:
                    new_record = SysTraffic(date, oper, size, org_id)
                    self.edb_session.add(new_record)

            except Exception as e:
                logging.warning('Failed to update traffic info: %s.', e)

class MonthlyTrafficCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()

    def start_count(self):
        time_start = time.time()
        logging.info('Start counting monthly traffic info..')

        # Count traffic between first day of this month and today.
        dt = datetime.utcnow()
        today = dt.date()
        delta = timedelta(days=dt.day - 1)
        first_day = today - delta

        self.user_item_count = 0
        self.sys_item_count = 0

        try:
            # Get raw data from UserTraffic, then update MonthlyUserTraffic and MonthlySysTraffic.
            q = self.edb_session.query(UserTraffic.user,
                                       UserTraffic.org_id, UserTraffic.op_type,
                                       func.sum(UserTraffic.size).label('size')).filter(
                                       UserTraffic.timestamp.between(first_day, today)
                                       ).group_by(UserTraffic.user, UserTraffic.org_id,
                                       UserTraffic.op_type).order_by(UserTraffic.user)
            results = q.all()

            # The raw data is ordered by 'user', and also we count monthly data by user
            # format: user_size_dict[(username, org_id)] = {'web-file-upload': 10, 'web-file-download': 0...}
            last_key = ()
            cur_key = ()
            user_size_dict = {}
            init_size_dict = {'web_file_upload': 0, 'web_file_download': 0, 'sync_file_download': 0, \
                              'sync_file_upload':0, 'link_file_upload': 0, 'link_file_download': 0}

            org_size_dict = {}

            trans_count = 0
            # Update MonthlyUserTraffic.
            for result in results:
                trans_count += 1
                user = result.user
                org_id = result.org_id
                size = result.size
                # op_type in UserTraffic uses '-', convert to '_'
                oper = result.op_type.replace('-', '_')

                cur_key = (user, org_id)
                if not user_size_dict.has_key(cur_key):
                    user_size_dict[cur_key] = init_size_dict.copy()
                user_size_dict[cur_key][oper] += size

                # We reached a new user, update last user's data if exists.
                if cur_key != last_key:
                    if not last_key:
                        last_key = cur_key
                    else:
                        self.update_monthly_user_traffic_record (last_key[0], last_key[1], first_day, user_size_dict[last_key])
                        del user_size_dict[last_key]
                last_key = cur_key

                # Count org data
                if not org_size_dict.has_key(org_id):
                    org_size_dict[org_id] = init_size_dict.copy()
                    org_size_dict[org_id][oper] = size
                else:
                    org_size_dict[org_id][oper] += size

                # commit every 100 items.
                if trans_count >= 100:
                    self.edb_session.commit()
                    trans_count = 0

            # The above loop would miss a user, update it
            if user_size_dict.has_key(cur_key):
                self.update_monthly_user_traffic_record (cur_key[0], cur_key[1], first_day, user_size_dict[cur_key])
                del user_size_dict[cur_key]

            # Update MonthlySysTraffic.
            for org_id in org_size_dict:
                self.update_monthly_org_traffic_record(org_id, first_day, org_size_dict[org_id])

            try:
                self.edb_session.commit()
            except Exception as e:
                logging.warning('Failed to commit monthly traffic info: %s.', e)
            finally:
                logging.info('Monthly traffic counter finished, update %d user items, %d org items, total time: %s seconds.' %\
                            (self.user_item_count, self.sys_item_count, str(time.time() - time_start)))
                self.edb_session.close()

        except Exception as e:
            logging.warning('Failed to update monthly traffic info: %s.', e)
            self.edb_session.close()

    def update_monthly_user_traffic_record(self, user, org_id, timestamp, size_dict):
        q = self.edb_session.query(MonthlyUserTraffic.user).filter(
                                   MonthlyUserTraffic.timestamp==timestamp,
                                   MonthlyUserTraffic.user==user,
                                   MonthlyUserTraffic.org_id==org_id)
        if q.first():
            self.edb_session.query(MonthlyUserTraffic).filter(
                                   MonthlyUserTraffic.timestamp==timestamp,
                                   MonthlyUserTraffic.user==user,
                                   MonthlyUserTraffic.org_id==org_id).update(
                                   size_dict)
        else:
            new_record = MonthlyUserTraffic(user, org_id, timestamp, size_dict)
            self.edb_session.add(new_record)
        self.user_item_count += 1

    def update_monthly_org_traffic_record(self, org_id, timestamp, size_dict):
        q = self.edb_session.query(MonthlySysTraffic.org_id).filter(
                                   MonthlySysTraffic.timestamp==timestamp,
                                   MonthlySysTraffic.org_id==org_id)
        if q.first():
            self.edb_session.query(MonthlySysTraffic).filter(
                                   MonthlySysTraffic.timestamp==timestamp,
                                   MonthlySysTraffic.org_id==org_id).update(
                                   size_dict)
        else:
            new_record = MonthlySysTraffic(timestamp, org_id, size_dict)
            self.edb_session.add(new_record)
        self.sys_item_count += 1

class FileTypesCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()
        self.seafdb_session = appconfig.seaf_session_cls()

    def start_count(self):
        if not appconfig.type_list and not appconfig.count_all_file_types:
            return

        time_start = time.time()
        logging.info("Start counting file types..")

        self.type_list = appconfig.type_list
        self.count_all_file_types = appconfig.count_all_file_types
        try:
            Branch = SeafBase.classes.branch
            VirtualRepo= SeafBase.classes.virtualrepo

            q = self.seafdb_session.query(Branch.repo_id, Branch.commit_id).outerjoin(VirtualRepo,\
                                          Branch.repo_id==VirtualRepo.repo_id).filter(
                                          Branch.name=='master', VirtualRepo.repo_id == None)

            results = q.all()
        except Exception as e:
            logging.warning('Failed to get repo_id, commit_id from seafile db: %s', e)
            self.edb_session.close()
            self.seafdb_session.close()
            return

        now = datetime.utcnow()

        for result in results:
            repo_id = result.repo_id
            commit_id = result.commit_id
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
                logging.info('FileTypesCounter: updated repo %.8s.', repo_id)
            except GetObjectError as e:
                logging.warning('FileTypesCounter: %s', e)
                continue
            except Exception as e:
                self.edb_session.close()
                self.seafdb_session.close()
                logging.warning('FileTypesCounter query error : %s.', e)
                return

        try:
            self.edb_session.commit()
            logging.info("Finish counting file types, total time: %s seconds.", str(time.time() - time_start))
        except Exception as e:
            logging.warning('Failed to commit db.')
        finally:
            self.edb_session.close()
            self.seafdb_session.close()

class HistoryTotalStorageCounter(object):
    def __init__(self):
        self.edb_session = appconfig.session_cls()
        self.seafdb_session = appconfig.seaf_session_cls()
        self.objstore_factory = SeafObjStoreFactory()

    def start_count(self):
        blocks_obj_store = self.objstore_factory.get_obj_store('blocks')

        try:
            Repo = SeafBase.classes.repo
            VirtualRepo= SeafBase.classes.virtualrepo

            q = self.seafdb_session.query(Repo.repo_id).outerjoin(VirtualRepo,\
                                          Repo.repo_id==VirtualRepo.repo_id).filter(VirtualRepo.repo_id == None)
            results = q.all()
        except Exception as e:
            self.edb_session.close()
            self.seafdb_session.close()
            logging.warning('Failed to get repo_ids')
            return

        try:
            for result in results:
                repo_id = result[0]
                block_obj_size = 0 

                block_objs = blocks_obj_store.list_objs(repo_id)
                for block_obj in block_objs:
                    block_obj_size += block_obj[2]

                dt = datetime.utcnow()
                _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
                timestamp = datetime.strptime(_timestamp,'%Y-%m-%d %H:%M:%S')

                q = self.edb_session.query(HistoryTotalStorageStat).filter(HistoryTotalStorageStat.repo_id==repo_id)

                r = q.first()
                if not r:
                    newrecord = HistoryTotalStorageStat(repo_id, timestamp, block_obj_size)
                    self.edb_session.add(newrecord)
                elif r.timestamp != timestamp:
                    self.edb_session.query(HistoryTotalStorageStat).filter(HistoryTotalStorageStat.repo_id==repo_id\
                                           ).update({"timestamp": timestamp, "total_size": block_obj_size})

            self.edb_session.commit()

        except Exception as e:
            logging.warning('Failed to add record to HistoryTotalStorageStat: %s.', e)
            self.edb_session.close()
            self.seafdb_session.close()
            return
