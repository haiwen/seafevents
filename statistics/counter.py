import logging
import time
from datetime import datetime, timedelta
from sqlalchemy import func, select, update, null

from .models import FileOpsStat, TotalStorageStat, UserTraffic, SysTraffic, \
        MonthlyUserTraffic, MonthlySysTraffic
from seafevents.events.models import FileUpdate
from seafevents.events.models import FileAudit
from seafevents.db import SeafBase, init_db_session_class
from seafevents.utils.seafile_db import SeafileDB


download_rate_limit_users = {}
upload_rate_limit_users = {}

download_rate_limit_orgs = {}
upload_rate_limit_orgs = {}

reset_rate_limit_dates = []


class FileOpsCounter(object):
    def __init__(self):
        self.edb_session = init_db_session_class()()

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

        s_timestamp = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        e_timestamp = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

        total_added = total_deleted = total_visited = total_modified = 0
        org_added = {}
        org_deleted = {}
        org_visited = {}
        org_modified = {}
        try:
            stmt = select(FileOpsStat).where(FileOpsStat.timestamp == s_timestamp).limit(1)
            if self.edb_session.scalars(stmt).first():
                self.edb_session.close()
                return

            # Select 'Added', 'Deleted', 'Modified' info from FileUpdate
            stmt = select(FileUpdate).where(
                          FileUpdate.timestamp.between(s_timestamp, e_timestamp))
            rows = self.edb_session.scalars(stmt).all()
            for row in rows:
                org_id = row.org_id
                if 'Added' in row.file_oper:
                    total_added += 1
                    if org_id not in org_added:
                        org_added[org_id] = 1
                    else:
                        org_added[org_id] += 1
                elif 'Deleted' in row.file_oper or 'Removed' in row.file_oper:
                    total_deleted += 1
                    if org_id not in org_deleted:
                        org_deleted[org_id] = 1
                    else:
                        org_deleted[org_id] += 1
                elif 'Modified' in row.file_oper:
                    total_modified += 1
                    if org_id not in org_modified:
                        org_modified[org_id] = 1
                    else:
                        org_modified[org_id] += 1

            # Select 'Visited' info from FileAudit
            stmt = select(FileAudit.org_id, func.count(FileAudit.eid)).where(
                          FileAudit.timestamp.between(s_timestamp, e_timestamp)).group_by(FileAudit.org_id)
            rows = self.edb_session.execute(stmt).all()
            for row in rows:
                org_id = row[0]
                total_visited += row[1]
                org_visited[org_id] = row[1]

        except Exception as e:
            self.edb_session.close()
            logging.warning('[FileOpsCounter] query error : %s.', e)
            return

        for k, v in org_added.items():
            new_record = FileOpsStat(k, s_timestamp, 'Added', v)
            self.edb_session.add(new_record)

        for k, v in org_deleted.items():
            new_record = FileOpsStat(k, s_timestamp, 'Deleted', v)
            self.edb_session.add(new_record)

        for k, v in org_visited.items():
            new_record = FileOpsStat(k, s_timestamp, 'Visited', v)
            self.edb_session.add(new_record)

        for k, v in org_modified.items():
            new_record = FileOpsStat(k, s_timestamp, 'Modified', v)
            self.edb_session.add(new_record)

        logging.info('[FileOpsCounter] Finish counting file operations in %s seconds, %d added, %d deleted, %d visited,'
                     ' %d modified',
                     str(time.time() - time_start), total_added, total_deleted, total_visited, total_modified)

        self.edb_session.commit()
        self.edb_session.close()

class TotalStorageCounter(object):
    def __init__(self):
        self.edb_session = init_db_session_class()()
        self.seafdb_session = init_db_session_class(db='seafile')()

    def start_count(self):
        logging.info('Start counting total storage..')
        time_start = time.time()
        try:
            RepoSize = SeafBase.classes.RepoSize
            VirtualRepo = SeafBase.classes.VirtualRepo
            OrgRepo = SeafBase.classes.OrgRepo

            stmt = select(func.sum(RepoSize.size).label("size"), OrgRepo.org_id).outerjoin(
                          VirtualRepo, RepoSize.repo_id == VirtualRepo.repo_id).outerjoin(
                          OrgRepo, RepoSize.repo_id == OrgRepo.repo_id).where(
                          VirtualRepo.repo_id == null()).group_by(OrgRepo.org_id)
            results = self.seafdb_session.execute(stmt).all()
        except Exception as e:
            self.seafdb_session.close()
            self.edb_session.close()
            logging.warning('[TotalStorageCounter] Failed to get total storage occupation: %s', e)
            return

        if not results:
            self.seafdb_session.close()
            self.edb_session.close()
            logging.info('[TotalStorageCounter] No results from seafile-db.')
            return

        dt = datetime.utcnow()
        _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
        timestamp = datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S')

        try:
            for result in results:
                org_id = result[1]
                org_size = result[0]
                if not org_id:
                    org_id = -1

                stmt = select(TotalStorageStat).where(TotalStorageStat.org_id == org_id,
                                                      TotalStorageStat.timestamp == timestamp).limit(1)
                r = self.edb_session.scalars(stmt).first()
                if not r:
                    newrecord = TotalStorageStat(org_id, timestamp, org_size)
                    self.edb_session.add(newrecord)

            self.edb_session.commit()
            logging.info('[TotalStorageCounter] Finish counting total storage in %s seconds.',
                         str(time.time() - time_start))
        except Exception as e:
            logging.warning('[TotalStorageCounter] Failed to add record to TotalStorageStat: %s.', e)

        self.seafdb_session.close()
        self.edb_session.close()


class MonthlyTrafficCounter(object):
    def __init__(self):
        self.edb_session = init_db_session_class()()

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

        # reset rate limit
        if today == first_day and first_day not in reset_rate_limit_dates:

            if len(reset_rate_limit_dates) > 2:
                reset_rate_limit_dates.pop(0)

            with SeafileDB() as seafile_db:

                seafile_db.reset_download_rate_limit()
                download_rate_limit_orgs.clear()
                download_rate_limit_users.clear()

                seafile_db.reset_upload_rate_limit()
                upload_rate_limit_orgs.clear()
                upload_rate_limit_users.clear()

                reset_rate_limit_dates.append(first_day)

        try:
            # Get raw data from UserTraffic, then update MonthlyUserTraffic and MonthlySysTraffic.
            stmt = select(UserTraffic.user, UserTraffic.org_id,
                          UserTraffic.op_type, func.sum(UserTraffic.size).label('size')).where(
                          UserTraffic.timestamp.between(first_day, today)).group_by(
                          UserTraffic.user, UserTraffic.org_id, UserTraffic.op_type).order_by(UserTraffic.user)
            results = self.edb_session.execute(stmt).all()

            # The raw data is ordered by 'user', and also we count monthly data by user
            # format: user_size_dict[(username, org_id)] = {'web-file-upload': 10, 'web-file-download': 0...}
            last_key = ()
            cur_key = ()
            user_size_dict = {}
            init_size_dict = {'web_file_upload': 0, 'web_file_download': 0, 'sync_file_download': 0,
                              'sync_file_upload': 0, 'link_file_upload': 0, 'link_file_download': 0}

            org_size_dict = {}

            trans_count = 0
            # Update MonthlyUserTraffic.
            for result in results:
                trans_count += 1
                user = result[0]
                org_id = result[1]
                size = result[3]
                # op_type in UserTraffic uses '-', convert to '_'
                oper = result[2].replace('-', '_')

                cur_key = (user, org_id)
                if cur_key not in user_size_dict:
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
                if org_id not in org_size_dict:
                    org_size_dict[org_id] = init_size_dict.copy()
                    org_size_dict[org_id][oper] = size
                else:
                    org_size_dict[org_id][oper] += size

                # commit every 100 items.
                if trans_count >= 100:
                    self.edb_session.commit()
                    trans_count = 0

            # The above loop would miss a user, update it
            if cur_key in user_size_dict:
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
        stmt = select(MonthlyUserTraffic).where(
                                   MonthlyUserTraffic.timestamp == timestamp,
                                   MonthlyUserTraffic.user == user,
                                   MonthlyUserTraffic.org_id == org_id).limit(1)
        if self.edb_session.scalars(stmt).first():
            stmt = update(MonthlyUserTraffic).where(
                                   MonthlyUserTraffic.timestamp == timestamp,
                                   MonthlyUserTraffic.user == user,
                                   MonthlyUserTraffic.org_id == org_id).values(size_dict)
            self.edb_session.execute(stmt)
        else:
            new_record = MonthlyUserTraffic(user, org_id, timestamp, size_dict)
            self.edb_session.add(new_record)
        self.user_item_count += 1

    def update_monthly_org_traffic_record(self, org_id, timestamp, size_dict):
        stmt = select(MonthlySysTraffic).where(
                                   MonthlySysTraffic.timestamp == timestamp,
                                   MonthlySysTraffic.org_id == org_id).limit(1)
        if self.edb_session.scalars(stmt).first():
            stmt = update(MonthlySysTraffic).where(
                                   MonthlySysTraffic.timestamp == timestamp,
                                   MonthlySysTraffic.org_id == org_id).values(size_dict)
            self.edb_session.execute(stmt)
        else:
            new_record = MonthlySysTraffic(timestamp, org_id, size_dict)
            self.edb_session.add(new_record)
        self.sys_item_count += 1
