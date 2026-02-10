import logging
import hashlib
import time
from datetime import timedelta
from datetime import datetime
from sqlalchemy import func, select, update, null
from sqlalchemy.sql import text

from .models import FileOpsStat, TotalStorageStat, UserTraffic, SysTraffic, \
        MonthlyUserTraffic, MonthlySysTraffic
from seafevents.events.models import FileUpdate
from seafevents.events.models import FileAudit
from seafevents.db import SeafBase, init_db_session_class
from seaserv import seafile_api
from seafevents.utils.seafile_db import SeafileDB
from seafevents.utils.ccnet_db import CcnetDB
from seafevents.utils.seahub_db import SeahubDB
from seafevents.utils import get_quota_from_string
from seafevents.app.config import ENABLED_ROLE_PERMISSIONS, \
        DOWNLOAD_LIMIT_WHEN_THROTTLE, UPLOAD_LIMIT_WHEN_THROTTLE
from .db import get_org_id

# This is a throwaway variable to deal with a python bug
throwaway = datetime.strptime('20110101', '%Y%m%d')

login_records = {}
traffic_info = {}


download_rate_limit_users = {}
upload_rate_limit_users = {}

download_rate_limit_orgs = {}
upload_rate_limit_orgs = {}

reset_rate_limit_dates = []

DEFAULT_USER = 'default'
GUEST_USER = 'guest'

MONTHLY_DOWNLOAD_TRAFFIC_LIMIT = 'monthly_rate_limit'
MONTHLY_DOWNLOAD_TRAFFIC_LIMIT_PER_USER = 'monthly_rate_limit_per_user'

MONTHLY_UPLOAD_TRAFFIC_LIMIT = 'monthly_upload_traffic_limit'
MONTHLY_UPLOAD_TRAFFIC_LIMIT_PER_USER = 'monthly_upload_traffic_limit_per_user'


def get_org_user_quota(local_traffic_info, date_str):
    org_user_dict = {}
    for row in local_traffic_info[date_str]:
        org_id = row[0]
        if org_id > 0 and org_id not in org_user_dict:
            with SeahubDB() as seahub_db:
                user_count = seahub_db.get_org_member_quota(org_id)
                org_user_dict[org_id] = user_count
    return org_user_dict


def update_hash_record(session, login_name, login_time, org_id):
    time_str = login_time.strftime('%Y-%m-%d 00:00:00')
    time_by_day = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    md5_key = hashlib.md5((login_name + time_str).encode('utf-8')).hexdigest()
    login_records[md5_key] = (login_name, time_by_day, org_id)

def save_traffic_info(session, timestamp, user_name, repo_id, oper, size):
    org_id = get_org_id(repo_id)
    time_str = timestamp.strftime('%Y-%m-%d')
    if time_str not in traffic_info:
        traffic_info[time_str] = {}
    if (org_id, user_name, oper) not in traffic_info[time_str]:
        traffic_info[time_str][(org_id, user_name, oper)] = size
    else:
        traffic_info[time_str][(org_id, user_name, oper)] += size


def get_role_traffic_limit_dict():

    if not ENABLED_ROLE_PERMISSIONS:
        return None

    role_traffic_limit_dict = {}
    for role, v in ENABLED_ROLE_PERMISSIONS.items():

        traffic_limit = {}

        LIMIT_CONFIGS = [
            MONTHLY_DOWNLOAD_TRAFFIC_LIMIT,
            MONTHLY_DOWNLOAD_TRAFFIC_LIMIT_PER_USER,
            MONTHLY_UPLOAD_TRAFFIC_LIMIT,
            MONTHLY_UPLOAD_TRAFFIC_LIMIT_PER_USER
        ]

        for limit_key in LIMIT_CONFIGS:
            if limit_key in v:
                limit_value = get_quota_from_string(v[limit_key])
                traffic_limit[limit_key] = limit_value

        role_traffic_limit_dict[role] = traffic_limit

    return role_traffic_limit_dict


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


class TrafficInfoCounter(object):

    def __init__(self):
        self.edb_session = init_db_session_class()()
        self.download_type_list = ['web-file-download', 'link-file-download', 'sync-file-download']
        self.upload_type_list = ['web-file-upload', 'link-file-upload', 'sync-file-upload']

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

        if yesterday_str in local_traffic_info:
            s_time = time.time()
            self.update_record(local_traffic_info, yesterday, yesterday_str)
            logging.info(
                'Traffic Counter: %d items has been recorded on %s, time: %s seconds.' % (
                    len(local_traffic_info[yesterday_str]),
                    yesterday_str,
                    str(time.time() - s_time)
                )
            )

        if today_str in local_traffic_info:
            s_time = time.time()
            self.update_record(local_traffic_info, today, today_str)
            logging.info(
                'Traffic Counter: %d items has been updated on %s, time: %s seconds.' % (
                    len(local_traffic_info[today_str]),
                    today_str,
                    str(time.time() - s_time)
                )
            )

        try:
            self.edb_session.commit()
        except Exception as e:
            logging.warning('Failed to update traffic info: %s.', e)
        finally:
            logging.info(
                'Traffic counter finished, total time: %s seconds.' % (
                    str(time.time() - time_start)
                )
            )
            self.edb_session.close()
            del local_traffic_info

    def update_record(self, local_traffic_info, date, date_str):

        # org_delta format: org_delta[(org_id, oper)] = size
        # Calculate each org traffic into org_delta, then update SysTraffic.
        org_delta = {}

        trans_count = 0
        first_day_of_month = datetime(datetime.now().year, datetime.now().month, 1)
        org_user_quota_dict = get_org_user_quota(local_traffic_info, date_str)

        try:
            # get role traffic limit config
            role_traffic_limit_dict = get_role_traffic_limit_dict()
        except Exception as e:
            logging.warning('Failed get download rate limit info: %s.', e)
            role_traffic_limit_dict = None

        # Update UserTraffic
        for row in local_traffic_info[date_str]:

            trans_count += 1
            org_id = row[0]
            user = row[1]
            oper = row[2]

            size = local_traffic_info[date_str][row]
            if size == 0:
                continue

            download_traffic_threshold = None
            upload_traffic_threshold = None

            if oper in self.download_type_list:

                with CcnetDB() as ccnet_db:
                    user_role = ccnet_db.get_user_role(user)
                    role = DEFAULT_USER if user_role == '' else user_role

                # get common user download traffic limit
                if role_traffic_limit_dict and role in role_traffic_limit_dict:
                    download_traffic_threshold = role_traffic_limit_dict[role].get(
                        MONTHLY_DOWNLOAD_TRAFFIC_LIMIT
                    )
                else:
                    download_traffic_threshold = None

                # get org download traffic limit
                if org_id > 0:

                    with SeahubDB() as seahub_db:
                        org_role = seahub_db.get_org_role(org_id)
                        role = DEFAULT_USER if org_role == '' else org_role

                    if role_traffic_limit_dict and role in role_traffic_limit_dict:
                        limit_per_user = role_traffic_limit_dict[role].get(
                            MONTHLY_DOWNLOAD_TRAFFIC_LIMIT_PER_USER
                        )
                    else:
                        limit_per_user = None

                    org_user_quota = org_user_quota_dict.get(org_id)
                    if org_user_quota and limit_per_user:
                        download_traffic_threshold = org_user_quota * limit_per_user
                    else:
                        download_traffic_threshold = None

                    with SeahubDB() as seahub_db:
                        monthly_traffic_limit = seahub_db.get_org_monthly_traffic_limit(org_id)
                        if monthly_traffic_limit > 0:
                            download_traffic_threshold = monthly_traffic_limit

                if (org_id, oper, download_traffic_threshold) not in org_delta:
                    org_delta[(org_id, oper, download_traffic_threshold)] = size
                else:
                    org_delta[(org_id, oper, download_traffic_threshold)] += size

            elif oper in self.upload_type_list:

                with CcnetDB() as ccnet_db:
                    user_role = ccnet_db.get_user_role(user)
                    role = DEFAULT_USER if user_role == '' else user_role

                # get common user upload traffic limit
                if role_traffic_limit_dict and role in role_traffic_limit_dict:
                    upload_traffic_threshold = role_traffic_limit_dict[role].get(
                        MONTHLY_UPLOAD_TRAFFIC_LIMIT
                    )
                else:
                    upload_traffic_threshold = None

                # get org upload traffic limit
                if org_id > 0:

                    with SeahubDB() as seahub_db:
                        org_role = seahub_db.get_org_role(org_id)
                        role = DEFAULT_USER if org_role == '' else org_role

                    if role_traffic_limit_dict and role in role_traffic_limit_dict:
                        limit_per_user = role_traffic_limit_dict[role].get(
                            MONTHLY_UPLOAD_TRAFFIC_LIMIT_PER_USER
                        )
                    else:
                        limit_per_user = None

                    org_user_quota = org_user_quota_dict.get(org_id)
                    if org_user_quota and limit_per_user:
                        upload_traffic_threshold = org_user_quota * limit_per_user
                    else:
                        upload_traffic_threshold = None

                if (org_id, oper, upload_traffic_threshold) not in org_delta:
                    org_delta[(org_id, oper, upload_traffic_threshold)] = size
                else:
                    org_delta[(org_id, oper, upload_traffic_threshold)] += size

            else:
                if (org_id, oper) not in org_delta:
                    org_delta[(org_id, oper)] = size
                else:
                    org_delta[(org_id, oper)] += size

            try:
                # Check the download traffic for the current month.
                if (
                    download_traffic_threshold
                    and org_id < 0
                    and oper in self.download_type_list
                    and not download_rate_limit_users.get(user, False)
                ):

                    stmt2 = select(func.sum(UserTraffic.size).label("size")).where(
                        UserTraffic.timestamp.between(first_day_of_month, date),
                        UserTraffic.user == user,
                        UserTraffic.org_id == org_id,
                        UserTraffic.op_type.in_(self.download_type_list)
                    )
                    user_monthly_download_traffic_size = self.edb_session.scalars(stmt2).first()

                    # common user download tarffic limit
                    if user_monthly_download_traffic_size and \
                            user_monthly_download_traffic_size > download_traffic_threshold:
                        download_limit_format = get_quota_from_string(DOWNLOAD_LIMIT_WHEN_THROTTLE)
                        seafile_api.set_user_download_rate_limit(user, download_limit_format)
                        download_rate_limit_users[user] = True

                # Check upload traffic for the current month.
                if (
                    upload_traffic_threshold
                    and org_id < 0
                    and oper in self.upload_type_list
                    and not upload_rate_limit_users.get(user, False)
                ):

                    stmt2 = select(func.sum(UserTraffic.size).label("size")).where(
                        UserTraffic.timestamp.between(first_day_of_month, date),
                        UserTraffic.user == user,
                        UserTraffic.org_id == org_id,
                        UserTraffic.op_type.in_(self.upload_type_list)
                    )
                    user_monthly_upload_traffic_size = self.edb_session.scalars(stmt2).first()

                    # common user upload tarffic limit
                    if user_monthly_upload_traffic_size and \
                            user_monthly_upload_traffic_size > upload_traffic_threshold:
                        upload_limit_format = get_quota_from_string(UPLOAD_LIMIT_WHEN_THROTTLE)
                        seafile_api.set_user_upload_rate_limit(user, upload_limit_format)
                        upload_rate_limit_users[user] = True

                stmt = select(UserTraffic.size).where(
                                           UserTraffic.timestamp == date,
                                           UserTraffic.user == user,
                                           UserTraffic.org_id == org_id,
                                           UserTraffic.op_type == oper).limit(1)
                result = self.edb_session.scalars(stmt).first()

                if result is not None:
                    size_in_db = result
                    stmt = update(UserTraffic).where(UserTraffic.timestamp == date,
                                                     UserTraffic.user == user,
                                                     UserTraffic.org_id == org_id,
                                                     UserTraffic.op_type == oper).values(size=size + size_in_db)
                    self.edb_session.execute(stmt)
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
                # Check org download traffic for current month.
                if (
                    org_id > 0
                    and oper in self.download_type_list
                    and not download_rate_limit_orgs.get(org_id)
                ):

                    stmt2 = select(func.sum(SysTraffic.size).label("size")).where(
                        SysTraffic.timestamp.between(first_day_of_month, date),
                        SysTraffic.org_id == org_id,
                        SysTraffic.op_type.in_(self.download_type_list)
                    )
                    org_monthly_download_traffic_size = self.edb_session.scalars(stmt2).first()

                    download_traffic_threshold = row[2]

                    # org download rate limit
                    if (
                        org_monthly_download_traffic_size
                        and download_traffic_threshold
                        and org_monthly_download_traffic_size > download_traffic_threshold
                    ):

                        download_limit_format = get_quota_from_string(DOWNLOAD_LIMIT_WHEN_THROTTLE)
                        seafile_api.org_set_download_rate_limit(org_id, download_limit_format)
                        download_rate_limit_orgs[org_id] = True

                # Check org upload traffic for current month.
                if (
                    org_id > 0
                    and oper in self.upload_type_list
                    and not upload_rate_limit_orgs.get(org_id)
                ):

                    stmt2 = select(func.sum(SysTraffic.size).label("size")).where(
                        SysTraffic.timestamp.between(first_day_of_month, date),
                        SysTraffic.org_id == org_id,
                        SysTraffic.op_type.in_(self.upload_type_list)
                    )
                    org_monthly_upload_traffic_size = self.edb_session.scalars(stmt2).first()

                    upload_traffic_threshold = row[2]

                    # org upload rate limit
                    if (
                        org_monthly_upload_traffic_size
                        and upload_traffic_threshold
                        and org_monthly_upload_traffic_size > upload_traffic_threshold
                    ):

                        upload_limit_format = get_quota_from_string(UPLOAD_LIMIT_WHEN_THROTTLE)
                        seafile_api.org_set_upload_rate_limit(org_id, upload_limit_format)
                        upload_rate_limit_orgs[org_id] = True

                stmt = select(SysTraffic.size).where(
                                           SysTraffic.timestamp == date,
                                           SysTraffic.org_id == org_id,
                                           SysTraffic.op_type == oper).limit(1)
                result = self.edb_session.scalars(stmt).first()

                if result is not None:
                    size_in_db = result
                    stmt = update(SysTraffic).where(SysTraffic.timestamp == date,
                                                    SysTraffic.org_id == org_id,
                                                    SysTraffic.op_type == oper).values(size=size + size_in_db)
                    self.edb_session.execute(stmt)
                else:
                    new_record = SysTraffic(date, oper, size, org_id)
                    self.edb_session.add(new_record)

            except Exception as e:
                logging.warning('Failed to update traffic info: %s.', e)


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
                reset_rate_limit_dates.append(first_day)
                download_rate_limit_orgs.clear()
                upload_rate_limit_orgs.clear()
                download_rate_limit_users.clear()
                upload_rate_limit_users.clear()

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

class UserActivityCounter(object):
    def __init__(self):
        self.edb_session = init_db_session_class()()

    def start_count(self):
        logging.info('Start counting user activity info..')
        ret = 0
        try:
            while True:
                all_keys = list(login_records.keys())
                if len(all_keys) > 300:
                    keys = all_keys[:300]
                    self.update_login_record(keys)
                else:
                    keys = all_keys
                    self.update_login_record(keys)
                    break
            self.edb_session.commit()
            logging.info("[UserActivityCounter] update %s items." % len(all_keys))
        except Exception as e:
            logging.warning('[UserActivityCounter] Failed to update user activity info: %s.', e)
        finally:
            self.edb_session.close()

    def update_login_record(self, keys):
        """ example:
                cmd: 'REPLACE INTO UserActivityStat values (:key1, :name1, :tim1), (:key2, :name2, :time2)'
                data: {key1: xxx, name1: xxx, time1: xxx, key2: xxx, name2: xxx, time2: xxx}
        """
        l = len(keys)
        if l <= 0:
            return

        cmd = "REPLACE INTO UserActivityStat (name_time_md5, username, timestamp, org_id) values"
        cmd_extend = ''.join([' (:key' + str(i) +', :name'+ str(i) +', :time'+ str(i) + ', :org' + str(i) + '),'\
                     for i in range(l)])[:-1]
        cmd += cmd_extend
        data = {}
        for key in keys:
            pop_data = login_records.pop(key)
            i = str(keys.index(key))
            data['key'+i] = key
            data['name'+i] = pop_data[0]
            data['time'+i] = pop_data[1]
            data['org'+i] = pop_data[2]

        self.edb_session.execute(text(cmd), data)
