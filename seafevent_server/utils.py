import os
import ast
import logging
import openpyxl
import datetime
import pytz

from sqlalchemy import desc, select

from seaserv import seafile_api, ccnet_api
from seafevents.events.models import FileAudit, FileUpdate, PermAudit, UserLogin
from seafevents.app.config import TIME_ZONE

logger = logging.getLogger('seafevents')


def write_xls(sheet_name, head, data_list):
    """write listed data into excel
    """

    try:
        wb = openpyxl.Workbook()
        ws = wb.active
    except Exception as e:
        logger.error(e)
        return None

    ws.title = sheet_name

    row_num = 0

    # write table head
    for col_num in range(len(head)):
        c = ws.cell(row=row_num + 1, column=col_num + 1)
        c.value = head[col_num]

    # write table data
    for row in data_list:
        row_num += 1
        for col_num in range(len(row)):
            c = ws.cell(row=row_num + 1, column=col_num + 1)
            c.value = row[col_num]

    return wb


def utc_to_local(dt):
    # change from UTC timezone to current seahub timezone
    tz = pytz.timezone(TIME_ZONE)
    utc = dt.replace(tzinfo=datetime.timezone.utc)
    local = utc.astimezone(tz).replace(tzinfo=None)

    return local


def generate_file_audit_event_type(e):
    event_type_dict = {
        'file-download-web': ('web', ''),
        'file-download-share-link': ('share-link', ''),
        'file-download-api': ('API', e.device),
        'repo-download-sync': ('download-sync', e.device),
        'repo-upload-sync': ('upload-sync', e.device),
        'seadrive-download-file': ('seadrive-download', e.device),
    }

    if e.etype not in event_type_dict:
        event_type_dict[e.etype] = (e.etype, e.device if e.device else '')

    return event_type_dict[e.etype]


def get_event_log_by_time_to_excel(session, start_time, end_time, log_type, task_id):
    start_time = ast.literal_eval(start_time)
    end_time = ast.literal_eval(end_time)

    if not isinstance(start_time, (int, float)) or not isinstance(end_time, (int, float)):
        raise RuntimeError('Invalid time range parameter')

    if log_type not in ['fileaudit', 'fileupdate', 'permaudit', 'loginadmin']:
        raise RuntimeError('Invalid log_type parameter')

    with session() as session:
        if log_type == 'fileaudit':
            stmt = select(FileAudit).where(FileAudit.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                       datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(FileAudit.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Type", "IP", "Device", "Date", "Library Name", "Library ID", "Library Owner", "File Path"]
            data_list = []

            for ev in res:
                event_type, ev.show_device = generate_file_audit_event_type(ev)

                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''

                row = [username, event_type, ev.ip, ev.show_device,
                       date, repo_name, ev.repo_id, repo_owner, ev.file_path]
                data_list.append(row)

            wb = write_xls('file-access-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export file-access-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-access-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        elif log_type == 'fileupdate':
            stmt = select(FileUpdate).where(FileUpdate.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                         datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(FileUpdate.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Date", "Library Name", "Library ID", "Library Owner", "Action"]
            data_list = []

            for ev in res:
                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''
                row = [username, date, repo_name, ev.repo_id, repo_owner, ev.file_oper.strip()]
                data_list.append(row)

            wb = write_xls('file-update-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export file-update-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-update-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)

        elif log_type == 'permaudit':
            stmt = select(PermAudit).where(PermAudit.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                       datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(PermAudit.timestamp))
            res = session.scalars(stmt).all()

            head = ["From", "To", "Action", "Permission", "Library", "Folder Path", "Date"]
            data_list = []

            for ev in res:
                repo = seafile_api.get_repo(ev.repo_id)
                repo_name = repo.repo_name if repo else 'Deleted'

                if '@' in ev.to:
                    to = ev.to
                elif ev.to.isdigit():
                    group = ccnet_api.get_group(int(ev.to))
                    to = group.group_name if group else 'Deleted'
                elif 'all' in ev.to:
                    to = 'Organization'
                else:
                    to = '--'

                if 'add' in ev.etype:
                    action = 'Add'
                elif 'modify' in ev.etype:
                    action = 'Modify'
                elif 'delete' in ev.etype:
                    action = 'Delete'
                else:
                    action = '--'

                if ev.permission == 'rw':
                    permission = 'Read-Write'
                elif ev.permission == 'r':
                    permission = 'Read-Only'
                else:
                    permission = '--'

                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''
                row = [ev.from_user, to, action, permission, repo_name, ev.file_path, date]
                data_list.append(row)

            wb = write_xls('perm-audit-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export perm-audit-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'perm-audit-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        elif log_type == 'loginadmin':
            stmt = select(UserLogin).where(UserLogin.login_date.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                        datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(UserLogin.login_date))
            res = session.scalars(stmt).all()

            head = ["Name", "IP", "Status", "Time"]
            data_list = []
            for log in res:
                login_time = log.login_date.strftime("%Y-%m-%d %H:%M:%S")
                status = 'Success' if log.login_success else 'Failed'
                row = [log.username, log.login_ip, status, login_time]
                data_list.append(row)

            wb = write_xls('login-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export login-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'login-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)


def get_event_org_log_by_time_to_excel(session, start_time, end_time, log_type, task_id, org_id):
    start_time = ast.literal_eval(start_time)
    end_time = ast.literal_eval(end_time)

    if not isinstance(start_time, (int, float)) or not isinstance(end_time, (int, float)):
        raise RuntimeError('Invalid time range parameter')

    if log_type not in ['fileupdate', 'permaudit', 'fileaudit']:
        raise RuntimeError('Invalid log_type parameter')

    with session() as session:
        if log_type == 'fileupdate':
            stmt = select(FileUpdate).where(FileUpdate.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                         datetime.datetime.utcfromtimestamp(end_time)),
                                            FileUpdate.org_id == org_id)
            stmt = stmt.order_by(desc(FileUpdate.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Date", "Library Name", "Library ID", "Library Owner", "Action"]
            data_list = []

            for ev in res:
                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''
                row = [username, date, repo_name, ev.repo_id, repo_owner, ev.file_oper.strip()]
                data_list.append(row)

            wb = write_xls('file-update-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export file-update-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-update-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)

        elif log_type == 'permaudit':
            stmt = select(PermAudit).where(PermAudit.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                                       datetime.datetime.utcfromtimestamp(end_time)),
                                           PermAudit.org_id == org_id)
            stmt = stmt.order_by(desc(PermAudit.timestamp))
            res = session.scalars(stmt).all()

            head = ["From", "To", "Action", "Permission", "Library", "Folder Path", "Date"]
            data_list = []

            for ev in res:
                repo = seafile_api.get_repo(ev.repo_id)
                repo_name = repo.repo_name if repo else 'Deleted'

                if '@' in ev.to:
                    to = ev.to
                elif ev.to.isdigit():
                    group = ccnet_api.get_group(int(ev.to))
                    to = group.group_name if group else 'Deleted'
                elif 'all' in ev.to:
                    to = 'Organization'
                else:
                    to = '--'

                if 'add' in ev.etype:
                    action = 'Add'
                elif 'modify' in ev.etype:
                    action = 'Modify'
                elif 'delete' in ev.etype:
                    action = 'Delete'
                else:
                    action = '--'

                if ev.permission == 'rw':
                    permission = 'Read-Write'
                elif ev.permission == 'r':
                    permission = 'Read-Only'
                else:
                    permission = '--'

                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''
                row = [ev.from_user, to, action, permission, repo_name, ev.file_path, date]
                data_list.append(row)

            wb = write_xls('perm-audit-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export perm-audit-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'perm-audit-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        elif log_type == 'fileaudit':
            stmt = select(FileAudit).where(
                FileAudit.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                            datetime.datetime.utcfromtimestamp(end_time)),
                                            FileAudit.org_id == org_id)
            stmt = stmt.order_by(desc(FileAudit.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Type", "IP", "Device", "Date", "Library Name", "Library ID", "Library Owner",
                    "File Path"]
            data_list = []

            for ev in res:
                event_type, ev.show_device = generate_file_audit_event_type(ev)

                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if ev.timestamp else ''

                row = [username, event_type, ev.ip, ev.show_device,
                       date, repo_name, ev.repo_id, repo_owner, ev.file_path]
                data_list.append(row)

            wb = write_xls('file-access-logs', head, data_list)
            if not wb:
                raise RuntimeError('Failed to export file-access-logs to excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-access-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
