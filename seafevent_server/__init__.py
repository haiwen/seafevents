# -*- coding: utf-8 -*-
import os
import logging
import datetime
import ast

from flask import make_response
from sqlalchemy import desc, select, update

from seafevents.seafevent_server.utils import write_xls, utc_to_local, generate_file_audit_event_type
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seaserv import seafile_api, ccnet_api
from seafevents.events.models import FileAudit, FileUpdate, PermAudit, \
    UserLogin

logger = logging.getLogger('seafevents')


def get_event_log_by_time_to_excel(session, start_time, end_time, log_type, task_id):
    try:
        try:
            session = session()
        except Exception as e:
            session = None
            logger.error('create db session failed. ERROR: {}'.format(e))
            raise Exception('create db session failed. ERROR: {}'.format(e))
        try:
            start_time = ast.literal_eval(start_time)
            end_time = ast.literal_eval(end_time)
        except Exception as e:
            logger.error(e)
            raise RuntimeError('Invalid time range parameter')

        if not isinstance(start_time, (int, float)) or not isinstance(end_time, (int, float)):
            logger.error('Invalid time range parameter')
            raise RuntimeError('Invalid time range parameter')

        if log_type == 'fileaudit':
            obj = FileAudit
            stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                           datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(obj.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Type", "IP", "Device", "Date",
                    "Library Name", "Library ID", "Library Owner", "File Path"]
            data_list = []

            for ev in res:
                event_type, ev.show_device = generate_file_audit_event_type(ev)

                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or \
                                 seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                    ev.timestamp else ''

                row = [username, event_type, ev.ip, ev.show_device,
                       date, repo_name, ev.repo_id, repo_owner, ev.file_path]
                data_list.append(row)

            wb = write_xls('file-access-logs', head, data_list)
            if not wb:
                logger.error('Failed to export excel')
                raise RuntimeError('Failed to export excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-access-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        elif log_type == 'fileupdate':
            obj = FileUpdate
            stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                           datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(obj.timestamp))
            res = session.scalars(stmt).all()

            head = ["User", "Date", "Library Name", "Library ID",
                    "Library Owner", "Action"]
            data_list = []

            for ev in res:

                repo_id = ev.repo_id
                repo = seafile_api.get_repo(repo_id)
                if repo:
                    repo_name = repo.name
                    repo_owner = seafile_api.get_repo_owner(repo_id) or \
                                 seafile_api.get_org_repo_owner(repo_id)
                else:
                    repo_name = 'Deleted'
                    repo_owner = '--'

                username = ev.user if ev.user else 'Anonymous User'
                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                    ev.timestamp else ''

                row = [username, date, repo_name, ev.repo_id, repo_owner, ev.file_oper.strip()]
                data_list.append(row)

            wb = write_xls('file-update-logs', head, data_list)
            if not wb:
                logger.error('Failed to export excel')
                raise RuntimeError('Failed to export excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'file-update-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)

        elif log_type == 'permaudit':
            obj = PermAudit
            stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(start_time),
                                                           datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(obj.timestamp))
            res = session.scalars(stmt).all()

            head = ["From", "To", "Action", "Permission", "Library",
                    "Folder Path", "Date"]
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

                date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                    ev.timestamp else ''

                row = [ev.from_user, to, action, permission, repo_name,
                       ev.file_path, date]
                data_list.append(row)

            wb = write_xls('perm-audit-logs', head, data_list)
            if not wb:
                logger.error('Failed to export excel')
                raise RuntimeError('Failed to export excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'perm-audit-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        elif log_type == 'loginadmin':
            obj = UserLogin
            stmt = select(obj).where(obj.login_date.between(datetime.datetime.utcfromtimestamp(start_time),
                                                            datetime.datetime.utcfromtimestamp(end_time)))
            stmt = stmt.order_by(desc(obj.login_date))
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
                logger.error('Failed to export excel')
                raise RuntimeError('Failed to export excel')

            target_dir = os.path.join('/tmp/seafile_events/', task_id)
            os.makedirs(target_dir, exist_ok=True)
            excel_name = 'login-logs.xlsx'
            target_path = os.path.join(target_dir, excel_name)
            wb.save(target_path)
        else:
            logger.error('Invalid log_type parameter')
            raise RuntimeError('Invalid log_type parameter')
    except Exception as e:
        logger.error(e)
        raise RuntimeError('Internal Server Error')
    finally:
        if session:
            session.close()