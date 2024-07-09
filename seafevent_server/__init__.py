# -*- coding: utf-8 -*-
import os
import logging
import datetime
import ast

from flask import make_response
from sqlalchemy import desc, select, update, func, text, and_
from sqlalchemy.sql import exists

from seafevents.seafevent_server.utils import write_xls, utc_to_local, generate_file_audit_event_type
from seafevents.seafevent_server.export_task_manager import event_export_task_manager
from seaserv import seafile_api, ccnet_api
from seafevents.events.models import FileAudit, FileUpdate, PermAudit, \
    UserLogin

logger = logging.getLogger('seafevents')


def query_status(task_id):
    if not event_export_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id not found.', 404))

    try:
        is_finished, error = event_export_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return make_response((e, 500))

    if error:
        return make_response((error, 500))
    return make_response(({'is_finished': is_finished}, 200))


def get_event_log_by_time_to_excel(session, tstart, tend, log_type, task_id):
    try:
        session = session()
    except Exception as e:
        session = None
        logger.error('create db session failed. ERROR: {}'.format(e))
        raise Exception('create db session failed. ERROR: {}'.format(e))
    try:
        tstart = ast.literal_eval(tstart)
        tend = ast.literal_eval(tend)
    except Exception as e:
        logger.error(e)
        raise RuntimeError('Invalid time range parameter')

    if not isinstance(tstart, (int, float)) or not isinstance(tend, (int, float)):
        logger.error('Invalid time range parameter')
        raise RuntimeError('Invalid time range parameter')

    if log_type == 'fileaudit':
        obj = FileAudit
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()
        session.close()

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
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()
        session.close()

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
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()
        session.close()

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
        stmt = select(obj).where(obj.login_date.between(datetime.datetime.utcfromtimestamp(tstart),
                                                        datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.login_date))
        res = session.scalars(stmt).all()
        session.close()

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
