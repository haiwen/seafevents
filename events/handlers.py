# coding: utf-8

import copy
import logging
import logging.handlers
import datetime

from seaserv import get_org_id_by_repo_id, seafile_api, get_commit
from seafobj import CommitDiffer, commit_mgr
from seafevents.events.db import save_user_activity, \
        save_file_audit_event, save_file_update_event, save_perm_audit_event
from change_file_path import ChangeFilePathHandler

changer = ChangeFilePathHandler()

def RepoUpdateEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    commit_id = elements[2]

    commit = commit_mgr.load_commit(repo_id, 1, commit_id)
    if commit is None:
        commit = commit_mgr.load_commit(repo_id, 0, commit_id)

    # TODO: maybe handle merge commit.
    if commit is not None and commit.parent_id and not commit.second_parent_id:

        parent = commit_mgr.load_commit(repo_id, commit.version, commit.parent_id)

        if parent is not None:
            differ = CommitDiffer(repo_id, commit.version, parent.root_id, commit.root_id,
                                  True, True)
            added_files, deleted_files, added_dirs, deleted_dirs, modified_files,\
                    renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

            if renamed_files or renamed_dirs or moved_files or moved_dirs:
                for r_file in renamed_files:
                    changer.update_db_records(repo_id, r_file.path, r_file.new_path, 0)
                for r_dir in renamed_dirs:
                    changer.update_db_records(repo_id, r_dir.path, r_dir.new_path, 1)
                for m_file in moved_files:
                    changer.update_db_records(repo_id, m_file.path, m_file.new_path, 0)
                for m_dir in moved_dirs:
                    changer.update_db_records(repo_id, m_dir.path, m_dir.new_path, 1)

            org_id = get_org_id_by_repo_id(repo_id)
            if org_id > 0:
                users_obj = seafile_api.org_get_shared_users_by_repo(org_id, repo_id)
                owner = seafile_api.get_org_repo_owner(repo_id)
            else:
                users_obj = seafile_api.get_shared_users_by_repo(repo_id)
                owner = seafile_api.get_repo_owner(repo_id)

            users = [e.user for e in users_obj] + [owner]
            if not users:
                return

            # skip merged commit
            new_merge = False
            if commit.second_parent_id is not None and commit.new_merge is True and commit.conflict is False:
                new_merge = True

            if not new_merge:
                time = datetime.datetime.utcfromtimestamp(msg.ctime)
                if added_files or deleted_files or added_dirs or deleted_dirs or \
                        modified_files or renamed_files or moved_files or renamed_dirs or moved_dirs:

                    records = generate_records(added_files, deleted_files,
                            added_dirs, deleted_dirs, modified_files, renamed_files,
                            moved_files, renamed_dirs, moved_dirs, commit, repo_id,
                            parent, users, time)

                    save_records_to_activity(session, records)
                else:
                    save_record(session, commit, repo_id, parent, org_id, users, time)

def save_record(session, commit, repo_id, parent, org_id, related_users, time):
    repo = seafile_api.get_repo(repo_id)
    if org_id > 0:
        repo_owner = seafile_api.get_org_repo_owner(repo_id)
    else:
        repo_owner = seafile_api.get_repo_owner(repo_id)

    record = {
        'op_type': 'rename',
        'obj_type': 'repo',
        'timestamp': time,
        'repo_id': repo_id,
        'repo_name': repo.repo_name,
        'path': '/',
        'op_user': commit.creator_name,
        'related_users': related_users,
        'commit_id': commit.commit_id,
        'old_repo_name': parent.repo_name
    }
    save_user_activity(session, record)

def save_records_to_activity(session, records):
    if isinstance(records, list):
        for record in records:
            save_user_activity(session, record)

def generate_records(added_files, deleted_files, added_dirs,
        deleted_dirs, modified_files, renamed_files, moved_files, renamed_dirs,
        moved_dirs, commit, repo_id, parent, related_users, time):

    OP_CREATE = 'create'
    OP_DELETE = 'delete'
    OP_EDIT = 'edit'
    OP_RENAME = 'rename'
    OP_MOVE = 'move'
    OP_RECOVER = 'recover'

    OBJ_FILE = 'file'
    OBJ_DIR = 'dir'

    repo = seafile_api.get_repo(repo_id)
    base_record = {
        'commit_id': commit.commit_id,
        'timestamp': time,
        'repo_id': repo_id,
        'related_users': related_users,
        'op_user': commit.creator_name,
        'repo_name': repo.repo_name
    }
    records = []

    for de in added_files:
        record = copy.copy(base_record)
        op_type = ''
        if commit.description.encode('utf-8').startswith('Reverted'):
            op_type = OP_RECOVER
        else:
            op_type = OP_CREATE
        record['op_type'] = op_type
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_FILE
        record['path'] = de.path
        record['size'] = de.size
        records.append(record)

    for de in deleted_files:
        record = copy.copy(base_record)
        record['op_type'] = OP_DELETE
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_FILE
        record['size'] = de.size
        record['path'] = de.path
        records.append(record)

    for de in added_dirs:
        record = copy.copy(base_record)
        op_type = ''
        if commit.description.encode('utf-8').startswith('Recovered'):
            op_type = OP_RECOVER
        else:
            op_type = OP_CREATE
        record['op_type'] = op_type
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_DIR
        record['path'] = de.path
        records.append(record)

    for de in deleted_dirs:
        record = copy.copy(base_record)
        record['op_type'] = OP_DELETE
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_DIR
        record['path'] = de.path
        records.append(record)

    for de in modified_files:
        record = copy.copy(base_record)
        op_type = ''
        if commit.description.encode('utf-8').startswith('Reverted'):
            op_type = OP_RECOVER
        else:
            op_type = OP_EDIT
        record['op_type'] = op_type
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_FILE
        record['path'] = de.path
        record['size'] = de.size
        records.append(record)

    for de in renamed_files:
        record = copy.copy(base_record)
        record['op_type'] = OP_RENAME
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_FILE
        record['path'] = de.new_path
        record['size'] = de.size
        record['old_path'] = de.path
        records.append(record)

    for de in moved_files:
        record = copy.copy(base_record)
        record['op_type'] = OP_MOVE
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_FILE
        record['path'] = de.new_path
        record['size'] = de.size
        record['old_path'] = de.path
        records.append(record)

    for de in renamed_dirs:
        record = copy.copy(base_record)
        record['op_type'] = OP_RENAME
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_DIR
        record['path'] = de.new_path
        record['size'] = de.size
        record['old_path'] = de.path
        records.append(record)

    for de in moved_dirs:
        record = copy.copy(base_record)
        record['op_type'] = OP_MOVE
        record['obj_id'] = de.obj_id
        record['obj_type'] = OBJ_DIR
        record['path'] = de.new_path
        record['size'] = de.size
        record['old_path'] = de.path
        records.append(record)

    return records

def FileUpdateEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    commit_id = elements[2]

    org_id = get_org_id_by_repo_id(repo_id)

    commit = get_commit(repo_id, 1, commit_id)
    if commit is None:
        commit = get_commit(repo_id, 0, commit_id)
        if commit is None:
            return

    time = datetime.datetime.utcfromtimestamp(msg.ctime)

    save_file_update_event(session, time, commit.creator_name, org_id,
                           repo_id, commit_id, commit.desc)

def FileAuditEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 6:
        logging.warning("got bad message: %s", elements)
        return

    timestamp = datetime.datetime.utcfromtimestamp(msg.ctime)
    msg_type = elements[0]
    user_name = elements[1]
    ip = elements[2]
    user_agent = elements[3]
    repo_id = elements[4]
    file_path = elements[5].decode('utf-8')

    org_id = get_org_id_by_repo_id(repo_id)

    save_file_audit_event(session, timestamp, msg_type, user_name, ip,
                          user_agent, org_id, repo_id, file_path)

def PermAuditEventHandler(session, msg):
    elements = msg.body.split('\t')
    if len(elements) != 7:
        logging.warning("got bad message: %s", elements)
        return

    timestamp = datetime.datetime.utcfromtimestamp(msg.ctime)
    etype = elements[1]
    from_user = elements[2]
    to = elements[3]
    repo_id = elements[4]
    file_path = elements[5].decode('utf-8')
    perm = elements[6]

    org_id = get_org_id_by_repo_id(repo_id)

    save_perm_audit_event(session, timestamp, etype, from_user, to,
                          org_id, repo_id, file_path, perm)

def register_handlers(handlers, enable_audit):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdateEventHandler)
    if enable_audit:
        handlers.add_handler('seaf_server.event:repo-update', FileUpdateEventHandler)
        handlers.add_handler('seahub.stats:file-download-web', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:file-download-api', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:file-download-share-link', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:perm-update', PermAuditEventHandler)
        handlers.add_handler('seaf_server.event:repo-download-sync', FileAuditEventHandler)
        handlers.add_handler('seaf_server.event:repo-upload-sync', FileAuditEventHandler)
