# coding: utf-8

import os
import logging
import logging.handlers
import datetime

from seaserv import get_related_users_by_repo, get_org_id_by_repo_id, \
    get_related_users_by_org_repo
from .db import save_user_events, save_org_user_events, save_file_audit_event, \
        save_file_update_event, save_perm_audit_event
from seafobj import CommitDiffer, commit_mgr

def RepoUpdateEventHandler(session, msg, ali_mq=None):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    etype = 'repo-update'
    repo_id = elements[1]
    commit_id = elements[2]

    detail = {'repo_id': repo_id,
          'commit_id': commit_id,
    }

    org_id = get_org_id_by_repo_id(repo_id)
    if org_id > 0:
        users = get_related_users_by_org_repo(org_id, repo_id)
    else:
        users = get_related_users_by_repo(repo_id)

    if not users:
        return

    time = datetime.datetime.utcfromtimestamp(msg.ctime)
    if org_id > 0:
        save_org_user_events (session, org_id, etype, detail, users, time)
    else:
        save_user_events (session, etype, detail, users, time)

    commit = commit_mgr.load_commit(repo_id, 1, commit_id)
    if commit is None:
        commit = commit_mgr.load_commit(repo_id, 0, commit_id)
        if commit is None:
            return

    # TODO: maybe handle merge commit.
    if ali_mq and commit.parent_id and not commit.second_parent_id:
        import json

        OP_CREATE = 'create'
        OP_DELETE = 'delete'
        OP_EDIT = 'edit'
        OP_RENAME = 'rename'
        OP_MOVE = 'move'
        OP_RECOVER = 'recover'

        OBJ_FILE = 'file'
        OBJ_DIR = 'dir'

        parent = commit_mgr.load_commit(repo_id, commit.version, commit.parent_id)
        if parent is None:
            return

        differ = CommitDiffer(repo_id, commit.version, parent.root_id, commit.root_id,
                              True, True)
        added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
        renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

        for de in added_files:
            op_type = ''
            if commit.description.encode('utf-8').startswith('Reverted'):
                op_type = OP_RECOVER
            else:
                op_type = OP_CREATE
            msg = { 'op_type': op_type,
                    'obj_type': OBJ_FILE,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in deleted_files:
            msg = { 'op_type': OP_DELETE,
                    'obj_type': OBJ_FILE,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in added_dirs:
            op_type = ''
            if commit.description.encode('utf-8').startswith('Recovered'):
                op_type = OP_RECOVER
            else:
                op_type = OP_CREATE
            msg = { 'op_type': op_type,
                    'obj_type': OBJ_DIR,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'parent_commit_id': parent.commit_id,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in deleted_dirs:
            msg = { 'op_type': OP_DELETE,
                    'obj_type': OBJ_DIR,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'parent_commit_id': parent.commit_id,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in modified_files:
            op_type = ''
            if commit.description.encode('utf-8').startswith('Reverted'):
                op_type = OP_RECOVER
            else:
                op_type = OP_EDIT
            msg = { 'op_type': op_type,
                    'obj_type': OBJ_FILE,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in renamed_files:
            msg = { 'op_type': OP_RENAME,
                    'obj_type': OBJ_FILE,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
                    'new_path': de.new_path,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in moved_files:
            msg = { 'op_type': OP_MOVE,
                    'obj_type': OBJ_FILE,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
                    'new_path': de.new_path,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in renamed_dirs:
            msg = { 'op_type': OP_RENAME,
                    'obj_type': OBJ_DIR,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
                    'new_path': de.new_path,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

        for de in moved_dirs:
            msg = { 'op_type': OP_MOVE,
                    'obj_type': OBJ_DIR,
                    'commit_id': commit_id,
                    'user': commit.creator_name,
                    'date': commit.ctime,
                    'repo_id': repo_id,
                    'repo_name': commit.repo_name.encode('utf-8'),
                    'path': de.path,
                    'size': de.size,
                    'parent_commit_id': parent.commit_id,
                    'new_path': de.new_path,
            }
            msg_str = json.dumps(msg)
            ali_mq.send_msg(msg_str)

def FileUpdateEventHandler(session, msg, ali_mq=None):
    elements = msg.body.split('\t')
    if len(elements) != 3:
        logging.warning("got bad message: %s", elements)
        return

    repo_id = elements[1]
    commit_id = elements[2]

    org_id = get_org_id_by_repo_id(repo_id)

    commit = commit_mgr.load_commit(repo_id, 1, commit_id)
    if commit is None:
        commit = commit_mgr.load_commit(repo_id, 0, commit_id)
        if commit is None:
            return

    time = datetime.datetime.utcfromtimestamp(msg.ctime)

    save_file_update_event(session, time, commit.creator_name, org_id, \
                           repo_id, commit_id, commit.desc)

def FileAuditEventHandler(session, msg, ali_mq=None):
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

    save_file_audit_event(session, timestamp, msg_type, user_name, ip, \
                          user_agent, org_id, repo_id, file_path)

def PermAuditEventHandler(session, msg, ali_mq=None):
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

    save_perm_audit_event(session, timestamp, etype, from_user, to, \
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
