# coding: utf-8

import os
import logging
import logging.handlers
import datetime

from seaserv import get_related_users_by_repo, get_org_id_by_repo_id, \
    get_related_users_by_org_repo
from seafevents.events.db import save_user_events, save_org_user_events, \
        save_file_audit_event, save_file_update_event, save_perm_audit_event
from seafevents.app.config import appconfig
from seafobj import CommitDiffer, commit_mgr
from change_file_path import ChangeFilePathHandler

changer = ChangeFilePathHandler()

#def RepoMoveEventHandler(session, msg):
#    start = msg.body.find('\t')
#    if start < 0:
#        logging.warning("got bad message: %s", msg.body)
#        return
#    dic = eval(msg.body[start+1:])
#    if not dic['src_path']:
#        dic['src_path'] = '/'
#    if not dic['dst_path']:
#        dic['dst_path'] = '/'
#    path = os.path.join(dic['src_path'], dic['src_file_name'])
#    new_path = os.path.join(dic['dst_path'], dic['dst_file_name'])
#    changer.update_db_records(dic['dst_repo_id'], path, new_path,
#                    0 if dic['obj_type'] == 'file' else 1,
#                    dic['src_repo_id'])

def RepoUpdateEventHandler(session, msg):
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

    time = datetime.datetime.utcfromtimestamp(msg.ctime)

    commit = commit_mgr.load_commit(repo_id, 1, commit_id)
    if commit is None:
        commit = commit_mgr.load_commit(repo_id, 0, commit_id)

    # TODO: maybe handle merge commit.
    if commit is not None and commit.parent_id and not commit.second_parent_id:

        parent = commit_mgr.load_commit(repo_id, commit.version, commit.parent_id)

        if parent is not None:
            differ = CommitDiffer(repo_id, commit.version, parent.root_id, commit.root_id,
                                  True, True)
            added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
            renamed_files, moved_files, renamed_dirs, moved_dirs = differ.diff()

            if appconfig.ali:
                send_to_ali_mq(added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
                               renamed_files, moved_files, renamed_dirs, moved_dirs, commit, \
                               commit_id, repo_id, parent)
            else:
                logging.info("Didn't find ali mq config")

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
        users = get_related_users_by_org_repo(org_id, repo_id)
    else:
        users = get_related_users_by_repo(repo_id)

    if not users:
        return

    if org_id > 0:
        save_org_user_events (session, org_id, etype, detail, users, time)
    else:
        save_user_events (session, etype, detail, users, time)

def FileUpdateEventHandler(session, msg):
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
                           repo_id, commit_id, commit.description)

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

    save_file_audit_event(session, timestamp, msg_type, user_name, ip, \
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

    save_perm_audit_event(session, timestamp, etype, from_user, to, \
                          org_id, repo_id, file_path, perm)

def send_to_ali_mq(added_files, deleted_files, added_dirs, deleted_dirs, modified_files, \
                   renamed_files, moved_files, renamed_dirs, moved_dirs, commit, \
                   commit_id, repo_id, parent):
    from seafevents.events.alimq_producer import ali_mq
    import json

    OP_CREATE = 'create'
    OP_DELETE = 'delete'
    OP_EDIT = 'edit'
    OP_RENAME = 'rename'
    OP_MOVE = 'move'
    OP_RECOVER = 'recover'

    OBJ_FILE = 'file'
    OBJ_DIR = 'dir'

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

def register_handlers(handlers, enable_audit):
    handlers.add_handler('seaf_server.event:repo-update', RepoUpdateEventHandler)
    #handlers.add_handler('seaf_server.event:cross-repo-move', RepoMoveEventHandler)
    if enable_audit:
        handlers.add_handler('seaf_server.event:repo-update', FileUpdateEventHandler)
        handlers.add_handler('seahub.stats:file-download-web', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:file-download-api', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:file-download-share-link', FileAuditEventHandler)
        handlers.add_handler('seahub.stats:perm-update', PermAuditEventHandler)
        handlers.add_handler('seaf_server.event:repo-download-sync', FileAuditEventHandler)
        handlers.add_handler('seaf_server.event:repo-upload-sync', FileAuditEventHandler)
