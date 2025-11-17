import os
import time
import logging
from sqlalchemy import text
from datetime import datetime
from seafevents.repo_workflow.constants import NodeStatus

logger = logging.getLogger(__name__)

# workflow
def get_active_workflows(session, repo_id):
    sql = text("""
                SELECT id, name, graph, is_valid, created_by, updated_by, created_at, updated_at, trigger_from
                FROM workflows 
                WHERE repo_id = :repo_id AND is_valid = 1
            """)
            
    cursor = session.execute(sql, {'repo_id': repo_id})
    rows = cursor.fetchall()
    workflows = []
    
    for row in rows:
        workflows.append({
            'id': row[0],
            'name': row[1],
            'graph': row[2],
            'is_valid': row[3],
            'created_by': row[4],
            'updated_by': row[5],
            'created_at': row[6],
            'updated_at': row[7],
            'trigger_from': row[8]
        })
    
    return workflows


def set_workflow_valid(session, workflow_id, is_valid):
    sql = text("""
                UPDATE workflows 
                SET is_valid = :is_valid
                WHERE id = :workflow_id
            """)
            
    session.execute(sql, {'workflow_id': workflow_id, 'is_valid': is_valid})
    session.commit()
    return True


# workflow run
def record_workflow_run_start(session, context, graph_data):
    sql = text("""
        INSERT INTO workflow_run 
        (id, workflow_id, repo_id, status, created_by, created_at, total_steps)
        VALUES (:id, :workflow_id, :repo_id, :status, :created_by, :created_at, :total_steps)
    """)
    
    total_steps = len(graph_data.get('nodes', []))
    record = context.trigger_data.get('record')
    session.execute(sql, {
        'id': context.run_id,
        'workflow_id': context.workflow_id,
        'repo_id': context.repo_id,
        'status': NodeStatus.RUNNING,
        'created_by': record.get('op_user'),
        'created_at': datetime.now(),
        'total_steps': total_steps
    })
    session.commit()

def update_workflow_run_status(session, context, status, elapsed_time):
    sql = text("""
        UPDATE workflow_run 
        SET status = :status, finished_at = :finished_at, elapsed_time = :elapsed_time
        WHERE id = :id
    """)
    
    session.execute(sql, {
        'status': status,
        'finished_at': datetime.now(),
        'elapsed_time': elapsed_time,
        'id': context.run_id
    })
    session.commit()

# node action 
def set_file_status(metadata_server_api, repo_id, record, status):
    from seafevents.repo_metadata.constants import METADATA_TABLE
    commit_diff = record.get('commit_diff', [])
    if not commit_diff:
        logger.error("Missing commit_diff data")
        return False
    
    commit = commit_diff[0]
    file_path = commit.get('path')
    parent_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    sql = f'SELECT * FROM `{METADATA_TABLE.name}` WHERE \
        `{METADATA_TABLE.columns.parent_dir.name}`=? AND `{METADATA_TABLE.columns.file_name.name}`=?;'
    parameters = [parent_dir, file_name]
    try:
        time.sleep(0.2)
        query_result = metadata_server_api.query_rows(repo_id, sql, parameters)
    except Exception as e:
        logger.error(e)
        return False
    rows = []
    for row in query_result.get('results', []):
        record_id = row.get(METADATA_TABLE.columns.id.name)
        if record_id:
            rows.append({
                METADATA_TABLE.columns.id.name: record_id,
                '_status': status
            })
    if rows:
        metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, rows)
        return True
    else:
        return False
