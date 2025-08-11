import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any
from dataclasses import dataclass
import uuid


from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.utils import get_file_type_ext_by_name

from sqlalchemy import text

logger = logging.getLogger(__name__)

PENDING = 'pending'
RUNNING = 'running'
COMPLETED = 'completed'
FAILED = 'failed'


@dataclass
class ExecutionContext:
    repo_id: str
    workflow_id: str
    run_id: int
    trigger_data: Dict[str, Any]
    variables: Dict[str, Any]
    file_info: Dict[str, Any]


class WorkflowExecutor:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.metadata_server_api = MetadataServerAPI('seafevents')
        
    def execute_workflow(self, repo_id, workflow_data, trigger_data):
        try:
            graph_data = self._parse_graph(workflow_data.get('graph'))
            if not graph_data:
                logger.error(f"Invalid workflow graph for workflow {workflow_data.get('id')}")
                return False
            
            context = ExecutionContext(
                repo_id=repo_id,
                workflow_id=workflow_data.get('id'),
                run_id=uuid.uuid4(),
                trigger_data=trigger_data,
                variables={},
                file_info=trigger_data.get('file_info', {})
            )
            
            self._record_workflow_run_start(context, graph_data)
            
            start_nodes = self._find_trigger_nodes(graph_data['nodes'])
            if not start_nodes:
                logger.error(f"No trigger nodes found in workflow {context.workflow_id}")
                return False
            
            success = self._execute_nodes(context, graph_data, start_nodes)
            if success:
                self._record_workflow_run_completion(context)
                logger.info(f"Workflow {context.workflow_id} executed successfully")
            else:
                logger.error(f"Workflow {context.workflow_id} execution failed")
            
            return success
            
        except Exception as e:
            logger.error(f"Error executing workflow: {str(e)}")
            return False
    
    def _parse_graph(self, graph_str):
        if not graph_str:
            return None
        try:
            return json.loads(graph_str)
        except json.JSONDecodeError:
            return None
    
    def _find_trigger_nodes(self, nodes):
        return [node for node in nodes if node.get('type') == 'trigger']
    
    def _execute_nodes(self, context, graph_data, current_nodes):
        if not current_nodes:
            return True
        
        for node in current_nodes:
            success, outputs = self._execute_single_node(context, node)
            
            if not success:
                logger.error(f"Node {node['id']} execution failed")
                return False
            
            if outputs:
                context.variables.update(outputs)
            
            # 查找下一个节点
            next_nodes = self._find_next_nodes(graph_data, node, outputs)
            
            # 递归执行下一个节点
            if next_nodes:
                success = self._execute_nodes(context, graph_data, next_nodes)
                if not success:
                    return False
        
        return True
    
    def _execute_single_node(self, context, node):
        node_id = node['id']
        node_type = node['type']
        node_data = node.get('data', {})
        
        logger.info(f"Executing node {node_id} of type {node_type}")
        
        self._record_node_execution_start(context, node)
        
        try:
            # 根据节点类型执行不同的逻辑
            if node_type == 'trigger':
                success, outputs = self._execute_trigger_node(context, node_data)
            elif node_type == 'condition':
                success, outputs = self._execute_condition_node(context, node_data)
            elif node_type == 'action':
                success, outputs = self._execute_action_node(context, node_data)
            else:
                logger.warning(f"Unknown node type: {node_type}")
                success, outputs = True, {}
            
            if success:
                self._record_node_execution_completion(context, node, outputs)
            
            return success, outputs
            
        except Exception as e:
            error_msg = f"Error executing node {node_id}: {str(e)}"
            logger.error(error_msg)
            return False, {}
    
    def _execute_trigger_node(self, context, node_data):
        """执行触发器节点"""
        config_id = node_data.get('configId')
        params = node_data.get('params', {})
        
        if config_id == 'file_upload':
            file_info = context.file_info
            file_types = params.get('fileTypes', 'all')
            
            if file_types != 'all':
                file_type, suffix= get_file_type_ext_by_name(file_info.get('name', ''))
                print(file_type, suffix, '----===')
                if not self._match_file_type(file_type, file_types):
                    logger.info(f"File type {file_type} does not match {file_types}, skipping workflow")
                    return False, {}
            
            outputs = {
                'file_info': file_info,
                'trigger_type': 'file_upload',
                'file_name': file_info.get('name', ''),
                'file_size': file_info.get('size', 0),
                'file_path': file_info.get('path', '')
            }
            
            return True, outputs
        # elif ....(other config)
        
        return True, {}
    
    def _execute_condition_node(self, context, node_data):
        config_id = node_data.get('configId')
        params = node_data.get('params', {})
        
        if config_id == 'if_else':
            condition = params.get('file_type', '')
            file_info = context.file_info
            result = False
            
            if condition == 'file_type':
                file_name = file_info.get('name', '')
                result = file_name.lower().endswith('.pdf')
            
            outputs = {
                'condition_result': result,
                'condition_type': condition
            }
            
            return True, outputs
        
        return True, {'condition_result': False}
    
    def _execute_action_node(self, context, node_data):
        config_id = node_data.get('configId')
        params = node_data.get('params', {})
        
        if config_id == 'set_status':
            status = params.get('status', '_in_progress')
            file_info = context.file_info
            print(status,' --status')
            success = self._set_file_status(
                context.repo_id, 
                file_info.get('path', ''), 
                status, 
            )
            
            outputs = {
                'status_set': status,
                'file_path': file_info.get('path', '')
            }
            
            return success, outputs
        
        return True, {}
    
    def _find_next_nodes(self, graph_data, current_node, outputs):
        edges = graph_data.get('edges', [])
        nodes = graph_data.get('nodes', [])
        current_node_id = current_node['id']
        
        # 查找从当前节点出发的边
        outgoing_edges = [edge for edge in edges if edge['source'] == current_node_id]
        
        if not outgoing_edges:
            return []
        
        next_node_ids = []
        
        # 处理条件节点的分支逻辑
        if current_node['type'] == 'condition':
            condition_result = outputs.get('condition_result', False)
            
            for edge in outgoing_edges:
                edge_label = edge.get('label', '')
                if (condition_result and edge_label == 'True') or \
                   (not condition_result and edge_label == 'False'):
                    next_node_ids.append(edge['target'])
        else:
            # 非条件节点，执行所有连接的下一个节点
            next_node_ids = [edge['target'] for edge in outgoing_edges]
        
        # 根据ID查找节点对象
        next_nodes = [node for node in nodes if node['id'] in next_node_ids]
        
        return next_nodes

    
    def _match_file_type(self, file_type, target_type):
        """检查文件类型是否匹配"""
        if target_type == 'all':
            return True
        return file_type == target_type
    
    def _set_file_status(self, repo_id, file_path, status):
        try:
            from seafevents.repo_metadata.constants import METADATA_TABLE
            sql = f'SELECT * FROM `{METADATA_TABLE.name}` WHERE \
                `{METADATA_TABLE.columns.parent_dir.name}`=? AND `{METADATA_TABLE.columns.file_name.name}`=?;'
            parent_dir = os.path.dirname(file_path)
            file_name = os.path.basename(file_path)
            parameters = [parent_dir, file_name]
            try:
                time.sleep(0.2)
                query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            except Exception as e:
                logger.error(e)
                return False
            rows = []
            for row in query_result:
                record_id = row.get(METADATA_TABLE.columns.id.name)
                rows.append({
                    METADATA_TABLE.columns.id.name: record_id,
                    '_status': status
                })
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, rows)
            
            return True
        except Exception as e:
            logger.error(f"Failed to set file status: {str(e)}")
            return False
    
    def _record_workflow_run_start(self, context, graph_data):
        if not self.db:
            return
        
        try:
            sql = text("""
                INSERT INTO workflow_run 
                (id, workflow_id, repo_id, graph, status, triggered_from, created_by, created_at, total_steps)
                VALUES (:id, :workflow_id, :repo_id, :graph, :status, :triggered_from, :created_by, :created_at, :total_steps)
            """)
            
            graph_json = json.dumps(graph_data, ensure_ascii=False)
            total_steps = len(graph_data.get('nodes', []))
            
            self.db.execute(sql, {
               'id':context.run_id,
                'workflow_id':context.workflow_id,
                'repo_id':context.repo_id,
                'graph':graph_json,
                'status':'running',
                'triggered_from': 'file_upload',
                'created_by':'system',
                'created_at':datetime.now(),
                'total_steps':total_steps
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record workflow run start: {str(e)}")
    
    def _record_workflow_run_completion(self, context):
        if not self.db:
            return
        
        try:
            sql = text("""
                UPDATE workflow_run 
                SET status = :status, finished_at = :finished_at
                WHERE id = :id
            """)
            
            self.db.execute(sql, {
                'status':'completed',
                'finished_at':datetime.now(),
                'id':context.run_id
            })

            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to record workflow run completion: {str(e)}")
    
    def _record_node_execution_start(self, context: ExecutionContext, node: Dict):
        if not self.db:
            return
        
        try:
            sql = text("""
                INSERT INTO workflow_node_executions
                (repo_id, workflow_id, workflow_run_id, node_type, node_id, title, status, created_at)
                VALUES (:repo_id, :workflow_id, :workflow_run_id, :node_type, :node_id, :title, :status, :created_at)
            """)
            
            self.db.execute(sql, {
                'repo_id':context.repo_id,
                'workflow_id':context.workflow_id,
                'workflow_run_id':context.run_id,
                'node_type':node['type'],
                'node_id':node['id'],
                'title':node.get('data', {}).get('label', 'Unnamed Node'),
                'status':'running',
                'created_at':datetime.now()
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record node execution start: {str(e)}")
    
    def _record_node_execution_completion(self, context: ExecutionContext, node: Dict, outputs: Dict):
        if not self.db:
            return
        
        try:
            sql = text("""
                UPDATE workflow_node_executions 
                SET status = :status, finished_at = :finished_at, outputs = :outputs
                WHERE workflow_run_id = :workflow_run_id AND node_id = :node_id
            """)
            
            outputs_json = json.dumps(outputs, ensure_ascii=False)
            
            self.db.execute(sql, {
                'status':'completed',
                'finished_at':datetime.now(),
                'outputs':outputs_json,
                'workflow_run_id':context.run_id,
                'node_id':node['id']
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record node execution completion: {str(e)}")


class WorkflowTriggerHandler:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.executor = WorkflowExecutor(db_connection)
    
    def handle_file_upload(self, repo_id, file_info):
        try:
            workflows = self._get_active_workflows(repo_id)
            
            if not workflows:
                return
            
            for workflow in workflows:
                if self._has_file_upload_trigger(workflow):
                    logger.info(f"Triggering workflow {workflow['id']} for file upload in repo {repo_id}")
                    
                    trigger_data = {
                        'trigger_type': 'file_upload',
                        'file_info': file_info,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # 异步执行工作流（在实际环境中可能需要使用消息队列）
                    self._execute_workflow_async(repo_id, workflow, trigger_data)
                    
        except Exception as e:
            logger.error(f"Error handling file upload trigger: {str(e)}")
    
    def _get_active_workflows(self, repo_id: str):
        if not self.db:
            return []
        
        try:
            sql = text("""
                SELECT id, name, graph, is_valid, created_by, updated_by, created_at, updated_at
                FROM workflows 
                WHERE repo_id = :repo_id AND is_valid = 1
            """)
            
            cursor = self.db.execute(sql, {'repo_id':repo_id})
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
                    'updated_at': row[7]
                })
            return workflows
            
        except Exception as e:
            logger.error(f"Failed to get active workflows: {str(e)}")
            return []
    
    def _has_file_upload_trigger(self, workflow: Dict) -> bool:
        try:
            graph_data = json.loads(workflow.get('graph', '{}'))
            nodes = graph_data.get('nodes', [])
            
            for node in nodes:
                if (node.get('type') == 'trigger' and 
                    node.get('data', {}).get('configId') == 'file_upload'):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking file upload trigger: {str(e)}")
            return False
    
    def _execute_workflow_async(self, repo_id, workflow, trigger_data):
        try:
            success = self.executor.execute_workflow(repo_id, workflow, trigger_data)
            
            if success:
                logger.info(f"Workflow {workflow['id']} executed successfully")
            else:
                logger.error(f"Workflow {workflow['id']} execution failed")
                
        except Exception as e:
            logger.error(f"Error in async workflow execution: {str(e)}")



def on_file_upload_event(session, record):
    try:
        repo_id = record.get('repo_id')
        commit = record.get('commit_diff')[0]
        file_path = commit.get('path')
        file_size = commit.get('size')
        obj_id = commit.get('obj_id')
        # # 构造文件信息
        file_info = {
            'name': file_path.split('/')[-1] if '/' in file_path else file_path,
            'path': file_path,
            'size': file_size,
            'obj_id': obj_id,
            'upload_time': datetime.now().isoformat()
        }
        
        trigger_handler = WorkflowTriggerHandler(db_connection=session)
        trigger_handler.handle_file_upload(repo_id, file_info)
        
    except Exception as e:
        logger.error(f"Error in file upload event handler: {str(e)}")

