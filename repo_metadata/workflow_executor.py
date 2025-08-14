import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import uuid

from collections import deque, defaultdict
from sqlalchemy import text


from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.utils import get_metadata_by_obj_ids

logger = logging.getLogger(__name__)

PENDING = 'pending'
RUNNING = 'running'
COMPLETED = 'completed'
FAILED = 'failed'

TRIGGER = 'trigger'
CONDITION = 'condition'
ACTION = 'action'

@dataclass
class NodeExecution:
    node_id: str
    status: PENDING
    inputs: Dict[str, Any] = None
    outputs: Dict[str, Any] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0
    
    def __post_init__(self):
        if self.inputs is None:
            self.inputs = {}
        if self.outputs is None:
            self.outputs = {}

@dataclass
class ExecutionContext:
    repo_id: str
    workflow_id: str
    run_id: str
    trigger_data: Dict[str, Any]
    global_variables: Dict[str, Any]
    node_executions: Dict[str, NodeExecution]


    def __post_init__(self):
        if self.global_variables is None:
            self.global_variables = {}
        if self.node_executions is None:
            self.node_executions = {}


class WorkflowGraph:
    def __init__(self, graph_data):
        self.nodes = {node['id']: node for node in graph_data.get('nodes', [])}
        self.edges = graph_data.get('edges', [])
        self.adjacency_list = self._build_adjacency_list()
        self.reverse_adjacency_list = self._build_reverse_adjacency_list()
    

    def _build_adjacency_list(self):
        adj_list = defaultdict(list)
        for edge in self.edges:
            adj_list[edge['source']].append({
                'target': edge['target'],
                'label': edge.get('label', ''),
                'animated': edge.get('animated')
            })
        return dict(adj_list)
    
    def _build_reverse_adjacency_list(self):
        reverse_adj = defaultdict(list)
        for edge in self.edges:
            reverse_adj[edge['target']].append(edge['source'])
        return dict(reverse_adj)
    
    def get_start_nodes(self):
        return [node_id for node_id, node in self.nodes.items() 
                if node.get('type') == 'trigger']
    
    def get_next_nodes(self, node_id, outputs):
        if node_id not in self.adjacency_list:
            return []
        
        next_nodes = []
        node = self.nodes[node_id]
        
        for edge in self.adjacency_list[node_id]:
            target_id = edge['target']
            if node.get('type') == 'condition':
                condition_result = outputs.get('condition_result', False) if outputs else False
                edge_label = edge.get('label', '').lower()
                if (condition_result and edge_label == 'true') or \
                   (not condition_result and edge_label == 'false'):
                    next_nodes.append(target_id)
            else:
                next_nodes.append(target_id)
        
        return next_nodes
    
    def get_node_dependencies(self, node_id: str) -> List[str]:
        return self.reverse_adjacency_list.get(node_id, [])
    

class NodeExecutor:

    def __init__(self, db_connection=None):
        self.db = db_connection
        self.metadata_server_api = MetadataServerAPI('seafevents')
    
    def execute_node(self, context, node_id, graph):
        node = graph.nodes[node_id]
        execution = context.node_executions.get(node_id, NodeExecution(node_id, RUNNING))
        
        try:
            execution.status = RUNNING
            execution.start_time = datetime.now()
            
            inputs = self._prepare_node_inputs(context, node_id, graph)
            execution.inputs = inputs
            
            node_type = node.get('type')
            if node_type == 'trigger':
                success, outputs = self._execute_trigger_node(context, node)
            elif node_type == 'condition':
                success, outputs = self._execute_condition_node(context, node, inputs)
            elif node_type == 'action':
                success, outputs = self._execute_action_node(context, node, inputs)
            else:
                logger.warning(f"Unknown node type: {node_type}")
                success, outputs = True, {}
            
            if success:
                execution.status = COMPLETED
                execution.outputs = outputs
                if outputs:
                    context.global_variables.update(outputs)
            else:
                execution.status = FAILED
            
            execution.end_time = datetime.now()
            
            return execution
        except Exception as e:
            execution.status = FAILED
            execution.end_time = datetime.now()
            logger.error(f"Error executing node {node_id}: {str(e)}")
            return execution
    
    def _prepare_node_inputs(self, context, node_id, graph):
        inputs = {}
        
        dependencies = graph.get_node_dependencies(node_id)
        for dep_node_id in dependencies:
            dep_execution = context.node_executions.get(dep_node_id)
            if dep_execution and dep_execution.outputs:
                inputs.update(dep_execution.outputs)
        inputs.update(context.global_variables)
        inputs.update(context.trigger_data)
        
        return inputs

    def _execute_trigger_node(self, context, node_data):
        config_id = node_data.get('configId')
        if config_id == 'file_upload':
            # file_info = context.trigger_data.record.file_info
            # outputs = {
            #     'file_info': file_info,
            #     'trigger_type': 'file_upload',
            #     'file_name': file_info.get('name', ''),
            #     'file_size': file_info.get('size', 0),
            #     'file_path': file_info.get('path', '')
            # }
            
            return True, {}
        
        return True, {}
    

    def _execute_condition_node(self, context, node, inputs):
        node_data = node.get('data', {})
        config_id = node_data.get('configId')
        params = node_data.get('params', {})
        record = inputs.get('record', {})
        if config_id == 'if_else':
            for key, value in params.items():
                if key == 'file_type':
                    commit = record.get('commit_diff')[0]
                    file_name = os.path.basename(commit.get('path'))
                    file_type = file_name.split('.')[-1]
                    result = file_type == value

                outputs = {
                    'condition_result': result,
                    'condition_key': key,
                    'condition_value': value
                }
                
                return True, outputs
        
        return True, {'condition_result': False}


    def _execute_action_node(self, context, node, inputs):
        node_data = node.get('data', {})
        config_id = node_data.get('configId')
        params = node_data.get('params', {})
        
        if config_id == 'set_status':
            status = params.get('status', '_in_progress')
            record = inputs.get('record', {})
            success = self._set_file_status(
                context.repo_id,
                record,
                status
            )
            
            outputs = {
                'status_set': status,
                'success': success
            }
            
            return success, outputs
        
        return True, {}

    
    def _set_file_status(self, repo_id, record, status):
        try:
            from seafevents.repo_metadata.constants import METADATA_TABLE
            
            commit = record.get('commit_diff')[0]
            obj_id = commit.get('obj_id')
            
            time.sleep(0.1)
            query_result = get_metadata_by_obj_ids(repo_id, [obj_id], self.metadata_server_api)
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
    

    def _record_node_execution_start(self, context, node, execution):
        try:
            sql = text("""
                INSERT INTO workflow_node_executions
                (repo_id, workflow_id, workflow_run_id, node_type, node_id, title, status, created_at, inputs)
                VALUES (:repo_id, :workflow_id, :workflow_run_id, :node_type, :node_id, :title, :status, :created_at, :inputs)
            """)
            
            self.db.execute(sql, {
                'repo_id': context.repo_id,
                'workflow_id': context.workflow_id,
                'workflow_run_id': context.run_id,
                'node_type': node['type'],
                'node_id': node['id'],
                'title': node.get('data', {}).get('label', 'Unnamed Node'),
                'status': 'running',
                'created_at': datetime.now(),
                'inputs': json.dumps(execution.inputs, ensure_ascii=False)
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record node execution start: {str(e)}")
    
    def _record_node_execution_completion(self, context, node, execution):
        
        try:
            sql = text("""
                UPDATE workflow_node_executions 
                SET status = :status, finished_at = :finished_at, outputs = :outputs
                WHERE workflow_run_id = :workflow_run_id AND node_id = :node_id
            """)
            
            self.db.execute(sql, {
                'status': execution.status,
                'finished_at': execution.end_time,
                'outputs': json.dumps(execution.outputs, ensure_ascii=False),
                'workflow_run_id': context.run_id,
                'node_id': execution.node_id
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record node execution completion: {str(e)}")


class WorkflowExecutor:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.node_executor = NodeExecutor(db_connection)
    
    def execute_workflow(self, repo_id, workflow_data, trigger_data):
        try:
            graph_data = self._parse_graph(workflow_data.get('graph'))
            if not graph_data:
                logger.error(f"Invalid workflow graph for workflow {workflow_data.get('id')}")
                return False
            
            graph = WorkflowGraph(graph_data)
            context = ExecutionContext(
                repo_id=repo_id,
                workflow_id=workflow_data.get('id'),
                run_id=str(uuid.uuid4()),
                trigger_data=trigger_data,
                global_variables={},
                node_executions={}
                )
            
            self._record_workflow_run_start(context, graph_data)
            
            success = self._execute_workflow_with_queue(context, graph)
            
            if success:
                self._update_workflow_run_status(context, COMPLETED)
                logger.info(f"Workflow {context.workflow_id} executed successfully")
            else:
                self._update_workflow_run_status(context, FAILED)
                logger.error(f"Workflow {context.workflow_id} execution failed")
            
            return success
            
        except Exception as e:
            logger.error(f"Error executing workflow: {str(e)}")
            return False
    
    def _execute_workflow_with_queue(self, context, graph):
        start_nodes = graph.get_start_nodes()
        if not start_nodes:
            logger.error("No start nodes found")
            return False
        
        execution_queue = deque(start_nodes)
        completed_nodes = set()
        failed_nodes = set()
        
        while execution_queue:
            node_id = execution_queue.popleft()
            dependencies = graph.get_node_dependencies(node_id)
            if not all(dep in completed_nodes for dep in dependencies):
                # 如果前置节点未执行完，则将节点放回队列中
                execution_queue.append(node_id)
                continue
            
            try:
                execution_result = self.node_executor.execute_node(context, node_id, graph)
                context.node_executions[node_id] = execution_result
                
                if execution_result.status == COMPLETED:
                    completed_nodes.add(node_id)
                    next_nodes = graph.get_next_nodes(node_id, execution_result.outputs)
                    for next_node in next_nodes:
                        if (next_node not in completed_nodes and 
                            next_node not in failed_nodes and 
                            next_node not in execution_queue):
                            execution_queue.append(next_node)
                else:
                    failed_nodes.add(node_id)
                    
            except Exception as e:
                failed_nodes.add(node_id)
                logger.exception(f"Error executing node {node_id}: {str(e)}")
        
        if failed_nodes:
            logger.error(f"Workflow failed - failed nodes: {failed_nodes}")
            return False
        
        logger.info(f"Workflow completed successfully - executed {len(completed_nodes)} nodes")
        return True
    
    def _parse_graph(self, graph_str: str) -> Optional[Dict[str, Any]]:
        if not graph_str:
            return None
        try:
            return json.loads(graph_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse graph JSON: {str(e)}")
            return None
    
    def _record_workflow_run_start(self, context, graph_data):
        try:
            sql = text("""
                INSERT INTO workflow_run 
                (id, workflow_id, repo_id, graph, status, triggered_from, created_by, created_at, total_steps)
                VALUES (:id, :workflow_id, :repo_id, :graph, :status, :triggered_from, :created_by, :created_at, :total_steps)
            """)
            
            graph_json = json.dumps(graph_data, ensure_ascii=False)
            total_steps = len(graph_data.get('nodes', []))
            record = context.trigger_data.get('record')
            self.db.execute(sql, {
                'id': context.run_id,
                'workflow_id': context.workflow_id,
                'repo_id': context.repo_id,
                'graph': graph_json,
                'status': RUNNING,
                'triggered_from': 'file_upload',
                'created_by': record.get('op_user'),
                'created_at': datetime.now(),
                'total_steps': total_steps
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record workflow run start: {str(e)}")
    
    def _update_workflow_run_status(self, context, status):
        try:
            sql = text("""
                UPDATE workflow_run 
                SET status = :status, finished_at = :finished_at
                WHERE id = :id
            """)
            
            self.db.execute(sql, {
                'status': status,
                'finished_at': datetime.now(),
                'id': context.run_id
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record workflow run completion: {str(e)}")



class WorkflowTriggerHandler:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.executor = WorkflowExecutor(db_connection)
    
    def handle_file_upload_workflow(self, repo_id, record):
        """file upload trigger"""
        workflows = self._get_active_workflows(repo_id)
        
        if not workflows:
            logger.info(f"No active workflows found for repo {repo_id}")
            return
        
        for workflow in workflows:
            if workflow.get('trigger_from') == 'file_upload':
                logger.info(f"Triggering workflow {workflow['id']} for file upload in repo {repo_id}")
                
                trigger_data = {
                    'trigger_type': 'file_upload',
                    'record': record,
                    'timestamp': datetime.now().isoformat()
                }
                
                success = self.executor.execute_workflow(repo_id, workflow, trigger_data)
                if not success:
                    logger.error(f"Workflow {workflow['id']} execution failed")

    
    def _get_active_workflows(self, repo_id):
        if not self.db:
            return []
        
        try:
            sql = text("""
                SELECT id, name, graph, is_valid, created_by, updated_by, created_at, updated_at, trigger_from
                FROM workflows 
                WHERE repo_id = :repo_id AND is_valid = 1
            """)
            
            cursor = self.db.execute(sql, {'repo_id': repo_id})
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
            
        except Exception as e:
            logger.error(f"Failed to get active workflows: {str(e)}")
            return []

def on_file_upload_event(session, record):
    try:
        repo_id = record.get('repo_id')
        trigger_handler = WorkflowTriggerHandler(db_connection=session)
        trigger_handler.handle_file_upload_workflow(repo_id, record)
        
    except Exception as e:
        logger.error(f"Error in file upload workflow event handler: {str(e)}")

