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
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_workflow.constants import NodeStatus, NodeType, ActionType

logger = logging.getLogger(__name__)


@dataclass
class NodeExecution:
    node_id: str
    status: str = NodeStatus.PENDING
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
class WorkflowRunContext:
    repo_id: str
    workflow_id: str
    run_id: str
    trigger_data: Dict[str, Any]
    global_variables: Dict[str, Any] = None
    node_executions: Dict[str, NodeExecution] = None

    # def __post_init__(self):
    #     if self.global_variables is None:
    #         self.global_variables = {}
    #     if self.node_executions is None:
    #         self.node_executions = {}


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
                'sourceHandle': edge.get('sourceHandle', ''),
            })
        return dict(adj_list)
    
    def _build_reverse_adjacency_list(self):
        reverse_adj = defaultdict(list)
        for edge in self.edges:
            reverse_adj[edge['target']].append(edge['source'])
        return dict(reverse_adj)
    
    def get_start_nodes(self):
        return [node_id for node_id, node in self.nodes.items() 
                if node.get('type') == NodeType.TRIGGER]
    
    def get_next_nodes(self, node_id, outputs):
        if node_id not in self.adjacency_list:
            return []
        
        next_nodes = []
        node = self.nodes[node_id]
        for edge in self.adjacency_list[node_id]:
            target_id = edge['target']
            if node.get('type') == NodeType.CONDITION:
                condition_result = outputs.get('condition_result', False) if outputs else False
                source_handle = edge.get('sourceHandle', '')
                if (condition_result and source_handle == 'if') or \
                   (not condition_result and source_handle == 'else'):
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
        execution = context.node_executions.get(node_id, NodeExecution(node_id, NodeStatus.RUNNING))
        
        try:
            execution.status = NodeStatus.RUNNING
            execution.start_time = datetime.now()
            
            inputs = self._prepare_node_inputs(context, node_id, graph)
            execution.inputs = inputs
            
            node_type = node.get('type')
            if node_type == NodeType.TRIGGER:
                success, outputs = self._execute_trigger_node(context, node)
            elif node_type == NodeType.CONDITION:
                success, outputs = self._execute_condition_node(context, node, inputs)
            elif node_type == NodeType.ACTION:
                success, outputs = self._execute_action_node(context, node, inputs)
            else:
                logger.warning(f"Unknown node type: {node_type}")
                success, outputs = True, {}
            
            if success:
                execution.status = NodeStatus.COMPLETED
                execution.outputs = outputs
                if outputs:
                    context.global_variables.update(outputs)
            else:
                execution.status = NodeStatus.FAILED
            
            execution.end_time = datetime.now()
            
            return execution
        except Exception as e:
            execution.status = NodeStatus.FAILED
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
        config_id = node_data.get('data', {}).get('config_id')
        #TODO: handle other trigger types
        if config_id == 'file_upload':
            return True, {}
        
        return True, {}
    

    def _execute_condition_node(self, context, node, inputs):
        node_data = node.get('data', {})
        config_id = node_data.get('config_id')
        params = node_data.get('params', {})
        record = inputs.get('record', {})
        
        if config_id == 'if_else':
            basic_filters = params.get('basic_filters', [])
            
            all_conditions_met = True
            
            for filter_config in basic_filters:
                column_key = filter_config.get('column_key')
                filter_predicate = filter_config.get('filter_predicate')
                filter_term = filter_config.get('filter_term', [])
                
                condition_met = False
                
                if column_key == '_suffix':
                    commit = record.get('commit_diff', [{}])[0]
                    file_name = os.path.basename(commit.get('path', ''))
                    file_suffix = file_name.split('.')[-1] if '.' in file_name else ''
                    
                    if filter_predicate == 'is_any_of':
                        condition_met = file_suffix in filter_term
                    elif filter_predicate == 'is_not_any_of':
                        condition_met = file_suffix not in filter_term
                    elif filter_predicate == 'equals':
                        condition_met = file_suffix == filter_term[0] if filter_term else False
                    elif filter_predicate == 'not_equals':
                        condition_met = file_suffix != filter_term[0] if filter_term else True
                
                if not condition_met:
                    all_conditions_met = False
                    break
            
            outputs = {
                'condition_result': all_conditions_met,
                'filters_applied': basic_filters
            }
            
            return True, outputs
        
        return True, {'condition_result': False}
    

    def _execute_action_node(self, context, node, inputs):
        node_data = node.get('data', {})
        config_id = node_data.get('config_id')
        params = node_data.get('params', {})
        
        if config_id == ActionType.SET_STATUS:
            return self._execute_set_status_action(context, params, inputs)
        
        logger.warning(f"Unknown action type: {config_id}")
        return True, {}
    
    def _execute_set_status_action(self, context, params, inputs):
        status = params.get('status')
        record = inputs.get('record', {})
        
        if not status or not record:
            logger.error("Missing status or record data, cannot set status")
            return False, {'error': 'Missing status or record data'}
        
        success = self._set_file_status(context.repo_id, record, status)
        
        outputs = {
            'status_set': status,
            'success': success,
            'action_type': ActionType.SET_STATUS
        }
        
        return success, outputs

    
    def _set_file_status(self, repo_id, record, status):
        try:
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
                query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters)
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
                self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, rows)
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"Failed to set file status: {str(e)}")
            return False
    

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
            context = WorkflowRunContext(
                repo_id=repo_id,
                workflow_id=workflow_data.get('id'),
                run_id=str(uuid.uuid4()),
                trigger_data=trigger_data,
                global_variables={},
                node_executions={}
                )
            
            self._record_workflow_run_start(context, graph_data)
            start_time = time.time()
            success = self._execute_graph_with_node_queue(context, graph)
            elapsed_time = time.time() - start_time
            if success:
                self._update_workflow_run_status(context, NodeStatus.COMPLETED, elapsed_time)
            else:
                self._update_workflow_run_status(context, NodeStatus.FAILED, elapsed_time)
            
            return success
            
        except Exception as e:
            logger.error(f"Error executing workflow: {str(e)}")
            return False
    
    def _execute_graph_with_node_queue(self, context, graph):
        start_nodes = graph.get_start_nodes()
        if not start_nodes:
            logger.error("No start nodes found")
            return False
        
        execution_queue = deque(start_nodes)
        completed_nodes = set()
        failed_nodes = set()
        
        while execution_queue:
            node_id = execution_queue.popleft()
            # dependencies = graph.get_node_dependencies(node_id)
            # if not all(dep in completed_nodes for dep in dependencies):
            #     execution_queue.append(node_id)
            #     continue
            
            try:
                execution_result = self.node_executor.execute_node(context, node_id, graph)
                context.node_executions[node_id] = execution_result
                
                if execution_result.status == NodeStatus.COMPLETED:
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
            logger.error(f"Workflow {context.workflow_id} failed - failed nodes: {failed_nodes}")
            return False
        
        logger.info(f"Workflow {context.workflow_id} completed successfully - executed {len(completed_nodes)} nodes")
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
                (id, workflow_id, repo_id, status, created_by, created_at, total_steps)
                VALUES (:id, :workflow_id, :repo_id, :status, :created_by, :created_at, :total_steps)
            """)
            
            total_steps = len(graph_data.get('nodes', []))
            record = context.trigger_data.get('record')
            self.db.execute(sql, {
                'id': context.run_id,
                'workflow_id': context.workflow_id,
                'repo_id': context.repo_id,
                'status': NodeStatus.RUNNING,
                'created_by': record.get('op_user'),
                'created_at': datetime.now(),
                'total_steps': total_steps
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record workflow run start: {str(e)}")
    
    def _update_workflow_run_status(self, context, status, elapsed_time):
        try:
            sql = text("""
                UPDATE workflow_run 
                SET status = :status, finished_at = :finished_at, elapsed_time = :elapsed_time
                WHERE id = :id
            """)
            
            self.db.execute(sql, {
                'status': status,
                'finished_at': datetime.now(),
                'elapsed_time': elapsed_time,
                'id': context.run_id
            })
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to record workflow run completion: {str(e)}")


class WorkflowTriggerHandler:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.executor = WorkflowExecutor(db_connection)
    
    def handle_add_file_event(self, record):
        """add file trigger"""
        repo_id = record.get('repo_id')
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
        except ProgrammingError as e:
            logger.error(e)
            return []
        except Exception as e:
            logger.error(f"Failed to get active workflows: {str(e)}")
            return []

def on_add_file_event(session, record):
    try:
        trigger_handler = WorkflowTriggerHandler(db_connection=session)
        trigger_handler.handle_add_file_event(record)
    except Exception as e:
        logger.error(f"Error in file upload workflow event handler: {str(e)}")

