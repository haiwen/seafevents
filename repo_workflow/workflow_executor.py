import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import uuid

from collections import deque, defaultdict
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_workflow.constants import NodeStatus, NodeType, ActionType, TriggerType, ConditionType

from seafevents.repo_workflow.utils import get_active_workflows, set_workflow_valid, set_file_status, \
    record_workflow_run_start, update_workflow_run_status

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
    

@dataclass
class WorkflowRunContext:
    repo_id: str
    workflow_id: str
    run_id: str
    trigger_data: Dict[str, Any] # trigger file data and trigger information
    global_variables: Dict[str, Any] = None # global variables for the entire workflow run
    node_executions: Dict[str, NodeExecution] = None # execution status for each node in the workflow

class WorkflowGraph:
    def __init__(self, graph_data):
        self.nodes = {node['id']: node for node in graph_data.get('nodes', [])}
        self.edges = graph_data.get('edges', [])
        self.adjacency_list = self._build_adjacency_list()
        self.reverse_adjacency_list = self._build_reverse_adjacency_list()
    

    def _build_adjacency_list(self):
        """
          {
            <:source id>: [
                {'target': <:target_id>, 'sourceHandle': ''}
                {'target': <:target_id>, 'sourceHandle': ''}
            ],
            ...
        }
        """
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
    
    def get_node_dependencies(self, node_id: str):
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
                success, outputs = self._execute_trigger_node(node)
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
        
        return inputs

    def _execute_trigger_node(self, node_data):
        config_id = node_data.get('data', {}).get('config_id')
        #TODO: handle other trigger types
        if config_id == TriggerType.FILE_ADDED:
            return True, {}
        
        return True, {}
    

    def _execute_condition_node(self, context, node, inputs):
        node_data = node.get('data', {})
        config_id = node_data.get('config_id')
        params = node_data.get('params', {})
        record = context.trigger_data.get('record', {})
        
        if config_id == ConditionType.IF_ELSE:
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
        record = context.trigger_data.get('record')
        
        if not status or not record:
            logger.error("Missing status or record data, cannot set status")
            return False, {'error': 'Missing status or record data'}
        
        success = set_file_status(self.metadata_server_api, context.repo_id, record, status)
        outputs = {
            'status_set': status,
            'success': success,
            'action_type': ActionType.SET_STATUS
        }
        
        return success, outputs


class WorkflowExecutor:
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.node_executor = NodeExecutor(db_connection)
    
    def execute_workflow(self, repo_id, workflow_data, trigger_data):
        try:
            graph_data = json.loads(workflow_data.get('graph'))
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
            
            record_workflow_run_start(self.db, context, graph_data)
            start_time = time.time()
            success = self._execute_graph_with_node_queue(context, graph)
            elapsed_time = time.time() - start_time
            if success:
                update_workflow_run_status(self.db, context, NodeStatus.COMPLETED, elapsed_time)
            else:
                update_workflow_run_status(self.db, context, NodeStatus.FAILED, elapsed_time)
            
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
            dependencies = graph.get_node_dependencies(node_id)
            if not all(dep in completed_nodes for dep in dependencies):
                execution_queue.append(node_id)
                continue
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


class WorkflowTriggerHandler:
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.executor = WorkflowExecutor(db_connection)
    
    def handle_add_file_event(self, record):
        """add file trigger"""
        repo_id = record.get('repo_id')
        workflows = get_active_workflows(self.db, repo_id)
        if not workflows:
            logger.info(f"No active workflows found for repo {repo_id}")
            return
        
        for workflow in workflows:
            if workflow.get('trigger_from') == TriggerType.FILE_ADDED:
                logger.info(f"Triggering workflow {workflow['id']} for file upload in repo {repo_id}")
                
                trigger_data = {
                    'trigger_type': TriggerType.FILE_ADDED,
                    'record': record,
                    'timestamp': datetime.now().isoformat()
                }
                
                success = self.executor.execute_workflow(repo_id, workflow, trigger_data)
                if not success:
                    logger.error(f"Workflow {workflow['id']} execution failed")
                    set_workflow_valid(self.db, workflow['id'], False)


def on_add_file_event(session, record):
    try:
        trigger_handler = WorkflowTriggerHandler(db_connection=session)
        trigger_handler.handle_add_file_event(record)
    except Exception as e:
        logger.error(f"Error in file upload workflow event handler: {str(e)}")
