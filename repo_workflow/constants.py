

class NodeStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'


class NodeType:
    TRIGGER = 'trigger'
    CONDITION = 'condition'
    ACTION = 'action'

class FilterPredicate:
    IS_ANY_OF = 'is_any_of'
    IS_NOT_ANY_OF = 'is_not_any_of'
    EQUALS = 'equals'
    NOT_EQUALS = 'not_equals'
    CONTAINS = 'contains'
    NOT_CONTAINS = 'not_contains'


class ActionType:
    SET_STATUS = 'set_status'
