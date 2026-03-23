"""
BPMN Konstanten - Enthält alle Konstanten für BPMN-Tabellennamen und andere Konfigurationen.
"""

# Tabellennamen-Konstanten
TBL_ACTIVITY = "activity"
TBL_TASK = "task"
TBL_EVENT = "event"
TBL_GATEWAY = "gateway"

# Weitere BPMN-Tabellennamen (aus dem Code abgeleitet)
TBL_BPMN_ELEMENT = "bpmn_element"
TBL_PROCESS_ELEMENT = "process_element"
TBL_BPMN_PROCESS = "bpmn_process"

# Pool/Lane Tabellen
TBL_POOL = "pool"
TBL_LANE = "lane"

# Activity-Typen
TBL_SUB_PROCESS = "sub_process"
TBL_CALL_ACTIVITY = "call_activity"
TBL_USER_TASK = "user_task"
TBL_SERVICE_TASK = "service_task"
TBL_SCRIPT_TASK = "script_task"
TBL_SEND_TASK = "send_task"
TBL_RECEIVE_TASK = "receive_task"
TBL_MANUAL_TASK = "manual_task"
TBL_BUSINESS_RULE_TASK = "business_rule_task"

# Event-Typen
TBL_START_EVENT = "start_event"
TBL_END_EVENT = "end_event"
TBL_INTERMEDIATE_CATCH_EVENT = "intermediate_catch_event"
TBL_INTERMEDIATE_THROW_EVENT = "intermediate_throw_event"
TBL_BOUNDARY_EVENT = "boundary_event"

# Gateway-Typen
TBL_EXCLUSIVE_GATEWAY = "exclusive_gateway"
TBL_PARALLEL_GATEWAY = "parallel_gateway"
TBL_INCLUSIVE_GATEWAY = "inclusive_gateway"
TBL_EVENT_BASED_GATEWAY = "event_based_gateway"
TBL_COMPLEX_GATEWAY = "complex_gateway"

# Connection-Typen
TBL_SEQUENCE_FLOW = "sequence_flow"
TBL_DATA_ASSOCIATION = "data_association"
# TBL_DATA_INPUT_ASSOCIATION = "data_input_association"  # Nicht genutzt - nur eine data_association Tabelle
# TBL_DATA_OUTPUT_ASSOCIATION = "data_output_association"  # Nicht genutzt - nur eine data_association Tabelle

# Data Objects
TBL_DATA_OBJECT = "data_object"
