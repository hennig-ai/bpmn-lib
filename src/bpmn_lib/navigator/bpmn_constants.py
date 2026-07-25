"""
BPMN Konstanten - Tabellennamen, die der Navigator direkt nachschlaegt.

Bibliotheks-intern: Diese Konstanten sind bewusst NICHT Teil der oeffentlichen
API. Die Bibliothek ist schema-getrieben - Tabellennamen kommen zur Laufzeit
aus der Schema-Datei. Hier stehen ausschliesslich die Tabellen, auf die der
Navigator-Code selbst fest zugreift.
"""

# Struktur-Tabellen
TBL_BPMN_ELEMENT = "bpmn_element"
TBL_PROCESS_ELEMENT = "process_element"
TBL_BPMN_PROCESS = "bpmn_process"

# Container-Tabellen (Behaelter direkt unter dem Modell, selbst keine bpmn_elements).
# Welche Tabellen Container sind, leitet der Navigator aus dem Schema ab: jede Tabelle
# mit einem Fremdschluessel auf bpmn_model. Namentlich gebraucht werden sie nur dort,
# wo die Mitgliedschaft aufgeloest wird - die laeuft je Containerart anders.
TBL_BPMN_MODEL = "bpmn_model"
TBL_COLLABORATION = "collaboration"

# Flow-Object-Tabellen (Oberbegriff 'flow_object' der Regeldefinitionen)
TBL_ACTIVITY = "activity"
TBL_TASK = "task"
TBL_EVENT = "event"
TBL_GATEWAY = "gateway"

# Connection-Tabellen
TBL_SEQUENCE_FLOW = "sequence_flow"
TBL_MESSAGE_FLOW = "message_flow"
TBL_DATA_ASSOCIATION = "data_association"

# Swimlane-Tabellen
TBL_POOL = "pool"
TBL_LANE = "lane"
TBL_LANE_ELEMENT = "lane_element"

# Message-Detailtabellen
# message_event_definition haengt an event.event_id und ist bewusst NICHT Teil der
# bpmn_element-Hierarchie - sie ist deshalb nur ueber direkten Tabellenzugriff erreichbar.
TBL_MESSAGE_DEFINITION = "message_definition"
TBL_MESSAGE_EVENT_DEFINITION = "message_event_definition"
