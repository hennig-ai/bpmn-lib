# BPMN Rule Coverage Example — Order Fulfilment Collaboration

Ein gueltiges BPMN-Modell, das jede Regel des Regelkatalogs mindestens einmal zur
Auswertung bringt. Es ergaenzt `bpmn-instance-example.md` aus dem bpmn-modeling-Skill,
das als Lehrbeispiel bewusst schlicht bleibt und deshalb 15 Regeln nie beruehrt:
paralleles Gateway, fangendes und werfendes Message-Zwischenereignis, Message-Boundary
und Message-End-Event.

**Das Modell ist absichtlich fehlerfrei.** Es dient als Positivnachweis: Laeuft es auf
Ebene `best_practice` sauber durch, meldet keine Regel einen Fehlbefund. Fuer den
Negativnachweis werden gezielt einzelne Defekte eingebaut.

## Der modellierte Ablauf

Zwei Teilnehmer tauschen vier Nachrichten aus:

```
Pool "Order Desk" (Prozess 001)
  Start --> [AND-Fork] --+--> Check Stock ------------------+
                         |                                  |
                         +--> Request Quote (throw, msg 1) --+
                              --> Quote Received (catch, msg 2)
                                                             |
                         +-----------------------------------+
                         v
                    [AND-Join] --> [XOR] --+-- ${stockAvailable} --> Prepare Order --+
                                           |                          o (boundary,   |
                                           |                             msg 3)      |
                                           |                          --> Cancelled  |
                                           +-- default --> Reject Order -------------+
                                                                                     v
                                                                                  [XOR-Merge]
                                                                                     |
                                                                Confirm Order (message end, msg 4)

Pool "Fulfilment Partner" (Prozess 002)
  Order Confirmed (message start, msg 4) --> Fulfil Order --> Done
```

An `Prepare Order` haengen zusaetzlich zwei Artefakte des Prozesses 001: eine Text
Annotation ueber eine Association und ein Data Object ueber eine Data Association. Sie
tragen keinen Kontrollfluss, sondern die Container-Regeln fuer Association und Data
Association.

## Was hier zusaetzlich abgedeckt ist

| Regelgruppe | Traeger im Modell |
|---|---|
| AND-001, AND-002, AND-003 | Fork `002` und Join `006` |
| MSG-011, MSG-021, MSG-031 | Quote Received `005` (intermediate_catch) |
| MSG-013, MSG-023, MSG-033 | Request Quote `004` (intermediate_throw) |
| MSG-012, MSG-022, MSG-032 | Cancellation Received `012` (boundary) |
| MSG-014, MSG-024, MSG-034 | Confirm Order `011` (message end) |
| MSG-010, MSG-020, MSG-030 | Order Confirmed `014` (message start) |
| XOR-001 bis XOR-004 | Stock Decision `007` und Merge `010` |
| BND-001 bis BND-006 | Cancellation Received `012` an Prepare Order `008` |
| ASS-001, ASS-002 | Note to Prepare Order `039` (Text Annotation `038` an Prepare Order `008`) |
| DAS-001, DAS-002 | Order Data to Prepare Order `041` (Data Object `040` an Prepare Order `008`) |
| PRC-001, PRC-002, COL-001 | zwei Prozesse, eine Kollaboration |

## bpmn_model
| bpmn_model_id | name | version | author | creation_date | modification_date | documentation |
|---------------|------|---------|--------|---------------|-------------------|---------------|
| 20260725_1200_00_001 | Order Fulfilment Model | 1.0 | bpmn-lib | 2026-07-25T12:00:00Z | 2026-07-25T12:00:00Z | Coverage model exercising every validation rule |

## bpmn_process
| bpmn_process_id | bpmn_model_id | name | is_executable | documentation |
|-----------------|---------------|------|---------------|---------------|
| 001 | 20260725_1200_00_001 | Order Handling | true | Ordering party side of the collaboration |
| 002 | 20260725_1200_00_001 | Fulfilment | true | Partner side of the collaboration |

## collaboration
| collaboration_id | bpmn_model_id | name | documentation |
|------------------|---------------|------|---------------|
| 001 | 20260725_1200_00_001 | Order Fulfilment | Interaction between the order desk and the fulfilment partner |

## bpmn_element
| bpmn_element_id | name | documentation | element_type |
|-----------------|------|---------------|--------------|
| 001 | Order Received | Order handling starts | event |
| 002 | Fork | Splits stock check and quote request | gateway |
| 003 | Check Stock | Clerk checks stock availability | user_task |
| 004 | Request Quote | Throws a quote request to the partner | event |
| 005 | Quote Received | Catches the quote from the partner | event |
| 006 | Join | Waits for stock check and quote | gateway |
| 007 | Stock Decision | Decides whether the order can be prepared | gateway |
| 008 | Prepare Order | Assembles the order for fulfilment | service_task |
| 009 | Reject Order | Rejects the order and records the reason | script_task |
| 010 | Merge | Merges the decision branches | gateway |
| 011 | Confirm Order | Sends the order confirmation and ends | event |
| 012 | Cancellation Received | Customer cancelled while the order was prepared | event |
| 013 | Order Cancelled | Order handling ended by cancellation | event |
| 014 | Order Confirmed | Partner starts on the confirmation message | event |
| 015 | Fulfil Order | Partner fulfils the order | user_task |
| 016 | Fulfilment Done | Partner side completed | event |
| 017 | Order Desk | Participant handling orders | pool |
| 018 | Fulfilment Partner | Participant fulfilling orders | pool |
| 019 | Start to Fork | Flow from start to the fork | sequence_flow |
| 020 | Fork to Check Stock | Flow to the stock check | sequence_flow |
| 021 | Fork to Request Quote | Flow to the quote request | sequence_flow |
| 022 | Check Stock to Join | Flow from the stock check to the join | sequence_flow |
| 023 | Request to Quote Received | Flow from request to the waiting event | sequence_flow |
| 024 | Quote Received to Join | Flow from the quote event to the join | sequence_flow |
| 025 | Join to Decision | Flow from the join to the decision | sequence_flow |
| 026 | Stock Available | Flow taken when stock is available | sequence_flow |
| 027 | No Stock | Default flow when stock is missing | sequence_flow |
| 028 | Prepare to Merge | Flow from preparation to the merge | sequence_flow |
| 029 | Reject to Merge | Flow from rejection to the merge | sequence_flow |
| 030 | Merge to Confirm | Flow from the merge to the confirmation | sequence_flow |
| 031 | Cancellation to End | Flow from the boundary event to the end | sequence_flow |
| 032 | Partner Start to Fulfil | Flow from the partner start to fulfilment | sequence_flow |
| 033 | Fulfil to Partner End | Flow from fulfilment to the partner end | sequence_flow |
| 034 | Quote Request Flow | Message flow carrying the quote request | message_flow |
| 035 | Quote Flow | Message flow carrying the quote | message_flow |
| 036 | Cancellation Flow | Message flow carrying the cancellation | message_flow |
| 037 | Confirmation Flow | Message flow carrying the order confirmation | message_flow |
| 038 | Preparation Note | Annotation on how the order is assembled | text_annotation |
| 039 | Note to Prepare Order | Connects the note with the preparation task | association |
| 040 | Order Data | Order payload read while the order is prepared | data_object |
| 041 | Order Data to Prepare Order | Data flow from the order data into the preparation | data_association |

## message_definition
| message_definition_id | name | item_id |
|----------------------|------|---------|
| 001 | Quote Request | item_quote_request |
| 002 | Quote | item_quote |
| 003 | Cancellation Notice | item_cancellation |
| 004 | Order Confirmation | item_confirmation |

## process_element
| process_element_id | bpmn_process_id | bpmn_element_id |
|-------------------|-----------------|-----------------|
| 001 | 001 | 001 |
| 002 | 001 | 002 |
| 003 | 001 | 003 |
| 004 | 001 | 004 |
| 005 | 001 | 005 |
| 006 | 001 | 006 |
| 007 | 001 | 007 |
| 008 | 001 | 008 |
| 009 | 001 | 009 |
| 010 | 001 | 010 |
| 011 | 001 | 011 |
| 012 | 001 | 012 |
| 013 | 001 | 013 |
| 014 | 001 | 019 |
| 015 | 001 | 020 |
| 016 | 001 | 021 |
| 017 | 001 | 022 |
| 018 | 001 | 023 |
| 019 | 001 | 024 |
| 020 | 001 | 025 |
| 021 | 001 | 026 |
| 022 | 001 | 027 |
| 023 | 001 | 028 |
| 024 | 001 | 029 |
| 025 | 001 | 030 |
| 026 | 001 | 031 |
| 027 | 002 | 014 |
| 028 | 002 | 015 |
| 029 | 002 | 016 |
| 030 | 002 | 032 |
| 031 | 002 | 033 |
| 032 | 001 | 038 |
| 033 | 001 | 039 |
| 034 | 001 | 040 |
| 035 | 001 | 041 |

## activity
| activity_id | bpmn_element_id | activity_type | is_multi_instance | loop_type | is_ad_hoc | is_compensation | start_quantity | completion_quantity |
|-------------|-----------------|---------------|-------------------|-----------|-----------|-----------------|----------------|---------------------|
| 001 | 003 | task | false | none | false | false | 1 | 1 |
| 002 | 008 | task | false | none | false | false | 1 | 1 |
| 003 | 009 | task | false | none | false | false | 1 | 1 |
| 004 | 015 | task | false | none | false | false | 1 | 1 |

## task
| task_id | activity_id | task_type | implementation |
|---------|-------------|-----------|----------------|
| 001 | 001 | user | ##unspecified |
| 002 | 002 | service | webservice |
| 003 | 003 | script | ##unspecified |
| 004 | 004 | user | ##unspecified |

## service_task
| service_task_id | task_id | operation_id | implementation_id |
|-----------------|---------|--------------|-------------------|
| 001 | 002 | operation_prepare | com.example.OrderService |

## user_task
| user_task_id | task_id | implementation | assignment_expression |
|--------------|---------|----------------|----------------------|
| 001 | 001 | webform | ${role == 'order-clerk'} |
| 002 | 004 | webform | ${role == 'fulfilment-agent'} |

## script_task
| script_task_id | task_id | script | script_format |
|----------------|---------|--------|---------------|
| 001 | 003 | reason = 'out of stock'; return reason; | javascript |

## business_rule_task
| business_rule_task_id | task_id | implementation | rule_names |
|----------------------|---------|----------------|------------|

## subprocess
| subprocess_id | activity_id | is_transaction | triggered_by_event |
|---------------|-------------|----------------|-------------------|

## call_activity
| call_activity_id | activity_id | bpmn_process_id_reference |
|------------------|-------------|---------------------------|

## event
| event_id | bpmn_element_id | event_type | event_definition_type | is_interrupting | attached_to_bpmn_element_id |
|----------|-----------------|------------|----------------------|-----------------|------------------------------|
| 001 | 001 | start | none | ##!empty!## | ##!empty!## |
| 002 | 004 | intermediate_throw | message | ##!empty!## | ##!empty!## |
| 003 | 005 | intermediate_catch | message | ##!empty!## | ##!empty!## |
| 004 | 011 | end | message | ##!empty!## | ##!empty!## |
| 005 | 012 | boundary | message | true | 008 |
| 006 | 013 | end | none | ##!empty!## | ##!empty!## |
| 007 | 014 | start | message | ##!empty!## | ##!empty!## |
| 008 | 016 | end | none | ##!empty!## | ##!empty!## |

## message_event_definition
| message_event_definition_id | event_id | message_definition_id | operation_id |
|----------------------------|----------|----------------------|--------------|
| 001 | 002 | 001 | ##!empty!## |
| 002 | 003 | 002 | ##!empty!## |
| 003 | 004 | 004 | ##!empty!## |
| 004 | 005 | 003 | ##!empty!## |
| 005 | 007 | 004 | ##!empty!## |

## sequence_flow
| sequence_flow_id | bpmn_element_id | source_bpmn_element_id | target_bpmn_element_id | is_default | condition_expression |
|------------------|-----------------|------------------------|------------------------|------------|---------------------|
| 001 | 019 | 001 | 002 | false | ##!empty!## |
| 002 | 020 | 002 | 003 | false | ##!empty!## |
| 003 | 021 | 002 | 004 | false | ##!empty!## |
| 004 | 022 | 003 | 006 | false | ##!empty!## |
| 005 | 023 | 004 | 005 | false | ##!empty!## |
| 006 | 024 | 005 | 006 | false | ##!empty!## |
| 007 | 025 | 006 | 007 | false | ##!empty!## |
| 008 | 026 | 007 | 008 | false | ${stockAvailable} |
| 009 | 027 | 007 | 009 | true | ##!empty!## |
| 010 | 028 | 008 | 010 | false | ##!empty!## |
| 011 | 029 | 009 | 010 | false | ##!empty!## |
| 012 | 030 | 010 | 011 | false | ##!empty!## |
| 013 | 031 | 012 | 013 | false | ##!empty!## |
| 014 | 032 | 014 | 015 | false | ##!empty!## |
| 015 | 033 | 015 | 016 | false | ##!empty!## |

## gateway
| gateway_id | bpmn_element_id | gateway_type | gateway_direction | sequence_flow_id |
|------------|-----------------|--------------|-------------------|------------------|
| 001 | 002 | parallel | diverging | ##!empty!## |
| 002 | 006 | parallel | converging | ##!empty!## |
| 003 | 007 | exclusive | diverging | 027 |
| 004 | 010 | exclusive | converging | ##!empty!## |

## message_flow
| message_flow_id | bpmn_element_id | collaboration_id | source_bpmn_element_id | target_bpmn_element_id | message_definition_id |
|-----------------|-----------------|------------------|------------------------|------------------------|-----------------------|
| 001 | 034 | 001 | 004 | 018 | 001 |
| 002 | 035 | 001 | 018 | 005 | 002 |
| 003 | 036 | 001 | 018 | 012 | 003 |
| 004 | 037 | 001 | 011 | 014 | 004 |

## association
| association_id | bpmn_element_id | source_bpmn_element_id | target_bpmn_element_id | association_direction |
|----------------|-----------------|------------------------|------------------------|-----------------------|
| 001 | 039 | 038 | 008 | none |

## pool
| pool_id | bpmn_element_id | collaboration_id | bpmn_process_id | is_closed |
|---------|-----------------|------------------|-----------------|-----------|
| 001 | 017 | 001 | 001 | false |
| 002 | 018 | 001 | 002 | false |

## lane
| lane_id | bpmn_element_id | pool_id |
|---------|-----------------|---------|

## lane_element
| lane_element_id | lane_bpmn_element_id | bpmn_element_id |
|-----------------|---------------------|-----------------|

## data_object
| data_object_id | bpmn_element_id | is_collection | state |
|----------------|-----------------|---------------|-------|
| 001 | 040 | false | received |

## data_store
| data_store_id | bpmn_element_id | capacity | is_unlimited |
|---------------|-----------------|----------|--------------|

## text_annotation
| text_annotation_id | bpmn_element_id | text |
|-------------------|-----------------|------|
| 001 | 038 | Preparation is done manually by the order desk |

## data_input
| data_input_id | bpmn_element_id | name | is_collection |
|---------------|-----------------|------|---------------|

## data_output
| data_output_id | bpmn_element_id | name | is_collection |
|----------------|-----------------|------|---------------|

## data_association
| data_association_id | bpmn_element_id | source_bpmn_element_id | target_bpmn_element_id | transformation_expression | assignment_expression |
|--------------------|-----------------|------------------------|------------------------|---------------------------|----------------------|
| 001 | 041 | 040 | 008 | ##!empty!## | ##!empty!## |
