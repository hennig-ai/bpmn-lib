"""
Unit tests for message and pool navigation in BPMNHierarchyNavigator.

Tests cover:
- get_outgoing_message_flows / get_incoming_message_flows
- get_message_definition / get_message_definition_for_event
- get_pool_of_element (pool itself, via lane, via process)
"""

from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import Mock

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    IncomingMessageFlowInfo,
    MessageDefinitionInfo,
    MessageEventDefinitionInfo,
    OutgoingMessageFlowInfo,
)


# ==================== Test Infrastructure ====================

def _make_table(rows: List[Dict[str, Any]]) -> Mock:
    """Create a mock table backed by a list of row dicts.

    Supports create_iterator() and create_iterator(True, ConditionEquals).
    """
    table = Mock()

    def create_iterator(filter_active: bool = False, condition: Any = None) -> Mock:
        if filter_active and condition is not None:
            matching = [r for r in rows if r.get(condition._column) == condition._value]
        else:
            matching = list(rows)

        state = {"index": 0}
        iterator = Mock()
        iterator.is_empty.side_effect = lambda: state["index"] >= len(matching)
        iterator.value.side_effect = lambda field: matching[state["index"]].get(field)
        iterator.pp.side_effect = lambda: state.__setitem__("index", state["index"] + 1)
        return iterator

    table.create_iterator.side_effect = create_iterator
    return table


def _make_navigator(
    tables: Dict[str, List[Dict[str, Any]]],
    pool_element_ids: Optional[List[str]] = None,
    event_ids: Optional[Dict[str, str]] = None,
    message_flow_element_ids: Optional[List[str]] = None,
    element_attrs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> BPMNHierarchyNavigator:
    """Create a navigator mock with real methods bound and mock tables behind it.

    Args:
        tables: Table name -> list of row dicts
        pool_element_ids: Element IDs for which is_element_descendant_of(id, "pool") is True
        event_ids: bpmn_element_id -> event_id, as _get_record_id_in_table would resolve
        message_flow_element_ids: Element IDs that are message flows
        element_attrs: Element ID -> attributes, as get_element_attribute would return
    """
    navigator = Mock(spec=BPMNHierarchyNavigator)

    mock_tables = {name: _make_table(rows) for name, rows in tables.items()}

    def get_table(name: str) -> Mock:
        if name not in mock_tables:
            raise ValueError(f"Tabelle '{name}' nicht vorhanden")
        return mock_tables[name]

    database = Mock()
    database.get_table.side_effect = get_table

    def get_by_primary_key(table_name: str, key: Any) -> Optional[Mock]:
        pk_column = table_name + "_id"
        for row in tables.get(table_name, []):
            if row.get(pk_column) == key:
                iterator = Mock()
                iterator.value.side_effect = lambda field: row.get(field)
                return iterator
        return None

    database.get_by_primary_key.side_effect = get_by_primary_key
    navigator.m_database = database

    resolved_pools = pool_element_ids if pool_element_ids is not None else []
    resolved_message_flows = message_flow_element_ids if message_flow_element_ids is not None else []

    def is_element_descendant_of(element_id: str, table: str) -> bool:
        if table == "pool":
            return element_id in resolved_pools
        if table == "message_flow":
            return element_id in resolved_message_flows
        return False

    navigator.is_element_descendant_of.side_effect = is_element_descendant_of

    resolved_attrs = element_attrs if element_attrs is not None else {}
    navigator.get_element_attribute.side_effect = (
        lambda element_id, attribute: resolved_attrs.get(element_id, {}).get(attribute)
    )

    resolved_events = event_ids if event_ids is not None else {}
    navigator._get_record_id_in_table.side_effect = (
        lambda element_id, table: resolved_events.get(element_id, "") if table == "event" else ""
    )

    # Bind the real implementations under test
    for method_name in [
        "get_outgoing_message_flows",
        "get_incoming_message_flows",
        "get_message_definition",
        "get_message_event_definitions",
        "get_message_definition_for_event",
        "get_pool_of_element",
        "get_process_of_element",
        "get_collaboration_of_element",
        "_read_collaboration_id",
        "_get_pool_via_lane",
        "_get_pools_via_process",
        "_format_db_internal_id",
    ]:
        setattr(
            navigator,
            method_name,
            getattr(BPMNHierarchyNavigator, method_name).__get__(navigator, BPMNHierarchyNavigator),
        )

    return navigator


# ==================== Message Flows ====================

class TestOutgoingMessageFlows:
    """Test get_outgoing_message_flows."""

    def test_single_outgoing_flow(self):
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": "002"},
            ]
        })

        result = navigator.get_outgoing_message_flows("003")

        assert result == [
            OutgoingMessageFlowInfo(
                message_flow_id="036", target_element_id="025", message_definition_id="002"
            )
        ]

    def test_multiple_outgoing_flows(self):
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": "001"},
                {"bpmn_element_id": "037", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "040", "message_definition_id": "002"},
            ]
        })

        result = navigator.get_outgoing_message_flows("003")

        assert len(result) == 2
        assert {flow.target_element_id for flow in result} == {"025", "040"}

    def test_no_outgoing_flows_returns_empty_list(self):
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": "002"},
            ]
        })

        assert navigator.get_outgoing_message_flows("999") == []

    def test_integer_id_is_formatted(self):
        """Integer IDs must be padded to three digits like the sequence flow API."""
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": "002"},
            ]
        })

        assert len(navigator.get_outgoing_message_flows(3)) == 1

    def test_message_definition_id_may_be_none(self):
        """message_definition_id is optional per schema."""
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": None},
            ]
        })

        assert navigator.get_outgoing_message_flows("003")[0].message_definition_id is None


class TestIncomingMessageFlows:
    """Test get_incoming_message_flows."""

    def test_single_incoming_flow(self):
        navigator = _make_navigator({
            "message_flow": [
                {"bpmn_element_id": "036", "source_bpmn_element_id": "003",
                 "target_bpmn_element_id": "025", "message_definition_id": "002"},
            ]
        })

        result = navigator.get_incoming_message_flows("025")

        assert result == [
            IncomingMessageFlowInfo(
                message_flow_id="036", source_element_id="003", message_definition_id="002"
            )
        ]

    def test_no_incoming_flows_returns_empty_list(self):
        navigator = _make_navigator({"message_flow": []})

        assert navigator.get_incoming_message_flows("025") == []


# ==================== Message Definitions ====================

class TestGetMessageDefinition:
    """Test get_message_definition."""

    def test_returns_definition(self):
        navigator = _make_navigator({
            "message_definition": [
                {"message_definition_id": "001", "name": "Invoice Received", "item_id": "item_1"},
            ]
        })

        result = navigator.get_message_definition("001")

        assert result == MessageDefinitionInfo(
            message_definition_id="001", name="Invoice Received", item_id="item_1"
        )

    def test_unknown_definition_raises(self):
        navigator = _make_navigator({"message_definition": []})

        with pytest.raises(Exception):
            navigator.get_message_definition("999")


class TestGetMessageEventDefinitions:
    """Test get_message_event_definitions."""

    def _navigator(self, med_rows: List[Dict[str, Any]]) -> BPMNHierarchyNavigator:
        return _make_navigator(
            tables={"message_event_definition": med_rows},
            event_ids={"002": "002"},
        )

    def test_returns_the_single_definition(self):
        navigator = self._navigator([
            {"message_event_definition_id": "001", "event_id": "002",
             "message_definition_id": "001", "operation_id": None},
        ])

        assert navigator.get_message_event_definitions("002") == [
            MessageEventDefinitionInfo(
                message_event_definition_id="001",
                message_definition_id="001",
                operation_id=None,
            )
        ]

    def test_event_without_definition_returns_empty_list(self):
        """Distinguishing 'no row' from 'row without message type' is what the rules need."""
        assert self._navigator([]).get_message_event_definitions("002") == []

    def test_definition_without_message_reference_is_still_returned(self):
        navigator = self._navigator([
            {"message_event_definition_id": "001", "event_id": "002",
             "message_definition_id": None, "operation_id": None},
        ])

        result = navigator.get_message_event_definitions("002")

        assert len(result) == 1
        assert result[0].message_definition_id is None

    def test_non_event_element_raises(self):
        with pytest.raises(Exception):
            self._navigator([]).get_message_event_definitions("003")


class TestGetMessageDefinitionForEvent:
    """Test get_message_definition_for_event."""

    def _navigator_with_message_event(self, med_rows: List[Dict[str, Any]]) -> BPMNHierarchyNavigator:
        return _make_navigator(
            tables={
                "message_event_definition": med_rows,
                "message_definition": [
                    {"message_definition_id": "001", "name": "Invoice Received", "item_id": "item_1"},
                ],
            },
            event_ids={"002": "002"},
        )

    def test_resolves_event_to_message_definition(self):
        navigator = self._navigator_with_message_event([
            {"message_event_definition_id": "001", "event_id": "002",
             "message_definition_id": "001", "operation_id": None},
        ])

        result = navigator.get_message_definition_for_event("002")

        assert result == MessageDefinitionInfo(
            message_definition_id="001", name="Invoice Received", item_id="item_1"
        )

    def test_event_without_message_event_definition_returns_none(self):
        """A timer or signal event has no message_event_definition row."""
        navigator = self._navigator_with_message_event([])

        assert navigator.get_message_definition_for_event("002") is None

    def test_definition_without_message_reference_returns_none(self):
        """message_definition_id is optional in message_event_definition."""
        navigator = self._navigator_with_message_event([
            {"message_event_definition_id": "001", "event_id": "002",
             "message_definition_id": None, "operation_id": None},
        ])

        assert navigator.get_message_definition_for_event("002") is None

    def test_non_event_element_raises(self):
        """Elements outside the event hierarchy must fail fast."""
        navigator = self._navigator_with_message_event([])

        with pytest.raises(Exception):
            navigator.get_message_definition_for_event("003")


# ==================== Pool Membership ====================

class TestGetPoolOfElement:
    """Test get_pool_of_element."""

    def test_element_is_pool_itself(self):
        navigator = _make_navigator(tables={}, pool_element_ids=["025"])

        assert navigator.get_pool_of_element("025") == "025"

    def test_pool_via_lane(self):
        navigator = _make_navigator({
            "lane_element": [{"lane_element_id": "001", "lane_bpmn_element_id": "026",
                              "bpmn_element_id": "003"}],
            "lane": [{"lane_id": "001", "bpmn_element_id": "026", "pool_id": "025"}],
        })

        assert navigator.get_pool_of_element("003") == "025"

    def test_pool_via_process_when_no_lane(self):
        navigator = _make_navigator({
            "lane_element": [],
            "lane": [],
            "process_element": [{"process_element_id": "001", "bpmn_process_id": "001",
                                 "bpmn_element_id": "002"}],
            "pool": [{"pool_id": "001", "bpmn_element_id": "025", "bpmn_process_id": "001"}],
        })

        assert navigator.get_pool_of_element("002") == "025"

    def test_lane_takes_precedence_over_process(self):
        navigator = _make_navigator({
            "lane_element": [{"lane_element_id": "001", "lane_bpmn_element_id": "026",
                              "bpmn_element_id": "003"}],
            "lane": [{"lane_id": "001", "bpmn_element_id": "026", "pool_id": "025"}],
            "process_element": [{"process_element_id": "001", "bpmn_process_id": "001",
                                 "bpmn_element_id": "003"}],
            "pool": [{"pool_id": "001", "bpmn_element_id": "099", "bpmn_process_id": "001"}],
        })

        assert navigator.get_pool_of_element("003") == "025"

    def test_no_pool_returns_none(self):
        """Models without pools must not raise."""
        navigator = _make_navigator({
            "lane_element": [],
            "lane": [],
            "process_element": [{"process_element_id": "001", "bpmn_process_id": "001",
                                 "bpmn_element_id": "002"}],
            "pool": [],
        })

        assert navigator.get_pool_of_element("002") is None

    def test_element_in_no_process_returns_none(self):
        navigator = _make_navigator({
            "lane_element": [],
            "lane": [],
            "process_element": [],
            "pool": [],
        })

        assert navigator.get_pool_of_element("777") is None

    def test_dangling_lane_reference_raises(self):
        """lane_element pointing at a missing lane must fail fast."""
        navigator = _make_navigator({
            "lane_element": [{"lane_element_id": "001", "lane_bpmn_element_id": "026",
                              "bpmn_element_id": "003"}],
            "lane": [],
        })

        with pytest.raises(Exception):
            navigator.get_pool_of_element("003")

    def test_integer_id_is_formatted(self):
        navigator = _make_navigator({
            "lane_element": [{"lane_element_id": "001", "lane_bpmn_element_id": "026",
                              "bpmn_element_id": "003"}],
            "lane": [{"lane_id": "001", "bpmn_element_id": "026", "pool_id": "025"}],
        })

        assert navigator.get_pool_of_element(3) == "025"


class TestPoolResolutionAcrossCollaborations:
    """Schema v4.1: two pools of different collaborations may carry the same process."""

    def _navigator(self) -> BPMNHierarchyNavigator:
        return _make_navigator({
            "lane_element": [],
            "lane": [],
            "process_element": [{"process_element_id": "001", "bpmn_process_id": "002",
                                 "bpmn_element_id": "040"}],
            "pool": [
                {"pool_id": "002", "bpmn_element_id": "038", "collaboration_id": "001",
                 "bpmn_process_id": "002"},
                {"pool_id": "003", "bpmn_element_id": "047", "collaboration_id": "002",
                 "bpmn_process_id": "002"},
            ],
        })

    def test_ambiguous_membership_raises(self):
        """Silently picking the first pool would corrupt every pool-based rule."""
        with pytest.raises(Exception):
            self._navigator().get_pool_of_element("040")

    def test_collaboration_scope_resolves_the_ambiguity(self):
        navigator = self._navigator()

        assert navigator.get_pool_of_element("040", "001") == "038"
        assert navigator.get_pool_of_element("040", "002") == "047"

    def test_scope_without_matching_pool_returns_none(self):
        assert self._navigator().get_pool_of_element("040", "003") is None


class TestGetProcessOfElement:
    """Test get_process_of_element."""

    def test_returns_the_process(self):
        navigator = _make_navigator({
            "process_element": [{"process_element_id": "001", "bpmn_process_id": "001",
                                 "bpmn_element_id": "003"}],
        })

        assert navigator.get_process_of_element("003") == "001"

    def test_element_without_process_returns_none(self):
        """Pools and message flows have no process_element row since schema v4.1."""
        navigator = _make_navigator({"process_element": []})

        assert navigator.get_process_of_element("038") is None


class TestGetCollaborationOfElement:
    """Test get_collaboration_of_element."""

    def test_message_flow_carries_the_collaboration_itself(self):
        navigator = _make_navigator(
            tables={},
            message_flow_element_ids=["036"],
            element_attrs={"036": {"collaboration_id": "001"}},
        )

        assert navigator.get_collaboration_of_element("036") == "001"

    def test_element_inherits_the_collaboration_from_its_pool(self):
        navigator = _make_navigator(
            tables={
                "lane_element": [{"lane_element_id": "001", "lane_bpmn_element_id": "026",
                                  "bpmn_element_id": "003"}],
                "lane": [{"lane_id": "001", "bpmn_element_id": "026", "pool_id": "025"}],
            },
            element_attrs={"025": {"collaboration_id": "001"}},
        )

        assert navigator.get_collaboration_of_element("003") == "001"

    def test_pool_reports_its_own_collaboration(self):
        navigator = _make_navigator(
            tables={},
            pool_element_ids=["025"],
            element_attrs={"025": {"collaboration_id": "001"}},
        )

        assert navigator.get_collaboration_of_element("025") == "001"

    def test_element_without_pool_returns_none(self):
        navigator = _make_navigator({
            "lane_element": [],
            "lane": [],
            "process_element": [],
            "pool": [],
        })

        assert navigator.get_collaboration_of_element("003") is None
