"""
Unit tests for the container API of BPMNHierarchyNavigator.

Containers are the holders directly below the model — bpmn_process and collaboration.
They are deliberately not bpmn_elements, so they need their own selection, attribute
access and membership resolution.
"""

from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import Mock

from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator


# ==================== Test Infrastructure ====================

def _make_relationship(source_table: str, source_column: str, target_table: str) -> Mock:
    """Create a mock ForeignKeyRelationship."""
    relationship = Mock()
    relationship.get_source_table.return_value = source_table
    relationship.get_source_column.return_value = source_column
    relationship.get_target_table.return_value = target_table
    return relationship


def _make_table(rows: List[Dict[str, Any]], columns: List[str]) -> Mock:
    """Create a mock table backed by a list of row dicts."""
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
    table.field_exists.side_effect = lambda field: field in columns
    return table


def _make_navigator(
    tables: Dict[str, List[Dict[str, Any]]],
    columns: Dict[str, List[str]],
    relationships: List[Mock],
    process_elements: Optional[Dict[str, List[str]]] = None,
    child_to_parent: Optional[Dict[str, str]] = None,
) -> BPMNHierarchyNavigator:
    """Create a navigator mock with the real container methods bound.

    Args:
        tables: Table name -> list of row dicts
        columns: Table name -> column names, for field_exists
        relationships: Foreign key relationships the schema reports
        process_elements: bpmn_process_id -> member element IDs
        child_to_parent: The bpmn_element inheritance hierarchy
    """
    navigator = Mock(spec=BPMNHierarchyNavigator)

    mock_tables = {name: _make_table(rows, columns[name]) for name, rows in tables.items()}

    def get_table(name: str) -> Mock:
        if name not in mock_tables:
            raise ValueError(f"Tabelle '{name}' nicht vorhanden")
        return mock_tables[name]

    def get_by_primary_key(table_name: str, key: Any) -> Optional[Mock]:
        pk_column = table_name + "_id"
        for row in tables.get(table_name, []):
            if row.get(pk_column) == key:
                iterator = Mock()
                iterator.value.side_effect = lambda field: row.get(field)
                iterator.field_exists.side_effect = lambda field: field in columns[table_name]
                return iterator
        return None

    schema = Mock()
    schema.get_relationships.return_value = relationships

    database = Mock()
    database.get_table.side_effect = get_table
    database.get_by_primary_key.side_effect = get_by_primary_key
    database.get_schema.return_value = schema
    navigator.m_database = database

    navigator.m_root_table = "bpmn_element"
    navigator.m_child_to_parent = child_to_parent if child_to_parent is not None else {}
    navigator.m_process_elements = process_elements if process_elements is not None else {}

    for method_name in [
        "_collect_container_tables",
        "get_container_tables",
        "is_container_table",
        "is_element_table",
        "get_container_ids",
        "get_container_attribute",
        "get_container_members",
        "_get_collaboration_members",
        "_get_referencing_columns",
        "_require_container_table",
        "_get_primary_key_column",
        "get_process_elements",
    ]:
        setattr(
            navigator,
            method_name,
            getattr(BPMNHierarchyNavigator, method_name).__get__(navigator, BPMNHierarchyNavigator),
        )

    navigator.m_container_tables = navigator._collect_container_tables()
    return navigator


_RELATIONSHIPS = [
    _make_relationship("bpmn_process", "bpmn_model_id", "bpmn_model"),
    _make_relationship("collaboration", "bpmn_model_id", "bpmn_model"),
    _make_relationship("pool", "collaboration_id", "collaboration"),
    _make_relationship("pool", "bpmn_process_id", "bpmn_process"),
    _make_relationship("message_flow", "collaboration_id", "collaboration"),
    _make_relationship("process_element", "bpmn_process_id", "bpmn_process"),
    _make_relationship("event", "bpmn_element_id", "bpmn_element"),
]

_COLUMNS = {
    "bpmn_process": ["bpmn_process_id", "bpmn_model_id", "name", "is_executable"],
    "collaboration": ["collaboration_id", "bpmn_model_id", "name"],
    "pool": ["pool_id", "bpmn_element_id", "collaboration_id", "bpmn_process_id"],
    "message_flow": ["message_flow_id", "bpmn_element_id", "collaboration_id"],
    "process_element": ["process_element_id", "bpmn_process_id", "bpmn_element_id"],
}


def _default_navigator() -> BPMNHierarchyNavigator:
    """A navigator over two processes, one collaboration and two participants."""
    return _make_navigator(
        tables={
            "bpmn_process": [
                {"bpmn_process_id": "001", "name": "Invoice", "is_executable": "true"},
                {"bpmn_process_id": "002", "name": "Supplier", "is_executable": "false"},
            ],
            "collaboration": [
                {"collaboration_id": "001", "name": "Invoice Collaboration"},
            ],
            "pool": [
                {"pool_id": "001", "bpmn_element_id": "025",
                 "collaboration_id": "001", "bpmn_process_id": "001"},
                {"pool_id": "002", "bpmn_element_id": "038",
                 "collaboration_id": "001", "bpmn_process_id": "002"},
            ],
            "message_flow": [
                {"message_flow_id": "001", "bpmn_element_id": "036", "collaboration_id": "001"},
            ],
            "process_element": [],
        },
        columns=_COLUMNS,
        relationships=_RELATIONSHIPS,
        process_elements={"001": ["001", "002"], "002": ["039"]},
        child_to_parent={"event": "bpmn_element", "pool": "bpmn_element", "task": "activity"},
    )


# ==================== Container detection ====================

class TestContainerDetection:
    """Container tables are derived from the schema, not enumerated in code."""

    def test_tables_with_a_model_foreign_key_are_containers(self):
        navigator = _default_navigator()

        assert navigator.get_container_tables() == ["bpmn_process", "collaboration"]

    def test_element_tables_are_not_containers(self):
        navigator = _default_navigator()

        assert navigator.is_container_table("pool") is False
        assert navigator.is_container_table("bpmn_element") is False

    def test_containers_are_not_element_tables(self):
        navigator = _default_navigator()

        assert navigator.is_element_table("collaboration") is False
        assert navigator.is_element_table("bpmn_process") is False

    def test_hierarchy_tables_are_element_tables(self):
        navigator = _default_navigator()

        assert navigator.is_element_table("bpmn_element") is True
        assert navigator.is_element_table("event") is True
        assert navigator.is_element_table("task") is True

    def test_detail_table_outside_the_hierarchy_is_neither(self):
        """message_definition exists as a table but no rule can select it."""
        navigator = _default_navigator()

        assert navigator.is_element_table("message_definition") is False
        assert navigator.is_container_table("message_definition") is False


# ==================== Selection and attributes ====================

class TestContainerIds:

    def test_returns_primary_keys(self):
        navigator = _default_navigator()

        assert navigator.get_container_ids("bpmn_process") == ["001", "002"]
        assert navigator.get_container_ids("collaboration") == ["001"]

    def test_non_container_raises(self):
        navigator = _default_navigator()

        with pytest.raises(Exception, match="ist kein Container"):
            navigator.get_container_ids("pool")


class TestContainerAttribute:

    def test_reads_own_column(self):
        navigator = _default_navigator()

        assert navigator.get_container_attribute("bpmn_process", "001", "is_executable") == "true"
        assert navigator.get_container_attribute("collaboration", "001", "name") == "Invoice Collaboration"

    def test_unknown_container_raises(self):
        navigator = _default_navigator()

        with pytest.raises(Exception, match="nicht gefunden"):
            navigator.get_container_attribute("bpmn_process", "999", "name")

    def test_unknown_column_raises(self):
        """A container has no inheritance chain, so a missing column is unambiguous."""
        navigator = _default_navigator()

        with pytest.raises(Exception, match="existiert nicht in Container-Tabelle"):
            navigator.get_container_attribute("bpmn_process", "001", "gateway_direction")


# ==================== Membership ====================

class TestContainerMembers:

    def test_process_members_come_from_process_element(self):
        navigator = _default_navigator()

        assert navigator.get_container_members("bpmn_process", "001") == ["001", "002"]

    def test_collaboration_members_are_pools_and_message_flows(self):
        navigator = _default_navigator()

        assert sorted(navigator.get_container_members("collaboration", "001")) == [
            "025", "036", "038",
        ]

    def test_pool_is_no_member_of_the_process_it_references(self):
        """pool.bpmn_process_id names the process carried out, not a containment."""
        navigator = _default_navigator()

        members = navigator.get_container_members("bpmn_process", "002")

        assert members == ["039"]
        assert "038" not in members

    def test_empty_container_has_no_members(self):
        navigator = _default_navigator()

        assert navigator.get_container_members("bpmn_process", "003") == []

    def test_non_container_raises(self):
        navigator = _default_navigator()

        with pytest.raises(Exception, match="ist kein Container"):
            navigator.get_container_members("pool", "025")


class TestUnknownContainerKind:
    """A new container table is selectable, but its membership needs a decision."""

    def test_membership_of_an_unhandled_container_raises(self):
        navigator = _make_navigator(
            tables={
                "choreography": [{"choreography_id": "001", "name": "Order talk"}],
                "bpmn_process": [],
                "collaboration": [],
                "pool": [],
                "message_flow": [],
                "process_element": [],
            },
            columns={**_COLUMNS, "choreography": ["choreography_id", "bpmn_model_id", "name"]},
            relationships=_RELATIONSHIPS
            + [_make_relationship("choreography", "bpmn_model_id", "bpmn_model")],
            child_to_parent={},
        )

        assert navigator.is_container_table("choreography") is True
        assert navigator.get_container_ids("choreography") == ["001"]

        with pytest.raises(Exception, match="keine Mitgliedschaft"):
            navigator.get_container_members("choreography", "001")
