"""
Unit tests for BPMNHierarchyNavigator.

Tests cover:
- next_elements_in_flow method
- Sequence flow navigation
- Edge cases (no flows, multiple flows)
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import List, Optional

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    OutgoingSequenceFlowInfo,
    IncomingSequenceFlowInfo,
)


class TestNextElementsInFlow:
    """Test next_elements_in_flow method."""

    def test_next_elements_single_flow(self):
        """Test element with single outgoing flow."""
        # Arrange
        navigator = self._create_navigator_with_flows([
            {"source": "task_1", "target": "task_2"}
        ])

        # Act
        result = navigator.next_elements_in_flow("task_1")

        # Assert
        assert result is not None
        assert len(result) == 1
        assert result[0] == "task_2"

    def test_next_elements_multiple_flows(self):
        """Test element with multiple outgoing flows (e.g., gateway)."""
        # Arrange
        navigator = self._create_navigator_with_flows([
            {"source": "gateway_1", "target": "task_1"},
            {"source": "gateway_1", "target": "task_2"},
            {"source": "gateway_1", "target": "task_3"}
        ])

        # Act
        result = navigator.next_elements_in_flow("gateway_1")

        # Assert
        assert result is not None
        assert len(result) == 3
        assert "task_1" in result
        assert "task_2" in result
        assert "task_3" in result

    def test_next_elements_no_flows(self):
        """Test element with no outgoing flows (e.g., end event)."""
        # Arrange
        navigator = self._create_navigator_with_flows([
            {"source": "task_1", "target": "end_event"}
        ])

        # Act
        result = navigator.next_elements_in_flow("end_event")

        # Assert
        assert result is None

    def test_next_elements_element_not_in_flows(self):
        """Test element that doesn't appear in sequence_flow table."""
        # Arrange
        navigator = self._create_navigator_with_flows([
            {"source": "task_1", "target": "task_2"}
        ])

        # Act
        result = navigator.next_elements_in_flow("nonexistent_element")

        # Assert
        assert result is None

    def test_next_elements_complex_flow_network(self):
        """Test navigation in complex flow network."""
        # Arrange
        navigator = self._create_navigator_with_flows([
            {"source": "start", "target": "task_1"},
            {"source": "task_1", "target": "gateway_split"},
            {"source": "gateway_split", "target": "task_2"},
            {"source": "gateway_split", "target": "task_3"},
            {"source": "task_2", "target": "gateway_join"},
            {"source": "task_3", "target": "gateway_join"},
            {"source": "gateway_join", "target": "end"}
        ])

        # Act & Assert
        # From start event
        result = navigator.next_elements_in_flow("start")
        assert result == ["task_1"]

        # From task before gateway
        result = navigator.next_elements_in_flow("task_1")
        assert result == ["gateway_split"]

        # From splitting gateway
        result = navigator.next_elements_in_flow("gateway_split")
        assert len(result) == 2
        assert "task_2" in result
        assert "task_3" in result

        # From parallel tasks
        result = navigator.next_elements_in_flow("task_2")
        assert result == ["gateway_join"]

        result = navigator.next_elements_in_flow("task_3")
        assert result == ["gateway_join"]

        # From joining gateway
        result = navigator.next_elements_in_flow("gateway_join")
        assert result == ["end"]

        # From end event
        result = navigator.next_elements_in_flow("end")
        assert result is None

    def test_next_elements_sequence_flow_table_not_found(self):
        """Test behavior when sequence_flow table doesn't exist."""
        # Arrange
        navigator_mock = Mock(spec=BPMNHierarchyNavigator)
        mock_database = Mock()
        mock_database.get_table.side_effect = Exception("Tabelle 'sequence_flow' existiert nicht in der Datenbankinstanz.")
        navigator_mock.m_database = mock_database

        # Bind the real method to the mock
        navigator_mock.next_elements_in_flow = BPMNHierarchyNavigator.next_elements_in_flow.__get__(
            navigator_mock, BPMNHierarchyNavigator
        )

        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            navigator_mock.next_elements_in_flow("task_1")

        assert "sequence_flow" in str(exc_info.value).lower()

    # ==================== Helper Methods ====================

    def _create_navigator_with_flows(self, flows: List[dict]):
        """
        Create a mock Navigator with sequence_flow data.

        Args:
            flows: List of dicts with 'source' and 'target' keys

        Returns:
            Mock Navigator instance configured for flow queries
        """
        # Create mock navigator
        navigator_mock = Mock(spec=BPMNHierarchyNavigator)

        # Create mock sequence_flow table
        mock_sequence_flow_table = Mock()

        # Create mock iterator behavior
        def create_iterator_side_effect(filter_active, condition):
            """Mock iterator that returns flows matching the condition."""
            # Extract source_ref from condition
            source_ref = condition._value  # ConditionEquals stores value in _value

            # Find matching flows
            matching_flows = [f for f in flows if f["source"] == source_ref]

            # Create mock iterator
            mock_iterator = Mock()

            # Set up iterator state
            iterator_state = {"index": 0, "flows": matching_flows}

            def is_empty():
                return iterator_state["index"] >= len(iterator_state["flows"])

            def value(field_name):
                if field_name == "target_bpmn_element_id":
                    return iterator_state["flows"][iterator_state["index"]]["target"]
                return None

            def pp():
                iterator_state["index"] += 1

            mock_iterator.is_empty.side_effect = is_empty
            mock_iterator.value.side_effect = value
            mock_iterator.pp.side_effect = pp

            return mock_iterator

        mock_sequence_flow_table.create_iterator.side_effect = create_iterator_side_effect

        # Create mock database
        mock_database = Mock()
        mock_database.get_table.return_value = mock_sequence_flow_table

        # Assign mock database to navigator
        navigator_mock.m_database = mock_database

        # Bind the real method to the mock
        navigator_mock.next_elements_in_flow = BPMNHierarchyNavigator.next_elements_in_flow.__get__(
            navigator_mock, BPMNHierarchyNavigator
        )

        return navigator_mock


class TestNextElementsInFlowIntegration:
    """Integration tests with real BPMNHierarchyNavigator (if needed)."""

    @pytest.mark.skip(reason="Requires full DatabaseInstance setup - to be implemented")
    def test_next_elements_with_real_database(self):
        """Test with real DatabaseInstance and schema."""
        # TODO: Implement when DatabaseInstance test fixtures are available
        pass


class TestSequenceFlowInfoDataclasses:
    """Tests for OutgoingSequenceFlowInfo and IncomingSequenceFlowInfo with is_default field."""

    def test_outgoing_with_is_default_true(self):
        info = OutgoingSequenceFlowInfo(
            sequence_flow_id="f1", target_element_id="t1",
            condition_expression=None, is_default=True,
        )
        assert info.is_default is True

    def test_outgoing_with_is_default_none(self):
        info = OutgoingSequenceFlowInfo(
            sequence_flow_id="f1", target_element_id="t1",
            condition_expression=None, is_default=None,
        )
        assert info.is_default is None

    def test_incoming_with_is_default_none(self):
        info = IncomingSequenceFlowInfo(
            sequence_flow_id="f1", source_element_id="s1",
            condition_expression=None, is_default=None,
        )
        assert info.is_default is None

    def test_incoming_with_is_default_false(self):
        info = IncomingSequenceFlowInfo(
            sequence_flow_id="f1", source_element_id="s1",
            condition_expression="x > 1", is_default=False,
        )
        assert info.is_default is False
        assert info.condition_expression == "x > 1"

    def test_frozen_dataclass_immutability(self):
        info = OutgoingSequenceFlowInfo(
            sequence_flow_id="f1", target_element_id="t1",
            condition_expression=None, is_default=True,
        )
        with pytest.raises(AttributeError):
            info.is_default = False  # type: ignore[misc]


class TestConvertToOptionalBool:
    """Tests for _convert_to_optional_bool helper method."""

    def _create_navigator(self) -> BPMNHierarchyNavigator:
        nav_mock = Mock(spec=BPMNHierarchyNavigator)
        nav_mock._convert_to_optional_bool = BPMNHierarchyNavigator._convert_to_optional_bool.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        return nav_mock

    def test_none_returns_none(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool(None) is None

    def test_empty_string_returns_none(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("") is None

    def test_true_string_returns_true(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("true") is True

    def test_true_uppercase_returns_true(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("TRUE") is True

    def test_one_returns_true(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("1") is True

    def test_minus_one_returns_true(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("-1") is True

    def test_false_string_returns_false(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("FALSE") is False

    def test_zero_returns_false(self):
        nav = self._create_navigator()
        assert nav._convert_to_optional_bool("0") is False

    def test_invalid_value_raises(self):
        nav = self._create_navigator()
        with pytest.raises(ValueError):
            nav._convert_to_optional_bool("invalid")


class TestGetOutgoingSequenceFlowsIsDefault:
    """Tests for get_outgoing_sequence_flows reading is_default from data (TC-003)."""

    def _create_navigator_with_outgoing_flows(self, flows: List[dict]):
        """Create a mock navigator with outgoing flow data including is_default."""
        nav_mock = Mock(spec=BPMNHierarchyNavigator)

        # Bind real methods
        nav_mock.get_outgoing_sequence_flows = BPMNHierarchyNavigator.get_outgoing_sequence_flows.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        nav_mock._convert_to_optional_bool = BPMNHierarchyNavigator._convert_to_optional_bool.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        nav_mock._format_db_internal_id = lambda x: str(x)

        mock_table = Mock()

        def create_iterator_side_effect(filter_active, condition):
            source_ref = condition._value
            matching = [f for f in flows if f["source"] == source_ref]
            mock_iter = Mock()
            state = {"index": 0}

            def is_empty():
                return state["index"] >= len(matching)

            def value(field):
                f = matching[state["index"]]
                return f.get(field)

            def pp():
                state["index"] += 1

            mock_iter.is_empty.side_effect = is_empty
            mock_iter.value.side_effect = value
            mock_iter.pp.side_effect = pp
            return mock_iter

        mock_table.create_iterator.side_effect = create_iterator_side_effect
        mock_db = Mock()
        mock_db.get_table.return_value = mock_table
        nav_mock.m_database = mock_db
        return nav_mock

    def test_outgoing_flow_with_is_default_true(self):
        """Flow with is_default='TRUE' returns OutgoingSequenceFlowInfo.is_default == True."""
        nav = self._create_navigator_with_outgoing_flows([
            {"source": "e1", "bpmn_element_id": "f1", "target_bpmn_element_id": "t1",
             "source_bpmn_element_id": "e1", "condition_expression": None, "is_default": "TRUE"},
        ])
        result = nav.get_outgoing_sequence_flows("e1")
        assert len(result) == 1
        assert result[0].is_default is True

    def test_outgoing_flow_with_empty_is_default(self):
        """Flow with empty is_default returns OutgoingSequenceFlowInfo.is_default is None."""
        nav = self._create_navigator_with_outgoing_flows([
            {"source": "e1", "bpmn_element_id": "f2", "target_bpmn_element_id": "t2",
             "source_bpmn_element_id": "e1", "condition_expression": None, "is_default": ""},
        ])
        result = nav.get_outgoing_sequence_flows("e1")
        assert len(result) == 1
        assert result[0].is_default is None


class TestGetIncomingSequenceFlowsIsDefault:
    """Tests for get_incoming_sequence_flows reading is_default from data (TC-004)."""

    def _create_navigator_with_incoming_flows(self, flows: List[dict]):
        """Create a mock navigator with incoming flow data including is_default."""
        nav_mock = Mock(spec=BPMNHierarchyNavigator)

        nav_mock.get_incoming_sequence_flows = BPMNHierarchyNavigator.get_incoming_sequence_flows.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        nav_mock._convert_to_optional_bool = BPMNHierarchyNavigator._convert_to_optional_bool.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        nav_mock._format_db_internal_id = lambda x: str(x)

        mock_table = Mock()

        def create_iterator_side_effect(filter_active, condition):
            target_ref = condition._value
            matching = [f for f in flows if f["target"] == target_ref]
            mock_iter = Mock()
            state = {"index": 0}

            def is_empty():
                return state["index"] >= len(matching)

            def value(field):
                f = matching[state["index"]]
                return f.get(field)

            def pp():
                state["index"] += 1

            mock_iter.is_empty.side_effect = is_empty
            mock_iter.value.side_effect = value
            mock_iter.pp.side_effect = pp
            return mock_iter

        mock_table.create_iterator.side_effect = create_iterator_side_effect
        mock_db = Mock()
        mock_db.get_table.return_value = mock_table
        nav_mock.m_database = mock_db
        return nav_mock

    def test_incoming_flow_with_is_default_true(self):
        """Flow with is_default='TRUE' returns IncomingSequenceFlowInfo.is_default == True."""
        nav = self._create_navigator_with_incoming_flows([
            {"target": "e1", "bpmn_element_id": "f1", "source_bpmn_element_id": "s1",
             "target_bpmn_element_id": "e1", "condition_expression": None, "is_default": "TRUE"},
        ])
        result = nav.get_incoming_sequence_flows("e1")
        assert len(result) == 1
        assert result[0].is_default is True

    def test_incoming_flow_with_empty_is_default(self):
        """Flow with empty is_default returns IncomingSequenceFlowInfo.is_default is None."""
        nav = self._create_navigator_with_incoming_flows([
            {"target": "e1", "bpmn_element_id": "f2", "source_bpmn_element_id": "s2",
             "target_bpmn_element_id": "e1", "condition_expression": None, "is_default": ""},
        ])
        result = nav.get_incoming_sequence_flows("e1")
        assert len(result) == 1
        assert result[0].is_default is None


class TestGetElementIdsByType:
    """Tests for get_element_ids_by_type method."""

    def _create_navigator_with_elements(self, elements: dict) -> BPMNHierarchyNavigator:
        """Create mock navigator with element mappings and descendant logic.

        Args:
            elements: Dict mapping element_id -> list of ancestor table names
        """
        nav_mock = Mock(spec=BPMNHierarchyNavigator)
        nav_mock.m_element_mapping = {eid: {} for eid in elements}
        nav_mock._FLOW_OBJECT_TABLES = BPMNHierarchyNavigator._FLOW_OBJECT_TABLES

        def mock_is_descendant(element_id: str, ancestor_table: str) -> bool:
            if element_id not in elements:
                return False
            return ancestor_table in elements[element_id]

        nav_mock.is_element_descendant_of.side_effect = mock_is_descendant
        nav_mock.get_element_ids_by_type = BPMNHierarchyNavigator.get_element_ids_by_type.__get__(
            nav_mock, BPMNHierarchyNavigator
        )
        return nav_mock

    def test_get_start_events(self):
        nav = self._create_navigator_with_elements({
            "e1": ["event", "start_event"],
            "e2": ["activity", "task"],
            "e3": ["event", "start_event"],
        })
        result = nav.get_element_ids_by_type("start_event")
        assert sorted(result) == ["e1", "e3"]

    def test_flow_object_returns_all_activities_events_gateways(self):
        nav = self._create_navigator_with_elements({
            "e1": ["activity", "task"],
            "e2": ["event", "start_event"],
            "e3": ["gateway", "exclusive_gateway"],
            "e4": [],  # sequence_flow - not a flow_object
        })
        result = nav.get_element_ids_by_type("flow_object")
        assert sorted(result) == ["e1", "e2", "e3"]

    def test_flow_object_no_duplicates(self):
        nav = self._create_navigator_with_elements({
            "e1": ["activity", "event"],  # hypothetical: descendant of both
        })
        result = nav.get_element_ids_by_type("flow_object")
        assert result == ["e1"]

    def test_nonexistent_type_returns_empty(self):
        nav = self._create_navigator_with_elements({
            "e1": ["activity", "task"],
        })
        result = nav.get_element_ids_by_type("nonexistent_type")
        assert result == []

    def test_flow_object_empty_bpmn_returns_empty(self):
        """TC-012: flow_object with no flow objects (only data objects) returns empty list."""
        nav = self._create_navigator_with_elements({
            "d1": [],  # data object — not a descendant of activity, event, or gateway
            "d2": [],  # another data object
        })
        result = nav.get_element_ids_by_type("flow_object")
        assert result == []
