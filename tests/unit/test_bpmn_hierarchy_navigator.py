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

from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator


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
