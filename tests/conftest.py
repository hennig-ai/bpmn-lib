"""
Global pytest fixtures for BPMN Library tests.

This module provides shared fixtures used across all test modules.
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Generator, Any
from unittest.mock import MagicMock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ==================== Configuration Fixtures ====================

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory that is cleaned up after the test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ==================== Markdown Fixtures ====================

@pytest.fixture
def sample_markdown_content():
    """Provide sample markdown content for testing."""
    return """# Test Document

## Section 1

This is a paragraph.

### Table 1

| Column1 | Column2 | Column3 |
|---------|---------|---------|
| Value1  | Value2  | Value3  |
| Value4  | Value5  | Value6  |

## Section 2

```python
print("Hello World")
```
"""


@pytest.fixture
def sample_schema_markdown():
    """Provide sample BPMN schema markdown for testing."""
    return """# BPMN Schema Definition

## Table: Process

| Column Name | Type | PK | NOT NULL | UNIQUE | FK | Value Domain |
|------------|------|-------|----------|--------|-------|--------------|
| id | TEXT | YES | YES | YES | - | - |
| name | TEXT | - | YES | - | - | - |
| description | TEXT | - | - | - | - | - |

## Table: Task

| Column Name | Type | PK | NOT NULL | UNIQUE | FK | Value Domain |
|------------|------|-------|----------|--------|-------|--------------|
| id | TEXT | YES | YES | YES | - | - |
| process_id | TEXT | - | YES | - | Process.id | - |
| name | TEXT | - | YES | - | - | - |
| type | TEXT | - | YES | - | - | UserTask,ServiceTask,ScriptTask |
"""


@pytest.fixture
def sample_instance_markdown():
    """Provide sample BPMN instance markdown for testing."""
    return """# BPMN Instance Data

## Process

| id | name | description |
|----|------|-------------|
| P1 | Main Process | The main business process |
| P2 | Sub Process | A sub process |

## Task

| id | process_id | name | type |
|----|------------|------|------|
| T1 | P1 | User Input | UserTask |
| T2 | P1 | Process Data | ServiceTask |
| T3 | P2 | Script Execution | ScriptTask |
"""


# ==================== File Fixtures ====================

@pytest.fixture
def create_markdown_file(temp_dir: Path):
    """Factory fixture to create markdown files for testing."""
    def _create_file(filename: str, content: str) -> Path:
        file_path = temp_dir / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding='utf-8')
        return file_path
    return _create_file


@pytest.fixture
def create_schema_file(temp_dir: Path, sample_schema_markdown: str):
    """Create a temporary schema markdown file."""
    schema_path = temp_dir / "bpmn-schema.md"
    schema_path.write_text(sample_schema_markdown, encoding='utf-8')
    return schema_path


@pytest.fixture
def create_instance_file(temp_dir: Path, sample_instance_markdown: str):
    """Create a temporary instance markdown file."""
    instance_path = temp_dir / "process_input" / "instance.md"
    instance_path.parent.mkdir(parents=True, exist_ok=True)
    instance_path.write_text(sample_instance_markdown, encoding='utf-8')
    return instance_path


# ==================== Navigator Mock Fixtures ====================

class NavigatorTestDataBuilder:
    """
    Builder pattern for creating test element data in Navigator mock.

    Provides fluent interface for adding BPMN elements with all required attributes.
    """

    def __init__(self) -> None:
        """Initialize empty test data storage."""
        self._elements: Dict[str, Dict[str, Any]] = {}

    def add_task(
        self,
        element_id: str,
        name: str = "Test Task",
        element_type: str = "task",
        specific_type: str = "task",
        process_id: str = "process_1",
        **additional_attributes: Any
    ) -> "NavigatorTestDataBuilder":
        """
        Add a task element to test data.

        Args:
            element_id: Unique element identifier
            name: Task name
            element_type: Type at bpmn_element level (typically "task")
            specific_type: Most specific type (e.g., "user_task", "service_task")
            process_id: ID of containing process
            **additional_attributes: Any additional element attributes

        Returns:
            Self for method chaining
        """
        self._elements[element_id] = {
            "id": element_id,
            "name": name,
            "element_type": element_type,
            "specific_type": specific_type,
            "process_id": process_id,
            **additional_attributes
        }
        return self

    def add_gateway(
        self,
        element_id: str,
        name: str = "Test Gateway",
        element_type: str = "gateway",
        specific_type: str = "exclusive",
        process_id: str = "process_1",
        **additional_attributes: Any
    ) -> "NavigatorTestDataBuilder":
        """
        Add a gateway element to test data.

        Args:
            element_id: Unique element identifier
            name: Gateway name
            element_type: Type at bpmn_element level (typically "gateway")
            specific_type: Gateway type (e.g., "exclusive", "parallel")
            process_id: ID of containing process
            **additional_attributes: Any additional element attributes

        Returns:
            Self for method chaining
        """
        self._elements[element_id] = {
            "id": element_id,
            "name": name,
            "element_type": element_type,
            "specific_type": specific_type,
            "process_id": process_id,
            **additional_attributes
        }
        return self

    def add_event(
        self,
        element_id: str,
        name: str = "Test Event",
        element_type: str = "event",
        specific_type: str = "start",
        process_id: str = "process_1",
        **additional_attributes: Any
    ) -> "NavigatorTestDataBuilder":
        """
        Add an event element to test data.

        Args:
            element_id: Unique element identifier
            name: Event name
            element_type: Type at bpmn_element level (typically "event")
            specific_type: Event type (e.g., "start", "end", "intermediate")
            process_id: ID of containing process
            **additional_attributes: Any additional element attributes

        Returns:
            Self for method chaining
        """
        self._elements[element_id] = {
            "id": element_id,
            "name": name,
            "element_type": element_type,
            "specific_type": specific_type,
            "process_id": process_id,
            **additional_attributes
        }
        return self

    def add_flow(
        self,
        flow_id: str,
        source_ref: str,
        target_ref: str,
        name: str = "",
        element_type: str = "sequence_flow",
        specific_type: str = "sequence_flow",
        process_id: str = "process_1",
        **additional_attributes: Any
    ) -> "NavigatorTestDataBuilder":
        """
        Add a sequence flow element to test data.

        Args:
            flow_id: Unique flow identifier
            source_ref: ID of source element
            target_ref: ID of target element
            name: Flow name (optional)
            element_type: Type at bpmn_element level (typically "sequence_flow")
            specific_type: Most specific type
            process_id: ID of containing process
            **additional_attributes: Any additional element attributes

        Returns:
            Self for method chaining
        """
        self._elements[flow_id] = {
            "id": flow_id,
            "name": name,
            "element_type": element_type,
            "specific_type": specific_type,
            "source_ref": source_ref,
            "target_ref": target_ref,
            "process_id": process_id,
            **additional_attributes
        }
        return self

    def build(self) -> MagicMock:
        """
        Build and return configured Navigator mock.

        Returns:
            Mock Navigator with test data configured
        """
        from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator

        navigator_mock = MagicMock(spec=BPMNHierarchyNavigator)

        # Configure get_element_attribute to return from test data
        def mock_get_attribute(element_id: str, attribute_name: str) -> Any:
            if element_id not in self._elements:
                raise ValueError(f"Element '{element_id}' not found in test data")

            element = self._elements[element_id]
            if attribute_name not in element:
                raise ValueError(
                    f"Attribute '{attribute_name}' not found for element '{element_id}'"
                )

            return element[attribute_name]

        navigator_mock.get_element_attribute.side_effect = mock_get_attribute

        # Configure get_element_specific_type
        def mock_get_specific_type(element_id: str) -> str:
            if element_id not in self._elements:
                raise ValueError(f"Element '{element_id}' not found in test data")
            return self._elements[element_id]["specific_type"]

        navigator_mock.get_element_specific_type.side_effect = mock_get_specific_type

        return navigator_mock


@pytest.fixture
def mock_navigator() -> MagicMock:
    """
    Provide a reusable Navigator mock fixture with common test data.

    Returns a Mock object with spec=BPMNHierarchyNavigator that simulates
    element data retrieval. Pre-configured with sample elements.

    Returns:
        Mock Navigator with test data for task, gateway, event, and flow

    Example:
        def test_something(mock_navigator):
            # Mock already has sample data
            element_type = mock_navigator.get_element_attribute("task_1", "element_type")
            assert element_type == "task"
    """
    from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator

    builder = NavigatorTestDataBuilder()

    # Add common test elements
    builder.add_task("task_1", name="User Task 1", specific_type="user_task")
    builder.add_task("task_2", name="Service Task 1", specific_type="service_task")
    builder.add_gateway("gateway_1", name="Exclusive Gateway", specific_type="exclusive")
    builder.add_event("event_start", name="Start Event", specific_type="start")
    builder.add_event("event_end", name="End Event", specific_type="end")
    builder.add_flow("flow_1", source_ref="event_start", target_ref="task_1")
    builder.add_flow("flow_2", source_ref="task_1", target_ref="gateway_1")
    builder.add_flow("flow_3", source_ref="gateway_1", target_ref="task_2")
    builder.add_flow("flow_4", source_ref="task_2", target_ref="event_end")

    return builder.build()


@pytest.fixture
def navigator_builder() -> NavigatorTestDataBuilder:
    """
    Provide NavigatorTestDataBuilder for custom test scenarios.

    Use this fixture when you need to create custom test data that differs
    from the standard mock_navigator fixture.

    Returns:
        Empty NavigatorTestDataBuilder instance

    Example:
        def test_custom_scenario(navigator_builder):
            navigator = navigator_builder.add_task("custom_task").build()
            # Use custom navigator
    """
    return NavigatorTestDataBuilder()


# ==================== Marker Configuration ====================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow tests")
    config.addinivalue_line("markers", "network: Tests requiring network access")
