"""
Integration tests for database building pipeline.

Tests cover:
- Complete pipeline from schema to read-only database
- Schema parsing integration
- Data loading integration
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.utils.validation_result import ValidationResult
from tests.utils.test_helpers import (
    DatabaseSchemaFactory,
    TableDefinitionFactory,
)


class TestDatabaseBuildingPipeline:
    """Integration tests for the complete database building pipeline."""

    def test_schema_creation_with_tables(self):
        """Test creating a schema with tables."""
        # Create validation result
        val_result = ValidationResult()

        # Create schema
        schema = DatabaseSchema(val_result, "test_schema")

        # Add tables
        process_table = TableDefinitionFactory.create_process_table()
        task_table = TableDefinitionFactory.create_task_table()

        schema.add_table_definition(process_table)
        schema.add_table_definition(task_table)

        # Verify
        assert len(schema.get_table_names()) == 2
        assert "Process" in schema.get_table_names()
        assert "Task" in schema.get_table_names()

    def test_schema_validation_passes(self):
        """Test that valid schema passes validation."""
        # Create schema with factory
        schema = DatabaseSchemaFactory.create_with_tables()

        # Validate
        result = schema.validate_schema()
        assert result is True

    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_builder_initialization_from_schema(self, mock_log):
        """Test builder can be initialized from schema."""
        # Create schema
        schema = DatabaseSchemaFactory.create_with_tables()
        val_result = ValidationResult()

        # Create builder
        builder = DatabaseBuilder(schema, val_result)

        # Verify builder is properly initialized
        assert builder._schema == schema
        assert builder._instance is not None
        assert builder._bulk_validator is not None
        assert builder._index_builder is not None


class TestValidationResultIntegration:
    """Test ValidationResult integration with schema and builder."""

    def test_validation_result_collects_errors(self):
        """Test that validation result collects schema errors."""
        # Create validation result
        val_result = ValidationResult()

        # Create schema
        schema = DatabaseSchema(val_result, "test")

        # Add duplicate table (should create error)
        table1 = TableDefinitionFactory.create("duplicate")
        table2 = TableDefinitionFactory.create("duplicate")

        schema.add_table_definition(table1)
        schema.add_table_definition(table2)

        # Error should be recorded
        assert val_result.count() > 0

    def test_validation_result_with_hook(self):
        """Test that ValidationResult hook is called on errors."""
        # Create hook
        hook_calls = []

        class TestHook:
            def on_error_added(self, message: str) -> None:
                hook_calls.append(("error", message))

            def on_warning_added(self, message: str) -> None:
                hook_calls.append(("warning", message))

            def on_check_validation(self, result) -> None:
                hook_calls.append(("check", result))

        # Create validation result with hook
        val_result = ValidationResult(hook=TestHook())

        # Add error
        val_result.add_error("Test error")

        # Hook should have been called
        assert len(hook_calls) == 1
        assert hook_calls[0][0] == "error"
        assert "Test error" in hook_calls[0][1]
