"""
Unit tests for DatabaseSchema class.

Tests cover:
- Schema initialization
- Table definition management
- Relationship management
- Schema validation
- Getter methods
"""

import pytest
from unittest.mock import patch
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship
from bpmn_lib.utils.validation_result import ValidationResult
from tests.utils.test_helpers import (
    DatabaseSchemaFactory,
    TableDefinitionFactory,
    ColumnDefinitionFactory,
)


def create_validation_result() -> ValidationResult:
    """Create a ValidationResult for testing."""
    # Light version without file operations
    return ValidationResult()


class TestDatabaseSchemaBasics:
    """Test basic database schema functionality."""

    def test_schema_initialization(self):
        """Test basic schema initialization."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test_schema")

        assert schema is not None

    def test_schema_name(self):
        """Test schema name getter."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "my_database")

        assert schema.get_schema_name() == "my_database"

    def test_empty_schema(self):
        """Test that new schema has no tables."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "empty")

        assert len(schema.get_table_names()) == 0

    def test_schema_with_validation_result(self):
        """Test schema with validation result."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        assert schema is not None
        assert val_result.count() == 0


class TestDatabaseSchemaTables:
    """Test table management."""

    def test_add_table_definition(self):
        """Test adding table definition."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table = TableDefinitionFactory.create("users")
        schema.add_table_definition(table)

        assert len(schema.get_table_names()) == 1

    def test_add_multiple_tables(self):
        """Test adding multiple tables."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table1 = TableDefinitionFactory.create("users")
        table2 = TableDefinitionFactory.create("orders")
        table3 = TableDefinitionFactory.create("products")

        schema.add_table_definition(table1)
        schema.add_table_definition(table2)
        schema.add_table_definition(table3)

        assert len(schema.get_table_names()) == 3

    def test_get_table_definition(self):
        """Test retrieving table definition."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table = TableDefinitionFactory.create("users")
        schema.add_table_definition(table)

        retrieved = schema.get_table_definition("users")
        assert retrieved is not None
        assert retrieved.get_table_name() == "users"

    def test_get_table_not_found(self):
        """Test retrieving non-existent table."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # get_table_definition calls log_and_raise which raises ValueError
        with pytest.raises(ValueError, match="existiert nicht im Schema"):
            schema.get_table_definition("nonexistent")

    def test_add_duplicate_table(self):
        """Test that duplicate table is rejected."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table1 = TableDefinitionFactory.create("users")
        table2 = TableDefinitionFactory.create("users")

        schema.add_table_definition(table1)
        initial_count = len(schema.get_table_names())
        schema.add_table_definition(table2)

        # Second should be rejected
        assert len(schema.get_table_names()) == initial_count
        assert val_result.count() > 0

    def test_table_names_list(self):
        """Test getting all table names."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table1 = TableDefinitionFactory.create("users")
        table2 = TableDefinitionFactory.create("orders")
        schema.add_table_definition(table1)
        schema.add_table_definition(table2)

        names = schema.get_table_names()
        assert "users" in names
        assert "orders" in names


class TestDatabaseSchemaRelationships:
    """Test foreign key relationship management."""

    def test_add_relationship(self):
        """Test adding foreign key relationship."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Add tables first
        table1 = TableDefinitionFactory.create("customers")
        table2 = TableDefinitionFactory.create("orders")
        schema.add_table_definition(table1)
        schema.add_table_definition(table2)

        # Add relationship
        fk = ForeignKeyRelationship("orders", "customer_id", "customers", "id")
        schema.add_relationship(fk)

        assert len(schema.get_relationships()) == 1

    def test_multiple_relationships(self):
        """Test adding multiple relationships."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Add tables
        tables = ["customers", "orders", "products"]
        for table_name in tables:
            table = TableDefinitionFactory.create(table_name)
            schema.add_table_definition(table)

        # Add relationships
        fk1 = ForeignKeyRelationship("orders", "customer_id", "customers", "id")
        fk2 = ForeignKeyRelationship("orders", "product_id", "products", "id")

        schema.add_relationship(fk1)
        schema.add_relationship(fk2)

        assert len(schema.get_relationships()) == 2

    def test_get_relationships_for_table(self):
        """Test getting relationships involving a specific table."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Add tables
        for table_name in ["customers", "orders"]:
            table = TableDefinitionFactory.create(table_name)
            schema.add_table_definition(table)

        # Add relationships
        fk = ForeignKeyRelationship("orders", "customer_id", "customers", "id")
        schema.add_relationship(fk)

        # Filter relationships for specific table manually
        relationships = [
            rel for rel in schema.get_relationships()
            if rel.get_source_table() == "orders" or rel.get_target_table() == "orders"
        ]
        assert len(relationships) == 1

    def test_get_relationships_empty(self):
        """Test getting relationships when none exist."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Filter relationships for specific table manually
        relationships = [
            rel for rel in schema.get_relationships()
            if rel.get_source_table() == "users" or rel.get_target_table() == "users"
        ]
        assert len(relationships) == 0

    def test_has_relationships_true(self):
        """Test has_relationships returns true."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table1 = TableDefinitionFactory.create("customers")
        table2 = TableDefinitionFactory.create("orders")
        schema.add_table_definition(table1)
        schema.add_table_definition(table2)

        fk = ForeignKeyRelationship("orders", "customer_id", "customers", "id")
        schema.add_relationship(fk)

        assert len(schema.get_relationships()) > 0

    def test_has_relationships_false(self):
        """Test has_relationships returns false."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        table = TableDefinitionFactory.create("users")
        schema.add_table_definition(table)

        assert len(schema.get_relationships()) == 0


class TestDatabaseSchemaValidation:
    """Test schema validation."""

    def test_validate_valid_schema(self):
        """Test validation of a valid schema."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Create and add a valid table
        table = TableDefinitionFactory.create_process_table()
        schema.add_table_definition(table)

        result = schema.validate_schema()
        assert result

    def test_validate_schema_missing_table_reference(self):
        """Test validation detects missing referenced table."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Add only source table
        table = TableDefinitionFactory.create("orders")
        schema.add_table_definition(table)

        # Add FK referencing missing table
        fk = ForeignKeyRelationship("orders", "customer_id", "customers", "id")
        schema.add_relationship(fk)

        result = schema.validate_schema()
        # Should fail because target table doesn't exist
        assert not result


class TestDatabaseSchemaIntegration:
    """Integration tests for schema operations."""

    def test_create_complete_schema(self):
        """Test creating a complete BPMN schema."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "bpmn_schema")

        # Create Process table
        process_table = TableDefinitionFactory.create_process_table()
        schema.add_table_definition(process_table)

        # Create Task table with FK to Process
        task_table = TableDefinitionFactory.create_task_table()
        schema.add_table_definition(task_table)

        # Add FK relationship
        fk = ForeignKeyRelationship("Task", "process_id", "Process", "id", "fk_task_process")
        schema.add_relationship(fk)

        # Verify
        assert len(schema.get_table_names()) == 2
        assert schema.get_table_definition("Process") is not None
        assert schema.get_table_definition("Task") is not None
        assert len(schema.get_relationships()) > 0

    def test_schema_with_multiple_relationships(self):
        """Test schema with multiple foreign key relationships."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "test")

        # Add tables
        tables = ["customers", "orders", "products", "order_items"]
        for table_name in tables:
            table = TableDefinitionFactory.create(table_name)
            schema.add_table_definition(table)

        # Add relationships
        fk_relationships = [
            ("orders", "customer_id", "customers", "id"),
            ("orders", "product_id", "products", "id"),
            ("order_items", "order_id", "orders", "id"),
            ("order_items", "product_id", "products", "id"),
        ]

        for source_table, source_col, target_table, target_col in fk_relationships:
            fk = ForeignKeyRelationship(source_table, source_col, target_table, target_col)
            schema.add_relationship(fk)

        assert len(schema.get_relationships()) == 4

    def test_schema_getter_methods(self):
        """Test all getter methods work correctly."""
        val_result = create_validation_result()
        schema = DatabaseSchema(val_result, "full_schema")

        # Add tables
        table1 = TableDefinitionFactory.create("users")
        table2 = TableDefinitionFactory.create("posts")
        schema.add_table_definition(table1)
        schema.add_table_definition(table2)

        # Test getters
        assert schema.get_schema_name() == "full_schema"
        assert len(schema.get_table_names()) == 2
