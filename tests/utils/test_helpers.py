"""
Helper functions and utilities for test development.

This module provides factory functions, builders, and utility functions
for creating test objects and data.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import re


# ==================== Factory Functions ====================

class ColumnDefinitionFactory:
    """Factory for creating ColumnDefinition test objects."""

    @staticmethod
    def create(
        name: str = "test_col",
        data_type: str = "TEXT",
        description: str = "",
        is_primary_key: bool = False,
        is_not_null: bool = False,
        is_unique: bool = False,
        is_foreign_key: bool = False,
        value_domain: Optional[List[str]] = None,
    ) -> "ColumnDefinition":  # type: ignore
        """Create a ColumnDefinition object."""
        from bpmn_lib.database.schema.column_definition import ColumnDefinition

        col = ColumnDefinition(name, data_type, description)
        if is_primary_key:
            col.set_primary_key(True)
        if is_not_null and not is_primary_key:
            col.set_nullable(False)
        if is_unique and not is_primary_key:
            col.set_unique(True)
        if is_foreign_key:
            col.set_foreign_key(True)
        if value_domain:
            col.set_value_domain(value_domain)

        return col

    @staticmethod
    def create_primary_key(name: str = "id", description: str = "") -> "ColumnDefinition":  # type: ignore
        """Create a primary key column."""
        return ColumnDefinitionFactory.create(
            name=name,
            data_type="TEXT",
            description=description,
            is_primary_key=True,
        )

    @staticmethod
    def create_foreign_key(
        name: str = "parent_id",
        table_ref: str = "Parent",
        description: str = "",
    ) -> "ColumnDefinition":  # type: ignore
        """Create a foreign key column."""
        return ColumnDefinitionFactory.create(
            name=name,
            data_type="TEXT",
            description=description,
            is_not_null=True,
            is_foreign_key=True,
        )

    @staticmethod
    def create_enum_column(
        name: str = "status",
        values: Optional[List[str]] = None,
        description: str = "",
    ) -> "ColumnDefinition":  # type: ignore
        """Create an enum/domain column."""
        if values is None:
            values = ["Active", "Inactive", "Pending"]

        return ColumnDefinitionFactory.create(
            name=name,
            data_type="TEXT",
            description=description,
            is_not_null=True,
            value_domain=values,
        )


class TableDefinitionFactory:
    """Factory for creating TableDefinition test objects."""

    @staticmethod
    def create(name: str = "test_table", description: str = "") -> "TableDefinition":  # type: ignore
        """Create a basic TableDefinition object."""
        from bpmn_lib.database.schema.table_definition import TableDefinition

        # Use new single-phase initialization
        table = TableDefinition(name, description)
        return table

    @staticmethod
    def create_with_columns(
        name: str = "test_table",
        description: str = "",
        columns: Optional[List[Dict[str, Any]]] = None,
    ) -> "TableDefinition":  # type: ignore
        """Create a TableDefinition with columns."""
        table = TableDefinitionFactory.create(name, description)

        if columns is None:
            columns = [
                {"name": "id", "type": "TEXT", "pk": True, "nn": True},
                {"name": "name", "type": "TEXT", "pk": False, "nn": True},
            ]

        for col_spec in columns:
            col = ColumnDefinitionFactory.create(
                name=col_spec.get("name", "col"),
                data_type=col_spec.get("type", "TEXT"),
                is_primary_key=col_spec.get("pk", False),
                is_not_null=col_spec.get("nn", False),
                is_unique=col_spec.get("unique", False),
            )
            table.add_column(col)

        return table

    @staticmethod
    def create_process_table() -> "TableDefinition":  # type: ignore
        """Create a Process table for BPMN testing."""
        return TableDefinitionFactory.create_with_columns(
            name="Process",
            columns=[
                {"name": "id", "type": "TEXT", "pk": True, "nn": True},
                {"name": "name", "type": "TEXT", "nn": True},
                {"name": "description", "type": "TEXT"},
                {"name": "version", "type": "TEXT"},
            ],
        )

    @staticmethod
    def create_task_table() -> "TableDefinition":  # type: ignore
        """Create a Task table for BPMN testing."""
        return TableDefinitionFactory.create_with_columns(
            name="Task",
            columns=[
                {"name": "id", "type": "TEXT", "pk": True, "nn": True},
                {"name": "process_id", "type": "TEXT", "nn": True},
                {"name": "name", "type": "TEXT", "nn": True},
                {"name": "type", "type": "TEXT", "nn": True},
                {"name": "status", "type": "TEXT"},
            ],
        )


class DatabaseSchemaFactory:
    """Factory for creating DatabaseSchema test objects."""

    @staticmethod
    def create(schema_name: str = "TestSchema") -> "DatabaseSchema":  # type: ignore
        """Create a basic DatabaseSchema object."""
        from bpmn_lib.database.schema.database_schema import DatabaseSchema
        from bpmn_lib.utils.validation_result import ValidationResult

        # Create ValidationResult (light version without file operations)
        val_result = ValidationResult()
        schema = DatabaseSchema(val_result, schema_name)
        return schema

    @staticmethod
    def create_with_tables(
        tables: Optional[List["TableDefinition"]] = None,  # type: ignore
        schema_name: str = "TestSchema",
    ) -> "DatabaseSchema":  # type: ignore
        """Create a DatabaseSchema with tables."""
        schema = DatabaseSchemaFactory.create(schema_name)

        if tables is None:
            tables = [
                TableDefinitionFactory.create_process_table(),
                TableDefinitionFactory.create_task_table(),
            ]

        for table in tables:
            schema.add_table_definition(table)

        return schema


# ==================== Markdown Generators ====================

class MarkdownTableGenerator:
    """Generate markdown table content for testing."""

    @staticmethod
    def create_schema_table(
        table_name: str = "TestTable",
        columns: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Generate a schema table in markdown format."""
        if columns is None:
            columns = [
                {"name": "id", "type": "TEXT", "pk": True, "nn": True},
                {"name": "name", "type": "TEXT", "nn": True},
            ]

        # Build header
        lines: List[str] = [f"## Table: {table_name}\n"]
        lines.append(
            "| Column Name | Type | PK | NOT NULL | UNIQUE | FK | Value Domain |"
        )
        lines.append("|------------|------|-------|----------|--------|-------|--------------|")

        # Build rows
        for col in columns:
            name = col.get("name", "col")
            dtype = col.get("type", "TEXT")
            pk = "YES" if col.get("pk", False) else "-"
            nn = "YES" if col.get("nn", False) else "-"
            unique = "YES" if col.get("unique", False) else "-"
            fk = col.get("fk", "-")
            domain = col.get("domain", "-")

            lines.append(f"| {name} | {dtype} | {pk} | {nn} | {unique} | {fk} | {domain} |")

        return "\n".join(lines)

    @staticmethod
    def create_data_table(
        table_name: str = "TestTable",
        columns: Optional[List[str]] = None,
        rows: Optional[List[List[str]]] = None,
    ) -> str:
        """Generate a data table in markdown format."""
        if columns is None:
            columns = ["id", "name"]
        if rows is None:
            rows = [["1", "Test"], ["2", "Example"]]

        # Build header
        lines: List[str] = [f"## {table_name}\n"]
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join(["---"] * len(columns)) + "|")

        # Build rows
        for row in rows:
            lines.append("| " + " | ".join(str(val) for val in row) + " |")

        return "\n".join(lines)

    @staticmethod
    def create_full_document(
        title: str = "Test Document",
        schema_tables: Optional[List[Tuple[str, List[Dict]]]] = None,
        data_tables: Optional[List[Tuple[str, List[str], List[List[str]]]]] = None,
    ) -> str:
        """Generate a complete markdown document."""
        lines: List[str] = [f"# {title}\n"]

        # Add schema tables
        if schema_tables:
            for table_name, columns in schema_tables:
                lines.append(MarkdownTableGenerator.create_schema_table(table_name, columns))
                lines.append("")

        # Add data tables
        if data_tables:
            for table_name, columns, rows in data_tables:
                lines.append(MarkdownTableGenerator.create_data_table(table_name, columns, rows))
                lines.append("")

        return "\n".join(lines)


# ==================== Validation Helpers ====================

class ValidationTestHelper:
    """Helper functions for validation testing."""

    @staticmethod
    def create_data_row(values: Dict[str, str]) -> Dict[str, str]:
        """Create a data row for testing."""
        return values.copy()

    @staticmethod
    def create_data_rows(count: int = 5) -> List[Dict[str, str]]:
        """Create multiple data rows for testing."""
        rows: List[Dict[str, str]] = []
        for i in range(1, count + 1):
            rows.append({
                "id": f"ID{i}",
                "name": f"Item {i}",
                "value": f"Value{i}",
            })
        return rows

    @staticmethod
    def extract_error_messages(validation_result) -> List[str]:  # type: ignore
        """Extract error messages from a ValidationResult."""
        if hasattr(validation_result, 'm_validation_messages'):
            return validation_result.m_validation_messages
        return []

    @staticmethod
    def extract_warning_messages(validation_result) -> List[str]:  # type: ignore
        """Extract warning messages from a ValidationResult."""
        # Light ValidationResult stores all in m_validation_messages
        return []


# ==================== Markdown Parsing Helpers ====================

class MarkdownTestHelper:
    """Helper functions for markdown testing."""

    @staticmethod
    def extract_tables(markdown_content: str) -> List[Dict[str, Any]]:
        """Extract tables from markdown content."""
        tables: List[Dict[str, Any]] = []
        lines = markdown_content.split('\n')

        i = 0
        while i < len(lines):
            line = lines[i]

            # Look for table headers (indicated by |)
            if '|' in line and i + 1 < len(lines):
                next_line = lines[i + 1]
                if '|' in next_line and '-' in next_line:
                    # Found a table header
                    headers = [h.strip() for h in line.split('|')[1:-1]]
                    rows: List[List[str]] = []

                    i += 2  # Skip header and separator
                    while i < len(lines) and '|' in lines[i]:
                        row_data = lines[i].split('|')[1:-1]
                        row = [cell.strip() for cell in row_data]
                        if row:  # Skip empty rows
                            rows.append(row)
                        i += 1

                    tables.append({
                        'headers': headers,
                        'rows': rows,
                    })
                    continue

            i += 1

        return tables

    @staticmethod
    def extract_headings(markdown_content: str) -> List[Tuple[int, str]]:
        """Extract headings from markdown content."""
        headings: List[Tuple[int, str]] = []
        lines = markdown_content.split('\n')

        for line in lines:
            match = re.match(r'^(#+)\s+(.+)$', line)
            if match:
                level = len(match.group(1))
                title = match.group(2).strip()
                headings.append((level, title))

        return headings

    @staticmethod
    def extract_code_blocks(markdown_content: str) -> List[Tuple[str, str]]:
        """Extract code blocks from markdown content."""
        code_blocks: List[Tuple[str, str]] = []
        lines = markdown_content.split('\n')

        i = 0
        while i < len(lines):
            if lines[i].startswith('```'):
                language = lines[i][3:].strip() or "unknown"
                code_lines: List[str] = []

                i += 1
                while i < len(lines) and not lines[i].startswith('```'):
                    code_lines.append(lines[i])
                    i += 1

                code_blocks.append((language, '\n'.join(code_lines)))
            i += 1

        return code_blocks


# ==================== Assertion Helpers ====================

class AssertionHelper:
    """Helper functions for test assertions."""

    @staticmethod
    def assert_column_properties(
        column,  # type: ignore
        name: str,
        data_type: str,
        is_primary_key: bool = False,
        is_not_null: bool = False,
        is_unique: bool = False,
    ) -> None:
        """Assert column properties."""
        assert column.get_column_name() == name, f"Expected name {name}, got {column.get_column_name()}"
        assert column.get_data_type() == data_type, f"Expected type {data_type}, got {column.get_data_type()}"
        assert column.is_primary_key() == is_primary_key
        assert column.is_nullable() == (not is_not_null)
        assert column.is_unique() == is_unique

    @staticmethod
    def assert_table_properties(
        table,  # type: ignore
        name: str,
        column_count: Optional[int] = None,
    ) -> None:
        """Assert table properties."""
        assert table.get_table_name() == name, f"Expected name {name}, got {table.get_table_name()}"
        if column_count is not None:
            actual_count = table.get_column_count()
            assert actual_count == column_count, \
                f"Expected {column_count} columns, got {actual_count}"

    @staticmethod
    def assert_no_errors(validation_result) -> None:  # type: ignore
        """Assert that validation result has no errors."""
        if hasattr(validation_result, 'count') and validation_result.count() > 0:
            raise AssertionError(f"Expected no errors, but got: {validation_result.m_validation_messages}")

    @staticmethod
    def assert_has_error(
        validation_result,  # type: ignore
        expected_message: Optional[str] = None,
    ) -> None:
        """Assert that validation result has errors."""
        if not hasattr(validation_result, 'count') or validation_result.count() == 0:
            raise AssertionError("Expected validation errors, but none found")

        if expected_message:
            errors = validation_result.m_validation_messages
            matching = [e for e in errors if expected_message in e]
            assert matching, \
                f"Expected error containing '{expected_message}', got: {errors}"
