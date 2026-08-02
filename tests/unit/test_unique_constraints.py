"""
Unit tests for UNIQUE column constraints.

A column marked UNIQUE in the schema document ended up as a flag on the column
definition and was then dropped: the constraint list of the table stayed empty and
validate_unique_constraints() iterated over nothing. Every UNIQUE of the BPMN
schema was dead, among them the bpmn_element_id of each subtype table and the
model-wide unique name of an element.

These tests pin the whole chain, from the schema markdown down to the reported
violation, plus the two cases NULL brings along: repeated NULL is allowed, and it
must not break the index construction that follows a successful validation.
"""

from typing import List, Tuple

from basic_framework import MarkdownDocument

from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.schema.database_schema_parser import DatabaseSchemaParser
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.utils.validation_result import ValidationResult

_SCHEMA = """# Test Schema

## Tables

### Table: bpmn_element

**Description**: The base entity of every element.

| Column | Data Type | Description | Constraints | References |
|--------|-----------|-------------|------------|------------|
| bpmn_element_id | INTEGER | Unique identifier | PK, AUTO_INCREMENT | |
| name | VARCHAR(255) | Name of the element | NOT NULL, UNIQUE | |
| element_type | VARCHAR(50) | Discriminator | NOT NULL | |

### Table: gateway

**Description**: Specialization of an element.

| Column | Data Type | Description | Constraints | References |
|--------|-----------|-------------|------------|------------|
| gateway_id | INTEGER | Unique identifier | PK, AUTO_INCREMENT | |
| bpmn_element_id | INTEGER | Reference to the base element | FK, UNIQUE | bpmn_element.bpmn_element_id |
| gateway_type | VARCHAR(50) | Type of gateway | | |
"""

_TWO_ELEMENTS = """# Test Data

## bpmn_element

| bpmn_element_id | name | element_type |
|-----------------|------|--------------|
| 001 | Start Event | event |
| 002 | Route Invoice | gateway |
"""


def _parse_schema() -> DatabaseSchema:
    """Parse the schema document above into a DatabaseSchema."""
    document = MarkdownDocument()
    document.load_from_string(_SCHEMA)
    return DatabaseSchemaParser().parse_documents(ValidationResult(), document, "test-schema")


def _build(data_markdown: str) -> Tuple[DatabaseBuilder, ValidationResult]:
    """Run schema parsing, data loading and constraint validation over a data document."""
    result = ValidationResult()

    schema_document = MarkdownDocument()
    schema_document.load_from_string(_SCHEMA)
    schema = DatabaseSchemaParser().parse_documents(result, schema_document, "test-schema")

    data_document = MarkdownDocument()
    data_document.load_from_string(data_markdown)

    builder = DatabaseBuilder(schema, result)
    builder.load_all_data(data_document.create_table_dictionary())
    builder.validate_all_constraints()

    return builder, result


class TestUniqueConstraintRegistration:
    """A UNIQUE in the constraint column has to reach the table definition."""

    def test_unique_column_becomes_a_single_column_constraint(self) -> None:
        schema = _parse_schema()

        constraints = schema.get_table_definition("bpmn_element").get_unique_constraints()

        assert constraints == [["name"]]

    def test_unique_foreign_key_becomes_a_constraint(self) -> None:
        """The bpmn_element_id of a subtype table is 'FK, UNIQUE' throughout the schema."""
        schema = _parse_schema()

        constraints = schema.get_table_definition("gateway").get_unique_constraints()

        assert constraints == [["bpmn_element_id"]]

    def test_primary_key_is_not_registered_a_second_time(self) -> None:
        """A PK is unique by definition - checking it twice only doubles the message."""
        schema = _parse_schema()

        for table_name in ["bpmn_element", "gateway"]:
            table = schema.get_table_definition(table_name)
            for constraint in table.get_unique_constraints():
                assert constraint != table.get_primary_key_columns()

    def test_add_column_derives_the_constraint(self) -> None:
        """The derivation sits in add_column, next to the one for primary keys."""
        table = TableDefinition("element", "Test table")

        column = ColumnDefinition("name", "VARCHAR(255)", "Name of the element")
        column.set_nullable(False)
        column.set_unique(True)
        table.add_column(column)

        assert table.get_unique_constraints() == [["name"]]

    def test_column_without_unique_stays_unconstrained(self) -> None:
        table = TableDefinition("element", "Test table")

        column = ColumnDefinition("documentation", "TEXT", "Free text")
        table.add_column(column)

        assert table.get_unique_constraints() == []

    def test_explicit_multi_column_constraint_still_works(self) -> None:
        table = TableDefinition("element", "Test table")
        table.add_column(ColumnDefinition("process_id", "INTEGER", "Process"))
        table.add_column(ColumnDefinition("position", "INTEGER", "Position"))

        table.add_unique_constraint(["process_id", "position"])

        assert table.get_unique_constraints() == [["process_id", "position"]]


class TestUniqueConstraintValidation:
    """What the registration buys: the bulk validator finally has something to check."""

    def test_distinct_values_pass(self) -> None:
        _, result = _build(_TWO_ELEMENTS)

        assert result.get_messages() == []

    def test_duplicate_name_is_reported(self) -> None:
        data = _TWO_ELEMENTS.replace("| 002 | Route Invoice |", "| 002 | Start Event |")

        _, result = _build(data)

        assert result.count() == 1
        message = result.get_messages()[0]
        assert "bpmn_element" in message
        assert "name" in message
        assert "Start Event" in message

    def test_two_subtype_rows_on_the_same_element_are_reported(self) -> None:
        """One element cannot be two gateways - that is what the UNIQUE on the FK says."""
        data = _TWO_ELEMENTS + """
## gateway

| gateway_id | bpmn_element_id | gateway_type |
|------------|-----------------|--------------|
| 001 | 002 | exclusive |
| 002 | 002 | parallel |
"""

        _, result = _build(data)

        assert result.count() == 1
        message = result.get_messages()[0]
        assert "gateway" in message
        assert "bpmn_element_id" in message

    def test_repeated_null_is_allowed(self) -> None:
        """A unique constraint compares values, and NULL is the absence of one."""
        data = _TWO_ELEMENTS + """
## gateway

| gateway_id | bpmn_element_id | gateway_type |
|------------|-----------------|--------------|
| 001 | ##!empty!## | exclusive |
| 002 | ##!empty!## | parallel |
"""

        _, result = _build(data)

        assert result.get_messages() == []


class TestUniqueIndexCreation:
    """Indexes are built once the validation is through - the constraints reach them too."""

    def test_index_is_built_for_a_not_null_column(self) -> None:
        builder, result = _build(_TWO_ELEMENTS)
        assert result.get_messages() == []

        builder.build_indexes_if_valid()

        # No accessor for the indexes exists; the dictionary is the only evidence.
        indexes: List[str] = list(builder.get_instance()._unique_indexes.keys())  # type: ignore[reportPrivateUsage]
        assert indexes == ["bpmn_element_UNIQUE_0"]

    def test_repeated_null_does_not_break_the_index(self) -> None:
        """ContainerUniqueIndexed reads NULL as a value and would reject the second row."""
        data = _TWO_ELEMENTS + """
## gateway

| gateway_id | bpmn_element_id | gateway_type |
|------------|-----------------|--------------|
| 001 | ##!empty!## | exclusive |
| 002 | ##!empty!## | parallel |
"""
        builder, result = _build(data)
        assert result.get_messages() == []

        builder.build_indexes_if_valid()

        indexes: List[str] = list(builder.get_instance()._unique_indexes.keys())  # type: ignore[reportPrivateUsage]
        assert "gateway_UNIQUE_0" not in indexes
