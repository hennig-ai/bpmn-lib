"""
Unit tests for value domain parsing in DatabaseSchemaParser.

Value domains are written as plain paragraphs between the tables of the schema
document. They were silently never loaded, which disabled two checks at once: the
subtype validation of rules and the domain validation of instance data.
"""

import pytest

from basic_framework import MarkdownDocument

from bpmn_lib.database.schema.database_schema_parser import DatabaseSchemaParser
from bpmn_lib.utils.validation_result import ValidationResult

_SCHEMA_HEADER = """# Test Schema

## Tables

"""

_GATEWAY_SECTION = """### Table: gateway

**Description**: Decision points.

| Column | Data Type | Description | Constraints | References |
|--------|-----------|-------------|------------|------------|
| gateway_id | INTEGER | Unique identifier | PK | |
| gateway_type | VARCHAR(50) | Type of gateway | NOT NULL | |
| implementation | VARCHAR(50) | Type of implementation | | |

**Value Domain for gateway_type**: ["exclusive", "parallel"]
**Value Domain for implementation**: ["webservice", "other"]

"""

_USER_TASK_SECTION = """### Table: user_task

**Description**: Human work steps.

| Column | Data Type | Description | Constraints | References |
|--------|-----------|-------------|------------|------------|
| user_task_id | INTEGER | Unique identifier | PK | |
| implementation | VARCHAR(100) | Free text description | | |

"""


def _parse(markdown: str):
    """Parse a schema document and return the resulting DatabaseSchema."""
    document = MarkdownDocument()
    document.load_from_string(markdown)
    return DatabaseSchemaParser().parse_documents(ValidationResult(), document, "test-schema")


class TestValueDomainParsing:

    def test_domain_is_attached_to_its_column(self):
        schema = _parse(_SCHEMA_HEADER + _GATEWAY_SECTION)

        domain = schema.get_table_definition("gateway").get_value_domain("gateway_type")

        assert domain == ["exclusive", "parallel"]

    def test_column_without_domain_stays_open(self):
        schema = _parse(_SCHEMA_HEADER + _GATEWAY_SECTION)

        assert schema.get_table_definition("gateway").get_value_domain("gateway_id") is None

    def test_domain_belongs_to_its_own_table_only(self):
        """A column name repeated in another table must not inherit the domain.

        'implementation' exists in gateway and user_task here, mirroring task,
        user_task and business_rule_task in the real schema — only one of them
        declares a domain, and the free-text ones must stay unconstrained.
        """
        schema = _parse(_SCHEMA_HEADER + _GATEWAY_SECTION + _USER_TASK_SECTION)

        assert schema.get_table_definition("gateway").get_value_domain("implementation") == [
            "webservice", "other",
        ]
        assert schema.get_table_definition("user_task").get_value_domain("implementation") is None

    def test_domain_for_unknown_column_raises(self):
        markdown = _SCHEMA_HEADER + _GATEWAY_SECTION.replace(
            "**Value Domain for gateway_type**", "**Value Domain for gateway_flavour**"
        )

        with pytest.raises(Exception, match="die es in Tabelle 'gateway' nicht gibt"):
            _parse(markdown)

    def test_domain_without_values_raises(self):
        markdown = _SCHEMA_HEADER + _GATEWAY_SECTION.replace(
            '**Value Domain for gateway_type**: ["exclusive", "parallel"]',
            "**Value Domain for gateway_type**: []",
        )

        with pytest.raises(Exception, match="ohne Werteliste|enthaelt keine Werte"):
            _parse(markdown)


class TestValueDomainEnforcement:
    """The parsed domains are what makes the downstream checks work at all."""

    def test_domain_rejects_a_value_outside_it(self):
        schema = _parse(_SCHEMA_HEADER + _GATEWAY_SECTION)
        table = schema.get_table_definition("gateway")

        assert table.is_value_in_domain("gateway_type", "parallel") is True
        assert table.is_value_in_domain("gateway_type", "parralel") is False

    def test_column_without_domain_accepts_anything(self):
        schema = _parse(_SCHEMA_HEADER + _GATEWAY_SECTION + _USER_TASK_SECTION)
        table = schema.get_table_definition("user_task")

        assert table.is_value_in_domain("implementation", "whatever the project needs") is True
