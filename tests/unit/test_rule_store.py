"""Unit tests for build_rule_store()."""

import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from bpmn_lib.validation.rule_store import build_rule_store


@pytest.fixture
def mock_navigator_with_schema() -> MagicMock:
    """Create a Navigator mock with schema support for validation."""
    navigator_mock = MagicMock()

    # Create schema mock
    schema_mock = MagicMock()

    # Mock has_table to return True for known tables
    def mock_has_table(table_name: str) -> bool:
        return table_name in ["event", "gateway", "activity", "flow_object"]

    schema_mock.has_table.side_effect = mock_has_table

    # Mock get_table_definition
    def mock_get_table_definition(table_name: str) -> MagicMock:
        table_def = MagicMock()

        # Define value domains
        if table_name == "event":
            domain = ["start", "end", "intermediate"]
        elif table_name == "gateway":
            domain = ["parallel", "exclusive", "inclusive"]
        else:
            domain = None

        # Mock has_column
        def mock_has_column(col_name: str) -> bool:
            return col_name in [f"{table_name}_type"]

        table_def.has_column.side_effect = mock_has_column

        # Mock is_value_in_domain
        def mock_is_value_in_domain(col_name: str, value: str) -> bool:
            if domain is None:
                return True
            return value in domain

        table_def.is_value_in_domain.side_effect = mock_is_value_in_domain

        return table_def

    schema_mock.get_table_definition.side_effect = mock_get_table_definition

    # Mock navigator.get_schema()
    navigator_mock.get_schema.return_value = schema_mock

    return navigator_mock


@pytest.fixture
def rules_dir(tmp_path: Path) -> Path:
    """Create a temporary directory with valid rule files."""
    rule_content = """# Test Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | Start event {element_id} has no outgoing flow |
| XOR-001 | gateway | exclusive | COUNT(outgoing_flows) >= 2 | | spec_v2 | XOR gateway {element_id} needs at least 2 outgoing flows |
"""
    (tmp_path / "process_rules.md").write_text(rule_content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def rules_dir_two_files(tmp_path: Path) -> Path:
    """Create a temp dir with two valid rule files."""
    file1 = """# Rules A

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
"""
    file2 = """# Rules B

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| END-001 | event | end | COUNT(incoming_flows) >= 1 | | basic | msg2 |
"""
    (tmp_path / "rules_a.md").write_text(file1, encoding="utf-8")
    (tmp_path / "rules_b.md").write_text(file2, encoding="utf-8")
    return tmp_path


class TestBuildRuleStore:

    def test_loads_valid_rules(self, rules_dir: Path, mock_navigator_with_schema: MagicMock):
        store = build_rule_store(str(rules_dir), mock_navigator_with_schema)
        # Iterate and count rules
        iterator = store.create_iterator()
        count = 0
        while not iterator.is_empty():
            count += 1
            iterator.pp()
        assert count == 2

    def test_loads_rules_from_multiple_files(self, rules_dir_two_files: Path, mock_navigator_with_schema: MagicMock):
        store = build_rule_store(str(rules_dir_two_files), mock_navigator_with_schema)
        iterator = store.create_iterator()
        count = 0
        while not iterator.is_empty():
            count += 1
            iterator.pp()
        assert count == 2

    def test_rule_values_accessible(self, rules_dir: Path, mock_navigator_with_schema: MagicMock):
        store = build_rule_store(str(rules_dir), mock_navigator_with_schema)
        iterator = store.create_iterator()
        assert iterator.value("rule_id") == "SRT-001"
        assert iterator.value("element_type") == "event"
        assert iterator.value("subtype") == "start"
        assert iterator.value("level") == "basic"


class TestBuildRuleStoreIgnoresNonMd:
    """TC-036: build_rule_store ignores non-MD files in rules directory."""

    def test_ignores_txt_and_json_files(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        rule_content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(rule_content, encoding="utf-8")
        (tmp_path / "notes.txt").write_text("not a rule file", encoding="utf-8")
        (tmp_path / "config.json").write_text('{"key": "value"}', encoding="utf-8")

        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)
        iterator = store.create_iterator()
        count = 0
        while not iterator.is_empty():
            count += 1
            iterator.pp()
        assert count == 1  # Only the .md file rule


class TestBuildRuleStoreFailFastMissingColumn:
    """TC-040: build_rule_store fail-fast on missing mandatory column."""

    def test_missing_message_template_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level |
|---------|-------------|---------|-----------|-------------|-------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        with pytest.raises((ValueError, Exception), match="message_template"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)


class TestRuleStoreQueryByLevel:
    """TC-042: Rule store supports querying by level via AbstractContainer."""

    def test_query_by_level_basic(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        rule_content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| R1 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
| R2 | gateway | exclusive | COUNT(outgoing_flows) >= 2 | | spec_v2 | msg2 |
| R3 | event | end | COUNT(incoming_flows) >= 1 | | best_practice | msg3 |
"""
        (tmp_path / "rules.md").write_text(rule_content, encoding="utf-8")
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)

        # Iterate and filter by level == "basic"
        from basic_framework.conditions.condition_equals import ConditionEquals
        iterator = store.create_iterator(True, ConditionEquals("level", "basic"))
        basic_rules = []
        while not iterator.is_empty():
            basic_rules.append(iterator.value("rule_id"))
            iterator.pp()
        assert basic_rules == ["R1"]


class TestRuleStoreQueryByElementType:
    """TC-043: Rule store supports querying by element_type via AbstractContainer."""

    def test_query_by_element_type(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        rule_content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| R1 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
| R2 | gateway | exclusive | COUNT(outgoing_flows) >= 2 | | basic | msg2 |
| R3 | gateway | parallel | COUNT(incoming_flows) >= 1 | | basic | msg3 |
"""
        (tmp_path / "rules.md").write_text(rule_content, encoding="utf-8")
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)

        from basic_framework.conditions.condition_equals import ConditionEquals
        iterator = store.create_iterator(True, ConditionEquals("element_type", "gateway"))
        gateway_rules = []
        while not iterator.is_empty():
            gateway_rules.append(iterator.value("rule_id"))
            iterator.pp()
        assert sorted(gateway_rules) == ["R2", "R3"]


class TestBuildRuleStoreErrors:

    def test_empty_directory_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        with pytest.raises(ValueError, match="No .md rule files"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)

    def test_duplicate_rule_id_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
| SRT-001 | event | end | COUNT(incoming_flows) >= 1 | | basic | msg2 |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        with pytest.raises(Exception, match="nicht eindeutig|not unique|eindeutig"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)

    def test_syntax_error_in_assertion_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| BAD-001 | event | start | INVALID ASSERTION | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        with pytest.raises((ValueError, Exception)):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)

    def test_cross_file_duplicate_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        file1 = """# Rules A

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
"""
        file2 = """# Rules B

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | end | COUNT(incoming_flows) >= 1 | | basic | msg2 |
"""
        (tmp_path / "rules_a.md").write_text(file1, encoding="utf-8")
        (tmp_path / "rules_b.md").write_text(file2, encoding="utf-8")
        with pytest.raises(Exception, match="nicht eindeutig|not unique|eindeutig"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)


class TestRuleStoreSchemaValidation:
    """TC-044: build_rule_store validates rules against database schema (3-stage validation)."""

    def test_invalid_element_type_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: Ungültiger element_type → Fehler."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| BAD-001 | invalid_table | | COUNT(outgoing_flows) >= 1 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        with pytest.raises(ValueError, match="not a valid table"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)

    def test_invalid_subtype_raises(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: Ungültiger subtype Wert → Fehler."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| BAD-002 | event | invalid_type | COUNT(outgoing_flows) >= 1 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        with pytest.raises(ValueError, match="not in domain"):
            build_rule_store(str(tmp_path), mock_navigator_with_schema)

    def test_valid_subtype_values_accepted(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: Gültige subtype Werte werden akzeptiert."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg1 |
| END-001 | event | end | COUNT(incoming_flows) >= 1 | | basic | msg2 |
| XOR-001 | gateway | exclusive | COUNT(outgoing_flows) >= 2 | | basic | msg3 |
| AND-001 | gateway | parallel | COUNT(outgoing_flows) >= 2 | | basic | msg4 |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)
        iterator = store.create_iterator()
        count = 0
        while not iterator.is_empty():
            count += 1
            iterator.pp()
        assert count == 4

    def test_empty_subtype_accepted(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: Leerer subtype (alle Elemente des Typs) wird akzeptiert."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| FLO-001 | activity | | COUNT(outgoing_flows) >= 0 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)
        iterator = store.create_iterator()
        assert not iterator.is_empty()
        assert iterator.value("subtype") == ""

    def test_subtype_column_preserved_in_rule(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: subtype-Spalte wird in geladene Regel übernommen."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | msg |
| XOR-001 | gateway | exclusive | COUNT(outgoing_flows) >= 2 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)
        iterator = store.create_iterator()

        # Check first rule: SRT-001 with subtype "start"
        assert iterator.value("rule_id") == "SRT-001"
        assert iterator.value("subtype") == "start"

        # Move to next rule: XOR-001 with subtype "exclusive"
        iterator.pp()
        assert iterator.value("rule_id") == "XOR-001"
        assert iterator.value("subtype") == "exclusive"

    def test_flow_object_subtype_ignored(self, tmp_path: Path, mock_navigator_with_schema: MagicMock):
        """Test: Sonderfall flow_object ignoriert subtype Validierung."""
        content = """# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| FLO-001 | flow_object | any_value | COUNT(outgoing_flows) >= 0 | | basic | msg |
"""
        (tmp_path / "rules.md").write_text(content, encoding="utf-8")
        # Sollte nicht fehlschlagen, weil flow_object die subtype Validierung ignoriert
        store = build_rule_store(str(tmp_path), mock_navigator_with_schema)
        iterator = store.create_iterator()
        assert iterator.value("rule_id") == "FLO-001"
        assert iterator.value("element_type") == "flow_object"
        assert iterator.value("subtype") == "any_value"
