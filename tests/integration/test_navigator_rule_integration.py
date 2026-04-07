"""Integration tests for create_navigator with BPMN rule validation (TC-072 to TC-075)."""

import pytest
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    OutgoingSequenceFlowInfo,
)
from bpmn_lib.utils.validation_result import ValidationResult


def _create_valid_rule_file(rules_dir: Path) -> None:
    """Create a valid rule file that a start_event must have >= 1 outgoing flow."""
    content = """# Test Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | start_event | | COUNT(outgoing_flows) >= 1 | | basic | Start event {element_id} has no outgoing flow |
"""
    (rules_dir / "test_rules.md").write_text(content, encoding="utf-8")


def _create_mock_navigator(
    element_ids_by_type: dict,
    outgoing_flows: dict,
) -> BPMNHierarchyNavigator:
    """Create a mock navigator with specified element and flow data."""
    nav = Mock(spec=BPMNHierarchyNavigator)
    nav.get_element_ids_by_type.side_effect = lambda t: element_ids_by_type.get(t, [])
    nav.get_element_attribute.side_effect = lambda eid, attr: None
    nav.get_outgoing_sequence_flows.side_effect = lambda eid: outgoing_flows.get(eid, [])
    nav.get_incoming_sequence_flows.side_effect = lambda eid: []
    return nav


class TestCreateNavigatorWithValidation:
    """TC-072: create_navigator with valid rules_dir and validation_level runs validation."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_valid_bpmn_passes_validation(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        # Set up rule file
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_valid_rule_file(rules_dir)

        # Mock navigator that has a valid start event with outgoing flow
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)]},
        )
        mock_nav_cls.return_value = mock_nav

        # Mock builder pipeline
        mock_builder = MagicMock()
        mock_builder_cls.return_value = mock_builder

        mock_parser = MagicMock()
        mock_parser_cls.return_value = mock_parser

        from bpmn_lib.navigator.navigator_factory import create_navigator

        result = create_navigator(
            schema_file="schema.md",
            data_file="data.md",
            hierarchy_file="hierarchy.md",
            rules_dir=str(rules_dir),
            validation_level="basic",
        )
        assert result is mock_nav


class TestCreateNavigatorFailingRules:
    """TC-073: create_navigator raises error when BPMN rules are violated."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_failing_rules_raise_with_report(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        # Set up rule file
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_valid_rule_file(rules_dir)

        report_dir = tmp_path / "reports"

        # Mock navigator where start event has NO outgoing flow — violation
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder = MagicMock()
        mock_builder_cls.return_value = mock_builder

        mock_parser = MagicMock()
        mock_parser_cls.return_value = mock_parser

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="schema.md",
                data_file="data.md",
                hierarchy_file="hierarchy.md",
                rules_dir=str(rules_dir),
                validation_level="basic",
                report_target=str(report_dir),
            )


class TestCreateNavigatorFailingWithoutReport:
    """TC-074: create_navigator failing rules without report_target raises compact error."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_failing_rules_no_report_target(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_valid_rule_file(rules_dir)

        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder = MagicMock()
        mock_builder_cls.return_value = mock_builder

        mock_parser = MagicMock()
        mock_parser_cls.return_value = mock_parser

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen.*Fehler"):
            create_navigator(
                schema_file="schema.md",
                data_file="data.md",
                hierarchy_file="hierarchy.md",
                rules_dir=str(rules_dir),
                validation_level="basic",
                report_target=None,
            )


class TestCreateNavigatorSkipsValidation:
    """TC-075: create_navigator both params None — skip validation."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_both_none_skips_validation(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        # Mock navigator that would fail validation if it ran
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},  # would fail COUNT(outgoing_flows) >= 1
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder = MagicMock()
        mock_builder_cls.return_value = mock_builder

        mock_parser = MagicMock()
        mock_parser_cls.return_value = mock_parser

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Should succeed even though BPMN data would violate rules
        result = create_navigator(
            schema_file="schema.md",
            data_file="data.md",
            hierarchy_file="hierarchy.md",
            rules_dir=None,
            validation_level=None,
        )
        assert result is mock_nav


def _create_exists_is_default_rule_file(rules_dir: Path) -> None:
    """Create a rule requiring gateway to have a default outgoing flow."""
    content = """# Test Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| XOR-003 | gateway | | EXISTS outgoing_flows: is_default == true | | basic | Gateway {element_id} has no default outgoing flow |
"""
    (rules_dir / "test_rules.md").write_text(content, encoding="utf-8")


def _create_flow_object_rule_file(rules_dir: Path) -> None:
    """Create a rule applying to all flow objects."""
    content = """# Test Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| FLO-001 | flow_object | | COUNT(outgoing_flows) >= 1 | | basic | Flow object {element_id} has no outgoing flow |
"""
    (rules_dir / "test_rules.md").write_text(content, encoding="utf-8")


class TestCountRuleViaNavigator:
    """TC-076: Rule engine evaluates COUNT rule via real navigator data."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_count_rule_uses_navigator_api(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_valid_rule_file(rules_dir)

        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)]},
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        result = create_navigator(
            schema_file="schema.md",
            data_file="data.md",
            hierarchy_file="hierarchy.md",
            rules_dir=str(rules_dir),
            validation_level="basic",
        )
        assert result is mock_nav
        # Verify navigator API was used (not direct DB access)
        mock_nav.get_element_ids_by_type.assert_called()
        mock_nav.get_outgoing_sequence_flows.assert_called_with("se1")


class TestExistsRuleWithIsDefault:
    """TC-077: Rule engine uses is_default field in EXISTS rule via navigator."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_exists_is_default_via_navigator(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_exists_is_default_rule_file(rules_dir)

        mock_nav = _create_mock_navigator(
            element_ids_by_type={"gateway": ["gw1"]},
            outgoing_flows={"gw1": [OutgoingSequenceFlowInfo("f1", "t1", None, True)]},
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # No error when gateway has default outgoing flow
        result = create_navigator(
            schema_file="schema.md",
            data_file="data.md",
            hierarchy_file="hierarchy.md",
            rules_dir=str(rules_dir),
            validation_level="basic",
        )
        assert result is mock_nav


class TestFlowObjectTypeSelection:
    """TC-078: Rule engine handles flow_object type in element selection."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_flow_object_selects_all_subtypes(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        _create_flow_object_rule_file(rules_dir)

        mock_nav = _create_mock_navigator(
            element_ids_by_type={"flow_object": ["act1", "evt1", "gw1"]},
            outgoing_flows={
                "act1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],
                "evt1": [OutgoingSequenceFlowInfo("f2", "t2", None, None)],
                "gw1": [OutgoingSequenceFlowInfo("f3", "t3", None, None)],
            },
        )
        mock_nav_cls.return_value = mock_nav

        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        result = create_navigator(
            schema_file="schema.md",
            data_file="data.md",
            hierarchy_file="hierarchy.md",
            rules_dir=str(rules_dir),
            validation_level="basic",
        )
        assert result is mock_nav
        # Verify flow_object type was requested from navigator
        mock_nav.get_element_ids_by_type.assert_called_with("flow_object")
        # Verify all three elements were evaluated (no duplicates)
        assert mock_nav.get_outgoing_sequence_flows.call_count == 3
