"""E2E and acceptance tests for BPMN rule validation (TC-079 to TC-087)."""

import pytest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch, Mock, MagicMock

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    OutgoingSequenceFlowInfo,
)
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.rule_engine import BPMNRuleEngine
from basic_framework.container_utils.container_in_memory import ContainerInMemory


# ===================================================================
# Helpers
# ===================================================================


def _create_mock_navigator(
    element_ids_by_type: Dict[str, List[str]],
    outgoing_flows: Dict[str, List[OutgoingSequenceFlowInfo]],
) -> BPMNHierarchyNavigator:
    """Create a mock navigator with specified element and flow data."""
    nav = Mock(spec=BPMNHierarchyNavigator)
    nav.get_element_ids_by_type.side_effect = lambda t: element_ids_by_type.get(t, [])
    nav.get_element_attribute.side_effect = lambda eid, attr: None
    nav.get_outgoing_sequence_flows.side_effect = lambda eid: outgoing_flows.get(eid, [])
    nav.get_incoming_sequence_flows.side_effect = lambda eid: []
    return nav


def _make_rule_store(rules: List[Dict[str, Any]]) -> ContainerInMemory:
    """Build a ContainerInMemory with rule data."""
    columns = ["rule_id", "element_type", "subtype", "assertion", "where_clause", "level", "message_template"]
    store = ContainerInMemory()
    store.init_new(columns, "test_rules", "test_rules")
    for rule in rules:
        idx = store.add_empty_row()
        for col in columns:
            store.set_value(idx, col, rule.get(col, ""))
    return store


def _make_rule_store_with_personal(rules: List[Dict[str, Any]]) -> ContainerInMemory:
    """Build a ContainerInMemory with rule data including personal column."""
    columns = ["rule_id", "element_type", "subtype", "assertion", "where_clause", "level", "message_template", "personal"]
    store = ContainerInMemory()
    store.init_new(columns, "rule_store", "rule_store")
    for rule in rules:
        idx = store.add_empty_row()
        for col in columns:
            store.set_value(idx, col, rule.get(col, ""))
    return store


def _setup_level_test(
    mock_nav_cls: Mock,
    mock_builder_cls: Mock,
    mock_parser_cls: Mock,
    tmp_path: Path,
) -> tuple:
    """Common setup for validation level tests (TC-081 to TC-083).

    Creates three rules at different levels (basic, spec_v2, best_practice)
    and a mock navigator where all three rules fail.
    """
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    report_dir = tmp_path / "reports"

    (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| B001 | start_event | | COUNT(outgoing_flows) >= 1 | | basic | B001: {element_id} basic violation |
| S001 | start_event | | COUNT(outgoing_flows) >= 2 | | spec_v2 | S001: {element_id} spec_v2 violation |
| P001 | start_event | | COUNT(outgoing_flows) >= 3 | | best_practice | P001: {element_id} best_practice violation |
""", encoding="utf-8")

    mock_nav = _create_mock_navigator(
        element_ids_by_type={"start_event": ["se1"]},
        outgoing_flows={"se1": []},
    )
    mock_nav_cls.return_value = mock_nav
    mock_builder_cls.return_value = MagicMock()
    mock_parser_cls.return_value = MagicMock()

    return rules_dir, report_dir


# ===================================================================
# TC-079: E2E - New rule in MD takes effect without code change
# ===================================================================


class TestNewRuleTakesEffect:
    """TC-079: Adding new rule to Markdown file takes effect without code change."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_new_rule_enforced_without_code_change(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()

        # Phase 1: Rule only for end_event (no end_events in mock — no violations)
        (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| END-001 | end_event | | COUNT(incoming_flows) >= 1 | | basic | End {element_id} no incoming |
""", encoding="utf-8")

        # Mock navigator: start_event with 0 outgoing flows, no end_events
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Phase 1: No error — rule applies to end_event, none exist
        result = create_navigator(
            schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
            rules_dir=str(rules_dir), validation_level="basic",
        )
        assert result is mock_nav

        # Phase 2: Add new rule for start_event (will fail — se1 has 0 outgoing)
        (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| END-001 | end_event | | COUNT(incoming_flows) >= 1 | | basic | End {element_id} no incoming |
| SRT-001 | start_event | | COUNT(outgoing_flows) >= 1 | | basic | Start {element_id} no outgoing |
""", encoding="utf-8")

        # No Python code changed — re-call with same parameters
        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="basic",
            )


# ===================================================================
# TC-080: E2E - All 10 rule table columns present
# ===================================================================


class TestAllColumnsPresent:
    """TC-080: All 10 rule table columns present and accessible."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_ten_columns_load_and_message_template_used(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        report_dir = tmp_path / "reports"

        # Rule file with all 10 columns (7 required + 3 optional)
        (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template | description | spec_reference | spec_verified | personal |
|---------|-------------|---------|-----------|-------------|-------|-----------------|-------------|---------------|--------------|----------|
| SRT-001 | start_event | | COUNT(outgoing_flows) >= 1 | | basic | Start event {element_id} missing outgoing | Must have outgoing | BPMN 2.0 s10 | yes | |
""", encoding="utf-8")

        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="basic",
                report_target=str(report_dir),
            )

        # Verify report uses message_template for error formatting
        report_files = list(report_dir.glob("*.txt"))
        assert len(report_files) == 1
        report_content = report_files[0].read_text(encoding="utf-8")
        assert "Start event se1 missing outgoing" in report_content


# ===================================================================
# TC-081: Acceptance - basic level runs only basic-level rules
# ===================================================================


class TestBasicLevelFiltering:
    """TC-081: basic level runs only basic-level rules."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_basic_level_one_error(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir, report_dir = _setup_level_test(
            mock_nav_cls, mock_builder_cls, mock_parser_cls, tmp_path,
        )

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="basic",
                report_target=str(report_dir),
            )

        report_files = list(report_dir.glob("*.txt"))
        report_content = report_files[0].read_text(encoding="utf-8")
        assert "B001:" in report_content
        assert "S001:" not in report_content
        assert "P001:" not in report_content


# ===================================================================
# TC-082: Acceptance - spec_v2 level runs basic + spec_v2 rules
# ===================================================================


class TestSpecV2LevelFiltering:
    """TC-082: spec_v2 level runs basic + spec_v2 rules."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_spec_v2_level_two_errors(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir, report_dir = _setup_level_test(
            mock_nav_cls, mock_builder_cls, mock_parser_cls, tmp_path,
        )

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="spec_v2",
                report_target=str(report_dir),
            )

        report_files = list(report_dir.glob("*.txt"))
        report_content = report_files[0].read_text(encoding="utf-8")
        assert "B001:" in report_content
        assert "S001:" in report_content
        assert "P001:" not in report_content


# ===================================================================
# TC-083: Acceptance - best_practice level runs all rules
# ===================================================================


class TestBestPracticeLevelFiltering:
    """TC-083: best_practice level runs all rules."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_best_practice_level_three_errors(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path: Path,
    ):
        rules_dir, report_dir = _setup_level_test(
            mock_nav_cls, mock_builder_cls, mock_parser_cls, tmp_path,
        )

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="best_practice",
                report_target=str(report_dir),
            )

        report_files = list(report_dir.glob("*.txt"))
        report_content = report_files[0].read_text(encoding="utf-8")
        assert "B001:" in report_content
        assert "S001:" in report_content
        assert "P001:" in report_content


# ===================================================================
# TC-084: Acceptance - personal mode includes rule with personal="include"
# ===================================================================


class TestPersonalModeInclude:
    """TC-084: personal mode includes rule with personal='yes' (code value: 'include')."""

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_personal_include_rule_executed(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        store = _make_rule_store_with_personal([{
            "rule_id": "BP-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "best_practice",
            "message_template": "BP-001: {element_id} missing outgoing",
            "personal": "include",
        }])
        mock_build_rule_store.return_value = store

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Rule with personal="include" is executed — violation reported
        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir="dummy", validation_level="personal",
            )


# ===================================================================
# TC-085: Acceptance - personal mode suppresses rule with personal="skip"
# ===================================================================


class TestPersonalModeSkip:
    """TC-085: personal mode suppresses rule with personal='no' (code value: 'skip')."""

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_personal_skip_rule_suppressed(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        store = _make_rule_store_with_personal([{
            "rule_id": "BP-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "best_practice",
            "message_template": "BP-001: {element_id} missing outgoing",
            "personal": "skip",
        }])
        mock_build_rule_store.return_value = store

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Rule with personal="skip" is suppressed — no error
        result = create_navigator(
            schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
            rules_dir="dummy", validation_level="personal",
        )
        assert result is mock_nav


# ===================================================================
# TC-086: Acceptance - personal mode with empty personal falls back
# ===================================================================


class TestPersonalModeEmptyFallback:
    """TC-086: personal mode with empty personal column falls back to best_practice."""

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_personal_empty_falls_back_to_best_practice(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"start_event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        store = _make_rule_store_with_personal([{
            "rule_id": "BP-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "best_practice",
            "message_template": "BP-001: {element_id} missing outgoing",
            "personal": "",
        }])
        mock_build_rule_store.return_value = store

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Empty personal falls back to best_practice — rule included, violation reported
        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir="dummy", validation_level="personal",
            )


# ===================================================================
# TC-087: Acceptance - all rule violations reported as errors
# ===================================================================


class TestAllViolationsAsErrors:
    """TC-087: All rule violations reported as errors, not warnings."""

    def test_violations_are_errors_not_warnings(self) -> None:
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {"start_event": ["se1"]}.get(t, [])
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: []
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []
        nav.get_element_attribute.side_effect = lambda eid, attr: None

        val_result = Mock(spec=ValidationResult)
        engine = BPMNRuleEngine(nav, val_result)

        store = _make_rule_store([
            {"rule_id": "B001", "element_type": "start_event",
             "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
             "level": "basic", "message_template": "B001: {element_id} fail"},
            {"rule_id": "S001", "element_type": "start_event",
             "assertion": "COUNT(outgoing_flows) >= 2", "where_clause": "",
             "level": "spec_v2", "message_template": "S001: {element_id} fail"},
            {"rule_id": "P001", "element_type": "start_event",
             "assertion": "COUNT(outgoing_flows) >= 3", "where_clause": "",
             "level": "best_practice", "message_template": "P001: {element_id} fail"},
        ])
        engine.validate(store, "best_practice")

        # All violations use add_error (not add_warning)
        assert val_result.add_error.call_count == 3
        val_result.add_warning.assert_not_called()


# ===================================================================
# TC-090: Integration - AND-001 Rule with subtype-based validation
# ===================================================================


class TestAnd001RuleWithSubtype:
    """TC-090: AND-001 rule (Parallel Gateway) is correctly evaluated with subtype filtering."""

    def test_and_001_violation_detected(self) -> None:
        """Test: AND-001 Regel erkennt Parallel Gateway mit nur 1 ausgehenden Flow."""
        # Setup: Parallel Gateway mit nur 1 ausgehenden Flow
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {"gateway": ["pg1"]}.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "pg1": {"gateway_type": "parallel", "gateway_direction": "diverging"}
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "pg1": [OutgoingSequenceFlowInfo("flow1", "t1", None, None)]  # Nur 1 Flow
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # AND-001 Rule: Parallel Gateway benötigt mindestens 2 ausgehende Flows
        store = _make_rule_store([{
            "rule_id": "AND-001",
            "element_type": "gateway",
            "subtype": "parallel",  # Selektion nach subtype
            "assertion": "COUNT(outgoing_flows) >= 2",
            "where_clause": "",
            "level": "basic",
            "message_template": "AND-001: Parallel gateway {element_id} needs at least 2 outgoing flows",
        }])

        engine.validate(store, "basic")

        # Erwartet: Violation gefunden
        assert val_result.has_errors()
        assert val_result.count() == 1
        messages = val_result.get_messages()
        assert "AND-001" in messages[0]
        assert "pg1" in messages[0]

    def test_and_001_passes_with_two_outgoing_flows(self) -> None:
        """Test: AND-001 Regel erfolgt mit 2 ausgehenden Flows."""
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {"gateway": ["pg1"]}.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "pg1": {"gateway_type": "parallel"}
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "pg1": [
                OutgoingSequenceFlowInfo("flow1", "t1", None, None),
                OutgoingSequenceFlowInfo("flow2", "t2", None, None),  # 2 Flows
            ]
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        store = _make_rule_store([{
            "rule_id": "AND-001",
            "element_type": "gateway",
            "subtype": "parallel",
            "assertion": "COUNT(outgoing_flows) >= 2",
            "where_clause": "",
            "level": "basic",
            "message_template": "AND-001: {element_id} needs at least 2 outgoing",
        }])

        engine.validate(store, "basic")

        # Erwartet: Keine Violations
        assert not val_result.has_errors()

    def test_and_001_ignores_exclusive_gateways(self) -> None:
        """Test: AND-001 Regel ignoriert Exclusive Gateways (subtype = "exclusive")."""
        # Setup: Exclusive Gateway mit nur 1 ausgehenden Flow (sollte nicht von AND-001 geprüft werden)
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {"gateway": ["eg1"]}.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "eg1": {"gateway_type": "exclusive"}  # NOT parallel
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "eg1": [OutgoingSequenceFlowInfo("flow1", "t1", None, None)]  # Nur 1 Flow
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # AND-001 Regel mit subtype = "parallel"
        store = _make_rule_store([{
            "rule_id": "AND-001",
            "element_type": "gateway",
            "subtype": "parallel",
            "assertion": "COUNT(outgoing_flows) >= 2",
            "where_clause": "",
            "level": "basic",
            "message_template": "AND-001: {element_id} needs at least 2 outgoing",
        }])

        engine.validate(store, "basic")

        # Erwartet: KEINE Violation, weil Exclusive Gateway nicht dem subtype = "parallel" entspricht
        assert not val_result.has_errors()


# ===================================================================
# TC-091: Integration - All Rule Types (SRT, END, XOR, FLO)
# ===================================================================


class TestAllRuleTypesWithSubtype:
    """TC-091: All rule types (SRT, END, XOR, FLO) correctly evaluate with subtype filtering."""

    def test_srt_001_start_event_rule(self) -> None:
        """Test: SRT-001 Regel für Start Events (subtype = "start")."""
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {
            "event": ["se1", "ee1"]  # Start Event und End Event
        }.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "se1": {"event_type": "start"},
            "ee1": {"event_type": "end"},
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "se1": [],  # Start event ohne outgoing flow → Violation
            "ee1": [],
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # SRT-001: Start Event benötigt mindestens 1 ausgehenden Flow
        store = _make_rule_store([{
            "rule_id": "SRT-001",
            "element_type": "event",
            "subtype": "start",
            "assertion": "COUNT(outgoing_flows) >= 1",
            "where_clause": "",
            "level": "basic",
            "message_template": "SRT-001: Start event {element_id} needs outgoing flow",
        }])

        engine.validate(store, "basic")

        # Erwartet: 1 Violation (nur für se1, nicht für ee1)
        assert val_result.count() == 1
        assert "se1" in val_result.get_messages()[0]

    def test_end_001_end_event_rule(self) -> None:
        """Test: END-001 Regel für End Events (subtype = "end")."""
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {
            "event": ["se1", "ee1"]
        }.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "se1": {"event_type": "start"},
            "ee1": {"event_type": "end"},
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: []
        nav.get_incoming_sequence_flows.side_effect = lambda eid: {
            "se1": [],
            "ee1": [],  # End event ohne incoming flow → Violation
        }.get(eid, [])

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # END-001: End Event benötigt mindestens 1 eingehenden Flow
        store = _make_rule_store([{
            "rule_id": "END-001",
            "element_type": "event",
            "subtype": "end",
            "assertion": "COUNT(incoming_flows) >= 1",
            "where_clause": "",
            "level": "basic",
            "message_template": "END-001: End event {element_id} needs incoming flow",
        }])

        engine.validate(store, "basic")

        # Erwartet: 1 Violation (nur für ee1, nicht für se1)
        assert val_result.count() == 1
        assert "ee1" in val_result.get_messages()[0]

    def test_xor_001_exclusive_gateway_rule(self) -> None:
        """Test: XOR-001 Regel für Exclusive Gateways (subtype = "exclusive")."""
        nav = Mock(spec=BPMNHierarchyNavigator)
        nav.get_element_ids_by_type.side_effect = lambda t: {
            "gateway": ["xor1", "and1"]
        }.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: {
            "xor1": {"gateway_type": "exclusive"},
            "and1": {"gateway_type": "parallel"},
        }.get(eid, {}).get(attr)
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "xor1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],  # Nur 1 Flow → Violation
            "and1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # XOR-001: Exclusive Gateway benötigt mindestens 2 ausgehende Flows
        store = _make_rule_store([{
            "rule_id": "XOR-001",
            "element_type": "gateway",
            "subtype": "exclusive",
            "assertion": "COUNT(outgoing_flows) >= 2",
            "where_clause": "",
            "level": "spec_v2",
            "message_template": "XOR-001: Exclusive gateway {element_id} needs at least 2 outgoing",
        }])

        engine.validate(store, "spec_v2")

        # Erwartet: 1 Violation (nur für xor1, nicht für and1)
        assert val_result.count() == 1
        assert "xor1" in val_result.get_messages()[0]

    def test_flo_001_flow_object_no_subtype(self) -> None:
        """Test: FLO-001 Regel für Flow Objects (kein subtype)."""
        nav = Mock(spec=BPMNHierarchyNavigator)
        # flow_object ist ein übergreifendes Konzept (activity + event + gateway)
        nav.get_element_ids_by_type.side_effect = lambda t: {
            "activity": ["a1", "a2"],
            "event": ["e1"],
            "gateway": ["g1"],
        }.get(t, [])
        nav.get_element_attribute.side_effect = lambda eid, attr: None
        nav.get_outgoing_sequence_flows.side_effect = lambda eid: {
            "a1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],
            "a2": [],  # Activity ohne outgoing → Violation
            "e1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],
            "g1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],
        }.get(eid, [])
        nav.get_incoming_sequence_flows.side_effect = lambda eid: []

        val_result = ValidationResult()
        engine = BPMNRuleEngine(nav, val_result)

        # FLO-001: Flow Object benötigt mindestens 1 ausgehenden Flow (ohne subtype)
        store = _make_rule_store([{
            "rule_id": "FLO-001",
            "element_type": "activity",  # activity ist ein Flow Object
            "subtype": "",  # Kein subtype
            "assertion": "COUNT(outgoing_flows) >= 1",
            "where_clause": "",
            "level": "basic",
            "message_template": "FLO-001: Activity {element_id} needs outgoing flow",
        }])

        engine.validate(store, "basic")

        # Erwartet: 1 Violation (nur für a2)
        assert val_result.count() == 1
        assert "a2" in val_result.get_messages()[0]


# ===================================================================
# TC-092: Factory Integration - Navigator & RuleStore together
# ===================================================================


class TestFactoryIntegration:
    """TC-092: Factory integration — Navigator and RuleStore work together."""

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_factory_passes_navigator_to_rule_store(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        """Test: Factory übergibt Navigator an build_rule_store()."""
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"event": ["e1"]},
            outgoing_flows={"e1": [OutgoingSequenceFlowInfo("f1", "t1", None, None)]},
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        rule_store = _make_rule_store([{
            "rule_id": "SRT-001",
            "element_type": "event",
            "subtype": "start",
            "assertion": "COUNT(outgoing_flows) >= 1",
            "where_clause": "",
            "level": "basic",
            "message_template": "msg",
        }])
        mock_build_rule_store.return_value = rule_store

        from bpmn_lib.navigator.navigator_factory import create_navigator

        nav = create_navigator(
            schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
            rules_dir="rules", validation_level="basic",
        )

        # Verify build_rule_store was called with navigator
        mock_build_rule_store.assert_called_once()
        call_args = mock_build_rule_store.call_args
        assert call_args[0][1] == mock_nav  # Second argument is navigator
        assert nav == mock_nav

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_factory_skips_validation_when_no_rules(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        """Test: Factory überspringt Rule-Validierung wenn rules_dir=None."""
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"event": ["e1"]},
            outgoing_flows={"e1": []},  # Würde gegen Regel verstoßen
        )
        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Ohne rules_dir/validation_level sollte keine Regelvalidierung stattfinden
        nav = create_navigator(
            schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
        )

        # build_rule_store sollte nie aufgerufen werden
        mock_build_rule_store.assert_not_called()
        assert nav == mock_nav

    @patch("bpmn_lib.navigator.navigator_factory.build_rule_store")
    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_factory_rule_validation_with_subtype_filtering(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, mock_build_rule_store,
    ):
        """Test: Factory führt Regelvalidierung mit subtype-Filterung durch."""
        # Setup: 2 Events, eine Start Event, eine End Event
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"event": ["e1", "e2"]},
            outgoing_flows={"e1": [], "e2": [OutgoingSequenceFlowInfo("f1", "t1", None, None)]},
        )
        # Mock get_element_attribute für subtype-Filterung
        original_get_attr = mock_nav.get_element_attribute
        def mock_get_attr(eid, attr):
            if attr == "event_type":
                return "start" if eid == "e1" else "end"
            return original_get_attr(eid, attr)
        mock_nav.get_element_attribute = mock_get_attr

        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        store = _make_rule_store([{
            "rule_id": "SRT-001",
            "element_type": "event",
            "subtype": "start",
            "assertion": "COUNT(outgoing_flows) >= 1",
            "where_clause": "",
            "level": "basic",
            "message_template": "Start event {element_id} needs outgoing flow",
        }])
        mock_build_rule_store.return_value = store

        from bpmn_lib.navigator.navigator_factory import create_navigator

        # Sollte Exception werfen wegen e1 (Start Event ohne outgoing flow)
        with pytest.raises(Exception, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir="rules", validation_level="basic",
            )
