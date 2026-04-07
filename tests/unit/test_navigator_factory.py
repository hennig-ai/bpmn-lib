"""Unit tests for navigator_factory.py — validation parameter extensions."""

import pytest
from unittest.mock import patch, MagicMock

from bpmn_lib.validation.exceptions import BPMNValidationError


class TestBPMNValidationError:
    """Tests for BPMNValidationError exception class."""

    def test_is_subclass_of_exception(self) -> None:
        assert issubclass(BPMNValidationError, Exception)

    def test_carries_error_message(self) -> None:
        err = BPMNValidationError("some error text")
        assert str(err) == "some error text"

    def test_catchable_as_exception(self) -> None:
        with pytest.raises(Exception, match="test message"):
            raise BPMNValidationError("test message")

    def test_catchable_as_bpmn_validation_error(self) -> None:
        with pytest.raises(BPMNValidationError, match="test message"):
            raise BPMNValidationError("test message")

    def test_importable_from_validation_package(self) -> None:
        from bpmn_lib.validation import BPMNValidationError as Imported
        assert Imported is BPMNValidationError


class TestCreateNavigatorRaisesBPMNValidationError:
    """Tests that create_navigator raises BPMNValidationError on BPMN rule validation failures."""

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_bpmn_rule_failure_raises_bpmn_validation_error(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path,
    ) -> None:
        """BPMN rule validation failure raises BPMNValidationError, not generic Exception."""
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | SRT-001: {element_id} fail |
""", encoding="utf-8")

        from tests.integration.test_e2e_rule_validation import _create_mock_navigator
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        # Mock get_element_attribute to return "start" for event_type
        mock_nav.get_element_attribute.side_effect = lambda eid, attr: "start" if attr == "event_type" else None

        # Mock schema validation chain
        mock_schema = MagicMock()
        mock_schema.has_table.return_value = True
        mock_table_def = MagicMock()
        mock_table_def.has_column.return_value = True
        mock_table_def.is_value_in_domain.return_value = True
        mock_schema.get_table_definition.return_value = mock_table_def
        mock_nav.get_schema.return_value = mock_schema

        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(BPMNValidationError, match="BPMN-Regelvalidierung fehlgeschlagen"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="basic",
            )

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_bpmn_rule_failure_with_report_raises_bpmn_validation_error(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file, tmp_path,
    ) -> None:
        """BPMNValidationError is raised even when report_target is set."""
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        report_dir = tmp_path / "reports"
        (rules_dir / "rules.md").write_text("""# Rules

| rule_id | element_type | subtype | assertion | where_clause | level | message_template |
|---------|-------------|---------|-----------|-------------|-------|-----------------|
| SRT-001 | event | start | COUNT(outgoing_flows) >= 1 | | basic | SRT-001: {element_id} fail |
""", encoding="utf-8")

        from tests.integration.test_e2e_rule_validation import _create_mock_navigator
        mock_nav = _create_mock_navigator(
            element_ids_by_type={"event": ["se1"]},
            outgoing_flows={"se1": []},
        )
        # Mock get_element_attribute to return "start" for event_type
        mock_nav.get_element_attribute.side_effect = lambda eid, attr: "start" if attr == "event_type" else None

        # Mock schema validation chain
        mock_schema = MagicMock()
        mock_schema.has_table.return_value = True
        mock_table_def = MagicMock()
        mock_table_def.has_column.return_value = True
        mock_table_def.is_value_in_domain.return_value = True
        mock_schema.get_table_definition.return_value = mock_table_def
        mock_nav.get_schema.return_value = mock_schema

        mock_nav_cls.return_value = mock_nav
        mock_builder_cls.return_value = MagicMock()
        mock_parser_cls.return_value = MagicMock()

        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises(BPMNValidationError, match="Details siehe"):
            create_navigator(
                schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                rules_dir=str(rules_dir), validation_level="basic",
                report_target=str(report_dir),
            )

    @patch("bpmn_lib.navigator.navigator_factory._validate_file_exists")
    @patch("bpmn_lib.navigator.navigator_factory.MarkdownDocument")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseSchemaParser")
    @patch("bpmn_lib.navigator.navigator_factory.DatabaseBuilder")
    @patch("bpmn_lib.navigator.navigator_factory.BPMNHierarchyNavigator")
    def test_constraint_failure_does_not_raise_bpmn_validation_error(
        self, mock_nav_cls, mock_builder_cls, mock_parser_cls, mock_md_cls,
        mock_validate_file,
    ) -> None:
        """Constraint validation failures raise generic Exception, not BPMNValidationError."""
        from bpmn_lib.utils.validation_result import ValidationResult

        mock_val_result = MagicMock(spec=ValidationResult)
        mock_val_result.has_errors.return_value = True
        mock_val_result.count.return_value = 1
        mock_val_result.write_report.return_value = None

        with patch("bpmn_lib.navigator.navigator_factory.ValidationResult", return_value=mock_val_result):
            mock_builder_cls.return_value = MagicMock()
            mock_parser_cls.return_value = MagicMock()

            from bpmn_lib.navigator.navigator_factory import create_navigator

            with pytest.raises(Exception, match="Constraint-Validierung fehlgeschlagen") as exc_info:
                create_navigator(
                    schema_file="s.md", data_file="d.md", hierarchy_file="h.md",
                )

            assert not isinstance(exc_info.value, BPMNValidationError)


class TestCreateNavigatorValidationParams:
    """Test the new rules_dir and validation_level parameters."""

    def test_inconsistent_params_rules_dir_only_raises(self):
        """rules_dir without validation_level raises ValueError."""
        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises((ValueError, Exception), match="rules_dir and validation_level"):
            create_navigator(
                schema_file="dummy.md",
                data_file="dummy.md",
                hierarchy_file="dummy.md",
                rules_dir="/some/path",
                validation_level=None,
            )

    def test_inconsistent_params_validation_level_only_raises(self):
        """validation_level without rules_dir raises ValueError."""
        from bpmn_lib.navigator.navigator_factory import create_navigator

        with pytest.raises((ValueError, Exception), match="rules_dir and validation_level"):
            create_navigator(
                schema_file="dummy.md",
                data_file="dummy.md",
                hierarchy_file="dummy.md",
                rules_dir=None,
                validation_level="basic",
            )


class TestValidationInitExports:
    """Test that validation/__init__.py exports the public API."""

    def test_import_build_rule_store(self):
        from bpmn_lib.validation import build_rule_store
        assert callable(build_rule_store)

    def test_import_bpmn_rule_engine(self):
        from bpmn_lib.validation import BPMNRuleEngine
        assert BPMNRuleEngine is not None

    def test_expression_parser_not_exported(self):
        """ExpressionParser is internal and not part of public API."""
        import bpmn_lib.validation as val_mod
        assert not hasattr(val_mod, "ExpressionParser")


class TestNavigatorInitExports:
    """Verify navigator __init__.py exports still work after changes."""

    def test_import_create_navigator(self):
        from bpmn_lib.navigator import create_navigator
        assert callable(create_navigator)

    def test_import_outgoing_sequence_flow_info(self):
        from bpmn_lib.navigator import OutgoingSequenceFlowInfo
        info = OutgoingSequenceFlowInfo(
            sequence_flow_id="f1", target_element_id="t1",
            condition_expression=None, is_default=None,
        )
        assert info.sequence_flow_id == "f1"
