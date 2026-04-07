"""Unit tests for BPMNRuleEngine."""

import pytest
from unittest.mock import Mock, MagicMock
from typing import Any, Dict, List

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    OutgoingSequenceFlowInfo,
    IncomingSequenceFlowInfo,
)
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.rule_engine import BPMNRuleEngine, _LEVEL_ORDER
from bpmn_lib.validation.expression_ast import (
    Check,
    CheckTerm,
    CountAssertion,
    ExistsAssertion,
    ForEachAssertion,
    WhereEquals,
    WhereNotIn,
)
from basic_framework.container_utils.container_in_memory import ContainerInMemory


def _make_rule_store(rules: List[Dict[str, Any]]) -> ContainerInMemory:
    """Build a ContainerInMemory with rule data."""
    columns = ["rule_id", "element_type", "subtype", "assertion", "where_clause", "level", "message_template", "personal"]
    store = ContainerInMemory()
    store.init_new(columns, "test_rules", "test_rules")
    for rule in rules:
        idx = store.add_empty_row()
        for col in columns:
            store.set_value(idx, col, rule.get(col, ""))
    return store


def _make_engine(
    element_ids_by_type: Dict[str, List[str]] = None,
    element_attrs: Dict[str, Dict[str, Any]] = None,
    outgoing_flows: Dict[str, List[OutgoingSequenceFlowInfo]] = None,
    incoming_flows: Dict[str, List[IncomingSequenceFlowInfo]] = None,
) -> tuple:
    """Create a BPMNRuleEngine with mocked navigator."""
    nav = Mock(spec=BPMNHierarchyNavigator)
    val_result = ValidationResult()

    element_ids_by_type = element_ids_by_type if element_ids_by_type is not None else {}
    element_attrs = element_attrs if element_attrs is not None else {}
    outgoing_flows = outgoing_flows if outgoing_flows is not None else {}
    incoming_flows = incoming_flows if incoming_flows is not None else {}

    nav.get_element_ids_by_type.side_effect = lambda t: element_ids_by_type.get(t, [])
    nav.get_element_attribute.side_effect = lambda eid, attr: element_attrs.get(eid, {}).get(attr)
    nav.get_outgoing_sequence_flows.side_effect = lambda eid: outgoing_flows.get(eid, [])
    nav.get_incoming_sequence_flows.side_effect = lambda eid: incoming_flows.get(eid, [])

    engine = BPMNRuleEngine(nav, val_result)
    return engine, val_result


# ===================================================================
# Step 1 tests: Core structure
# ===================================================================


class TestFilterRulesByLevel:

    def test_basic_level_returns_basic_only(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "element_type": "event", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "level": "basic", "message_template": "msg"},
            {"rule_id": "R2", "element_type": "event", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "level": "spec_v2", "message_template": "msg"},
        ])
        result = engine._filter_rules_by_level(store, "basic")
        assert len(result) == 1
        assert result[0]["rule_id"] == "R1"

    def test_spec_v2_returns_basic_and_spec_v2(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "level": "basic", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m"},
            {"rule_id": "R2", "level": "spec_v2", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m"},
            {"rule_id": "R3", "level": "best_practice", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m"},
        ])
        result = engine._filter_rules_by_level(store, "spec_v2")
        assert len(result) == 2
        ids = {r["rule_id"] for r in result}
        assert ids == {"R1", "R2"}

    def test_best_practice_returns_all(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "level": "basic", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m"},
            {"rule_id": "R2", "level": "best_practice", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m"},
        ])
        result = engine._filter_rules_by_level(store, "best_practice")
        assert len(result) == 2

    def test_unknown_level_raises(self):
        engine, _ = _make_engine()
        store = _make_rule_store([])
        with pytest.raises(ValueError, match="Unknown validation level"):
            engine._filter_rules_by_level(store, "unknown")


class TestFilterRulesByLevelPersonal:
    """TC-048: _filter_rules_by_level handles personal mode (skip/include)."""

    def test_personal_skip_excludes_rule(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "level": "basic", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m", "personal": "skip"},
        ])
        result = engine._filter_rules_by_level(store, "personal")
        assert len(result) == 0

    def test_personal_include_adds_rule(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "level": "best_practice", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m", "personal": "include"},
        ])
        result = engine._filter_rules_by_level(store, "personal")
        assert len(result) == 1
        assert result[0]["rule_id"] == "R1"

    def test_personal_empty_falls_back_to_best_practice(self):
        engine, _ = _make_engine()
        store = _make_rule_store([
            {"rule_id": "R1", "level": "basic", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m", "personal": ""},
            {"rule_id": "R2", "level": "best_practice", "element_type": "e", "assertion": "COUNT(outgoing_flows) >= 1",
             "where_clause": "", "message_template": "m", "personal": ""},
        ])
        result = engine._filter_rules_by_level(store, "personal")
        assert len(result) == 2  # Both included (best_practice threshold includes all)


class TestCompare:
    """TC-055: _compare evaluates integer comparison operators correctly."""

    def test_greater_equal_true(self):
        engine, _ = _make_engine()
        assert engine._compare(2, ">=", 1) is True

    def test_greater_equal_false(self):
        engine, _ = _make_engine()
        assert engine._compare(1, ">=", 2) is False

    def test_less_equal_true(self):
        engine, _ = _make_engine()
        assert engine._compare(1, "<=", 2) is True

    def test_less_equal_false(self):
        engine, _ = _make_engine()
        assert engine._compare(2, "<=", 1) is False

    def test_greater_than(self):
        engine, _ = _make_engine()
        assert engine._compare(3, ">", 2) is True
        assert engine._compare(2, ">", 2) is False

    def test_less_than(self):
        engine, _ = _make_engine()
        assert engine._compare(1, "<", 2) is True
        assert engine._compare(2, "<", 2) is False

    def test_equals(self):
        engine, _ = _make_engine()
        assert engine._compare(5, "==", 5) is True
        assert engine._compare(5, "==", 6) is False

    def test_not_equals(self):
        engine, _ = _make_engine()
        assert engine._compare(5, "!=", 6) is True
        assert engine._compare(5, "!=", 5) is False


class TestCompareValues:
    """TC-056: _compare_values evaluates equality/inequality for arbitrary types."""

    def test_none_equals_none(self):
        engine, _ = _make_engine()
        assert engine._compare_values(None, "==", None) is True

    def test_string_not_equals_none(self):
        engine, _ = _make_engine()
        assert engine._compare_values("x", "!=", None) is True

    def test_bool_equals_bool(self):
        engine, _ = _make_engine()
        assert engine._compare_values(True, "==", True) is True
        assert engine._compare_values(True, "==", False) is False

    def test_none_not_equals_string(self):
        engine, _ = _make_engine()
        assert engine._compare_values(None, "!=", "x") is True

    def test_string_equals_string(self):
        engine, _ = _make_engine()
        assert engine._compare_values("abc", "==", "abc") is True
        assert engine._compare_values("abc", "==", "xyz") is False


class TestBestPracticeViolationAsError:
    """TC-088: best_practice rule violation reported as error not warning."""

    def test_best_practice_uses_add_error(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": []},
        )
        store = _make_rule_store([{
            "rule_id": "BP-001", "element_type": "gateway",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "best_practice", "message_template": "{element_id} needs outgoing",
        }])
        engine.validate(store, "best_practice")
        # Verify add_error was used (val_result.has_errors() returns True)
        assert val_result.has_errors()
        # Verify the message is in the error messages list (not warnings)
        assert "g1 needs outgoing" in val_result.get_messages()[0]


class TestApplyWhereClause:

    def test_none_returns_all(self):
        engine, _ = _make_engine()
        result = engine._apply_where_clause(["e1", "e2"], None)
        assert result == ["e1", "e2"]

    def test_where_equals_filters(self):
        engine, _ = _make_engine(
            element_attrs={
                "e1": {"element_type": "exclusiveGateway"},
                "e2": {"element_type": "parallelGateway"},
            }
        )
        wc = WhereEquals(attribute_name="element_type", value="exclusiveGateway")
        result = engine._apply_where_clause(["e1", "e2"], wc)
        assert result == ["e1"]

    def test_where_not_in_filters(self):
        engine, _ = _make_engine(
            element_attrs={
                "e1": {"element_type": "startEvent"},
                "e2": {"element_type": "task"},
                "e3": {"element_type": "endEvent"},
            }
        )
        wc = WhereNotIn(attribute_name="element_type", values=["startEvent", "endEvent"])
        result = engine._apply_where_clause(["e1", "e2", "e3"], wc)
        assert result == ["e2"]


class TestGetFlows:

    def test_outgoing_delegates(self):
        flows = [OutgoingSequenceFlowInfo("f1", "t1", None, None)]
        engine, _ = _make_engine(outgoing_flows={"e1": flows})
        result = engine._get_flows("e1", "outgoing_flows")
        assert len(result) == 1
        assert result[0].sequence_flow_id == "f1"

    def test_incoming_delegates(self):
        flows = [IncomingSequenceFlowInfo("f1", "s1", None, None)]
        engine, _ = _make_engine(incoming_flows={"e1": flows})
        result = engine._get_flows("e1", "incoming_flows")
        assert len(result) == 1

    def test_invalid_flow_raises(self):
        engine, _ = _make_engine()
        with pytest.raises(ValueError, match="Unknown flow direction"):
            engine._get_flows("e1", "invalid")


class TestFormatErrorMessage:

    def test_replaces_placeholders(self):
        engine, _ = _make_engine()
        rule = {"message_template": "Element {element_id} flow {flow_id} failed"}
        result = engine._format_error_message(rule, "E001", "F001")
        assert result == "Element E001 flow F001 failed"


# ===================================================================
# Step 2 tests: Assertion evaluation
# ===================================================================


class TestEvaluateCount:

    def test_count_fails_with_zero_outgoing(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"start_event": ["e1"]},
            outgoing_flows={"e1": []},
        )
        store = _make_rule_store([{
            "rule_id": "SRT-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "basic", "message_template": "{element_id} has no outgoing",
        }])
        engine.validate(store, "basic")
        assert val_result.has_errors()
        assert "e1 has no outgoing" in val_result.get_messages()[0]

    def test_count_passes_with_outgoing(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"start_event": ["e1"]},
            outgoing_flows={"e1": [
                OutgoingSequenceFlowInfo("f1", "t1", None, None),
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "SRT-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "basic", "message_template": "{element_id} no outgoing",
        }])
        engine.validate(store, "basic")
        assert not val_result.has_errors()

    def test_count_sum_incoming_outgoing(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            incoming_flows={"g1": [IncomingSequenceFlowInfo("f1", "s1", None, None)]},
            outgoing_flows={"g1": [OutgoingSequenceFlowInfo("f2", "t1", None, None)]},
        )
        store = _make_rule_store([{
            "rule_id": "GW-001", "element_type": "gateway",
            "assertion": "COUNT(incoming_flows) + COUNT(outgoing_flows) >= 2", "where_clause": "",
            "level": "basic", "message_template": "{element_id} fail",
        }])
        engine.validate(store, "basic")
        assert not val_result.has_errors()


class TestEvaluateForEach:

    def test_for_each_reports_per_failing_flow(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": [
                OutgoingSequenceFlowInfo("f1", "t1", None, None),       # condition_expression is None — fails
                OutgoingSequenceFlowInfo("f2", "t2", "x > 0", None),    # has condition — passes
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "XOR-002", "element_type": "gateway",
            "assertion": "FOR_EACH outgoing_flows: condition_expression != null",
            "where_clause": "", "level": "basic",
            "message_template": "{element_id} flow {flow_id} missing condition",
        }])
        engine.validate(store, "basic")
        assert val_result.count() == 1
        assert "f1" in val_result.get_messages()[0]


class TestEvaluateExists:

    def test_exists_passes_when_one_satisfies(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": [
                OutgoingSequenceFlowInfo("f1", "t1", None, False),
                OutgoingSequenceFlowInfo("f2", "t2", None, True),  # is_default=True
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "XOR-003", "element_type": "gateway",
            "assertion": "EXISTS outgoing_flows: is_default == true",
            "where_clause": "", "level": "basic",
            "message_template": "{element_id} no default flow",
        }])
        engine.validate(store, "basic")
        assert not val_result.has_errors()

    def test_exists_fails_when_none_satisfies(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": [
                OutgoingSequenceFlowInfo("f1", "t1", None, False),
                OutgoingSequenceFlowInfo("f2", "t2", None, False),
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "XOR-003", "element_type": "gateway",
            "assertion": "EXISTS outgoing_flows: is_default == true",
            "where_clause": "", "level": "basic",
            "message_template": "{element_id} no default",
        }])
        engine.validate(store, "basic")
        assert val_result.has_errors()
        # D.5: flow_id is empty string for EXISTS
        assert "g1 no default" in val_result.get_messages()[0]


class TestEvaluateCheck:

    def test_and_combinator_all_must_pass(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": [
                OutgoingSequenceFlowInfo("f1", "t1", "cond", False),  # condition != null: pass, is_default != true: pass
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "R1", "element_type": "gateway",
            "assertion": "FOR_EACH outgoing_flows: condition_expression != null AND is_default == false",
            "where_clause": "", "level": "basic",
            "message_template": "{element_id} {flow_id} fail",
        }])
        engine.validate(store, "basic")
        assert not val_result.has_errors()

    def test_or_combinator_at_least_one(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"gateway": ["g1"]},
            outgoing_flows={"g1": [
                OutgoingSequenceFlowInfo("f1", "t1", None, True),  # condition is None but is_default True
            ]},
        )
        store = _make_rule_store([{
            "rule_id": "R1", "element_type": "gateway",
            "assertion": "FOR_EACH outgoing_flows: condition_expression != null OR is_default == true",
            "where_clause": "", "level": "basic",
            "message_template": "{element_id} {flow_id} fail",
        }])
        engine.validate(store, "basic")
        assert not val_result.has_errors()


class TestSelectElements:
    """TC-089: _select_elements() filters elements by type and optional subtype."""

    def test_select_all_elements_without_subtype(self):
        """Test: Selektion ohne subtype → alle Elemente vom Typ."""
        engine, _ = _make_engine(
            element_ids_by_type={"event": ["e1", "e2", "e3"]},
        )
        result = engine._select_elements("event", "")
        assert sorted(result) == ["e1", "e2", "e3"]

    def test_select_with_subtype_filters_by_type_attribute(self):
        """Test: Selektion mit subtype → Filter nach {element_type}_type == subtype."""
        engine, _ = _make_engine(
            element_ids_by_type={"event": ["e1", "e2", "e3"]},
            element_attrs={
                "e1": {"event_type": "start"},
                "e2": {"event_type": "end"},
                "e3": {"event_type": "start"},
            }
        )
        result = engine._select_elements("event", "start")
        assert sorted(result) == ["e1", "e3"]

    def test_select_gateway_with_parallel_subtype(self):
        """Test: Selektion mit subtype = "parallel" → nur Parallel Gateways."""
        engine, _ = _make_engine(
            element_ids_by_type={"gateway": ["g1", "g2", "g3"]},
            element_attrs={
                "g1": {"gateway_type": "parallel"},
                "g2": {"gateway_type": "exclusive"},
                "g3": {"gateway_type": "parallel"},
            }
        )
        result = engine._select_elements("gateway", "parallel")
        assert sorted(result) == ["g1", "g3"]

    def test_select_gateway_with_exclusive_subtype(self):
        """Test: Selektion mit subtype = "exclusive" → nur Exclusive Gateways."""
        engine, _ = _make_engine(
            element_ids_by_type={"gateway": ["g1", "g2", "g3"]},
            element_attrs={
                "g1": {"gateway_type": "parallel"},
                "g2": {"gateway_type": "exclusive"},
                "g3": {"gateway_type": "inclusive"},
            }
        )
        result = engine._select_elements("gateway", "exclusive")
        assert result == ["g2"]

    def test_flow_object_ignores_subtype(self):
        """Test: Sonderfall flow_object ignoriert subtype-Filter."""
        engine, _ = _make_engine(
            element_ids_by_type={"flow_object": ["f1", "f2", "f3"]},
        )
        # Auch mit subtype sollten alle flow_object Elemente zurückgegeben werden
        result = engine._select_elements("flow_object", "any_value")
        assert sorted(result) == ["f1", "f2", "f3"]

    def test_empty_result_when_no_elements_match_subtype(self):
        """Test: Wenn kein Element dem subtype entspricht → leere Liste."""
        engine, _ = _make_engine(
            element_ids_by_type={"event": ["e1", "e2"]},
            element_attrs={
                "e1": {"event_type": "end"},
                "e2": {"event_type": "end"},
            }
        )
        result = engine._select_elements("event", "start")
        assert result == []

    def test_missing_type_attribute_handled_gracefully(self):
        """Test: Wenn element kein {element_type}_type Attribut hat → nicht zurückgegeben."""
        engine, _ = _make_engine(
            element_ids_by_type={"event": ["e1", "e2"]},
            element_attrs={
                "e1": {"event_type": "start"},
                # e2 hat kein event_type Attribut
            }
        )
        result = engine._select_elements("event", "start")
        assert result == ["e1"]


class TestValidateEndToEnd:

    def test_full_validation_with_violations(self):
        engine, val_result = _make_engine(
            element_ids_by_type={"start_event": ["se1", "se2"]},
            outgoing_flows={
                "se1": [],  # no outgoing — violation
                "se2": [OutgoingSequenceFlowInfo("f1", "t1", None, None)],  # OK
            },
        )
        store = _make_rule_store([{
            "rule_id": "SRT-001", "element_type": "start_event",
            "assertion": "COUNT(outgoing_flows) >= 1", "where_clause": "",
            "level": "basic", "message_template": "{element_id} has no outgoing flow",
        }])
        engine.validate(store, "basic")
        assert val_result.count() == 1
        assert "se1" in val_result.get_messages()[0]
