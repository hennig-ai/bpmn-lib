"""BPMN Rule Engine — evaluates validation rules against BPMN process data."""

from typing import Any, Dict, List

from basic_framework.proc_frame import log_and_raise, log_msg
from basic_framework.container_utils.abstract_container import AbstractContainer

from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.expression_ast import (
    Assertion,
    Check,
    CombinedAssertion,
    CountAssertion,
    ExistsAssertion,
    ForEachAssertion,
    WhereClause,
    WhereEquals,
    WhereNotIn,
)
from bpmn_lib.validation.expression_parser import ExpressionParser

_LEVEL_ORDER: Dict[str, int] = {"basic": 1, "spec_v2": 2, "best_practice": 3}


class BPMNRuleEngine:
    """Evaluates BPMN validation rules against process data via the navigator."""

    def __init__(self, navigator: BPMNHierarchyNavigator, val_result: ValidationResult) -> None:
        self._navigator: BPMNHierarchyNavigator = navigator
        self._val_result: ValidationResult = val_result
        self._parser: ExpressionParser = ExpressionParser()

    def validate(self, rule_store: AbstractContainer, validation_level: str) -> None:
        """Run all applicable rules against the loaded BPMN model."""
        filtered_rules = self._filter_rules_by_level(rule_store, validation_level)
        for rule in filtered_rules:
            self._evaluate_rule(rule)

    # ------------------------------------------------------------------
    # Level filtering
    # ------------------------------------------------------------------

    def _filter_rules_by_level(self, rule_store: AbstractContainer, level: str) -> List[Dict[str, Any]]:
        """Return rules that apply at the requested validation level."""
        all_rules = self._collect_rules(rule_store)

        if level in _LEVEL_ORDER:
            threshold = _LEVEL_ORDER[level]
            return [r for r in all_rules if _LEVEL_ORDER[r["level"]] <= threshold]

        if level == "personal":
            threshold = _LEVEL_ORDER["best_practice"]
            result: List[Dict[str, Any]] = []
            for r in all_rules:
                rule_level_order = _LEVEL_ORDER[r["level"]]
                personal_flag = r.get("personal")
                if personal_flag == "skip":
                    continue
                if rule_level_order <= threshold or personal_flag == "include":
                    result.append(r)
            return result

        allowed = list(_LEVEL_ORDER.keys()) + ["personal"]
        log_and_raise(ValueError(f"Unknown validation level: '{level}'. Allowed: {allowed}"))

    def _collect_rules(self, rule_store: AbstractContainer) -> List[Dict[str, Any]]:
        """Collect all rules from the container into a list of dicts."""
        rules: List[Dict[str, Any]] = []
        iterator = rule_store.create_iterator()
        fields = rule_store.get_list_of_fields_as_ref()
        while not iterator.is_empty():
            rule: Dict[str, Any] = {}
            for field in fields:
                rule[field] = iterator.value(field)
            rules.append(rule)
            iterator.pp()
        return rules

    # ------------------------------------------------------------------
    # Element selection & where-clause filtering
    # ------------------------------------------------------------------

    def _select_elements(self, element_type: str, subtype: str = "") -> List[str]:
        """Get element IDs matching type and optional subtype.

        If subtype is provided, filters further by {element_type}_type == subtype.
        Special case: flow_object ignores subtype (it's not a real table).
        """
        element_ids = self._navigator.get_element_ids_by_type(element_type)

        if subtype and element_type != "flow_object":
            column_name = f"{element_type}_type"
            element_ids = [
                eid for eid in element_ids
                if self._navigator.get_element_attribute(eid, column_name) == subtype
            ]

        return element_ids

    def _apply_where_clause(self, element_ids: List[str], where_clause: WhereClause) -> List[str]:
        """Filter element IDs by where-clause."""
        if where_clause is None:
            return element_ids

        if isinstance(where_clause, WhereEquals):
            return [
                eid for eid in element_ids
                if self._navigator.get_element_attribute(eid, where_clause.attribute_name) == where_clause.value
            ]

        if isinstance(where_clause, WhereNotIn):
            return [
                eid for eid in element_ids
                if self._navigator.get_element_attribute(eid, where_clause.attribute_name) not in where_clause.values
            ]

        log_and_raise(ValueError(f"Unknown where-clause type: {type(where_clause).__name__}"))

    # ------------------------------------------------------------------
    # Flow retrieval
    # ------------------------------------------------------------------

    def _get_flows(self, element_id: str, flow_name: str) -> List[Any]:
        """Get sequence flows by direction name."""
        if flow_name == "outgoing_flows":
            return self._navigator.get_outgoing_sequence_flows(element_id)
        if flow_name == "incoming_flows":
            return self._navigator.get_incoming_sequence_flows(element_id)
        log_and_raise(ValueError(f"Unknown flow direction: '{flow_name}'"))

    # ------------------------------------------------------------------
    # Error formatting
    # ------------------------------------------------------------------

    def _format_error_message(self, rule: Dict[str, Any], element_id: str, flow_id: str) -> str:
        """Format the rule's message_template with placeholders."""
        template = str(rule["message_template"])
        return template.replace("{element_id}", element_id).replace("{flow_id}", flow_id)

    # ------------------------------------------------------------------
    # Rule evaluation (Step 2)
    # ------------------------------------------------------------------

    def _evaluate_rule(self, rule: Dict[str, Any]) -> None:
        """Evaluate a single rule against all matching elements.

        Process flow:
        1. Select elements by element_type and optional subtype filtering
        2. Apply where_clause filtering (e.g., "gateway_direction == diverging")
        3. Parse and evaluate assertion against remaining elements

        Subtype filtering:
        - If subtype is provided: filters to elements where {element_type}_type == subtype
          Example: element_type='event', subtype='start' → only start events
        - If subtype is empty: selects ALL elements of the type
        - Special case: flow_object ignores subtype (applies to all flow objects)

        Args:
            rule: Dict with keys: element_type, subtype, assertion, where_clause
        """
        element_type = str(rule["element_type"])
        subtype = str(rule["subtype"]).strip()
        element_ids = self._select_elements(element_type, subtype)

        where_text = rule["where_clause"]
        where_clause = self._parser.parse_where_clause(str(where_text) if where_text else "")
        element_ids = self._apply_where_clause(element_ids, where_clause)

        assertion_text = str(rule["assertion"])
        assertion = self._parser.parse_assertion(assertion_text)

        for element_id in element_ids:
            self._evaluate_assertion(element_id, assertion, rule)

    def _evaluate_assertion(self, element_id: str, assertion: Assertion, rule: Dict[str, Any]) -> None:
        """Dispatch assertion evaluation by type."""
        if isinstance(assertion, CountAssertion):
            self._evaluate_count(element_id, assertion, rule)
        elif isinstance(assertion, ForEachAssertion):
            self._evaluate_for_each(element_id, assertion, rule)
        elif isinstance(assertion, ExistsAssertion):
            self._evaluate_exists(element_id, assertion, rule)
        elif isinstance(assertion, CombinedAssertion):
            self._evaluate_assertion(element_id, assertion.left, rule)
            self._evaluate_assertion(element_id, assertion.right, rule)

    def _evaluate_count(self, element_id: str, count_assertion: CountAssertion, rule: Dict[str, Any]) -> None:
        """Evaluate a COUNT assertion."""
        total = 0
        for flow_name in count_assertion.flows:
            flows = self._get_flows(element_id, flow_name)
            total += len(flows)

        if not self._compare(total, count_assertion.operator, count_assertion.number):
            self._val_result.add_error(self._format_error_message(rule, element_id, ""))

    def _evaluate_for_each(self, element_id: str, for_each: ForEachAssertion, rule: Dict[str, Any]) -> None:
        """Evaluate a FOR_EACH assertion — report error per failing flow."""
        flows = self._get_flows(element_id, for_each.flow)
        for flow_info in flows:
            if not self._evaluate_check(flow_info, for_each.check):
                self._val_result.add_error(
                    self._format_error_message(rule, element_id, flow_info.sequence_flow_id)
                )

    def _evaluate_exists(self, element_id: str, exists: ExistsAssertion, rule: Dict[str, Any]) -> None:
        """Evaluate an EXISTS assertion — report error if no flow satisfies."""
        flows = self._get_flows(element_id, exists.flow)
        for flow_info in flows:
            if self._evaluate_check(flow_info, exists.check):
                return  # At least one satisfies
        # None satisfied — per D.5: empty string for flow_id
        self._val_result.add_error(self._format_error_message(rule, element_id, ""))

    def _evaluate_check(self, flow_info: Any, check: Check) -> bool:
        """Evaluate check terms against a flow info object."""
        results = [self._evaluate_check_term(flow_info, term) for term in check.terms]

        if check.combinator == "AND":
            return all(results)
        if check.combinator == "OR":
            return any(results)
        if check.combinator is None:
            return results[0]
        log_and_raise(ValueError(f"Unknown check combinator: '{check.combinator}'"))

    def _evaluate_check_term(self, flow_info: Any, term: Any) -> bool:
        """Evaluate a single check term against a flow info attribute."""
        if not hasattr(flow_info, term.attribute_name):
            log_and_raise(ValueError(
                f"Flow info object has no attribute '{term.attribute_name}' "
                f"(referenced in rule check term)"
            ))
        actual = getattr(flow_info, term.attribute_name)
        return self._compare_values(actual, term.operator, term.value)

    # ------------------------------------------------------------------
    # Comparison helpers
    # ------------------------------------------------------------------

    def _compare(self, actual: int, operator: str, expected: int) -> bool:
        """Compare integer values with operator."""
        if operator == ">=":
            return actual >= expected
        if operator == "<=":
            return actual <= expected
        if operator == ">":
            return actual > expected
        if operator == "<":
            return actual < expected
        if operator == "==":
            return actual == expected
        if operator == "!=":
            return actual != expected
        log_and_raise(ValueError(f"Unknown operator: '{operator}'"))

    def _compare_values(self, actual: Any, operator: str, expected: Any) -> bool:
        """Compare arbitrary values with operator."""
        if operator == "==":
            return actual == expected
        if operator == "!=":
            return actual != expected
        if operator == ">=":
            return actual >= expected
        if operator == "<=":
            return actual <= expected
        if operator == ">":
            return actual > expected
        if operator == "<":
            return actual < expected
        log_and_raise(ValueError(f"Unknown operator: '{operator}'"))
