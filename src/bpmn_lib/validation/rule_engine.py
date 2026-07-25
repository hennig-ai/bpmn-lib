"""BPMN Rule Engine — evaluates validation rules against BPMN process data."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from basic_framework.proc_frame import log_and_raise
from basic_framework.container_utils.abstract_container import AbstractContainer

from bpmn_lib.navigator.bpmn_constants import TBL_MESSAGE_FLOW, TBL_POOL
from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.expression_ast import (
    Assertion,
    AttributeOperand,
    Check,
    CheckTerm,
    CountAssertion,
    ElementAssertion,
    ExistsAssertion,
    ForEachAssertion,
    LiteralOperand,
    Operand,
    ResolverOperand,
    ValueListOperand,
    WhereClause,
    WhereEquals,
)
from bpmn_lib.validation.expression_parser import ExpressionParser

_LEVEL_ORDER: Dict[str, int] = {"basic": 1, "spec_v2": 2, "best_practice": 3}

# Identifier attribute of the items a FOR_EACH iterates over, used for {flow_id}.
# The first attribute an item actually carries wins.
_ITEM_ID_ATTRIBUTES: List[str] = [
    "sequence_flow_id",
    "message_flow_id",
    "message_event_definition_id",
]


@dataclass(frozen=True)
class _EvaluationContext:
    """What a check term is evaluated against.

    flow_info is set while iterating flows (FOR_EACH / EXISTS) and None for
    element-level assertions (ASSERT).
    """

    element_id: str
    flow_info: Optional[Any]


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

        return [
            eid for eid in element_ids
            if self._navigator.get_element_attribute(eid, where_clause.attribute_name) not in where_clause.values
        ]

    # ------------------------------------------------------------------
    # Flow retrieval
    # ------------------------------------------------------------------

    def _get_flows(self, element_id: str, flow_name: str) -> List[Any]:
        """Get the items a COUNT/FOR_EACH/EXISTS iterates over, by name.

        Besides the two sequence flow directions this covers message flows and the
        message_event_definition of an event. The latter is not a flow at all — it
        is exposed here as a list of 0 or 1 entries so that rules can state its
        presence or absence with the same COUNT(...) syntax.
        """
        if flow_name == "outgoing_flows":
            return self._navigator.get_outgoing_sequence_flows(element_id)
        if flow_name == "incoming_flows":
            return self._navigator.get_incoming_sequence_flows(element_id)
        if flow_name == "outgoing_message_flows":
            return self._navigator.get_outgoing_message_flows(element_id)
        if flow_name == "incoming_message_flows":
            return self._navigator.get_incoming_message_flows(element_id)
        if flow_name == "message_event_definitions":
            return self._navigator.get_message_event_definitions(element_id)
        log_and_raise(ValueError(f"Unknown flow direction: '{flow_name}'"))

    def _item_identifier(self, flow_info: Any) -> str:
        """Get the identifier of an iterated item for the {flow_id} placeholder."""
        for attribute in _ITEM_ID_ATTRIBUTES:
            if hasattr(flow_info, attribute):
                return str(getattr(flow_info, attribute))

        log_and_raise(ValueError(
            f"Iterated item of type '{type(flow_info).__name__}' carries none of the "
            f"known identifier attributes {_ITEM_ID_ATTRIBUTES}"
        ))

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
        elif isinstance(assertion, ElementAssertion):
            self._evaluate_element_assertion(element_id, assertion, rule)
        else:
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
            context = _EvaluationContext(element_id=element_id, flow_info=flow_info)
            if not self._evaluate_check(context, for_each.check):
                self._val_result.add_error(
                    self._format_error_message(rule, element_id, self._item_identifier(flow_info))
                )

    def _evaluate_exists(self, element_id: str, exists: ExistsAssertion, rule: Dict[str, Any]) -> None:
        """Evaluate an EXISTS assertion — report error if no flow satisfies."""
        flows = self._get_flows(element_id, exists.flow)
        for flow_info in flows:
            context = _EvaluationContext(element_id=element_id, flow_info=flow_info)
            if self._evaluate_check(context, exists.check):
                return  # At least one satisfies
        # None satisfied — per D.5: empty string for flow_id
        self._val_result.add_error(self._format_error_message(rule, element_id, ""))

    def _evaluate_element_assertion(
        self, element_id: str, element_assertion: ElementAssertion, rule: Dict[str, Any]
    ) -> None:
        """Evaluate an ASSERT assertion against the selected element itself."""
        context = _EvaluationContext(element_id=element_id, flow_info=None)
        if not self._evaluate_check(context, element_assertion.check):
            self._val_result.add_error(self._format_error_message(rule, element_id, ""))

    def _evaluate_check(self, context: _EvaluationContext, check: Check) -> bool:
        """Evaluate check terms against the current evaluation context."""
        results = [self._evaluate_check_term(context, term) for term in check.terms]

        if check.combinator == "AND":
            return all(results)
        if check.combinator == "OR":
            return any(results)
        if check.combinator is None:
            return results[0]
        log_and_raise(ValueError(f"Unknown check combinator: '{check.combinator}'"))

    def _evaluate_check_term(self, context: _EvaluationContext, term: CheckTerm) -> bool:
        """Evaluate a single check term by resolving both operands."""
        left = self._resolve_operand(context, term.left)
        right = self._resolve_operand(context, term.right)
        return self._compare_values(left, term.operator, right)

    # ------------------------------------------------------------------
    # Operand resolution
    # ------------------------------------------------------------------

    def _resolve_operand(self, context: _EvaluationContext, operand: Operand) -> Any:
        """Resolve an operand to a concrete value."""
        if isinstance(operand, LiteralOperand):
            return operand.value
        if isinstance(operand, ValueListOperand):
            return operand.values
        if isinstance(operand, AttributeOperand):
            return self._resolve_attribute(context, operand.name)
        return self._call_resolver(context, operand)

    def _resolve_attribute(self, context: _EvaluationContext, attribute_name: str) -> Any:
        """Read an attribute from the current flow info, or from the element itself."""
        if context.flow_info is None:
            return self._navigator.get_element_attribute(context.element_id, attribute_name)

        if not hasattr(context.flow_info, attribute_name):
            log_and_raise(ValueError(
                f"Flow info object has no attribute '{attribute_name}' "
                f"(referenced in rule check term)"
            ))
        return getattr(context.flow_info, attribute_name)

    def _call_resolver(self, context: _EvaluationContext, resolver: ResolverOperand) -> Any:
        """Execute a resolver function via the navigator."""
        if resolver.function == "POOL_COUNT":
            return len(self._navigator.get_element_ids_by_type(TBL_POOL))

        element_id = self._resolve_element_reference(context, resolver.argument)

        if resolver.function == "POOL_OF":
            return self._navigator.get_pool_of_element(element_id)
        if resolver.function == "TYPE_OF":
            return self._navigator.get_element_attribute(element_id, "element_type")
        if resolver.function == "EXPECTED_MESSAGE_OF":
            definition = self._navigator.get_message_definition_for_event(element_id)
            return None if definition is None else definition.message_definition_id
        if resolver.function == "POOL_MESSAGE_FLOW_COUNT":
            return self._count_message_flows_of_pool(element_id)

        log_and_raise(ValueError(f"Resolver '{resolver.function}' is not implemented"))

    def _resolve_element_reference(self, context: _EvaluationContext, reference: str) -> str:
        """Resolve 'self', 'source' or 'target' to a concrete element ID.

        Inside FOR_EACH/EXISTS, source and target refer to the ends of the current
        flow; in an ASSERT they refer to the ends of the selected element itself.
        """
        if reference == "self":
            return context.element_id

        if reference not in ("source", "target"):
            log_and_raise(ValueError(f"Unknown element reference: '{reference}'"))

        if context.flow_info is None:
            column = "source_bpmn_element_id" if reference == "source" else "target_bpmn_element_id"
            return self._navigator.get_element_attribute(context.element_id, column)

        attribute = "source_element_id" if reference == "source" else "target_element_id"
        if not hasattr(context.flow_info, attribute):
            log_and_raise(ValueError(
                f"'{reference}' is not available on {type(context.flow_info).__name__} "
                f"(no attribute '{attribute}')"
            ))
        return getattr(context.flow_info, attribute)

    def _count_message_flows_of_pool(self, pool_element_id: str) -> int:
        """Count message flows with at least one end belonging to the given pool."""
        count = 0
        for flow_id in self._navigator.get_element_ids_by_type(TBL_MESSAGE_FLOW):
            source_id = self._navigator.get_element_attribute(flow_id, "source_bpmn_element_id")
            target_id = self._navigator.get_element_attribute(flow_id, "target_bpmn_element_id")
            if self._navigator.get_pool_of_element(source_id) == pool_element_id:
                count += 1
            elif self._navigator.get_pool_of_element(target_id) == pool_element_id:
                count += 1
        return count

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
        if operator == "IN":
            return actual in expected
        if operator == "NOT IN":
            return actual not in expected
        if operator == ">=":
            return actual >= expected
        if operator == "<=":
            return actual <= expected
        if operator == ">":
            return actual > expected
        if operator == "<":
            return actual < expected
        log_and_raise(ValueError(f"Unknown operator: '{operator}'"))
