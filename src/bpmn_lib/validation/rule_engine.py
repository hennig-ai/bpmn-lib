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
    FlowSet,
    ForEachAssertion,
    ItemSet,
    LiteralOperand,
    MemberSet,
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

    container_table names the container table when the rule targets processes or
    collaborations, and is empty for element rules. Container IDs live in their own
    ID space — a process '001' is not the element '001' — so everything that reads
    data has to know which of the two it is holding.
    """

    element_id: str
    flow_info: Optional[Any]
    container_table: str


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

    def _apply_where_clause(
        self, target_ids: List[str], where_clause: WhereClause, container_table: str
    ) -> List[str]:
        """Filter target IDs by where-clause."""
        if where_clause is None:
            return target_ids

        if isinstance(where_clause, WhereEquals):
            return [
                tid for tid in target_ids
                if self._read_attribute(container_table, tid, where_clause.attribute_name)
                == where_clause.value
            ]

        return [
            tid for tid in target_ids
            if self._read_attribute(container_table, tid, where_clause.attribute_name)
            not in where_clause.values
        ]

    def _read_attribute(self, container_table: str, target_id: str, attribute_name: str) -> Any:
        """Read an attribute of an element or of a container."""
        if container_table:
            return self._navigator.get_container_attribute(
                container_table, target_id, attribute_name
            )

        return self._navigator.get_element_attribute(target_id, attribute_name)

    # ------------------------------------------------------------------
    # Flow retrieval
    # ------------------------------------------------------------------

    def _get_items(self, context: _EvaluationContext, item_set: ItemSet) -> List[Any]:
        """Get the items a COUNT/FOR_EACH/EXISTS works on."""
        if isinstance(item_set, MemberSet):
            return self._get_container_members(context, item_set)

        if context.container_table:
            log_and_raise(ValueError(
                f"Flow set '{item_set.name}' is not available on container "
                f"'{context.container_table}'. A container has members, not flows — "
                f"count them with 'elements OF <element_type>'"
            ))

        return self._get_flows(context.element_id, item_set.name)

    def _get_container_members(
        self, context: _EvaluationContext, member_set: MemberSet
    ) -> List[str]:
        """Get the members of the current container, filtered by type and subtype."""
        if not context.container_table:
            log_and_raise(ValueError(
                f"'elements OF {member_set.element_type}' is only available on a "
                f"container. Element '{context.element_id}' has no members"
            ))

        member_ids = self._navigator.get_container_members(
            context.container_table, context.element_id
        )
        of_type = set(self._select_elements(member_set.element_type, member_set.subtype))

        return [member_id for member_id in member_ids if member_id in of_type]

    def _get_flows(self, element_id: str, flow_name: str) -> List[Any]:
        """Get the named collection of an element, by name.

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
        """Evaluate a single rule against all matching elements or containers.

        Process flow:
        1. Select the targets — either bpmn_elements by type and optional subtype,
           or the rows of a container table (bpmn_process, collaboration)
        2. Apply where_clause filtering (e.g., "gateway_direction == diverging")
        3. Parse and evaluate assertion against remaining targets

        Subtype filtering:
        - If subtype is provided: filters to elements where {element_type}_type == subtype
          Example: element_type='event', subtype='start' → only start events
        - If subtype is empty: selects ALL elements of the type
        - Special case: flow_object ignores subtype (applies to all flow objects)
        - Containers have no subtype; rule_store rejects one at load time

        Args:
            rule: Dict with keys: element_type, subtype, assertion, where_clause
        """
        element_type = str(rule["element_type"])
        subtype = str(rule["subtype"]).strip()

        container_table = element_type if self._navigator.is_container_table(element_type) else ""
        if container_table:
            target_ids = self._navigator.get_container_ids(container_table)
        else:
            target_ids = self._select_elements(element_type, subtype)

        where_text = rule["where_clause"]
        where_clause = self._parser.parse_where_clause(str(where_text) if where_text else "")
        target_ids = self._apply_where_clause(target_ids, where_clause, container_table)

        assertion_text = str(rule["assertion"])
        assertion = self._parser.parse_assertion(assertion_text)

        for target_id in target_ids:
            context = _EvaluationContext(
                element_id=target_id, flow_info=None, container_table=container_table
            )
            self._evaluate_assertion(context, assertion, rule)

    def _evaluate_assertion(
        self, context: _EvaluationContext, assertion: Assertion, rule: Dict[str, Any]
    ) -> None:
        """Dispatch assertion evaluation by type."""
        if isinstance(assertion, CountAssertion):
            self._evaluate_count(context, assertion, rule)
        elif isinstance(assertion, ForEachAssertion):
            self._evaluate_for_each(context, assertion, rule)
        elif isinstance(assertion, ExistsAssertion):
            self._evaluate_exists(context, assertion, rule)
        elif isinstance(assertion, ElementAssertion):
            self._evaluate_element_assertion(context, assertion, rule)
        else:
            self._evaluate_assertion(context, assertion.left, rule)
            self._evaluate_assertion(context, assertion.right, rule)

    def _evaluate_count(
        self, context: _EvaluationContext, count_assertion: CountAssertion, rule: Dict[str, Any]
    ) -> None:
        """Evaluate a COUNT assertion."""
        total = 0
        for item_set in count_assertion.item_sets:
            total += len(self._get_items(context, item_set))

        if not self._compare(total, count_assertion.operator, count_assertion.number):
            self._val_result.add_error(self._format_error_message(rule, context.element_id, ""))

    def _evaluate_for_each(
        self, context: _EvaluationContext, for_each: ForEachAssertion, rule: Dict[str, Any]
    ) -> None:
        """Evaluate a FOR_EACH assertion — report error per failing flow."""
        for flow_info in self._get_items(context, FlowSet(name=for_each.flow)):
            flow_context = _EvaluationContext(
                element_id=context.element_id,
                flow_info=flow_info,
                container_table=context.container_table,
            )
            if not self._evaluate_check(flow_context, for_each.check):
                self._val_result.add_error(
                    self._format_error_message(
                        rule, context.element_id, self._item_identifier(flow_info)
                    )
                )

    def _evaluate_exists(
        self, context: _EvaluationContext, exists: ExistsAssertion, rule: Dict[str, Any]
    ) -> None:
        """Evaluate an EXISTS assertion — report error if no flow satisfies."""
        for flow_info in self._get_items(context, FlowSet(name=exists.flow)):
            flow_context = _EvaluationContext(
                element_id=context.element_id,
                flow_info=flow_info,
                container_table=context.container_table,
            )
            if self._evaluate_check(flow_context, exists.check):
                return  # At least one satisfies
        # None satisfied — per D.5: empty string for flow_id
        self._val_result.add_error(self._format_error_message(rule, context.element_id, ""))

    def _evaluate_element_assertion(
        self, context: _EvaluationContext, element_assertion: ElementAssertion, rule: Dict[str, Any]
    ) -> None:
        """Evaluate an ASSERT assertion against the selected element or container."""
        if not self._evaluate_check(context, element_assertion.check):
            self._val_result.add_error(self._format_error_message(rule, context.element_id, ""))

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
        """Read an attribute from the current flow info, or from the target itself."""
        if context.flow_info is None:
            return self._read_attribute(context.container_table, context.element_id, attribute_name)

        if not hasattr(context.flow_info, attribute_name):
            log_and_raise(ValueError(
                f"Flow info object has no attribute '{attribute_name}' "
                f"(referenced in rule check term)"
            ))
        return getattr(context.flow_info, attribute_name)

    def _call_resolver(self, context: _EvaluationContext, resolver: ResolverOperand) -> Any:
        """Execute a resolver function via the navigator."""
        # Every resolver takes a bpmn_element. Container IDs live in a separate ID
        # space, so passing one through would not fail — it would silently resolve
        # a different element that happens to carry the same number.
        if context.container_table:
            log_and_raise(ValueError(
                f"Resolver '{resolver.function}' expects a bpmn_element, but the rule "
                f"targets container '{context.container_table}'"
            ))

        element_id = self._resolve_element_reference(context, resolver.argument)

        # An empty reference (e.g. a boundary event without attachment) resolves to
        # nothing. Reporting that is another rule's job, so stay quiet here.
        if element_id == "":
            return None

        if resolver.function == "POOL_OF":
            return self._navigator.get_pool_of_element(
                element_id, self._collaboration_scope(context)
            )
        if resolver.function == "TYPE_OF":
            return self._navigator.get_element_attribute(element_id, "element_type")
        if resolver.function == "PROCESS_OF":
            return self._navigator.get_process_of_element(element_id)
        if resolver.function == "COLLABORATION_OF":
            return self._navigator.get_collaboration_of_element(element_id)
        if resolver.function == "EXPECTED_MESSAGE_OF":
            definition = self._navigator.get_message_definition_for_event(element_id)
            return None if definition is None else definition.message_definition_id
        if resolver.function == "POOL_MESSAGE_FLOW_COUNT":
            return self._count_message_flows_of_pool(element_id)
        if resolver.function == "COLLABORATION_POOL_COUNT":
            return self._count_pools_in_collaboration_of(element_id)

        log_and_raise(ValueError(f"Resolver '{resolver.function}' is not implemented"))

    def _collaboration_scope(self, context: _EvaluationContext) -> Optional[str]:
        """Collaboration that scopes pool resolution for the current element.

        Since schema v4.1 two pools of different collaborations may reference the same
        process. Whenever the selected element names a collaboration — a message flow
        does — that name disambiguates which participant an element belongs to.
        """
        return self._navigator.get_collaboration_of_element(context.element_id)

    def _resolve_element_reference(self, context: _EvaluationContext, reference: str) -> str:
        """Resolve 'self', 'source', 'target' or 'attached' to a concrete element ID.

        Inside FOR_EACH/EXISTS, source and target refer to the ends of the current
        flow; in an ASSERT they refer to the ends of the selected element itself.
        'attached' always refers to the activity the selected event is attached to.
        Returns an empty string when the reference is not set on the element.
        """
        if reference == "self":
            return context.element_id

        if reference == "attached":
            return self._read_reference_column(context.element_id, "attached_to_bpmn_element_id")

        if reference not in ("source", "target"):
            log_and_raise(ValueError(f"Unknown element reference: '{reference}'"))

        if context.flow_info is None:
            column = "source_bpmn_element_id" if reference == "source" else "target_bpmn_element_id"
            return self._read_reference_column(context.element_id, column)

        attribute = "source_element_id" if reference == "source" else "target_element_id"
        if not hasattr(context.flow_info, attribute):
            log_and_raise(ValueError(
                f"'{reference}' is not available on {type(context.flow_info).__name__} "
                f"(no attribute '{attribute}')"
            ))
        return getattr(context.flow_info, attribute)

    def _read_reference_column(self, element_id: str, column: str) -> str:
        """Read an element reference column, normalising 'not set' to an empty string."""
        value = self._navigator.get_element_attribute(element_id, column)

        if value is None:
            return ""

        return str(value).strip()

    def _count_message_flows_of_pool(self, pool_element_id: str) -> int:
        """Count message flows with at least one end belonging to the given pool.

        Each flow scopes the pool lookup with its own collaboration, so a process
        carried by two participants does not make the membership ambiguous.
        """
        count = 0
        for flow_id in self._navigator.get_element_ids_by_type(TBL_MESSAGE_FLOW):
            collaboration_id = self._navigator.get_collaboration_of_element(flow_id)
            source_id = self._navigator.get_element_attribute(flow_id, "source_bpmn_element_id")
            target_id = self._navigator.get_element_attribute(flow_id, "target_bpmn_element_id")
            if self._navigator.get_pool_of_element(source_id, collaboration_id) == pool_element_id:
                count += 1
            elif self._navigator.get_pool_of_element(target_id, collaboration_id) == pool_element_id:
                count += 1
        return count

    def _count_pools_in_collaboration_of(self, element_id: str) -> int:
        """Count the participants of the collaboration the given element belongs to."""
        collaboration_id = self._navigator.get_collaboration_of_element(element_id)

        if collaboration_id is None:
            return 0

        count = 0
        for pool_id in self._navigator.get_element_ids_by_type(TBL_POOL):
            if self._navigator.get_collaboration_of_element(pool_id) == collaboration_id:
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
