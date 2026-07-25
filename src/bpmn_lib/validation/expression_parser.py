"""Recursive-descent parser for BPMN validation rule expressions."""

import re
from typing import Any, List, Tuple

from basic_framework.proc_frame import log_and_raise

from bpmn_lib.validation.expression_ast import (
    Assertion,
    AttributeOperand,
    Check,
    CheckTerm,
    CombinedAssertion,
    CountAssertion,
    ElementAssertion,
    ExistsAssertion,
    FLOW_SET_NAMES,
    FlowSet,
    ForEachAssertion,
    ItemSet,
    LiteralOperand,
    MemberSet,
    Operand,
    RESOLVER_ARGUMENTS,
    RESOLVER_ARITY,
    ResolverOperand,
    ValueListOperand,
    WhereClause,
    WhereEquals,
    WhereNotIn,
)

# 'elements OF <element_type>' with an optional '.<subtype>' suffix.
_MEMBER_SET_PATTERN = r'^elements\s+OF\s+([a-z_][a-z0-9_]*)(?:\.([a-z_][a-z0-9_]*))?$'

# Longest match wins, so ">=" must precede ">" and "NOT IN" must precede "IN".
_TERM_OPERATORS: List[str] = [" NOT IN ", " IN ", "==", "!=", ">=", "<=", ">", "<"]

_VALID_OPERATORS = {"==", ">=", "<=", ">", "<", "!=", "IN", "NOT IN"}


class ExpressionParser:
    """Parses assertion and where-clause text into typed AST objects."""

    def parse_assertion(self, text: str) -> Assertion:
        """Parse an assertion expression into an AST node."""
        text = text.strip()

        # Check for AND-combined assertions at top level
        # Split on " AND " that is NOT inside parentheses or after a colon
        and_parts = self._split_top_level_and(text)
        if len(and_parts) == 2:
            left = self._parse_single_assertion(and_parts[0].strip())
            right = self._parse_single_assertion(and_parts[1].strip())
            return CombinedAssertion(left=left, right=right)

        return self._parse_single_assertion(text)

    def _split_top_level_and(self, text: str) -> List[str]:
        """Split text on ' AND ' only at the top level (not inside FOR_EACH/EXISTS check).

        ASSERT is deliberately not split here: its check already supports AND/OR
        internally, so a top-level AND between two ASSERTs is never needed.
        """
        # Only split on AND that separates two top-level assertion expressions
        # Pattern: look for AND between a closing assertion and opening assertion
        # COUNT(...) ... AND COUNT/FOR_EACH/EXISTS
        pattern = r'^(COUNT\([^)]*\)(?:\s*\+\s*COUNT\([^)]*\))?\s*[><=!]+\s*\d+)\s+AND\s+((?:COUNT|FOR_EACH|EXISTS).+)$'
        match = re.match(pattern, text)
        if match:
            return [match.group(1), match.group(2)]

        # FOR_EACH/EXISTS ... AND COUNT/FOR_EACH/EXISTS — more complex, try generic split
        # Only split if AND appears between two top-level expressions
        # We look for "AND " followed by COUNT, FOR_EACH, or EXISTS
        pattern2 = r'^(.+?)\s+AND\s+((?:COUNT|FOR_EACH|EXISTS)\(.*)$'
        match2 = re.match(pattern2, text)
        if match2:
            left_candidate = match2.group(1).strip()
            # Verify left is a valid top-level assertion start
            if left_candidate.startswith(("COUNT(", "FOR_EACH ", "EXISTS ")):
                return [left_candidate, match2.group(2)]

        return [text]

    def _parse_single_assertion(self, text: str) -> Assertion:
        """Parse a single (non-combined) assertion."""
        text = text.strip()

        if text.startswith("COUNT("):
            return self._parse_count_expr(text)
        if text.startswith("FOR_EACH "):
            return self._parse_for_each_expr(text)
        if text.startswith("EXISTS "):
            return self._parse_exists_expr(text)
        if text.startswith("ASSERT "):
            return self._parse_element_expr(text)

        log_and_raise(ValueError(
            f"Unrecognized assertion syntax: '{text}'"
        ))

    def _parse_count_expr(self, text: str) -> CountAssertion:
        """Parse COUNT(items) op number or COUNT(a) + COUNT(b) op number."""
        # Match patterns like:
        #   COUNT(outgoing_flows) >= 1
        #   COUNT(incoming_flows) + COUNT(outgoing_flows) >= 2
        #   COUNT(elements OF event.start) >= 1
        pattern = r'^(COUNT\([^)]+\)(?:\s*\+\s*COUNT\([^)]+\))*)\s*([><=!]+)\s*(\d+)$'
        match = re.match(pattern, text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid COUNT assertion syntax: '{text}'"
            ))

        count_part = match.group(1)
        operator = match.group(2)
        number = int(match.group(3))

        item_sets = [
            self._parse_item_set(inner.strip(), text)
            for inner in re.findall(r'COUNT\(([^)]+)\)', count_part)
        ]

        self._validate_operator(operator, text)

        return CountAssertion(item_sets=item_sets, operator=operator, number=number)

    def _parse_item_set(self, text: str, context: str) -> ItemSet:
        """Parse what a COUNT counts: a named flow set or the members of a container."""
        match = re.match(_MEMBER_SET_PATTERN, text)
        if match:
            subtype = match.group(2)
            return MemberSet(element_type=match.group(1), subtype=subtype if subtype else "")

        return FlowSet(name=self._validate_flow_name(text, context))

    def _validate_flow_name(self, name: str, context: str) -> str:
        """Reject an unknown flow set while the rules are being loaded, not while running."""
        if name not in FLOW_SET_NAMES:
            log_and_raise(ValueError(
                f"Unknown flow set '{name}' in: '{context}'. "
                f"Known flow sets: {FLOW_SET_NAMES}. "
                f"Container members are counted with 'elements OF <element_type>'"
            ))

        return name

    def _parse_for_each_expr(self, text: str) -> ForEachAssertion:
        """Parse FOR_EACH flow: check."""
        # Pattern: FOR_EACH flow_name: check_expression
        match = re.match(r'^FOR_EACH\s+(\w+)\s*:\s*(.+)$', text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid FOR_EACH assertion syntax: '{text}'"
            ))

        flow = self._validate_flow_name(match.group(1), text)
        check_text = match.group(2).strip()
        check = self._parse_check(check_text)

        return ForEachAssertion(flow=flow, check=check)

    def _parse_exists_expr(self, text: str) -> ExistsAssertion:
        """Parse EXISTS flow: check."""
        match = re.match(r'^EXISTS\s+(\w+)\s*:\s*(.+)$', text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid EXISTS assertion syntax: '{text}'"
            ))

        flow = self._validate_flow_name(match.group(1), text)
        check_text = match.group(2).strip()
        check = self._parse_check(check_text)

        return ExistsAssertion(flow=flow, check=check)

    def _parse_element_expr(self, text: str) -> ElementAssertion:
        """Parse ASSERT check — a check against the selected element itself."""
        match = re.match(r'^ASSERT\s+(.+)$', text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid ASSERT assertion syntax: '{text}'"
            ))

        check = self._parse_check(match.group(1).strip())

        return ElementAssertion(check=check)

    def _parse_check(self, text: str) -> Check:
        """Parse one or more check terms with optional AND/OR combinator."""
        text = text.strip()

        # Try splitting on AND or OR
        for combinator in ["AND", "OR"]:
            parts = self._split_check_on_combinator(text, combinator)
            if len(parts) > 1:
                terms = [self._parse_check_term(p.strip()) for p in parts]
                return Check(terms=terms, combinator=combinator)

        # Single term
        term = self._parse_check_term(text)
        return Check(terms=[term], combinator=None)

    def _split_check_on_combinator(self, text: str, combinator: str) -> List[str]:
        """Split check text on combinator keyword, ignoring parenthesised sections."""
        return self._split_at_depth_zero(text, f" {combinator} ")

    def _split_at_depth_zero(self, text: str, separator: str) -> List[str]:
        """Split text on a separator that occurs outside of any parentheses.

        Value lists like '(pool, event)' and resolver calls like 'POOL_OF(target)'
        must never be split apart.
        """
        parts: List[str] = []
        depth = 0
        start = 0
        index = 0

        while index < len(text):
            char = text[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and text.startswith(separator, index):
                parts.append(text[start:index])
                index += len(separator)
                start = index
                continue
            index += 1

        parts.append(text[start:])
        return parts

    def _parse_check_term(self, text: str) -> CheckTerm:
        """Parse left_operand operator right_operand."""
        left_text, operator, right_text = self._split_check_term(text.strip())

        self._validate_operator(operator, text)

        left = self._parse_operand(left_text, True)
        right = self._parse_operand(right_text, False)

        if operator in ("IN", "NOT IN") and not isinstance(right, ValueListOperand):
            log_and_raise(ValueError(
                f"Operator '{operator}' requires a parenthesised value list in: '{text}'"
            ))

        return CheckTerm(left=left, operator=operator, right=right)

    def _split_check_term(self, text: str) -> Tuple[str, str, str]:
        """Find the comparison operator outside parentheses and split around it."""
        depth = 0
        index = 0

        while index < len(text):
            char = text[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0:
                for operator in _TERM_OPERATORS:
                    if text.startswith(operator, index):
                        left = text[:index].strip()
                        right = text[index + len(operator):].strip()
                        return left, operator.strip(), right
            index += 1

        log_and_raise(ValueError(
            f"Invalid check term syntax: '{text}'"
        ))

    def _parse_operand(self, text: str, is_left: bool) -> Operand:
        """Convert operand text into a typed operand.

        A bare word is an attribute reference on the left and a literal on the
        right — this keeps the original 'attribute operator value' rules valid.
        """
        text = text.strip()

        # Resolver call, e.g. POOL_OF(target) or POOL_COUNT()
        match = re.match(r'^([A-Z][A-Z0-9_]*)\s*\((.*)\)$', text)
        if match:
            return self._build_resolver(match.group(1), match.group(2).strip(), text)

        # Parenthesised value list, e.g. (pool, event)
        if text.startswith("(") and text.endswith(")"):
            values = [self._parse_value(part.strip()) for part in text[1:-1].split(",")]
            return ValueListOperand(values=values)

        if is_left and re.match(r'^[a-z_][a-z0-9_]*$', text):
            return AttributeOperand(name=text)

        return LiteralOperand(value=self._parse_value(text))

    def _build_resolver(self, function: str, argument: str, context: str) -> ResolverOperand:
        """Validate a resolver call against the known resolvers and build the operand."""
        if function not in RESOLVER_ARITY:
            log_and_raise(ValueError(
                f"Unknown resolver function '{function}' in: '{context}'. "
                f"Known resolvers: {sorted(RESOLVER_ARITY.keys())}"
            ))

        expected_arity = RESOLVER_ARITY[function]

        if expected_arity == 0:
            if argument:
                log_and_raise(ValueError(
                    f"Resolver '{function}' takes no argument, got '{argument}' in: '{context}'"
                ))
        elif argument not in RESOLVER_ARGUMENTS:
            log_and_raise(ValueError(
                f"Invalid resolver argument '{argument}' in: '{context}'. "
                f"Allowed: {RESOLVER_ARGUMENTS}"
            ))

        return ResolverOperand(function=function, argument=argument)

    def _validate_operator(self, operator: str, context: str) -> None:
        """Validate that operator is one of the recognized operators."""
        if operator not in _VALID_OPERATORS:
            log_and_raise(ValueError(
                f"Invalid operator '{operator}' in: '{context}'"
            ))

    def _parse_value(self, text: str) -> Any:
        """Convert value text to typed Python value.

        A digit sequence with a leading zero stays a string: BPMN element IDs are
        zero-padded ('038') and must not silently turn into the number 38. Quote a
        literal whenever it is an ID rather than a count.
        """
        text = text.strip()

        if text == "null":
            return None
        if text == "true":
            return True
        if text == "false":
            return False

        # Integer literal — needed for resolvers that return counts
        if re.match(r'^-?(0|[1-9]\d*)$', text):
            return int(text)

        # Quoted string
        if (text.startswith('"') and text.endswith('"')) or \
           (text.startswith("'") and text.endswith("'")):
            return text[1:-1]

        # Unquoted string
        return text

    def parse_where_clause(self, text: str) -> WhereClause:
        """Parse a where-clause expression."""
        text = text.strip()

        if not text:
            return None

        # Check for NOT IN
        if "NOT IN" in text:
            return self._parse_not_in_condition(text)

        # WhereEquals: attribute_name == value
        match = re.match(r'^(\w+)\s*==\s*(.+)$', text)
        if match:
            attribute_name = match.group(1)
            value = match.group(2).strip()
            return WhereEquals(attribute_name=attribute_name, value=value)

        log_and_raise(ValueError(
            f"Unrecognized where-clause syntax: '{text}'"
        ))

    def _parse_not_in_condition(self, text: str) -> WhereNotIn:
        """Parse attr NOT IN (v1, v2, ...)."""
        match = re.match(r'^(\w+)\s+NOT\s+IN\s*\(([^)]+)\)$', text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid NOT IN syntax: '{text}'"
            ))

        attribute_name = match.group(1)
        values_text = match.group(2)
        values = [v.strip() for v in values_text.split(",")]

        return WhereNotIn(attribute_name=attribute_name, values=values)
