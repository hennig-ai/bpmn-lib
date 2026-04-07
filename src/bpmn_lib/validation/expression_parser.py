"""Recursive-descent parser for BPMN validation rule expressions."""

import re
from typing import Any, List

from basic_framework.proc_frame import log_and_raise

from bpmn_lib.validation.expression_ast import (
    Assertion,
    Check,
    CheckTerm,
    CombinedAssertion,
    CountAssertion,
    ExistsAssertion,
    ForEachAssertion,
    WhereClause,
    WhereEquals,
    WhereNotIn,
)


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
        """Split text on ' AND ' only at the top level (not inside FOR_EACH/EXISTS check)."""
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

        log_and_raise(ValueError(
            f"Unrecognized assertion syntax: '{text}'"
        ))

    def _parse_count_expr(self, text: str) -> CountAssertion:
        """Parse COUNT(flow) op number or COUNT(f1) + COUNT(f2) op number."""
        # Match patterns like:
        #   COUNT(outgoing) >= 1
        #   COUNT(incoming) + COUNT(outgoing) >= 2
        pattern = r'^(COUNT\([^)]+\)(?:\s*\+\s*COUNT\([^)]+\))*)\s*([><=!]+)\s*(\d+)$'
        match = re.match(pattern, text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid COUNT assertion syntax: '{text}'"
            ))

        count_part = match.group(1)
        operator = match.group(2)
        number = int(match.group(3))

        # Extract flow names from COUNT(flow) parts
        flows = re.findall(r'COUNT\(([^)]+)\)', count_part)

        self._validate_operator(operator, text)

        return CountAssertion(flows=flows, operator=operator, number=number)

    def _parse_for_each_expr(self, text: str) -> ForEachAssertion:
        """Parse FOR_EACH flow: check."""
        # Pattern: FOR_EACH flow_name: check_expression
        match = re.match(r'^FOR_EACH\s+(\w+)\s*:\s*(.+)$', text.strip())
        if not match:
            log_and_raise(ValueError(
                f"Invalid FOR_EACH assertion syntax: '{text}'"
            ))

        flow = match.group(1)
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

        flow = match.group(1)
        check_text = match.group(2).strip()
        check = self._parse_check(check_text)

        return ExistsAssertion(flow=flow, check=check)

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
        """Split check text on combinator keyword."""
        # Split on ' AND ' or ' OR ' as word boundaries
        pattern = rf'\s+{combinator}\s+'
        parts = re.split(pattern, text)
        return parts

    def _parse_check_term(self, text: str) -> CheckTerm:
        """Parse attribute_name operator value."""
        text = text.strip()

        # Match: attribute_name operator value
        match = re.match(r'^(\w+)\s*([><=!]+)\s*(.+)$', text)
        if not match:
            log_and_raise(ValueError(
                f"Invalid check term syntax: '{text}'"
            ))

        attribute_name = match.group(1)
        operator = match.group(2)
        value_text = match.group(3).strip()

        self._validate_operator(operator, text)
        value = self._parse_value(value_text)

        return CheckTerm(attribute_name=attribute_name, operator=operator, value=value)

    def _validate_operator(self, operator: str, context: str) -> None:
        """Validate that operator is one of the recognized operators."""
        valid_operators = {"==", ">=", "<=", ">", "<", "!="}
        if operator not in valid_operators:
            log_and_raise(ValueError(
                f"Invalid operator '{operator}' in: '{context}'"
            ))

    def _parse_value(self, text: str) -> Any:
        """Convert value text to typed Python value."""
        text = text.strip()

        if text == "null":
            return None
        if text == "true":
            return True
        if text == "false":
            return False

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
