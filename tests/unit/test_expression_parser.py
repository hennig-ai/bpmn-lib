"""Unit tests for ExpressionParser."""

import pytest

from bpmn_lib.validation.expression_parser import ExpressionParser
from bpmn_lib.validation.expression_ast import (
    CombinedAssertion,
    CountAssertion,
    ExistsAssertion,
    ForEachAssertion,
    WhereEquals,
    WhereNotIn,
)


class TestParseAssertionCount:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_count_single_flow(self):
        result = self.parser.parse_assertion("COUNT(outgoing_flows) >= 1")
        assert isinstance(result, CountAssertion)
        assert result.flows == ["outgoing_flows"]
        assert result.operator == ">="
        assert result.number == 1

    def test_count_combined_flows(self):
        result = self.parser.parse_assertion("COUNT(incoming_flows) + COUNT(outgoing_flows) >= 2")
        assert isinstance(result, CountAssertion)
        assert result.flows == ["incoming_flows", "outgoing_flows"]
        assert result.operator == ">="
        assert result.number == 2

    def test_count_equals(self):
        result = self.parser.parse_assertion("COUNT(outgoing_flows) == 0")
        assert isinstance(result, CountAssertion)
        assert result.operator == "=="
        assert result.number == 0


class TestParseAssertionForEach:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_for_each_basic(self):
        result = self.parser.parse_assertion("FOR_EACH outgoing_flows: condition_expression != null")
        assert isinstance(result, ForEachAssertion)
        assert result.flow == "outgoing_flows"
        assert len(result.check.terms) == 1
        assert result.check.terms[0].attribute_name == "condition_expression"
        assert result.check.terms[0].operator == "!="
        assert result.check.terms[0].value is None

    def test_for_each_with_and_check(self):
        result = self.parser.parse_assertion("FOR_EACH outgoing_flows: condition_expression != null AND is_default == false")
        assert isinstance(result, ForEachAssertion)
        assert result.check.combinator == "AND"
        assert len(result.check.terms) == 2


class TestParseAssertionExists:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_exists_basic(self):
        result = self.parser.parse_assertion("EXISTS outgoing_flows: is_default == true")
        assert isinstance(result, ExistsAssertion)
        assert result.flow == "outgoing_flows"
        assert result.check.terms[0].attribute_name == "is_default"
        assert result.check.terms[0].value is True


class TestParseAssertionCombined:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_combined_count_and_for_each(self):
        result = self.parser.parse_assertion(
            "COUNT(outgoing_flows) >= 1 AND FOR_EACH outgoing_flows: condition_expression != null"
        )
        assert isinstance(result, CombinedAssertion)
        assert isinstance(result.left, CountAssertion)
        assert isinstance(result.right, ForEachAssertion)


class TestParseValue:
    """Tests for _parse_value type conversion (TC-024)."""

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_null_returns_none(self):
        assert self.parser._parse_value("null") is None

    def test_true_returns_true(self):
        assert self.parser._parse_value("true") is True

    def test_false_returns_false(self):
        assert self.parser._parse_value("false") is False

    def test_double_quoted_string_unquoted(self):
        assert self.parser._parse_value('"quoted"') == "quoted"

    def test_single_quoted_string_unquoted(self):
        assert self.parser._parse_value("'quoted'") == "quoted"

    def test_unquoted_text_returned_as_is(self):
        assert self.parser._parse_value("plain") == "plain"


class TestParseAssertionErrors:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_invalid_syntax_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("INVALID syntax")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("")


class TestParseWhereClause:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_where_equals(self):
        result = self.parser.parse_where_clause("element_type == exclusiveGateway")
        assert isinstance(result, WhereEquals)
        assert result.attribute_name == "element_type"
        assert result.value == "exclusiveGateway"

    def test_where_not_in(self):
        result = self.parser.parse_where_clause("element_type NOT IN (startEvent, endEvent)")
        assert isinstance(result, WhereNotIn)
        assert result.attribute_name == "element_type"
        assert result.values == ["startEvent", "endEvent"]

    def test_empty_returns_none(self):
        result = self.parser.parse_where_clause("")
        assert result is None

    def test_whitespace_returns_none(self):
        result = self.parser.parse_where_clause("   ")
        assert result is None

    def test_invalid_where_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_where_clause("INVALID CLAUSE")
