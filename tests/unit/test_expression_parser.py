"""Unit tests for ExpressionParser."""

import pytest

from bpmn_lib.validation.expression_parser import ExpressionParser
from bpmn_lib.validation.expression_ast import (
    AttributeOperand,
    CombinedAssertion,
    CountAssertion,
    ElementAssertion,
    ExistsAssertion,
    FlowSet,
    ForEachAssertion,
    LiteralOperand,
    MemberSet,
    ResolverOperand,
    ValueListOperand,
    WhereEquals,
    WhereNotIn,
)


class TestParseAssertionCount:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_count_single_flow(self):
        result = self.parser.parse_assertion("COUNT(outgoing_flows) >= 1")
        assert isinstance(result, CountAssertion)
        assert result.item_sets == [FlowSet(name="outgoing_flows")]
        assert result.operator == ">="
        assert result.number == 1

    def test_count_combined_flows(self):
        result = self.parser.parse_assertion("COUNT(incoming_flows) + COUNT(outgoing_flows) >= 2")
        assert isinstance(result, CountAssertion)
        assert result.item_sets == [
            FlowSet(name="incoming_flows"),
            FlowSet(name="outgoing_flows"),
        ]
        assert result.operator == ">="
        assert result.number == 2

    def test_count_equals(self):
        result = self.parser.parse_assertion("COUNT(outgoing_flows) == 0")
        assert isinstance(result, CountAssertion)
        assert result.operator == "=="
        assert result.number == 0

    def test_unknown_flow_set_is_rejected_at_parse_time(self):
        """A misspelled flow set must fail while loading, not silently count nothing."""
        with pytest.raises(Exception, match="Unknown flow set"):
            self.parser.parse_assertion("COUNT(outgoing_flow) >= 1")

    def test_unknown_flow_set_in_for_each_is_rejected(self):
        with pytest.raises(Exception, match="Unknown flow set"):
            self.parser.parse_assertion("FOR_EACH telepathy: is_default == true")


class TestParseAssertionMemberSet:
    """'elements OF <element_type>[.<subtype>]' — what a container rule counts."""

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_member_set_without_subtype(self):
        result = self.parser.parse_assertion("COUNT(elements OF pool) >= 2")
        assert isinstance(result, CountAssertion)
        assert result.item_sets == [MemberSet(element_type="pool", subtype="")]
        assert result.number == 2

    def test_member_set_with_subtype(self):
        result = self.parser.parse_assertion("COUNT(elements OF event.start) >= 1")
        assert isinstance(result, CountAssertion)
        assert result.item_sets == [MemberSet(element_type="event", subtype="start")]

    def test_member_set_combines_with_flow_set(self):
        result = self.parser.parse_assertion(
            "COUNT(elements OF pool) + COUNT(elements OF message_flow) >= 3"
        )
        assert isinstance(result, CountAssertion)
        assert result.item_sets == [
            MemberSet(element_type="pool", subtype=""),
            MemberSet(element_type="message_flow", subtype=""),
        ]

    def test_member_set_is_rejected_in_for_each(self):
        """FOR_EACH iterates flows; members are counted, not iterated."""
        with pytest.raises(Exception, match="Invalid FOR_EACH"):
            self.parser.parse_assertion("FOR_EACH elements OF pool: is_closed == false")


class TestParseAssertionForEach:

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_for_each_basic(self):
        result = self.parser.parse_assertion("FOR_EACH outgoing_flows: condition_expression != null")
        assert isinstance(result, ForEachAssertion)
        assert result.flow == "outgoing_flows"
        assert len(result.check.terms) == 1
        assert result.check.terms[0].left.name == "condition_expression"
        assert result.check.terms[0].operator == "!="
        assert result.check.terms[0].right.value is None

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
        assert result.check.terms[0].left.name == "is_default"
        assert result.check.terms[0].right.value is True


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


class TestParseAssertionElement:
    """Tests for ASSERT, resolver operands and IN / NOT IN."""

    def setup_method(self):
        self.parser = ExpressionParser()

    def test_assert_with_two_resolvers(self):
        result = self.parser.parse_assertion("ASSERT POOL_OF(source) != POOL_OF(target)")
        assert isinstance(result, ElementAssertion)
        term = result.check.terms[0]
        assert term.left == ResolverOperand(function="POOL_OF", argument="source")
        assert term.operator == "!="
        assert term.right == ResolverOperand(function="POOL_OF", argument="target")

    def test_assert_with_attribute_and_literal(self):
        result = self.parser.parse_assertion("ASSERT message_definition_id != null")
        term = result.check.terms[0]
        assert term.left == AttributeOperand(name="message_definition_id")
        assert term.right == LiteralOperand(value=None)

    def test_assert_with_value_list(self):
        result = self.parser.parse_assertion("ASSERT TYPE_OF(target) IN (pool, event, user_task)")
        term = result.check.terms[0]
        assert term.operator == "IN"
        assert term.right == ValueListOperand(values=["pool", "event", "user_task"])

    def test_assert_with_not_in(self):
        result = self.parser.parse_assertion("ASSERT TYPE_OF(source) NOT IN (gateway, lane)")
        assert result.check.terms[0].operator == "NOT IN"

    def test_and_is_not_split_inside_a_value_list(self):
        """Two IN terms combined with AND must stay two terms of one check."""
        result = self.parser.parse_assertion(
            "ASSERT TYPE_OF(source) IN (pool, event) AND TYPE_OF(target) IN (pool, event)"
        )
        assert result.check.combinator == "AND"
        assert len(result.check.terms) == 2

    def test_or_combinator_with_resolvers(self):
        result = self.parser.parse_assertion(
            "ASSERT COLLABORATION_POOL_COUNT(self) < 2 OR POOL_MESSAGE_FLOW_COUNT(self) >= 1"
        )
        assert result.check.combinator == "OR"
        assert result.check.terms[0].left == ResolverOperand(
            function="COLLABORATION_POOL_COUNT", argument="self"
        )
        assert result.check.terms[0].right == LiteralOperand(value=2)

    def test_attached_reference_is_accepted(self):
        result = self.parser.parse_assertion("ASSERT PROCESS_OF(self) == PROCESS_OF(attached)")
        assert result.check.terms[0].right == ResolverOperand(
            function="PROCESS_OF", argument="attached"
        )

    def test_resolver_inside_for_each(self):
        result = self.parser.parse_assertion(
            "FOR_EACH incoming_message_flows: message_definition_id == EXPECTED_MESSAGE_OF(self)"
        )
        assert isinstance(result, ForEachAssertion)
        assert result.check.terms[0].right == ResolverOperand(
            function="EXPECTED_MESSAGE_OF", argument="self"
        )

    def test_unknown_resolver_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("ASSERT COLOUR_OF(target) == red")

    def test_invalid_resolver_argument_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("ASSERT POOL_OF(neighbour) != null")

    def test_missing_argument_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("ASSERT COLLABORATION_POOL_COUNT() < 2")

    def test_in_without_value_list_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("ASSERT TYPE_OF(target) IN pool")

    def test_assert_without_check_raises(self):
        with pytest.raises(ValueError):
            self.parser.parse_assertion("ASSERT")


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

    def test_plain_number_becomes_int(self):
        assert self.parser._parse_value("2") == 2

    def test_zero_padded_number_stays_a_string(self):
        """BPMN element IDs are zero-padded — '038' must not become 38."""
        assert self.parser._parse_value("038") == "038"


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
