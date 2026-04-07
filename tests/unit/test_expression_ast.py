"""Unit tests for expression_ast.py AST dataclasses."""

import pytest

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


class TestCountAssertion:

    def test_create_single_flow(self):
        ca = CountAssertion(flows=["outgoing_flows"], operator=">=", number=1)
        assert ca.flows == ["outgoing_flows"]
        assert ca.operator == ">="
        assert ca.number == 1

    def test_frozen(self):
        ca = CountAssertion(flows=["outgoing_flows"], operator=">=", number=1)
        with pytest.raises(AttributeError):
            ca.number = 2  # type: ignore[misc]


class TestForEachAssertion:

    def test_create_nested(self):
        check = Check(
            terms=[CheckTerm(attribute_name="condition_expression", operator="!=", value=None)],
            combinator=None,
        )
        fa = ForEachAssertion(flow="outgoing_flows", check=check)
        assert fa.flow == "outgoing_flows"
        assert len(fa.check.terms) == 1
        assert fa.check.terms[0].attribute_name == "condition_expression"


class TestExistsAssertion:

    def test_create(self):
        check = Check(
            terms=[CheckTerm(attribute_name="is_default", operator="==", value=True)],
            combinator=None,
        )
        ea = ExistsAssertion(flow="outgoing_flows", check=check)
        assert ea.flow == "outgoing_flows"
        assert ea.check.terms[0].value is True


class TestCombinedAssertion:

    def test_create_with_two_assertions(self):
        left = CountAssertion(flows=["outgoing_flows"], operator=">=", number=1)
        right = CountAssertion(flows=["incoming_flows"], operator=">=", number=1)
        ca = CombinedAssertion(left=left, right=right)
        assert isinstance(ca.left, CountAssertion)
        assert isinstance(ca.right, CountAssertion)


class TestCheckTerm:

    def test_with_none_value(self):
        ct = CheckTerm(attribute_name="condition_expression", operator="!=", value=None)
        assert ct.value is None

    def test_with_bool_value(self):
        ct = CheckTerm(attribute_name="is_default", operator="==", value=True)
        assert ct.value is True


class TestCheck:

    def test_single_term_no_combinator(self):
        check = Check(
            terms=[CheckTerm(attribute_name="a", operator="==", value="x")],
            combinator=None,
        )
        assert check.combinator is None
        assert len(check.terms) == 1

    def test_multiple_terms_with_and(self):
        check = Check(
            terms=[
                CheckTerm(attribute_name="a", operator="==", value="x"),
                CheckTerm(attribute_name="b", operator="!=", value=None),
            ],
            combinator="AND",
        )
        assert check.combinator == "AND"
        assert len(check.terms) == 2


class TestWhereClause:

    def test_where_equals(self):
        we = WhereEquals(attribute_name="element_type", value="exclusiveGateway")
        assert we.attribute_name == "element_type"
        assert we.value == "exclusiveGateway"

    def test_where_not_in(self):
        wni = WhereNotIn(attribute_name="element_type", values=["startEvent", "endEvent"])
        assert wni.attribute_name == "element_type"
        assert wni.values == ["startEvent", "endEvent"]

    def test_frozen_where_not_in(self):
        wni = WhereNotIn(attribute_name="element_type", values=["startEvent"])
        with pytest.raises(AttributeError):
            wni.attribute_name = "foo"  # type: ignore[misc]


class TestTypeAliases:

    def test_assertion_union_importable(self):
        """Assertion type alias is importable and usable."""
        a: Assertion = CountAssertion(flows=["outgoing_flows"], operator=">=", number=1)
        assert isinstance(a, CountAssertion)

    def test_where_clause_optional_union(self):
        """WhereClause is Optional[Union[...]]."""
        wc: WhereClause = None
        assert wc is None
        wc = WhereEquals(attribute_name="x", value="y")
        assert isinstance(wc, WhereEquals)
