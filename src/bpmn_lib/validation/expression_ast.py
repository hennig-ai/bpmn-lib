"""AST data types for parsed BPMN validation rule expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Union


@dataclass(frozen=True)
class CountAssertion:
    """COUNT(flow) operator number — or COUNT(f1) + COUNT(f2) operator number."""

    flows: List[str]
    operator: str
    number: int


@dataclass(frozen=True)
class CheckTerm:
    """Single comparison: attribute_name operator value."""

    attribute_name: str
    operator: str
    value: Any


@dataclass(frozen=True)
class Check:
    """One or more CheckTerms combined with AND/OR."""

    terms: List[CheckTerm]
    combinator: Optional[str]


@dataclass(frozen=True)
class ForEachAssertion:
    """FOR_EACH flow: check — every flow must satisfy the check."""

    flow: str
    check: Check


@dataclass(frozen=True)
class ExistsAssertion:
    """EXISTS flow: check — at least one flow must satisfy the check."""

    flow: str
    check: Check


@dataclass(frozen=True)
class CombinedAssertion:
    """Two assertions combined with AND."""

    left: Assertion
    right: Assertion


Assertion = Union[CountAssertion, ForEachAssertion, ExistsAssertion, CombinedAssertion]


@dataclass(frozen=True)
class WhereEquals:
    """Where-clause: attribute_name == value."""

    attribute_name: str
    value: str


@dataclass(frozen=True)
class WhereNotIn:
    """Where-clause: attribute_name NOT IN (v1, v2, ...)."""

    attribute_name: str
    values: List[str]


WhereClause = Optional[Union[WhereEquals, WhereNotIn]]
