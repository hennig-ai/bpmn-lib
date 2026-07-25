"""AST data types for parsed BPMN validation rule expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

# Resolver functions usable inside check terms: name -> number of arguments (0 or 1).
# The parser validates against this table, the rule engine implements the lookups.
RESOLVER_ARITY: Dict[str, int] = {
    "POOL_OF": 1,
    "TYPE_OF": 1,
    "PROCESS_OF": 1,
    "COLLABORATION_OF": 1,
    "EXPECTED_MESSAGE_OF": 1,
    "POOL_MESSAGE_FLOW_COUNT": 1,
    "COLLABORATION_POOL_COUNT": 1,
}

# Element references accepted as a resolver argument.
# 'attached' is the activity a boundary event is attached to.
RESOLVER_ARGUMENTS: List[str] = ["self", "source", "target", "attached"]


@dataclass(frozen=True)
class CountAssertion:
    """COUNT(flow) operator number — or COUNT(f1) + COUNT(f2) operator number."""

    flows: List[str]
    operator: str
    number: int


@dataclass(frozen=True)
class AttributeOperand:
    """Reference to an attribute of the current flow info or element."""

    name: str


@dataclass(frozen=True)
class LiteralOperand:
    """A constant value: string, number, boolean or null."""

    value: Any


@dataclass(frozen=True)
class ValueListOperand:
    """A parenthesised list of literals, used as right operand of IN / NOT IN."""

    values: List[Any]


@dataclass(frozen=True)
class ResolverOperand:
    """Call of a resolver function, e.g. POOL_OF(target) or POOL_COUNT().

    The argument names the element the resolver applies to: 'self', 'source',
    'target' — or the empty string for resolvers that take no argument.
    """

    function: str
    argument: str


Operand = Union[AttributeOperand, LiteralOperand, ValueListOperand, ResolverOperand]


@dataclass(frozen=True)
class CheckTerm:
    """Single comparison: left operator right."""

    left: Operand
    operator: str
    right: Operand


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
class ElementAssertion:
    """ASSERT check — the check is evaluated against the selected element itself.

    Unlike COUNT/FOR_EACH/EXISTS this assertion does not iterate over flows. It is
    the only way to state something about the selected element, which is what rules
    on connecting elements (message flows) need.
    """

    check: Check


@dataclass(frozen=True)
class CombinedAssertion:
    """Two assertions combined with AND."""

    left: Assertion
    right: Assertion


Assertion = Union[
    CountAssertion, ForEachAssertion, ExistsAssertion, ElementAssertion, CombinedAssertion
]


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
