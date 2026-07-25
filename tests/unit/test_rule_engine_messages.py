"""Unit tests for message-related rule evaluation in BPMNRuleEngine.

Covers the additions needed by the MSG/MFL/POL rule catalogue:
- message flows and message_event_definitions as iterable items
- the ASSERT assertion, which checks the selected element itself
- resolver functions (POOL_OF, TYPE_OF, EXPECTED_MESSAGE_OF, POOL_COUNT,
  POOL_MESSAGE_FLOW_COUNT) and the source/target references they take
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest
from unittest.mock import Mock

from basic_framework.container_utils.container_in_memory import ContainerInMemory

from bpmn_lib.navigator.bpmn_hierarchy_navigator import (
    BPMNHierarchyNavigator,
    IncomingMessageFlowInfo,
    MessageDefinitionInfo,
    MessageEventDefinitionInfo,
    OutgoingMessageFlowInfo,
    OutgoingSequenceFlowInfo,
)
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.rule_engine import BPMNRuleEngine


_RULE_COLUMNS: List[str] = [
    "rule_id", "element_type", "subtype", "assertion",
    "where_clause", "level", "message_template", "personal",
]


def _make_rule_store(rules: List[Dict[str, Any]]) -> ContainerInMemory:
    """Build a ContainerInMemory with rule data."""
    store = ContainerInMemory()
    store.init_new(_RULE_COLUMNS, "test_rules", "test_rules")
    for rule in rules:
        idx = store.add_empty_row()
        for col in _RULE_COLUMNS:
            store.set_value(idx, col, rule.get(col, ""))
    return store


def _make_engine(
    element_ids_by_type: Optional[Dict[str, List[str]]] = None,
    element_attrs: Optional[Dict[str, Dict[str, Any]]] = None,
    outgoing_message_flows: Optional[Dict[str, List[OutgoingMessageFlowInfo]]] = None,
    incoming_message_flows: Optional[Dict[str, List[IncomingMessageFlowInfo]]] = None,
    message_event_definitions: Optional[Dict[str, List[MessageEventDefinitionInfo]]] = None,
    expected_messages: Optional[Dict[str, MessageDefinitionInfo]] = None,
    pools: Optional[Dict[str, Optional[str]]] = None,
) -> Tuple[BPMNRuleEngine, ValidationResult]:
    """Create a BPMNRuleEngine with a navigator mock covering the message API."""
    navigator = Mock(spec=BPMNHierarchyNavigator)
    val_result = ValidationResult()

    ids_by_type = element_ids_by_type if element_ids_by_type is not None else {}
    attrs = element_attrs if element_attrs is not None else {}
    outgoing = outgoing_message_flows if outgoing_message_flows is not None else {}
    incoming = incoming_message_flows if incoming_message_flows is not None else {}
    definitions = message_event_definitions if message_event_definitions is not None else {}
    expected = expected_messages if expected_messages is not None else {}
    pool_map = pools if pools is not None else {}

    navigator.get_element_ids_by_type.side_effect = lambda t: ids_by_type.get(t, [])
    navigator.get_element_attribute.side_effect = lambda eid, attr: attrs.get(eid, {}).get(attr)
    navigator.get_outgoing_message_flows.side_effect = lambda eid: outgoing.get(eid, [])
    navigator.get_incoming_message_flows.side_effect = lambda eid: incoming.get(eid, [])
    navigator.get_message_event_definitions.side_effect = lambda eid: definitions.get(eid, [])
    navigator.get_message_definition_for_event.side_effect = lambda eid: expected.get(eid)
    navigator.get_pool_of_element.side_effect = lambda eid: pool_map.get(eid)
    navigator.get_outgoing_sequence_flows.side_effect = lambda eid: []
    navigator.get_incoming_sequence_flows.side_effect = lambda eid: []

    return BPMNRuleEngine(navigator, val_result), val_result


def _run(engine: BPMNRuleEngine, rule: Dict[str, Any], level: str = "spec_v2") -> None:
    """Run a single rule through the engine, parser included."""
    complete_rule = {"rule_id": "TST-001", "level": level, "message_template": "violated {element_id} {flow_id}"}
    complete_rule.update(rule)
    engine.validate(_make_rule_store([complete_rule]), level)


# ==================== Iterable items ====================

class TestGetFlows:
    """Test the item sources a COUNT/FOR_EACH/EXISTS can iterate over."""

    def test_outgoing_message_flows(self):
        flow = OutgoingMessageFlowInfo("036", "038", "002")
        engine, _ = _make_engine(outgoing_message_flows={"003": [flow]})

        assert engine._get_flows("003", "outgoing_message_flows") == [flow]

    def test_incoming_message_flows(self):
        flow = IncomingMessageFlowInfo("044", "040", "001")
        engine, _ = _make_engine(incoming_message_flows={"002": [flow]})

        assert engine._get_flows("002", "incoming_message_flows") == [flow]

    def test_message_event_definitions(self):
        definition = MessageEventDefinitionInfo("001", "001", None)
        engine, _ = _make_engine(message_event_definitions={"002": [definition]})

        assert engine._get_flows("002", "message_event_definitions") == [definition]

    def test_unknown_name_raises(self):
        engine, _ = _make_engine()

        with pytest.raises(Exception):
            engine._get_flows("002", "telepathy")


class TestItemIdentifier:
    """Test the {flow_id} placeholder source for each iterable item type."""

    def test_sequence_flow(self):
        engine, _ = _make_engine()
        assert engine._item_identifier(OutgoingSequenceFlowInfo("010", "004", None, False)) == "010"

    def test_message_flow(self):
        engine, _ = _make_engine()
        assert engine._item_identifier(IncomingMessageFlowInfo("044", "040", "001")) == "044"

    def test_message_event_definition(self):
        engine, _ = _make_engine()
        assert engine._item_identifier(MessageEventDefinitionInfo("001", "001", None)) == "001"

    def test_unknown_item_type_raises(self):
        """A FOR_EACH over an item without a known id must fail loudly, not silently."""
        engine, _ = _make_engine()

        with pytest.raises(Exception):
            engine._item_identifier(object())

    def test_message_flow_id_lands_in_message(self):
        """Regression: the placeholder used to be hardcoded to sequence_flow_id."""
        engine, result = _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message"}},
            incoming_message_flows={"002": [IncomingMessageFlowInfo("044", "040", "002")]},
            expected_messages={"002": MessageDefinitionInfo("001", "Invoice", None)},
        )

        _run(engine, {
            "element_type": "event",
            "where_clause": "event_definition_type == message",
            "assertion": "FOR_EACH incoming_message_flows: message_definition_id == EXPECTED_MESSAGE_OF(self)",
        })

        assert result.get_messages() == ["violated 002 044"]


# ==================== ASSERT ====================

class TestElementAssertion:
    """Test ASSERT, which checks the selected element instead of its flows."""

    def _engine_with_message_flow(self, source_pool: Optional[str], target_pool: Optional[str]):
        return _make_engine(
            element_ids_by_type={"message_flow": ["036"]},
            element_attrs={
                "036": {"source_bpmn_element_id": "003", "target_bpmn_element_id": "038"},
            },
            pools={"003": source_pool, "038": target_pool},
        )

    def test_cross_pool_flow_passes(self):
        engine, result = self._engine_with_message_flow("025", "038")

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT POOL_OF(source) != POOL_OF(target)",
        })

        assert result.get_messages() == []

    def test_same_pool_flow_fails(self):
        engine, result = self._engine_with_message_flow("025", "025")

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT POOL_OF(source) != POOL_OF(target)",
        })

        assert result.get_messages() == ["violated 036 "]

    def test_model_without_pools_fails(self):
        """Both ends unresolvable means the flow crosses no pool boundary either."""
        engine, result = self._engine_with_message_flow(None, None)

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT POOL_OF(source) != POOL_OF(target)",
        })

        assert len(result.get_messages()) == 1

    def test_element_attribute_is_read_from_the_element(self):
        """Without a flow info, a bare attribute refers to the selected element."""
        engine, result = _make_engine(
            element_ids_by_type={"message_flow": ["036"]},
            element_attrs={"036": {"message_definition_id": None}},
        )

        _run(engine, {
            "element_type": "message_flow",
            "assertion": 'ASSERT message_definition_id != null AND message_definition_id != ""',
        })

        assert len(result.get_messages()) == 1


class TestTypeResolver:
    """Test TYPE_OF combined with IN / NOT IN."""

    def _engine(self, target_type: str):
        return _make_engine(
            element_ids_by_type={"message_flow": ["036"]},
            element_attrs={
                "036": {"source_bpmn_element_id": "003", "target_bpmn_element_id": "038"},
                "038": {"element_type": target_type},
            },
        )

    def test_allowed_target_type_passes(self):
        engine, result = self._engine("pool")

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT TYPE_OF(target) IN (pool, event, user_task)",
        })

        assert result.get_messages() == []

    def test_forbidden_target_type_fails(self):
        engine, result = self._engine("gateway")

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT TYPE_OF(target) IN (pool, event, user_task)",
        })

        assert len(result.get_messages()) == 1

    def test_not_in_is_the_inverse(self):
        engine, result = self._engine("gateway")

        _run(engine, {
            "element_type": "message_flow",
            "assertion": "ASSERT TYPE_OF(target) NOT IN (gateway, data_object)",
        })

        assert len(result.get_messages()) == 1


# ==================== Message type matching ====================

class TestExpectedMessageResolver:
    """Test EXPECTED_MESSAGE_OF, which resolves event -> expected message type."""

    def _engine(self, delivered: Optional[str], expected: Optional[MessageDefinitionInfo]):
        return _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message"}},
            incoming_message_flows={"002": [IncomingMessageFlowInfo("044", "040", delivered)]},
            expected_messages={"002": expected} if expected is not None else {},
        )

    _ASSERTION = (
        "FOR_EACH incoming_message_flows: "
        "message_definition_id == EXPECTED_MESSAGE_OF(self) OR EXPECTED_MESSAGE_OF(self) == null"
    )

    def _run_match_rule(self, engine: BPMNRuleEngine) -> None:
        _run(engine, {
            "element_type": "event",
            "where_clause": "event_definition_type == message",
            "assertion": self._ASSERTION,
        })

    def test_matching_type_passes(self):
        engine, result = self._engine("001", MessageDefinitionInfo("001", "Invoice", None))
        self._run_match_rule(engine)
        assert result.get_messages() == []

    def test_mismatching_type_fails(self):
        engine, result = self._engine("002", MessageDefinitionInfo("001", "Invoice", None))
        self._run_match_rule(engine)
        assert result.get_messages() == ["violated 002 044"]

    def test_event_without_expected_type_is_skipped(self):
        """The missing type is MSG-003's finding — this rule must not report it twice."""
        engine, result = self._engine("002", None)
        self._run_match_rule(engine)
        assert result.get_messages() == []


# ==================== Pool resolvers ====================

class TestPoolResolvers:
    """Test POOL_COUNT and POOL_MESSAGE_FLOW_COUNT."""

    def _engine(self, pool_ids: List[str], flow_attrs: Dict[str, Dict[str, Any]],
                pools: Dict[str, Optional[str]]):
        attrs: Dict[str, Dict[str, Any]] = dict(flow_attrs)
        return _make_engine(
            element_ids_by_type={"pool": pool_ids, "message_flow": sorted(flow_attrs.keys())},
            element_attrs=attrs,
            pools=pools,
        )

    _ASSERTION = "ASSERT POOL_COUNT() < 2 OR POOL_MESSAGE_FLOW_COUNT(self) >= 1"

    def test_single_pool_is_never_reported(self):
        """A one-participant model legitimately has no message flows."""
        engine, result = self._engine(["025"], {}, {})

        _run(engine, {"element_type": "pool", "assertion": self._ASSERTION}, "best_practice")

        assert result.get_messages() == []

    def test_participating_pool_passes(self):
        engine, result = self._engine(
            ["025", "038"],
            {"036": {"source_bpmn_element_id": "003", "target_bpmn_element_id": "038"}},
            {"003": "025", "038": "038"},
        )

        _run(engine, {"element_type": "pool", "assertion": self._ASSERTION}, "best_practice")

        assert result.get_messages() == []

    def test_isolated_pool_is_reported(self):
        engine, result = self._engine(
            ["025", "038", "045"],
            {"036": {"source_bpmn_element_id": "003", "target_bpmn_element_id": "038"}},
            {"003": "025", "038": "038"},
        )

        _run(engine, {"element_type": "pool", "assertion": self._ASSERTION}, "best_practice")

        assert result.get_messages() == ["violated 045 "]

    def test_membership_counts_indirectly(self):
        """A pool participates when an element inside it sends, not only the pool itself."""
        engine, result = self._engine(
            ["025", "038"],
            {"044": {"source_bpmn_element_id": "040", "target_bpmn_element_id": "002"}},
            {"040": "038", "002": "025"},
        )

        _run(engine, {"element_type": "pool", "assertion": self._ASSERTION}, "best_practice")

        assert result.get_messages() == []


# ==================== source / target references ====================

class TestElementReferences:
    """Test how 'self', 'source' and 'target' resolve in the two contexts."""

    def test_source_and_target_come_from_the_flow_inside_for_each(self):
        engine, result = _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message"}},
            incoming_message_flows={"002": [IncomingMessageFlowInfo("044", "040", "001")]},
            pools={"040": "038"},
        )

        _run(engine, {
            "element_type": "event",
            "where_clause": "event_definition_type == message",
            "assertion": 'FOR_EACH incoming_message_flows: POOL_OF(source) == "038"',
        })

        assert result.get_messages() == []

    def test_reference_unavailable_on_flow_info_raises(self):
        """An incoming flow has no target — asking for it must fail loudly."""
        engine, _ = _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message"}},
            incoming_message_flows={"002": [IncomingMessageFlowInfo("044", "040", "001")]},
        )

        with pytest.raises(Exception):
            _run(engine, {
                "element_type": "event",
                "where_clause": "event_definition_type == message",
                "assertion": 'FOR_EACH incoming_message_flows: POOL_OF(target) == "038"',
            })


# ==================== COUNT over message flows ====================

class TestCountOverMessageFlows:
    """Test the plain COUNT rules of the MSG catalogue."""

    def test_catching_event_without_sender_is_reported(self):
        engine, result = _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message", "event_type": "start"}},
        )

        _run(engine, {
            "element_type": "event",
            "subtype": "start",
            "where_clause": "event_definition_type == message",
            "assertion": "COUNT(incoming_message_flows) >= 1",
        }, "best_practice")

        assert result.get_messages() == ["violated 002 "]

    def test_non_message_event_on_a_message_flow_is_reported(self):
        engine, result = _make_engine(
            element_ids_by_type={"event": ["041"]},
            element_attrs={"041": {"event_definition_type": "none"}},
            incoming_message_flows={"041": [IncomingMessageFlowInfo("036", "003", "002")]},
        )

        _run(engine, {
            "element_type": "event",
            "where_clause": "event_definition_type NOT IN (message)",
            "assertion": "COUNT(incoming_message_flows) + COUNT(outgoing_message_flows) == 0",
        })

        assert result.get_messages() == ["violated 041 "]

    def test_missing_message_event_definition_is_reported(self):
        engine, result = _make_engine(
            element_ids_by_type={"event": ["002"]},
            element_attrs={"002": {"event_definition_type": "message"}},
        )

        _run(engine, {
            "element_type": "event",
            "where_clause": "event_definition_type == message",
            "assertion": "COUNT(message_event_definitions) >= 1",
        })

        assert result.get_messages() == ["violated 002 "]
