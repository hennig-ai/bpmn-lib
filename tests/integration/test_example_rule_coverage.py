"""
Integration test for the rule coverage example in examples/.

Two guarantees are locked in here:
1. The example model is valid — it passes the full rule set without a finding.
2. Every rule of the catalogue is actually evaluated against it at least once.

The second one is the point of the model. A rule that selects no element reports
success without having looked at anything; before this model existed, 15 of 48 rules
were in exactly that state.

The rule catalogue and the schema live in the bpmn-modeling skill repository. Set
BPMN_METADATA_DIR to its `references` directory; the tests skip when it is absent.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

from bpmn_lib.navigator.navigator_factory import create_navigator
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.validation.rule_engine import BPMNRuleEngine
from bpmn_lib.validation.rule_store import build_rule_store

_DEFAULT_METADATA_DIR = r"C:\MyData\enviroments\skills\bpmn-modeling\references"

_EXAMPLE_MODEL = Path(__file__).parent.parent.parent / "examples" / "bpmn-rule-coverage-example.md"


def _metadata_dir() -> Path:
    """Directory holding bpmn-schema.md, bpmn-hierarchy.md and rules/."""
    return Path(os.environ.get("BPMN_METADATA_DIR", _DEFAULT_METADATA_DIR))


def _require_metadata() -> Path:
    """Skip the test when the schema and rule catalogue are not available."""
    metadata_dir = _metadata_dir()

    if not (metadata_dir / "bpmn-schema.md").exists():
        pytest.skip(f"BPMN metadata not found at {metadata_dir}; set BPMN_METADATA_DIR")

    return metadata_dir


class _CountingRuleEngine(BPMNRuleEngine):
    """Rule engine that records how often an assertion was actually evaluated."""

    def __init__(self, navigator: Any, val_result: ValidationResult) -> None:
        super().__init__(navigator, val_result)
        self.evaluations: int = 0

    def _evaluate_assertion(self, context: Any, assertion: Any, rule: Dict[str, Any]) -> None:
        self.evaluations += 1
        super()._evaluate_assertion(context, assertion, rule)


def _collect_rules(store: Any) -> List[Dict[str, Any]]:
    """Read all rules out of the rule store container."""
    rules: List[Dict[str, Any]] = []
    iterator = store.create_iterator()
    fields = store.get_list_of_fields_as_ref()

    while not iterator.is_empty():
        rules.append({field: iterator.value(field) for field in fields})
        iterator.pp()

    return rules


class TestRuleCoverageExample:

    def test_example_model_passes_the_full_rule_set(self, tmp_path: Path) -> None:
        """The model is a positive fixture: no rule may report a finding."""
        metadata_dir = _require_metadata()

        create_navigator(
            schema_file=str(metadata_dir / "bpmn-schema.md"),
            data_file=str(_EXAMPLE_MODEL),
            hierarchy_file=str(metadata_dir / "bpmn-hierarchy.md"),
            report_target=str(tmp_path),
            rules_dir=str(metadata_dir / "rules"),
            validation_level="best_practice",
        )

    def test_every_rule_is_evaluated_at_least_once(self, tmp_path: Path) -> None:
        """No rule may silently select nothing — that would report success blindly."""
        metadata_dir = _require_metadata()

        navigator = create_navigator(
            schema_file=str(metadata_dir / "bpmn-schema.md"),
            data_file=str(_EXAMPLE_MODEL),
            hierarchy_file=str(metadata_dir / "bpmn-hierarchy.md"),
            report_target=str(tmp_path),
        )
        rules = _collect_rules(build_rule_store(str(metadata_dir / "rules"), navigator))

        assert len(rules) > 0

        never_evaluated: Set[str] = set()
        for rule in rules:
            engine = _CountingRuleEngine(navigator, ValidationResult())
            engine._evaluate_rule(rule)
            if engine.evaluations == 0:
                never_evaluated.add(str(rule["rule_id"]))

        assert never_evaluated == set(), (
            f"These rules are never evaluated against the coverage model and are therefore "
            f"unproven: {sorted(never_evaluated)}. Extend examples/bpmn-rule-coverage-example.md "
            f"so each of them selects at least one element."
        )
