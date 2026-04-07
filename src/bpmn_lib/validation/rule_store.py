"""Rule store builder — loads and validates BPMN validation rules from Markdown files."""

from pathlib import Path
from typing import Any, Dict, List

from basic_framework.proc_frame import log_and_raise, log_msg
from basic_framework import MarkdownDocument
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.container_unique_indexed import ContainerUniqueIndexed
from basic_framework.container_utils.abstract_container import AbstractContainer

from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator
from bpmn_lib.validation.expression_parser import ExpressionParser

_REQUIRED_COLUMNS: List[str] = [
    "rule_id", "element_type", "subtype", "assertion", "where_clause", "level", "message_template",
]


def build_rule_store(rules_dir: str, navigator: BPMNHierarchyNavigator) -> AbstractContainer:
    """Load all Markdown rule files from a directory, validate, and return as container.

    Args:
        rules_dir: Path to directory containing *.md rule files.
        navigator: BPMNHierarchyNavigator for schema validation of rules.

    Returns:
        AbstractContainer with all validated rules.
    """
    md_files = sorted(Path(rules_dir).glob("*.md"))

    if len(md_files) == 0:
        log_and_raise(ValueError(
            f"No .md rule files found in directory: {rules_dir}"
        ))

    # Merge all rule rows into a single container
    merged_container: ContainerInMemory = ContainerInMemory()
    merged_container.init_new(_REQUIRED_COLUMNS, "rule_store", "rule_store")

    for md_file in md_files:
        log_msg(f"Lade Regeldatei: {md_file.name}")
        doc = MarkdownDocument()
        doc.load_from_file(str(md_file))

        table_dict = doc.create_table_dictionary()

        for _, source_container in table_dict.items():
            # Validate required columns exist
            _validate_columns(source_container, str(md_file))

            # Copy rows into merged container
            iterator = source_container.create_iterator()
            while not iterator.is_empty():
                row_idx = merged_container.add_empty_row()
                for col in _REQUIRED_COLUMNS:
                    merged_container.set_value(row_idx, col, iterator.value(col))
                iterator.pp()

    # Uniqueness check on rule_id across all files
    ContainerUniqueIndexed().init(merged_container, "rule_id")

    # Eager syntax validation (per D.2)
    _validate_syntax(merged_container)

    # Schema validation: Fail-fast for invalid rules
    _validate_schema(merged_container, navigator)

    log_msg(f"Rule store geladen: {_count_rows(merged_container)} Regeln aus {len(md_files)} Dateien.")
    return merged_container


def _validate_columns(container: AbstractContainer, file_path: str) -> None:
    """Validate that all required columns exist in the container."""
    for col in _REQUIRED_COLUMNS:
        if not container.field_exists(col):
            log_and_raise(ValueError(
                f"Required column '{col}' missing in rule file: {file_path}"
            ))


def _validate_syntax(container: ContainerInMemory) -> None:
    """Eagerly validate assertion and where_clause syntax for all rules."""
    parser = ExpressionParser()
    iterator = container.create_iterator()

    while not iterator.is_empty():
        rule_id = iterator.value("rule_id")
        assertion_text = iterator.value("assertion")
        where_text = iterator.value("where_clause")

        if assertion_text:
            try:
                parser.parse_assertion(str(assertion_text))
            except ValueError as e:
                log_and_raise(ValueError(
                    f"Syntax error in assertion of rule '{rule_id}': {e}"
                ))

        if where_text:
            try:
                parser.parse_where_clause(str(where_text))
            except ValueError as e:
                log_and_raise(ValueError(
                    f"Syntax error in where_clause of rule '{rule_id}': {e}"
                ))

        iterator.pp()


def _validate_schema(container: ContainerInMemory, navigator: BPMNHierarchyNavigator) -> None:
    """Validate all rules against database schema (fail-fast)."""
    iterator = container.create_iterator()

    while not iterator.is_empty():
        rule: Dict[str, Any] = {}
        fields = container.get_list_of_fields_as_ref()
        for field in fields:
            rule[field] = iterator.value(field)

        _validate_rule_schema(rule, navigator)
        iterator.pp()


def _validate_rule_schema(rule: Dict[str, Any], navigator: BPMNHierarchyNavigator) -> None:
    """Validate a single rule against database schema (fail-fast).

    Three-stage validation:
    1. Table validation: Does element_type exist as a table name?
       - Special case: 'flow_object' is allowed even though it's not a real table
       - flow_object represents an abstract union of (activity, event, gateway)

    2. Column validation: Does {element_type}_type column exist (if subtype specified)?
       - Only checked for concrete types (not flow_object)
       - Example: element_type='event', subtype='start' → checks for 'event_type' column
       - Skipped if subtype is empty

    3. Value validation: Is subtype value in the value domain of {element_type}_type?
       - Only checked for concrete types (not flow_object)
       - Example: element_type='gateway', subtype='parallel' → validates 'parallel' in gateway_type domain
       - Skipped if subtype is empty

    Note on flow_object:
    - flow_object rules ignore subtype filtering (subtype is not validated)
    - The rule applies to ALL flow objects (activities, events, gateways)
    - Use flow_object for cross-cutting rules that apply uniformly to all element types
    """
    rule_id = str(rule["rule_id"])
    element_type = str(rule["element_type"])

    if "subtype" not in rule:
        log_and_raise(ValueError(
            f"Rule '{rule_id}': required column 'subtype' is missing"
        ))

    subtype = str(rule["subtype"]).strip()

    schema = navigator.get_schema()

    # Stage 1: Table validation (special case: 'flow_object' is allowed even though it's not a real table)
    if element_type != "flow_object" and not schema.has_table(element_type):
        log_and_raise(ValueError(
            f"Rule '{rule_id}': element_type '{element_type}' is not a valid table name in schema"
        ))

    # Stage 2 & 3: Only if subtype is specified (exception: flow_object)
    if element_type != "flow_object" and subtype:
        column_name = f"{element_type}_type"
        table_def = schema.get_table_definition(element_type)

        # Stage 2: Column validation
        if not table_def.has_column(column_name):
            log_and_raise(ValueError(
                f"Rule '{rule_id}': column '{column_name}' not found in table '{element_type}'"
            ))

        # Stage 3: Value validation
        if not table_def.is_value_in_domain(column_name, subtype):
            log_and_raise(ValueError(
                f"Rule '{rule_id}': subtype value '{subtype}' not in domain of column '{column_name}'"
            ))


def _count_rows(container: ContainerInMemory) -> int:
    """Count rows in a container."""
    count = 0
    iterator = container.create_iterator()
    while not iterator.is_empty():
        count += 1
        iterator.pp()
    return count
