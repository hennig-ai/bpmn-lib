# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**bpmn-lib** is a BPMN (Business Process Model and Notation) Process Navigation Library for navigating BPMN element hierarchies and managing in-memory database operations.

## Development Commands

```bash
# Install
pip install -e .              # Standard
pip install -e ".[dev]"       # With dev dependencies (pytest, pyright, etc.)

# Tests
pytest tests/ -v                                          # All tests
pytest tests/unit/ -v                                     # Unit tests only
pytest tests/integration/ -v                              # Integration tests only
pytest tests/unit/test_bpmn_hierarchy_navigator.py -v     # Single file
pytest tests/unit/test_bpmn_hierarchy_navigator.py::test_name -v  # Single test
pytest tests/ --cov=bpmn_lib --cov-report=term-missing    # With coverage

# Type checking (strict mode)
pyright
```

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs on push/PR to `main`:
1. **Test matrix**: Python 3.10, 3.11, 3.12, 3.13 — runs `pytest tests/ -v` + `pyright`
2. **Auto version bump**: Increments patch version in `pyproject.toml` on main push

## Architecture

### Layer Structure

```
┌─────────────────────────────────────┐
│         Navigation Layer            │  ← BPMNHierarchyNavigator
├─────────────────────────────────────┤
│     Database Instance Layer         │  ← DatabaseInstance, DatabaseBuilder
├─────────────────────────────────────┤
│       Database Schema Layer         │  ← DatabaseSchema, TableDefinition
├─────────────────────────────────────┤
│          Parsing Layer              │  ← DatabaseSchemaParser, MarkdownDocument
└─────────────────────────────────────┘
```

### Primary Entry Point

Use `create_navigator()` factory function for standard usage:

```python
from bpmn_lib.navigator.navigator_factory import create_navigator

navigator = create_navigator(
    schema_file="path/to/schema.md",      # Table definitions, constraints
    data_file="path/to/data.md",          # BPMN element instances
    hierarchy_file="path/to/hierarchy.md", # Parent-child relationships
    report_target="path/to/logs",         # Optional: str (dir) or TextIO for validation reports
)
```

The factory orchestrates the complete pipeline: parse schema → load data → validate constraints → build indexes → create navigator. Schema name is derived from `schema_file` filename.

### Navigator API

Key methods for process navigation:

```python
# Get all elements in a process
element_ids = navigator.get_process_elements(process_id)

# Get start events for a process
start_events = navigator.get_all_start_events(process_id)

# Navigate sequence flows
next_ids = navigator.next_elements_in_flow(element_id)      # Forward
prev_ids = navigator.previous_elements_in_flow(element_id)  # Backward

# Detailed flow info with conditions
outgoing = navigator.get_outgoing_sequence_flows(element_id)  # → OutgoingSequenceFlowInfo
incoming = navigator.get_incoming_sequence_flows(element_id)  # → IncomingSequenceFlowInfo

# Data associations
inputs = navigator.get_data_inputs(element_id)
outputs = navigator.get_data_outputs(element_id)

# Element attributes (traverses hierarchy)
value = navigator.get_element_attribute(element_id, "attribute_name")

# Type checking
is_activity = navigator.is_element_descendant_of(element_id, "activity")
```

### Key Concepts

**Hierarchical Inheritance**: BPMN elements specialize through table hierarchies (e.g., `bpmn_element` → `activity` → `task` → `user_task`). The navigator traverses these hierarchies to find element attributes. FK column naming convention: `<parent_table>_id`.

**Element Type Specificity**: `_validate_element_type_specificity()` ensures that an element's `element_type` is the most specific type in its inheritance chain. Validation runs in two phases: (A) inheritance chain consistency, (B) type specificity check.

**Markdown-Driven Data**: Schema definitions and instance data are parsed from markdown documents. Tables are extracted from markdown sections using `MarkdownDocument`. Three files are required: schema, data, hierarchy.

### Dependencies

This library depends on `basic_framework` which provides:
- `log_msg()`, `log_and_raise()` - Logging utilities
- `ContainerInMemory`, `AbstractContainer`, `AbstractIterator` - Data structures
- `ConditionEquals` - Query conditions
- `KnotObject` - Tree node structure for markdown parsing

## Internal Documentation

`docs/internal/` contains design documents, validation rules, and solution designs. See `docs/internal/CLAUDE.md` for structure details.

For BPMN navigator validation requirements (in development):
- Requirements overview: `docs/internal/features_or_solution_designs/20260405_BPMN-validation/requirements-bpmn-navigator-validation.md`
- Process rules (`PRC-xxx`): `req_list/req_for_process/` — rules that validate a process as a whole
- Element rules (per element type): `req_list/req_for_elements/` — rules that validate an element and its surroundings
- Validation levels: `basic` (minimal) < `spec_v2` (BPMN 2.0.2 conformance) < `best_practice` (recommendations beyond spec)
- Rule ID prefixes encode element type: `PRC` (Process), `FLO` (Flow-Object), `SRT` (Start-Event), `END` (End-Event), `XOR` (Exclusive Gateway), `AND` (Parallel Gateway)

## Testing Infrastructure

Test fixtures in `tests/conftest.py` include:
- `NavigatorTestDataBuilder`: Fluent builder for creating mock navigator test data
- Markdown content fixtures for schema and instance data
- Custom pytest markers: `unit`, `integration`, `slow`, `network`
