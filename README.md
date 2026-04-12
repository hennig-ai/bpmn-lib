# bpmn-lib

A Python library for navigating BPMN (Business Process Model and Notation) element hierarchies and managing in-memory process data. Designed for use in Python-based process automation and tooling pipelines.

## What it does

bpmn-lib lets you load a BPMN process model from markdown-defined schemas and data files, then navigate it programmatically:

- Traverse parent-child element hierarchies (e.g. `bpmn_element → activity → task → user_task`)
- Navigate sequence flows forward and backward
- Query element attributes across inheritance chains
- Resolve data associations (inputs/outputs)
- Validate element type specificity and FK constraints

## Installation

```bash
pip install bpmn-lib
```

Install with dev dependencies (for development):

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from bpmn_lib.navigator.navigator_factory import create_navigator

navigator = create_navigator(
    schema_file="path/to/schema.md",
    data_file="path/to/data.md",
    hierarchy_file="path/to/hierarchy.md",
    log_dir="path/to/logs",
    schema_name="My BPMN Schema"
)

# Navigate sequence flows
next_elements = navigator.next_elements_in_flow(element_id)

# Get all elements in a process
elements = navigator.get_process_elements(process_id)

# Read an attribute (traverses inheritance chain)
value = navigator.get_element_attribute(element_id, "attribute_name")
```

## Architecture

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

Schema definitions and instance data are loaded from markdown files. The factory function orchestrates the full pipeline: parse schema → load data → validate constraints → build indexes → create navigator.

## Testing

```bash
pytest tests/ -v
pytest tests/unit/ -v
pytest tests/integration/ -v
pytest tests/ --cov=bpmn_lib --cov-report=term-missing
```

## License

MIT — see [LICENSE](LICENSE)
