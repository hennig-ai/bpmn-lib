"""
BPMN Process Navigation Library.

This library provides classes for navigating and managing BPMN element hierarchies,
database schema definitions, and validation utilities.
"""

from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.schema.database_schema_parser import DatabaseSchemaParser
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship
from bpmn_lib.utils.validation_result import ValidationResult
from basic_framework import MarkdownDocument

__all__ = [
    # Navigator
    "BPMNHierarchyNavigator",
    # Database Instance
    "DatabaseInstance",
    "DatabaseBuilder",
    # Database Schema
    "DatabaseSchema",
    "DatabaseSchemaParser",
    "TableDefinition",
    "ColumnDefinition",
    "ForeignKeyRelationship",
    # Utils
    "ValidationResult",
    "MarkdownDocument",
]

from importlib.metadata import version

__version__: str = version("bpmn-lib")
