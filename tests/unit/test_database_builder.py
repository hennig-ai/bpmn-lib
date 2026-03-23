"""
Unit tests for DatabaseBuilder class.

Tests cover:
- Builder initialization
- Schema initialization
- Data loading phases
- Constraint validation
- Index building
- Database instance creation
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.instance.database_bulk_validator import DatabaseBulkValidator
from bpmn_lib.database.instance.database_index_builder import DatabaseIndexBuilder
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from tests.utils.test_helpers import DatabaseSchemaFactory, TableDefinitionFactory


class TestDatabaseBuilderBasics:
    """Test basic DatabaseBuilder functionality."""

    def test_builder_initialization(self):
        """Test DatabaseBuilder initialization."""
        # Create schema and validation result
        schema = DatabaseSchemaFactory.create_with_tables()
        val_result = Mock(spec=ValidationResult)

        # Initialize builder with new single-phase pattern
        builder = DatabaseBuilder(schema, val_result)

        assert builder is not None
        assert builder._schema == schema
        assert builder._instance is not None
        assert builder._bulk_validator is not None
        assert builder._index_builder is not None

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_builder_init_with_schema(self, mock_log, mock_index_builder_class,
                                      mock_bulk_validator_class, mock_instance_class):
        """Test initializing builder with schema."""
        # Create mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create schema and validation result
        schema = DatabaseSchemaFactory.create_with_tables()
        val_result = Mock(spec=ValidationResult)

        # Initialize builder with new single-phase pattern
        builder = DatabaseBuilder(schema, val_result)

        # Verify initialization
        assert builder._schema == schema
        assert builder._instance == mock_instance
        assert builder._bulk_validator == mock_bulk_validator
        assert builder._index_builder == mock_index_builder


class TestDatabaseBuilderPhases:
    """Test multi-phase database construction."""

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_validate_constraints_phase(self, mock_log, mock_index_builder_class,
                                        mock_bulk_validator_class, mock_instance_class):
        """Test constraint validation phase."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override bulk_validator for test-specific behavior
        builder._bulk_validator = mock_bulk_validator

        # Validate constraints
        builder.validate_all_constraints()

        # Verify validator was called
        builder._bulk_validator.validate_all.assert_called_once()

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_build_indexes_phase(self, mock_log, mock_index_builder_class,
                                  mock_bulk_validator_class, mock_instance_class):
        """Test index building phase."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override index_builder for test-specific behavior
        builder._index_builder = mock_index_builder

        # Build indexes
        builder.build_indexes_if_valid()

        # Verify index builder was called
        builder._index_builder.build_all_indexes.assert_called_once()

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_create_read_only_instance(self, mock_log, mock_index_builder_class,
                                        mock_bulk_validator_class, mock_instance_class):
        """Test creating read-only database instance."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override instance for test-specific behavior
        builder._instance = mock_instance
        builder._instance.is_finalized.return_value = True
        builder._instance.set_read_only.return_value = None

        # Create read-only instance
        instance = builder.create_read_only_database()

        # Verify instance was returned and set to read-only
        assert instance is builder._instance
        builder._instance.is_finalized.assert_called_once()
        builder._instance.set_read_only.assert_called_once()


class TestDatabaseBuilderDataLoading:
    """Test data loading functionality."""

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_load_from_container(self, mock_log, mock_index_builder_class,
                                  mock_bulk_validator_class, mock_instance_class):
        """Test loading data from container."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override schema and instance for test-specific behavior
        builder._schema = Mock(spec=DatabaseSchema)
        builder._instance = mock_instance
        builder._table_exists_in_schema = Mock(return_value=True)

        # Create mock container
        mock_container = Mock(spec=ContainerInMemory)
        mock_iterator = Mock()
        mock_iterator.is_empty.side_effect = [False, True]  # One row, then empty
        mock_iterator.pp.return_value = None
        mock_container.create_iterator.return_value = mock_iterator

        builder._instance.insert_row_from_iterator.return_value = True

        # Load from container dictionary
        data_source = {"Process": mock_container}
        builder.load_all_data(data_source)

        # Verify loading
        assert builder._instance.insert_row_from_iterator.called

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_load_handles_empty_table(self, mock_log, mock_index_builder_class,
                                      mock_bulk_validator_class, mock_instance_class):
        """Test loading empty table."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override schema and instance for test-specific behavior
        builder._schema = Mock(spec=DatabaseSchema)
        builder._instance = mock_instance
        builder._table_exists_in_schema = Mock(return_value=True)

        # Create empty mock container
        mock_container = Mock(spec=ContainerInMemory)
        mock_iterator = Mock()
        mock_iterator.is_empty.return_value = True  # Empty from start
        mock_container.create_iterator.return_value = mock_iterator

        # Load empty table
        data_source = {"Process": mock_container}
        builder.load_all_data(data_source)

        # Should handle empty tables gracefully - no inserts
        assert builder._instance.insert_row_from_iterator.call_count == 0


class TestDatabaseBuilderOutput:
    """Test database instance creation."""

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_create_instance_after_building(self, mock_log, mock_index_builder_class,
                                            mock_bulk_validator_class, mock_instance_class):
        """Test creating instance after complete build."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override instance for test-specific behavior
        builder._instance = mock_instance
        builder._instance.is_finalized.return_value = True

        # Create read-only instance
        instance = builder.create_read_only_database()

        # Verify instance is returned
        assert instance is builder._instance
        builder._instance.set_read_only.assert_called_once()

    @patch('bpmn_lib.database.instance.database_builder.DatabaseInstance')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseBulkValidator')
    @patch('bpmn_lib.database.instance.database_builder.DatabaseIndexBuilder')
    @patch('bpmn_lib.database.instance.database_builder.log_msg')
    def test_instance_is_read_only(self, mock_log, mock_index_builder_class,
                                    mock_bulk_validator_class, mock_instance_class):
        """Test that created instance is read-only."""
        # Setup mocks
        mock_instance = Mock(spec=DatabaseInstance)
        mock_bulk_validator = Mock(spec=DatabaseBulkValidator)
        mock_index_builder = Mock(spec=DatabaseIndexBuilder)

        mock_instance_class.return_value = mock_instance
        mock_bulk_validator_class.return_value = mock_bulk_validator
        mock_index_builder_class.return_value = mock_index_builder

        # Create builder with mocked dependencies
        schema_mock = Mock(spec=DatabaseSchema)
        val_result_mock = Mock(spec=ValidationResult)
        builder = DatabaseBuilder(schema_mock, val_result_mock)

        # Override instance for test-specific behavior
        builder._instance = mock_instance
        builder._instance.is_finalized.return_value = True

        # Create read-only instance
        instance = builder.create_read_only_database()

        # Instance should be set to read-only
        builder._instance.set_read_only.assert_called_once()
