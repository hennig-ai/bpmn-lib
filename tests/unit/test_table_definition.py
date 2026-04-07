"""Unit tests für TableDefinition.is_value_in_domain() Methode."""

import pytest
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.column_definition import ColumnDefinition


@pytest.fixture
def table_with_value_domain() -> TableDefinition:
    """Erstelle eine Tabelle mit Value Domain."""
    table = TableDefinition("gateway", "Gateway table")

    # Spalte mit Value Domain hinzufügen
    col_type = ColumnDefinition("gateway_type", "str", "Gateway Type")
    col_type.set_nullable(False)
    table.add_column(col_type)

    # Value Domain für gateway_type definieren
    table.add_value_domain("gateway_type", ["parallel", "exclusive", "inclusive"])

    return table


@pytest.fixture
def table_without_value_domain() -> TableDefinition:
    """Erstelle eine Tabelle ohne Value Domain."""
    table = TableDefinition("activity", "Activity table")

    col_name = ColumnDefinition("activity_name", "str", "Activity Name")
    col_name.set_nullable(False)
    table.add_column(col_name)

    # Keine Value Domain hinzufügen

    return table


class TestIsValueInDomain:
    """Tests für TableDefinition.is_value_in_domain()."""

    def test_value_in_domain_returns_true(self, table_with_value_domain: TableDefinition) -> None:
        """Test: Wert liegt in Value Domain -> True."""
        assert table_with_value_domain.is_value_in_domain("gateway_type", "parallel") is True
        assert table_with_value_domain.is_value_in_domain("gateway_type", "exclusive") is True
        assert table_with_value_domain.is_value_in_domain("gateway_type", "inclusive") is True

    def test_value_not_in_domain_returns_false(self, table_with_value_domain: TableDefinition) -> None:
        """Test: Wert liegt nicht in Value Domain -> False."""
        assert table_with_value_domain.is_value_in_domain("gateway_type", "event") is False
        assert table_with_value_domain.is_value_in_domain("gateway_type", "invalid") is False
        assert table_with_value_domain.is_value_in_domain("gateway_type", "") is False

    def test_column_not_exist_returns_false(self, table_with_value_domain: TableDefinition) -> None:
        """Test: Spalte existiert nicht -> False."""
        assert table_with_value_domain.is_value_in_domain("nonexistent_column", "parallel") is False
        assert table_with_value_domain.is_value_in_domain("nonexistent_column", "any_value") is False

    def test_no_value_domain_defined_returns_true(self, table_without_value_domain: TableDefinition) -> None:
        """Test: Spalte existiert aber keine Value Domain -> True (alle Werte erlaubt)."""
        assert table_without_value_domain.is_value_in_domain("activity_name", "any_value") is True
        assert table_without_value_domain.is_value_in_domain("activity_name", "") is True
        assert table_without_value_domain.is_value_in_domain("activity_name", "123") is True

    def test_case_sensitive(self, table_with_value_domain: TableDefinition) -> None:
        """Test: Vergleich ist case-sensitive."""
        assert table_with_value_domain.is_value_in_domain("gateway_type", "Parallel") is False
        assert table_with_value_domain.is_value_in_domain("gateway_type", "PARALLEL") is False
        assert table_with_value_domain.is_value_in_domain("gateway_type", "parallel") is True

    def test_empty_value_domain(self) -> None:
        """Test: Spalte mit leerer Value Domain."""
        table = TableDefinition("test_table", "Test table")
        col = ColumnDefinition("test_col", "str", "Test Column")
        col.set_nullable(False)
        table.add_column(col)

        # Leere Value Domain hinzufügen
        table.add_value_domain("test_col", [])

        # Kein Wert sollte in leerer Domain liegen
        assert table.is_value_in_domain("test_col", "any_value") is False
        assert table.is_value_in_domain("test_col", "") is False


class TestIsValueInDomainIntegration:
    """Integration Tests mit realistischen Szenarien."""

    def test_event_type_domain(self) -> None:
        """Test: Realistische Szenario mit event_type Spalte."""
        table = TableDefinition("event", "Event table")
        col = ColumnDefinition("event_type", "str", "Event Type")
        col.set_nullable(False)
        table.add_column(col)
        table.add_value_domain("event_type", ["start", "end", "intermediate"])

        # Valide Werte
        assert table.is_value_in_domain("event_type", "start") is True
        assert table.is_value_in_domain("event_type", "end") is True
        assert table.is_value_in_domain("event_type", "intermediate") is True

        # Invalide Werte
        assert table.is_value_in_domain("event_type", "invalid") is False

    def test_gateway_type_domain(self) -> None:
        """Test: Realistische Szenario mit gateway_type Spalte."""
        table = TableDefinition("gateway", "Gateway table")
        col = ColumnDefinition("gateway_type", "str", "Gateway Type")
        col.set_nullable(False)
        table.add_column(col)
        table.add_value_domain("gateway_type", ["parallel", "exclusive", "inclusive"])

        # Valide Werte (Subtype-Werte)
        assert table.is_value_in_domain("gateway_type", "parallel") is True
        assert table.is_value_in_domain("gateway_type", "exclusive") is True
        assert table.is_value_in_domain("gateway_type", "inclusive") is True

        # Invalide Werte
        assert table.is_value_in_domain("gateway_type", "event") is False
