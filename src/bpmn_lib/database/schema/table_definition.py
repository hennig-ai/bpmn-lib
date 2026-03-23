"""
TableDefinition - Repräsentiert eine einzelne Tabelle mit allen Metadaten.
"""

from typing import Dict, List, Any, Optional
from basic_framework.proc_frame import log_and_raise, log_msg
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.abstract_iterator import AbstractIterator
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship


class TableDefinition:
    """Repräsentiert eine einzelne Tabelle mit allen Metadaten."""

    def __init__(self, table_name: str, description: str = "") -> None:
        """
        Initialisiert die TableDefinition.

        Args:
            table_name: Name der Tabelle
            description: Optionale Beschreibung der Tabelle

        Raises:
            ValueError: Wenn table_name leer ist
        """
        if not table_name:
            raise ValueError("table_name cannot be empty")

        self._table_name: str = table_name
        self._description: str = description
        self._columns: Dict[str, ColumnDefinition] = {}
        self._primary_key_columns: List[str] = []
        self._foreign_keys: List[ForeignKeyRelationship] = []
        self._value_domains: Dict[str, List[str]] = {}
        self._unique_constraints: List[List[str]] = []

    def add_column(self, oColumn: ColumnDefinition) -> None:
        """Fügt eine Spaltendefinition hinzu."""
        sColumnName = oColumn.get_column_name()

        # Prüfen ob Spalte bereits existiert
        if sColumnName in self._columns:
            log_and_raise(f"Spalte '{sColumnName}' existiert bereits in Tabelle '{self._table_name}'.")
            return

        # Spalte hinzufügen
        self._columns[sColumnName] = oColumn

        # Wenn PK, zur PK-Collection hinzufügen
        if oColumn.is_primary_key():
            self._primary_key_columns.append(sColumnName)

    def add_value_domain(self, sColumnName: str, oAllowedValues: List[str]) -> None:
        """Fügt eine Value Domain für eine Spalte hinzu."""
        # Prüfen ob Spalte existiert
        if sColumnName not in self._columns:
            log_and_raise(f"Spalte '{sColumnName}' existiert nicht in Tabelle '{self._table_name}'.")
            return

        # Value Domain hinzufügen
        self._value_domains[sColumnName] = oAllowedValues

        # Auch in der ColumnDefinition setzen
        oCol = self._columns[sColumnName]
        oCol.set_value_domain(oAllowedValues)

    def add_unique_constraint(self, oColumnNames: List[str]) -> None:
        """Fügt eine Unique-Constraint hinzu (kann mehrere Spalten umfassen)."""
        # Prüfen ob alle Spalten existieren
        for vColName in oColumnNames:
            if vColName not in self._columns:
                log_and_raise(f"Spalte '{vColName}' fuer Unique-Constraint existiert nicht in Tabelle '{self._table_name}'.")
                return

        # Constraint hinzufügen
        self._unique_constraints.append(oColumnNames)

    def add_foreign_key(self, oForeignKey: ForeignKeyRelationship) -> None:
        """Fügt eine Foreign Key Definition hinzu."""
        self._foreign_keys.append(oForeignKey)

    def create_empty_container(self) -> ContainerInMemory:
        """Erstellt einen leeren ContainerInMemory mit richtigen Spalten."""
        # Spaltennamen-Collection erstellen
        oHeaders = list(self._columns.keys())

        # Container erstellen
        oContainer = ContainerInMemory()
        oContainer.init_new(oHeaders, f"{self._table_name}_Data", self._table_name)

        return oContainer

    def validate_row(self, oIterator: AbstractIterator, sErrorDetail: str = "") -> bool:
        """Validiert ob eine Zeile alle Constraints erfüllt (Iterator-basiert)."""
        bValid = True
        error_details = []

        # 1. NOT NULL Constraints prüfen
        for vKey in self._columns.keys():
            oCol = self._columns[vKey]

            if not oCol.is_nullable():
                vValue = oIterator.value(vKey)
                if vValue is None or str(vValue) == "":
                    error_details.append(f"NOT NULL verletzt fuer Spalte '{vKey}'. ")
                    bValid = False

        # 2. Datentyp-Validierung
        for vKey in self._columns.keys():
            oCol = self._columns[vKey]
            sDataType = oCol.get_data_type()
            vValue = oIterator.value(vKey)

            if vValue is not None and str(vValue) != "":
                data_type_upper = sDataType[:3].upper()
                if data_type_upper in ["INT", "BIG"]:
                    try:
                        int(vValue)
                    except (ValueError, TypeError):
                        error_details.append(f"Datentyp-Fehler in Spalte '{vKey}': Integer erwartet. ")
                        bValid = False
                elif data_type_upper == "BOO":
                    # Boolean-Validierung
                    str_value = str(vValue).upper()
                    if str_value not in ["TRUE", "FALSE", "1", "0", "-1"]:
                        error_details.append(f"Datentyp-Fehler in Spalte '{vKey}': Boolean erwartet. ")
                        bValid = False
                elif data_type_upper == "TIM":
                    # Timestamp-Validierung - in Python prüfen wir mit dateutil oder datetime
                    from datetime import datetime
                    try:
                        datetime.fromisoformat(str(vValue))
                    except (ValueError, TypeError):
                        error_details.append(f"Datentyp-Fehler in Spalte '{vKey}': Timestamp erwartet. ")
                        bValid = False

        # 3. Value Domain Constraints prüfen
        for vKey in self._value_domains.keys():
            vValue = oIterator.value(vKey)
            if vValue is not None and str(vValue) != "":
                oAllowedValues = self._value_domains[vKey]

                if str(vValue) not in [str(v) for v in oAllowedValues]:
                    error_details.append(f"Value Domain verletzt fuer Spalte '{vKey}': '{vValue}' nicht erlaubt. ")
                    bValid = False

        # sErrorDetail als String zusammenfügen
        sErrorDetail = "".join(error_details)

        return bValid

    # Getter-Methoden
    def get_table_name(self) -> str:
        """Gibt den Tabellennamen zurück."""
        return self._table_name

    def get_description(self) -> str:
        """Gibt die Beschreibung zurück."""
        return self._description

    def get_column(self, sColumnName: str) -> Optional[ColumnDefinition]:
        """Gibt eine spezifische Spaltendefinition zurück."""
        return self._columns.get(sColumnName, None)

    def get_columns(self) -> Dict[str, ColumnDefinition]:
        """Gibt alle Spaltendefinitionen zurück."""
        return self._columns

    def get_column_count(self) -> int:
        """Gibt die Anzahl der Spalten zurück."""
        return len(self._columns)

    def has_column(self, sColumnName: str) -> bool:
        """Prüft ob eine Spalte existiert."""
        return sColumnName in self._columns

    def get_primary_key_columns(self) -> List[str]:
        """Gibt die Primary Key Spalten zurück."""
        return self._primary_key_columns

    def get_foreign_keys(self) -> List[ForeignKeyRelationship]:
        """Gibt alle Foreign Keys zurück."""
        return self._foreign_keys

    def get_value_domains(self) -> Dict[str, List[str]]:
        """Gibt alle Value Domains zurück."""
        return self._value_domains

    def get_unique_constraints(self) -> List[List[str]]:
        """Gibt alle Unique Constraints zurück."""
        return self._unique_constraints

    def get_column_names(self) -> List[str]:
        """Gibt eine Liste aller Spaltennamen zurück."""
        return list(self._columns.keys())

    def has_value_domain(self, sColumnName: str) -> bool:
        """Prüft ob eine Spalte einen Value Domain hat."""
        return sColumnName in self._value_domains

    def get_value_domain(self, sColumnName: str) -> Optional[List[str]]:
        """Gibt die Value Domain für eine Spalte zurück."""
        return self._value_domains.get(sColumnName, None)
