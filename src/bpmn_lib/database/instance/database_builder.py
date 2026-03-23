"""
DatabaseBuilder - Koordiniert den Aufbau der Datenbank in Phasen.
"""

from typing import Dict, List, Any, Union
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.abstract_container import AbstractContainer
from basic_framework.container_utils.abstract_iterator import AbstractIterator
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.instance.database_bulk_validator import DatabaseBulkValidator
from bpmn_lib.database.instance.database_index_builder import DatabaseIndexBuilder


class DatabaseBuilder:
    """Koordiniert den Aufbau der Datenbank in Phasen."""

    def __init__(self, schema: DatabaseSchema, result: ValidationResult) -> None:
        """
        Initialisiert den DatabaseBuilder.

        Args:
            schema: DatabaseSchema mit Tabellendefinitionen
            result: ValidationResult für Fehlerberichte

        Raises:
            TypeError: Wenn schema oder result None ist (Python's Typsystem)
        """
        self._schema: DatabaseSchema = schema

        # DatabaseInstance erstellen mit neuem Konstruktor
        self._instance: DatabaseInstance = DatabaseInstance(self._schema)

        # Validator und IndexBuilder erstellen mit neuen Konstruktoren
        self._bulk_validator: DatabaseBulkValidator = DatabaseBulkValidator(self._instance, result)
        self._index_builder: DatabaseIndexBuilder = DatabaseIndexBuilder(self._instance)

        log_msg(f"DatabaseBuilder fuer Schema '{self._schema.get_schema_name()}' initialisiert.")

    def load_all_data(self, o_data_source: Any) -> None:
        """Laedt alle Daten ohne Constraint-Pruefung."""
        log_msg("Starte Bulk-Load der Daten...")

        # Je nach Typ der Datenquelle
        if isinstance(o_data_source, dict):
            o_dict = o_data_source
            # Dictionary mit Tabellennamen -> ContainerInMemory
            self._load_from_container_dictionary(o_dict)
        elif isinstance(o_data_source, list):
            # Collection von Datensaetzen
            log_and_raise("load_all_data: Collection als Datenquelle noch nicht implementiert.")
        else:
            log_and_raise(f"load_all_data: Unbekannter Datenquellentyp '{type(o_data_source).__name__}'.")

    def _load_from_container_dictionary(self, o_container_dict: Dict[str, ContainerInMemory]) -> None:
        """Laedt Daten aus einem Dictionary von ContainerInMemory-Objekten."""
        n_total_rows = 0

        # Fuer jede Tabelle im Dictionary
        for v_table_name in o_container_dict.keys():
            s_table_name = str(v_table_name)

            # Pruefen ob Tabelle im Schema existiert
            if not self._table_exists_in_schema(s_table_name):
                log_msg(f"WARNUNG: Tabelle '{s_table_name}' existiert nicht im Schema und wird uebersprungen.")
                continue

            # Source Container holen
            o_source_container = o_container_dict[s_table_name]

            # Iterator fuer Source erstellen
            o_source_iterator = o_source_container.create_iterator()

            # Zeilen kopieren
            n_row_count = 0

            while not o_source_iterator.is_empty():
                # Zeile in Ziel-Tabelle einfuegen
                if self._instance.insert_row_from_iterator(s_table_name, o_source_iterator):
                    n_row_count += 1

                o_source_iterator.pp()

                # Log-Nachricht alle 1000 Zeilen
                if n_row_count % 1000 == 0:
                    log_msg(f"Tabelle '{s_table_name}': {n_row_count} Zeilen geladen")

            log_msg(f"Tabelle '{s_table_name}': {n_row_count} Zeilen geladen.")
            n_total_rows += n_row_count

        log_msg(f"Bulk-Load abgeschlossen. Insgesamt {n_total_rows} Zeilen geladen.")

    def load_table_data(self, s_table_name: str, o_source_container: AbstractContainer) -> None:
        """Laedt Daten fuer eine einzelne Tabelle."""
        # Pruefen ob Tabelle im Schema existiert
        if not self._table_exists_in_schema(s_table_name):
            log_and_raise(f"Tabelle '{s_table_name}' existiert nicht im Schema.")

        log_msg(f"Lade Daten fuer Tabelle '{s_table_name}'...")

        # Iterator fuer Source erstellen
        o_source_iterator = o_source_container.create_iterator()

        # Zeilen kopieren
        n_row_count = 0

        while not o_source_iterator.is_empty():
            # Zeile in Ziel-Tabelle einfuegen
            if self._instance.insert_row_from_iterator(s_table_name, o_source_iterator):
                n_row_count += 1

            o_source_iterator.pp()

            # Log-Nachricht alle 1000 Zeilen
            if n_row_count % 1000 == 0:
                log_msg(f"Tabelle '{s_table_name}': {n_row_count} Zeilen geladen")

        log_msg(f"Tabelle '{s_table_name}': {n_row_count} Zeilen geladen.")

    def validate_all_constraints(self) -> None:
        """Prueft nachtraeglich alle Constraints."""
        log_msg("Starte Validierung aller Constraints...")

        # BulkValidator ausfuehren
        self._bulk_validator.validate_all()

    def build_indexes_if_valid(self) -> None:
        """Erstellt Indizes nur bei erfolgreicher Validierung."""
        # log_msg("Erstelle Indizes...")

        # IndexBuilder ausfuehren
        self._index_builder.build_all_indexes()

        # log_msg("Indizes erfolgreich erstellt.")

    def create_read_only_database(self) -> DatabaseInstance:
        """Gibt fertige DatabaseInstance zurueck."""
        # Pruefen ob alle Schritte durchgefuehrt wurden
        if not self._instance.is_finalized():
            log_and_raise("create_read_only_database: Indizes wurden nicht erstellt.")

        # Instanz auf Read-Only setzen
        self._instance.set_read_only()

        log_msg("Read-Only Datenbank erfolgreich erstellt.")
        return self._instance

    def _table_exists_in_schema(self, s_table_name: str) -> bool:
        """Hilfsmethode - Prueft ob Tabelle im Schema existiert."""
        o_table_names = self._schema.get_table_names()

        for v_name in o_table_names:
            if str(v_name) == s_table_name:
                return True

        return False

    def get_instance(self) -> DatabaseInstance:
        """Gibt die DatabaseInstance zurueck (auch vor Finalisierung)."""
        return self._instance
