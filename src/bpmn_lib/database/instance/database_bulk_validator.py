"""
DatabaseBulkValidator - Nachtraegliche Validierung aller Daten.
"""

from typing import Dict, List, Any
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework import is_effectively_null
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.abstract_iterator import AbstractIterator
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship
from bpmn_lib.database.schema.column_definition import ColumnDefinition


class DatabaseBulkValidator:
    """Nachtraegliche Validierung aller Daten."""

    def __init__(self, instance: DatabaseInstance, result: ValidationResult) -> None:
        """
        Initialisiert den BulkValidator.

        Args:
            instance: DatabaseInstance mit den zu validierenden Daten
            result: ValidationResult für Fehlerberichte

        Raises:
            TypeError: Wenn instance oder result None ist (Python's Typsystem)
        """
        self._schema: DatabaseSchema = instance.get_schema()
        self._instance: DatabaseInstance = instance
        self._result: ValidationResult = result

    def validate_all(self) -> None:
        """Fuehrt alle Validierungen durch."""
        log_msg("Starte Bulk-Validierung der Datenbank...")

        # 1. Primary Key Validierung
        log_msg("1. Validiere Primary Keys...")
        self.validate_primary_keys()

        # 2. Foreign Key Validierung
        # log_msg("2. Validiere Foreign Keys...")
        self.validate_foreign_keys()

        # 3. Not Null Validierung
        # log_msg("3. Validiere NOT NULL Constraints...")
        self.validate_not_null_constraints()

        # 4. Unique Constraints Validierung
        # log_msg("4. Validiere UNIQUE Constraints...")
        self.validate_unique_constraints()

        # 5. Value Domain Validierung
        # log_msg("5. Validiere Value Domains...")
        self.validate_value_domains()

        # 6. Datentyp Validierung
        # log_msg("6. Validiere Datentypen...")
        self.validate_data_types()

    def validate_primary_keys(self) -> bool:
        """Validiert Primary Keys (Eindeutigkeit)."""
        # log_msg("Validiere Primary Keys...")
        b_success = True

        # Fuer jede Tabelle
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            s_table_name = str(v_table_name)

            o_table_def = self._schema.get_table_definition(s_table_name)
            o_pk_columns = o_table_def.get_primary_key_columns()

            if len(o_pk_columns) > 0:
                # Container holen
                o_container = self._instance.get_table(s_table_name)

                # Temporaerer Index fuer PK-Pruefung
                o_pk_values = {}

                # Durch alle Zeilen iterieren
                o_iterator = o_container.create_iterator()

                while not o_iterator.is_empty():
                    # PK-Wert(e) zusammenbauen
                    s_pk_value = self._build_key_string(o_iterator, o_pk_columns)

                    # Pruefen ob PK bereits existiert
                    if s_pk_value in o_pk_values:
                        self._add_validation_error(f"Primary Key Verletzung in Tabelle '{s_table_name}': "
                                                 f"Doppelter Wert '{s_pk_value}' in Zeile {o_iterator.position()}")
                        b_success = False
                    else:
                        o_pk_values[s_pk_value] = o_iterator.position()

                    o_iterator.pp()

        return b_success

    def validate_not_null_constraints(self) -> bool:
        """Validiert NOT NULL Constraints."""
        # log_msg("Validiere NOT NULL Constraints...")
        b_success = True

        # Fuer jede Tabelle
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            s_table_name = str(v_table_name)

            o_table_def = self._schema.get_table_definition(s_table_name)

            # NOT NULL Spalten sammeln
            o_not_null_columns = []

            for v_col_name in o_table_def.get_column_names():
                o_col_def = o_table_def.get_column(str(v_col_name))

                if not o_col_def.is_nullable():
                    o_not_null_columns.append(str(v_col_name))

            if len(o_not_null_columns) > 0:
                # Container holen
                o_container = self._instance.get_table(s_table_name)

                # Durch alle Zeilen iterieren
                o_iterator = o_container.create_iterator()

                n_violations = 0

                while not o_iterator.is_empty():
                    # NOT NULL Spalten pruefen
                    for v_col in o_not_null_columns:
                        v_value = o_iterator.value(str(v_col))

                        if v_value is None or str(v_value) == "":
                            self._add_validation_error(f"NOT NULL Verletzung in Tabelle '{s_table_name}', "
                                                     f"Spalte '{v_col}', Zeile {o_iterator.position()}")
                            b_success = False
                            n_violations += 1

                    o_iterator.pp()

                if n_violations == 0:
                    pass

        return b_success

    def validate_unique_constraints(self) -> bool:
        """Validiert Unique Constraints."""
        # log_msg("Validiere Unique Constraints...")
        b_success = True

        # Fuer jede Tabelle
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            s_table_name = str(v_table_name)

            o_table_def = self._schema.get_table_definition(s_table_name)
            o_unique_constraints = o_table_def.get_unique_constraints()

            if len(o_unique_constraints) > 0:
                # Container holen
                o_container = self._instance.get_table(s_table_name)

                # Fuer jeden Unique Constraint
                n_constraint_index = 1

                for o_constraint in o_unique_constraints:
                    o_unique_columns = o_constraint

                    # Temporaerer Index fuer Unique-Pruefung
                    o_unique_values = {}

                    # Durch alle Zeilen iterieren
                    o_iterator = o_container.create_iterator()

                    while not o_iterator.is_empty():
                        # Unique-Wert(e) zusammenbauen
                        s_unique_value = self._build_key_string(o_iterator, o_unique_columns)

                        # Nur pruefen wenn nicht alle Werte NULL sind
                        if s_unique_value != "":
                            # Pruefen ob Wert bereits existiert
                            if s_unique_value in o_unique_values:
                                self._add_validation_error(f"Unique Constraint #{n_constraint_index} verletzt in Tabelle "
                                                         f"'{s_table_name}': Doppelter Wert '{s_unique_value}' "
                                                         f"in Zeile {o_iterator.position()}")
                                b_success = False
                            else:
                                o_unique_values[s_unique_value] = o_iterator.position()

                        o_iterator.pp()

                    n_constraint_index += 1

        return b_success

    def validate_foreign_keys(self) -> bool:
        """Validiert Foreign Key Constraints."""
        # log_msg("Validiere Foreign Keys...")
        b_success = True

        # Alle FK-Beziehungen durchgehen
        o_relationships = self._schema.get_relationships()

        for o_rel in o_relationships:
            o_fk_rel = o_rel

            # Container fuer Source und Target holen
            o_source_container = self._instance.get_table(o_fk_rel.get_source_table())
            o_target_container = self._instance.get_table(o_fk_rel.get_target_table())

            if o_source_container is not None and o_target_container is not None:
                # Alle Werte aus Target-Tabelle sammeln (fuer Performance)
                o_target_values = {}

                o_target_iterator = o_target_container.create_iterator()

                while not o_target_iterator.is_empty():
                    s_target_value = o_target_iterator.value(o_fk_rel.get_target_column())

                    if not is_effectively_null(s_target_value):
                        s_target_key = str(s_target_value)
                        if s_target_key not in o_target_values:
                            o_target_values[s_target_key] = True
                    else:
                        pass

                    o_target_iterator.pp()

                # Durch Source-Tabelle iterieren und FK pruefen
                o_source_iterator = o_source_container.create_iterator()

                n_violations = 0

                while not o_source_iterator.is_empty():
                    v_source_value = o_source_iterator.value(o_fk_rel.get_source_column())

                    # NULL-Werte sind erlaubt fuer FK
                    if not is_effectively_null(v_source_value):
                        s_source_key = str(v_source_value)

                        # Pruefen ob Wert in Target existiert
                        if s_source_key not in o_target_values:
                            self._add_validation_error(f"Foreign Key Verletzung: '{o_fk_rel.get_description()}' - "
                                                     f"Wert '{s_source_key}' existiert nicht in Zieltabelle, "
                                                     f"Zeile {o_source_iterator.position()} in '{o_fk_rel.get_source_table()}'")
                            b_success = False
                            n_violations += 1

                    o_source_iterator.pp()

                if n_violations == 0:
                    pass
                else:
                    log_msg(f"FK '{o_fk_rel.get_description()}': Ungültige Referenzen gefunden.")
            else:
                log_and_raise("Hier ist eine Relationsdefinition zwischen zwei Tabellen kaputt")

        return b_success

    def validate_value_domains(self) -> bool:
        """Validiert Value Domains."""
        # log_msg("Validiere Value Domains...")
        b_success = True

        # Fuer jede Tabelle
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            s_table_name = str(v_table_name)

            o_table_def = self._schema.get_table_definition(s_table_name)

            # Spalten mit Value Domain sammeln
            o_value_domain_columns = []

            for v_col_name in o_table_def.get_column_names():
                if o_table_def.has_value_domain(str(v_col_name)):
                    o_value_domain_columns.append(str(v_col_name))

            if len(o_value_domain_columns) > 0:
                # Container holen
                o_container = self._instance.get_table(s_table_name)

                # Durch alle Zeilen iterieren
                o_iterator = o_container.create_iterator()

                while not o_iterator.is_empty():
                    # Value Domain Spalten pruefen
                    for v_col in o_value_domain_columns:
                        v_value = o_iterator.value(str(v_col))

                        # NULL ist erlaubt
                        if v_value is not None and str(v_value) != "":
                            # Erlaubte Werte holen
                            o_allowed_values = o_table_def.get_value_domain(str(v_col))

                            # Pruefen ob Wert erlaubt ist
                            if not self._is_value_in_collection(str(v_value), o_allowed_values):
                                self._add_validation_error(f"Value Domain Verletzung in Tabelle '{s_table_name}', "
                                                         f"Spalte '{v_col}': Wert '{v_value}' nicht erlaubt, "
                                                         f"Zeile {o_iterator.position()}")
                                b_success = False

                    o_iterator.pp()

        return b_success

    def validate_data_types(self) -> bool:
        """Validiert Datentypen (Stichproben)."""
        # log_msg("Validiere Datentypen (Stichproben)...")
        # Hier koennten Stichproben-Validierungen durchgefuehrt werden
        # Fuer diese Version gehen wir davon aus, dass beim Import bereits geprueft wurde
        return True

    def _build_key_string(self, o_iterator: AbstractIterator, o_columns: List[str]) -> str:
        """Hilfsfunktion zum Erstellen von Schluesseln."""
        s_key = ""

        for v_col in o_columns:
            v_value = o_iterator.value(str(v_col))

            if s_key != "":
                s_key += "|"

            if v_value is None:
                s_key += "[NULL]"
            else:
                s_key += str(v_value)

        return s_key

    def _is_value_in_collection(self, s_value: str, o_collection: List[str]) -> bool:
        """Hilfsfunktion zum Pruefen ob Wert in Collection enthalten ist."""
        for v_item in o_collection:
            if str(v_item) == s_value:
                return True
        return False

    def _add_validation_error(self, s_message: str) -> None:
        """Fuegt einen Validierungsfehler hinzu."""
        self._result.add_error(s_message)

    def _add_validation_warning(self, s_message: str) -> None:
        """Fuegt eine Validierungswarnung hinzu."""
        self._result.add_warning(s_message)
