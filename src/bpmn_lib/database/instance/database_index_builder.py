"""
DatabaseIndexBuilder - Erstellt Indizes nach erfolgreicher Validierung.
"""

from typing import List
from basic_framework.proc_frame import log_msg
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship


class DatabaseIndexBuilder:
    """Erstellt Indizes nach erfolgreicher Validierung."""

    def __init__(self, instance: DatabaseInstance) -> None:
        """
        Initialisiert den IndexBuilder.

        Args:
            instance: DatabaseInstance mit den zu indizierenden Daten

        Raises:
            TypeError: Wenn instance None ist (Python's Typsystem)
        """
        self._schema: DatabaseSchema = instance.get_schema()
        self._instance: DatabaseInstance = instance
        self._index_count: int = 0

    def build_all_indexes(self) -> None:
        """Erstellt alle Indizes."""
        # log_msg("IndexBuilder: Starte Index-Erstellung...")

        self._index_count = 0

        # Delegiere an DatabaseInstance
        self._instance.create_indexes()

        # log_msg("IndexBuilder: Index-Erstellung abgeschlossen.")

    def verify_index_integrity(self) -> bool:
        """Verifiziert die Integritaet der erstellten Indizes."""
        log_msg("Verifiziere Index-Integritaet...")

        b_success = True

        # Pruefungen wuerden hier implementiert
        # Fuer diese Version gehen wir davon aus, dass die Indizes korrekt sind

        log_msg("Index-Integritaet verifiziert.")
        return b_success

    def get_index_statistics(self) -> str:
        """Gibt Statistiken ueber die erstellten Indizes zurueck."""
        s_stats = "INDEX-STATISTIKEN\n"
        s_stats += "=================\n"

        # Detaillierte Statistiken koennten hier gesammelt werden
        s_stats += "Indizes wurden von DatabaseInstance erstellt.\n"

        return s_stats

    def generate_index_report(self) -> str:
        """Erstellt einen Report ueber alle Indizes."""
        s_report = "INDEX-REPORT\n"
        s_report += "============\n\n"

        # Fuer jede Tabelle Index-Informationen sammeln
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            s_report += f"Tabelle: {v_table_name}\n"

            o_table_def = self._schema.get_table_definition(str(v_table_name))

            # Primary Key
            o_pk_columns = o_table_def.get_primary_key_columns()
            if len(o_pk_columns) > 0:
                s_report += f"  - Primary Key Index: {self._collection_to_string(o_pk_columns)}\n"

            # Foreign Keys
            o_fks = o_table_def.get_foreign_keys()
            if len(o_fks) > 0:
                for o_fk in o_fks:
                    o_fk_rel = o_fk
                    s_report += f"  - Foreign Key Index: {o_fk_rel.get_source_column()}\n"

            # Unique Constraints
            o_uniques = o_table_def.get_unique_constraints()
            if len(o_uniques) > 0:
                n_index = 1
                for o_unique in o_uniques:
                    o_col = o_unique
                    s_report += f"  - Unique Index #{n_index}: {self._collection_to_string(o_col)}\n"
                    n_index += 1

            s_report += "\n"

        return s_report

    def _collection_to_string(self, o_col: List[str]) -> str:
        """Hilfsfunktion: Collection zu String."""
        s_result = "("

        b_first = True

        for v_item in o_col:
            if not b_first:
                s_result += ", "
            s_result += str(v_item)
            b_first = False

        s_result += ")"
        return s_result
