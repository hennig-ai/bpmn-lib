"""
DatabaseInstance - Eine konkrete Instanz des Schemas mit Daten (Read-Only nach Erstellung).
"""

from typing import Dict, List, Any, Union, Optional, cast
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.container_unique_indexed import ContainerUniqueIndexed
from basic_framework.container_utils.container_simple_indexed import ContainerSimpleIndexed
from basic_framework.container_utils.abstract_iterator import AbstractIterator
from basic_framework.container_utils.knot_object import KnotObject
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.schema.table_definition import TableDefinition


class DatabaseInstance:
    """Eine konkrete Instanz des Schemas mit Daten (Read-Only nach Erstellung)."""

    def __init__(self, schema: DatabaseSchema) -> None:
        """
        Initialisiert die DatabaseInstance.

        Args:
            schema: DatabaseSchema instance mit Tabellendefinitionen

        Raises:
            TypeError: Wenn schema None ist (Python's Typsystem)
        """
        self._schema: DatabaseSchema = schema
        self._tables: Dict[str, ContainerInMemory] = {}
        self._primary_key_indexes: Dict[str, ContainerUniqueIndexed] = {}
        self._foreign_key_indexes: Dict[str, ContainerSimpleIndexed] = {}
        self._unique_indexes: Dict[str, ContainerUniqueIndexed] = {}
        self._is_read_only: bool = False  # Startet im Schreibmodus fuer Datenimport
        self._is_finalized: bool = False

        # Leere Container fuer alle Tabellen erstellen
        self._create_empty_tables()

        log_msg(f"DatabaseInstance fuer Schema '{self._schema.get_schema_name()}' initialisiert.")

    def add_validation_error(self, s_error: str) -> None:
        """Fuegt einen Validierungsfehler zum Schema hinzu."""
        self._schema.add_validation_error(s_error)

    def get_schema(self) -> DatabaseSchema:
        """Gibt das DatabaseSchema zurueck."""
        return self._schema

    def _create_empty_tables(self) -> None:
        """Erstellt leere Container fuer alle Tabellen."""
        o_table_names = self._schema.get_table_names()

        for v_table_name in o_table_names:
            o_table_def = self._schema.get_table_definition(str(v_table_name))

            # Container erstellen
            o_container = o_table_def.create_empty_container()

            # Zum Dictionary hinzufuegen
            self._tables[str(v_table_name)] = o_container

            #log_msg(f"Leerer Container fuer Tabelle '{v_table_name}' erstellt.")

    def get_table(self, s_table_name: str) -> ContainerInMemory:
        """Gibt einen ContainerInMemory fuer eine Tabelle zurueck."""
        if s_table_name in self._tables:
            return self._tables[s_table_name]
        else:
            log_and_raise(f"Tabelle '{s_table_name}' existiert nicht in der Datenbankinstanz.")

    def insert_row(self, s_table_name: str, o_values: Dict[str, Any]) -> bool:
        """Fuegt eine Zeile mit Validierung ein (nur im Schreibmodus)."""
        # Pruefen ob Schreibmodus
        if self._is_read_only:
            log_and_raise("insert_row: Datenbank ist im Read-Only Modus.")

        # Container holen
        o_container: ContainerInMemory = self.get_table(s_table_name)

        # Neue Zeile hinzufuegen
        l_new_row = o_container.add_empty_row()

        # Iterator auf neue Zeile positionieren
        o_iterator = o_container.create_iterator()
        o_iterator.set_position(l_new_row)

        # Werte setzen
        for v_key in o_values.keys():
            if o_container.field_exists(str(v_key)):
                o_iterator.set_value(str(v_key), o_values[v_key])
            else:
                log_msg(f"WARNUNG: Spalte '{v_key}' existiert nicht in Tabelle '{s_table_name}'.")

        # Erfolg
        log_msg(f"Zeile in Tabelle '{s_table_name}' eingefuegt.")
        return True

    def insert_row_from_iterator(self, s_table_name: str, o_source_iterator: AbstractIterator) -> bool:
        """Fuegt eine Zeile direkt ueber Iterator ein (fuer Bulk-Load)."""
        # Container holen
        o_container: ContainerInMemory = self.get_table(s_table_name)

        # Neue Zeile hinzufuegen
        l_new_row = o_container.add_empty_row()

        # Iterator auf neue Zeile positionieren
        o_target_iterator = o_container.create_iterator()
        o_target_iterator.set_position(l_new_row)

        # Alle Spalten kopieren
        o_columns = o_container.get_list_of_fields_as_ref()

        for v_column in o_columns:
            if o_source_iterator.field_exists(str(v_column)):
                o_target_iterator.set_value(str(v_column), o_source_iterator.value(str(v_column)))

        return True

    def create_indexes(self) -> None:
        """Erstellt alle Indizes (nach Validierung)."""
        if self._is_finalized:
            log_msg("Indizes wurden bereits erstellt.")
            return

        # log_msg("Erstelle Indizes fuer alle Tabellen...")

        # Fuer jede Tabelle
        for v_table_name in self._tables.keys():
            s_table_name = str(v_table_name)

            o_table_def = self._schema.get_table_definition(s_table_name)
            o_container = self._tables[s_table_name]

            # Primary Key Index
            self._create_primary_key_index(s_table_name, o_table_def, o_container)

            # Foreign Key Indizes
            self._create_foreign_key_indexes(s_table_name, o_table_def, o_container)

            # Unique Indizes
            self._create_unique_indexes(s_table_name, o_table_def, o_container)

        self._is_finalized = True
        # log_msg("Alle Indizes erfolgreich erstellt.")

    def _create_primary_key_index(self, s_table_name: str, o_table_def: TableDefinition, o_container: ContainerInMemory) -> None:
        """Erstellt Primary Key Index fuer eine Tabelle."""
        o_pk_columns = o_table_def.get_primary_key_columns()

        if len(o_pk_columns) > 0:
            # ContainerUniqueIndexed erstellen
            o_pk_index = ContainerUniqueIndexed()
            o_pk_index.init(o_container, o_pk_columns, True)

            # Zum Dictionary hinzufuegen
            self._primary_key_indexes[s_table_name] = o_pk_index

            # log_msg(f"PK-Index fuer Tabelle '{s_table_name}' erstellt ({len(o_pk_columns)} Spalten).")

    def _create_foreign_key_indexes(self, s_table_name: str, o_table_def: TableDefinition, o_container: ContainerInMemory) -> None:
        """Erstellt Foreign Key Indizes fuer eine Tabelle."""
        o_fks = o_table_def.get_foreign_keys()

        n_fk_count = 0

        for o_fk in o_fks:
            o_fk_rel = o_fk

            # Collection mit FK-Spalte erstellen
            o_fk_columns = [o_fk_rel.get_source_column()]

            # ContainerSimpleIndexed erstellen
            o_fk_index = ContainerSimpleIndexed()
            o_fk_index.init(o_container, o_fk_columns, True)

            # Zum Dictionary hinzufuegen (mit speziellem Schluessel)
            s_index_key = f"{s_table_name}_{o_fk_rel.get_source_column()}"

            if s_index_key not in self._foreign_key_indexes:
                self._foreign_key_indexes[s_index_key] = o_fk_index
                n_fk_count += 1

        # if n_fk_count > 0:
        #     log_msg(f"FK-Indizes fuer Tabelle '{s_table_name}' erstellt ({n_fk_count} Indizes).")

    def _create_unique_indexes(self, s_table_name: str, o_table_def: TableDefinition, o_container: ContainerInMemory) -> None:
        """Erstellt Unique Indizes fuer eine Tabelle."""
        o_unique_constraints = o_table_def.get_unique_constraints()

        n_unique_count = 0

        for o_constraint in o_unique_constraints:
            o_unique_columns = o_constraint

            # ContainerUniqueIndexed erstellen
            o_unique_index = ContainerUniqueIndexed()
            o_unique_index.init(o_container, o_unique_columns, True)

            # Zum Dictionary hinzufuegen (mit speziellem Schluessel)
            s_index_key = f"{s_table_name}_UNIQUE_{n_unique_count}"

            self._unique_indexes[s_index_key] = o_unique_index
            n_unique_count += 1

        if n_unique_count > 0:
            log_msg(f"Unique-Indizes fuer Tabelle '{s_table_name}' erstellt ({n_unique_count} Indizes).")

    def set_read_only(self) -> None:
        """Setzt die Instanz auf Read-Only."""
        self._is_read_only = True
        log_msg("DatabaseInstance auf Read-Only gesetzt.")

    def get_by_primary_key(self, s_table_name: str, v_key_values: Union[Any, Dict[str, Any]]) -> Optional[AbstractIterator]:
        """Schneller Zugriff ueber Primary Key."""
        # Pruefen ob PK-Index existiert
        if s_table_name not in self._primary_key_indexes:
            log_and_raise(f"Kein PK-Index fuer Tabelle '{s_table_name}' vorhanden.")
            return None

        # PK-Index holen
        o_pk_index = self._primary_key_indexes[s_table_name]

        # Key-Dictionary erstellen
        o_key_dict: Dict[str, Any]
        if isinstance(v_key_values, dict):
            o_key_dict = cast(Dict[str, Any], v_key_values)
        else:
            # Einzelner Wert - nehme erste PK-Spalte
            o_table_def = self._schema.get_table_definition(s_table_name)
            o_pk_columns = o_table_def.get_primary_key_columns()

            if len(o_pk_columns) > 0:
                o_key_dict = {o_pk_columns[0]: v_key_values}
            else:
                return None

        # Zeile ueber Index finden
        l_row: int = o_pk_index.get_row_for_unique_key(o_key_dict)

        if l_row > 0:
            # Iterator erstellen und positionieren
            o_container = self._tables[s_table_name]

            o_iterator = o_container.create_iterator()
            o_iterator.set_position(l_row)

            return o_iterator
        else:
            return None

    def get_by_foreign_key(self, s_table_name: str, s_fk_column: str, v_value: Any) -> List[AbstractIterator]:
        """Zugriff ueber Foreign Key (kann mehrere Zeilen zurueckgeben)."""
        # Index-Schluessel erstellen
        s_index_key = f"{s_table_name}_{s_fk_column}"

        # Pruefen ob FK-Index existiert
        if s_index_key not in self._foreign_key_indexes:
            log_and_raise(f"Kein FK-Index fuer Spalte '{s_fk_column}' in Tabelle '{s_table_name}' vorhanden.")
            return []

        # FK-Index holen
        o_fk_index = self._foreign_key_indexes[s_index_key]

        # Key-Dictionary erstellen
        o_key_dict = {s_fk_column: v_value}

        # Zeilen ueber Index finden
        o_rows = o_fk_index.get_rows_for_key(o_key_dict)

        if o_rows is None:
            return []

        # Iterator-Collection erstellen
        o_iterators: List[AbstractIterator] = []
        o_container = self._tables[s_table_name]

        for v_row in o_rows:
            o_iterator = o_container.create_iterator()
            o_iterator.set_position(int(v_row))
            o_iterators.append(o_iterator)

        return o_iterators

    def get_statistics(self) -> str:
        """Gibt Statistiken ueber die Instanz zurueck."""
        s_stats = "DatabaseInstance Statistiken:\n"
        s_stats += f"Schema: {self._schema.get_schema_name()}\n"
        s_stats += f"Read-Only: {self._is_read_only}\n"
        s_stats += f"Indizes erstellt: {self._is_finalized}\n\n"

        # Tabellen-Statistiken
        for v_table_name in self._tables.keys():
            o_container = self._tables[v_table_name]

            # Zeilen zaehlen
            o_iterator = o_container.create_iterator()
            l_row_count = 0
            while not o_iterator.is_empty():
                l_row_count += 1
                o_iterator.pp()

            s_stats += f"Tabelle '{v_table_name}': {l_row_count} Zeilen\n"

        return s_stats

    def is_finalized(self) -> bool:
        """Prueft ob die Instanz finalisiert wurde."""
        return self._is_finalized

    def is_read_only(self) -> bool:
        """Prueft ob die Instanz Read-Only ist."""
        return self._is_read_only

    def _get_row_position_by_primary_key(self, s_table_name: str, v_key_values: Union[Any, Dict[str, Any]]) -> int:
        """Ermittelt die Zeilennummer eines Datensatzes anhand des Primary Keys."""
        # Pruefen ob PK-Index existiert
        if s_table_name not in self._primary_key_indexes:
            log_and_raise(f"_get_row_position_by_primary_key: Kein PK-Index fuer Tabelle '{s_table_name}' vorhanden.")
            return -1

        # PK-Index holen
        o_pk_index = self._primary_key_indexes[s_table_name]

        # Key-Dictionary erstellen
        o_key_dict: Dict[str, Any]
        if isinstance(v_key_values, dict):
            o_key_dict = cast(Dict[str, Any], v_key_values)
        else:
            # Einzelner Wert - nehme erste PK-Spalte
            o_table_def = self._schema.get_table_definition(s_table_name)
            o_pk_columns = o_table_def.get_primary_key_columns()

            if len(o_pk_columns) > 0:
                o_key_dict = {o_pk_columns[0]: v_key_values}
            else:
                return -1

        # Zeile ueber Index finden
        l_row = o_pk_index.get_row_for_unique_key(o_key_dict)

        if l_row > 0:
            return l_row
        else:
            return -1  # Nicht gefunden

    def _get_value(self, s_table_name: str, l_row_position: int, s_column_name: str) -> Any:
        """Gibt den Wert einer Spalte fuer eine bestimmte Zeile in einer Tabelle zurueck."""
        # Pruefen ob Tabelle existiert
        if s_table_name not in self._tables:
            log_and_raise(f"_get_value: Tabelle '{s_table_name}' existiert nicht in der Datenbankinstanz.")
            return None

        # Container holen
        o_container = self._tables[s_table_name]

        # Pruefen ob Spalte existiert
        if not o_container.field_exists(s_column_name):
            log_and_raise(f"_get_value: Spalte '{s_column_name}' existiert nicht in Tabelle '{s_table_name}'.")
            return None

        # Pruefen ob Zeilenposition gueltig ist
        if l_row_position <= 0:
            log_and_raise(f"_get_value: Ungueltige Zeilenposition '{l_row_position}' fuer Tabelle '{s_table_name}'.")
            return None

        # Direkter Zugriff ueber AbstractContainer Property
        return o_container.get_value(l_row_position, s_column_name)

    def _get_col_value_by_pk_internal(self, s_table_name: str, v_key_values: Union[Any, Dict[str, Any]], s_column_name: str) -> Any:
        """Gibt den Wert einer spezifischen Spalte fuer einen Datensatz zurueck, der ueber Primary Key identifiziert wird."""
        # Zeilenposition ueber PK ermitteln
        l_row_position = self._get_row_position_by_primary_key(s_table_name, v_key_values)

        # Pruefen ob Datensatz gefunden wurde
        if l_row_position == -1:
            log_and_raise(f"_get_col_value_by_pk_internal: Kein Datensatz mit dem angegebenen Primary Key in Tabelle '{s_table_name}' gefunden.")
            return None

        # Spaltenwert ueber allgemeine _get_value-Funktion holen
        return self._get_value(s_table_name, l_row_position, s_column_name)

    def get_col_value_by_pk(self, s_table_name: str, v_key_values: Union[Any, Dict[str, Any]], s_column_name: str) -> Any:
        """Gibt den Wert einer spezifischen Spalte fuer einen Datensatz zurueck (mit Hierarchie-Unterstuetzung)."""
        # Pruefen ob Tabelle existiert
        if s_table_name not in self._tables:
            log_and_raise(f"get_col_value_by_pk: Tabelle '{s_table_name}' existiert nicht in der Datenbankinstanz.")
            return None

        o_table = self._tables[s_table_name]

        # Ist das Feld in der Tabelle vorhanden dann koennen wir den Wert zurueck liefern
        if o_table.field_exists(s_column_name):
            return self._get_col_value_by_pk_internal(s_table_name, v_key_values, s_column_name)

        # Andernfalls holen wir uns die Vaterklasse in der Hierarchie
        o_h_table = self.get_hierarchy_data()
        if s_table_name not in o_h_table:
            # Gibt es fuer die Tabelle in der Hierarchie keine Vatertabelle dann kann auch die Spalte nicht abgerufen werden
            log_and_raise(f"get_col_value_by_pk: fuer die Tabelle {s_table_name} gibt es in der Hierarchie keinen Vater, das Attribut '{s_column_name}' kann nicht abgerufen werden")
            return None

        # Ansonsten hole ich den Vater (get_parent() raises wenn kein Parent existiert)
        o_node: KnotObject = o_h_table[s_table_name]
        o_parent: KnotObject = o_node.get_parent()

        return self.get_col_value_by_pk(o_parent.m_vValue, v_key_values, s_column_name)

    def get_hierarchy_data(self) -> Dict[str, KnotObject]:
        """Placeholder fuer Hierarchie-Daten - noch nicht implementiert."""
        # Originalcode referenziert m_Schema.GetHierarchyData() aber das ist nicht implementiert
        # Hier sollte die Hierarchie-Logik implementiert werden falls benoetigt
        return {}
