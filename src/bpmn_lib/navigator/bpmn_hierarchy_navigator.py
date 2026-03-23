"""
BPMN Hierarchy Navigator - Zentrale Klasse für die Navigation und Verwaltung der BPMN-Element-Hierarchie.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.abstract_container import AbstractContainer
from basic_framework.conditions.condition_equals import ConditionEquals
from bpmn_lib.database.instance.database_instance import DatabaseInstance
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.utils.validation_result import ValidationResult
from basic_framework import MarkdownDocument, MarkdownFileAsTable
# from archive.hierarchy_parser import HierarchyParser  # Unused - commented out
from bpmn_lib.navigator.bpmn_constants import *


@dataclass(frozen=True)
class OutgoingSequenceFlowInfo:
    """Informationen zu einem ausgehenden Sequence Flow."""

    sequence_flow_id: str
    target_element_id: str
    condition_expression: Optional[str]


@dataclass(frozen=True)
class IncomingSequenceFlowInfo:
    """Informationen zu einem eingehenden Sequence Flow."""

    sequence_flow_id: str
    source_element_id: str
    condition_expression: Optional[str]


class BPMNHierarchyNavigator:
    """Zentrale Klasse für die Navigation und Verwaltung der BPMN-Element-Hierarchie."""

    #def __init__(self):
    def __init__(self, val_result: ValidationResult, database: DatabaseInstance, hierarchy_doc: MarkdownDocument):
        """Konstruktor."""

        # Referenzen
        self.m_database = database
        self.m_val_result = val_result

        # Member-Variablen für Hierarchie-Struktur
        self.m_parent_to_children: Dict[str, List[str]] = {}  # parent -> [child1, child2, ...]
        self.m_child_to_parent: Dict[str, str] = {}  # child -> parent
        self.m_leaf_tables: List[str] = []  # Tabellen ohne Kinder
        self.m_root_table: str = ""  # Wurzeltabelle (bpmn_element)
        self.m_table_levels: Dict[str, int] = {}  # Tabelle -> Hierarchie-Ebene (0=root)

        # Member-Variablen für Element-Mapping
        self.m_element_mapping: Dict[str, Dict[str, Any]] = {}  # bpmn_element_id -> ElementInfo
        self.m_process_elements: Dict[str, List[str]] = {}  # process_id -> Collection of element_ids

        # Member-Variablen für Kategorisierung
        self.m_table_to_category: Dict[str, str] = {}  # Tabelle -> Kategorie (pool|lane|activity|connection)

        # HierarchyContainer-Datei einlesen
        self._hierarchy_container : AbstractContainer = MarkdownFileAsTable(hierarchy_doc)

        # Hierarchie aufbauen
        self._build_hierarchy_structure()

        # Kategorisierung initialisieren
        self._initialize_table_categories()

        # Element-Mapping aufbauen (muss nach Hierarchie-Struktur erfolgen)
        self._build_element_mapping()

        log_msg("BPMNHierarchyNavigator initialisiert")

    #def _build_hierarchy_structure(self, hierarchy_doc: MarkdownDocument) -> None:
    def _build_hierarchy_structure(self) -> None:
        """Baut die Hierarchie-Struktur aus dem Schema auf."""
        # 7. NEU: element_type Spezifität Validierung
        # log_msg("7. Validiere element_type Spezifität...")
        o_report: List[str] = []
        self._validate_element_type_specificity(o_report)
        self.m_val_result.check_validation()

        # Prüfen ob parent und child Spalten existieren
        if not self._hierarchy_container.field_exists("parent") or not self._hierarchy_container.field_exists("child"):
            log_and_raise("Hierarchie-Container muss 'parent' und 'child' Spalten enthalten")

        # Iterator über Hierarchie-Container erstellen
        o_iterator = self._hierarchy_container.create_iterator()

        # Sammlung aller Tabellen für Wurzel-Erkennung
        all_tables: Dict[str, bool] = {}

        # Durch alle Zeilen iterieren
        while not o_iterator.is_empty():
            s_parent_table = o_iterator.value("parent")
            s_child_table = o_iterator.value("child")

            # Leere Werte überspringen
            if s_parent_table.strip() != "" and s_child_table.strip() != "":
                # Child -> Parent mapping
                if s_child_table not in self.m_child_to_parent:
                    self.m_child_to_parent[s_child_table] = s_parent_table

                # Parent -> Children mapping
                if s_parent_table not in self.m_parent_to_children:
                    self.m_parent_to_children[s_parent_table] = []
                self.m_parent_to_children[s_parent_table].append(s_child_table)

                # Alle Tabellen sammeln
                if s_parent_table not in all_tables:
                    all_tables[s_parent_table] = True
                if s_child_table not in all_tables:
                    all_tables[s_child_table] = True

            o_iterator.pp()

        # Wurzel-Tabelle(n) identifizieren (Tabellen die nur als Parent vorkommen)
        for v_table in all_tables.keys():
            if v_table not in self.m_child_to_parent:
                # Diese Tabelle ist nie ein Child, also eine Wurzel
                if self.m_root_table == "":
                    self.m_root_table = v_table
                    # log_msg(f"Wurzel-Tabelle gefunden: {self.m_root_table}")
                else:
                    log_msg(f"Warnung: Mehrere Wurzel-Tabellen gefunden: {v_table}")

        # Falls keine Wurzel gefunden wurde
        if self.m_root_table == "":
            log_and_raise("Keine Wurzel-Tabelle in der Hierarchie gefunden")

        # Blatt-Tabellen identifizieren
        self._identify_leaf_tables()

        # Hierarchie-Ebenen berechnen
        self._calculate_table_levels()

        # log_msg(f"Hierarchie-Struktur aufgebaut: {len(self.m_parent_to_children)} Parent-Knoten, "
        #         f"{len(self.m_leaf_tables)} Blatt-Tabellen, Wurzel: {self.m_root_table}")

    def _identify_leaf_tables(self):
        """Identifiziert Tabellen ohne Kinder."""
        # Alle Tabellen aus m_child_to_parent durchgehen
        for v_table in self.m_child_to_parent.keys():
            if v_table not in self.m_parent_to_children:
                # Tabelle hat keine Kinder -> Blatt
                self.m_leaf_tables.append(v_table)

        # Auch Tabellen prüfen, die nur als Parent vorkommen
        for v_table in self.m_parent_to_children.keys():
            # Wenn eine Tabelle nur Parent ist aber nie Child, und keine Kinder hat
            if v_table not in self.m_child_to_parent and len(self.m_parent_to_children[v_table]) == 0:
                self.m_leaf_tables.append(v_table)

    def _calculate_table_levels(self):
        """Berechnet die Hierarchie-Ebene für jede Tabelle."""
        if self.m_root_table == "":
            log_and_raise("Keine Wurzeltabelle gefunden")

        # Root ist Ebene 0
        self.m_table_levels[self.m_root_table] = 0

        # BFS für Ebenenberechnung
        o_queue = [self.m_root_table]

        while len(o_queue) > 0:
            s_current_table = o_queue.pop(0)

            n_current_level = self.m_table_levels[s_current_table]

            # Kinder verarbeiten
            if s_current_table in self.m_parent_to_children:
                for s_child in self.m_parent_to_children[s_current_table]:
                    if s_child not in self.m_table_levels:
                        self.m_table_levels[s_child] = n_current_level + 1
                        o_queue.append(s_child)

    def _initialize_table_categories(self) -> None:
        """Initialisiert die Tabellen-Kategorien basierend auf Namenskonvention."""
        # Pool/Lane Kategorien
        self.m_table_to_category["pool"] = "container"
        self.m_table_to_category["lane"] = "container"

        # Activity Kategorien
        self.m_table_to_category[TBL_ACTIVITY] = "node"
        self.m_table_to_category[TBL_TASK] = "node"
        self.m_table_to_category["sub_process"] = "node"
        self.m_table_to_category["call_activity"] = "node"
        self.m_table_to_category["user_task"] = "node"
        self.m_table_to_category["service_task"] = "node"
        self.m_table_to_category["script_task"] = "node"
        self.m_table_to_category["send_task"] = "node"
        self.m_table_to_category["receive_task"] = "node"
        self.m_table_to_category["manual_task"] = "node"
        self.m_table_to_category["business_rule_task"] = "node"

        # Event Kategorien
        self.m_table_to_category[TBL_EVENT] = "node"
        self.m_table_to_category["start_event"] = "node"
        self.m_table_to_category["end_event"] = "node"
        self.m_table_to_category["intermediate_catch_event"] = "node"
        self.m_table_to_category["intermediate_throw_event"] = "node"
        self.m_table_to_category["boundary_event"] = "node"

        # Gateway Kategorien
        self.m_table_to_category[TBL_GATEWAY] = "node"
        self.m_table_to_category["exclusive_gateway"] = "node"
        self.m_table_to_category["parallel_gateway"] = "node"
        self.m_table_to_category["inclusive_gateway"] = "node"
        self.m_table_to_category["event_based_gateway"] = "node"
        self.m_table_to_category["complex_gateway"] = "node"

        # Data Object Kategorien
        self.m_table_to_category["data_object"] = "node"

        # Connection Kategorien
        self.m_table_to_category["sequence_flow"] = "connection"
        self.m_table_to_category["data_association"] = "connection"
        self.m_table_to_category["data_input_association"] = "connection"
        self.m_table_to_category["data_output_association"] = "connection"

        # Root
        self.m_table_to_category["bpmn_element"] = "root"
        self.m_table_to_category["process_element"] = "mapping"
        self.m_table_to_category["bpmn_process"] = "process"

    def _get_table_category(self, s_table_name: str) -> str:
        """Gibt die Kategorie für eine Tabelle zurück."""
        if s_table_name in self.m_table_to_category:
            return self.m_table_to_category[s_table_name]
        else:
            # Standard-Kategorie basierend auf Position in Hierarchie
            if self._is_leaf_table(s_table_name):
                return "node"  # Annahme: Blätter sind meist Nodes
            else:
                return "unknown"

    def _build_element_mapping(self) -> bool:
        """Baut das komplette Element-Mapping auf."""
        try:
            # log_msg("Starte Aufbau des Element-Mappings...")

            # Zuerst alle Elemente aus bpmn_element direkt verarbeiten
            self._process_root_table()

            # ÄNDERUNG: Alle Tabellen in der Hierarchie verarbeiten, nicht nur Blätter
            # Alle Child-Tabellen verarbeiten (alle die einen Parent haben)
            for v_table in self.m_child_to_parent.keys():
                self._process_hierarchy_table(v_table)

            # Process-Element Mapping aufbauen
            self._build_process_element_mapping()

            # log_msg(f"Element-Mapping abgeschlossen: {len(self.m_element_mapping)} Elemente gefunden")
            return True
        except Exception as e:
            log_and_raise(ValueError(f"Fehler beim Aufbau des Element-Mappings: {e}"))

    def _process_hierarchy_table(self, s_table_name: str) -> None:
        """Verarbeitet eine Tabelle in der Hierarchie (umbenennt von ProcessLeafTable)."""
        # Tabelle abrufen
        o_table = self.m_database.get_table(s_table_name)

        # Primary Key Spalte bestimmen
        s_pk_column = self._get_primary_key_column(s_table_name)

        if s_pk_column == "":
            log_and_raise(f"Kein Primary Key für Tabelle '{s_table_name}' gefunden")
            return

        # Durch alle Datensätze iterieren
        o_iterator = o_table.create_iterator()

        n_processed = 0

        while not o_iterator.is_empty():
            # Element-Kette aufbauen
            self._process_element_chain(s_table_name, o_iterator.value(s_pk_column), s_table_name)
            n_processed += 1
            o_iterator.pp()

        if n_processed > 0:
            pass  # log_msg(f"Tabelle '{s_table_name}' verarbeitet: {n_processed} Einträge")

    def _process_root_table(self) -> None:
        """Verarbeitet die Root-Tabelle (bpmn_element) direkt."""
        # Alle Elemente aus bpmn_element als Basis nehmen
        # get_table() raises via log_and_raise() if table doesn't exist
        o_bpmn_element_table = self.m_database.get_table("bpmn_element")

        # Durch alle Elemente iterieren
        o_iterator = o_bpmn_element_table.create_iterator()

        while not o_iterator.is_empty():
            s_bpmn_element_id = o_iterator.value("bpmn_element_id")
            s_element_type = o_iterator.value("element_type")

            # Basis-Element-Info erstellen
            if s_bpmn_element_id not in self.m_element_mapping:
                o_element_info: Dict[str, Any] = {
                    "element_id": s_bpmn_element_id,
                    "mappings": [],
                    "specific_type": s_element_type,  # Wird später überschrieben falls spezialisiert
                    "hierarchy_depth": 1,
                    "category": self._get_table_category(s_element_type),
                    "drawn": False
                }

                # Basis-Mapping hinzufügen
                o_mapping: Dict[str, Any] = {
                    "table": "bpmn_element",
                    "pk": s_bpmn_element_id
                }
                mappings_list: List[Dict[str, Any]] = o_element_info["mappings"]
                mappings_list.append(o_mapping)

                self.m_element_mapping[s_bpmn_element_id] = o_element_info

            o_iterator.pp()

        # log_msg(f"Root-Tabelle verarbeitet: {len(self.m_element_mapping)} Basis-Elemente gefunden")

    def _process_element_chain(self, s_start_table: str, v_start_pk: Any, s_specific_type: str) -> None:
        """Verarbeitet die komplette Hierarchie-Kette eines Elements."""
        o_table_mappings: List[Dict[str, Any]] = []

        # Dictionary für aktuelles Mapping
        o_mapping: Dict[str, Any] = {
            "table": s_start_table,
            "pk": v_start_pk
        }
        o_table_mappings.append(o_mapping)

        # Navigation von Blatt zur Wurzel
        s_current_table = s_start_table
        v_current_pk = v_start_pk
        s_bpmn_element_id = ""

        # Hierarchie aufwärts navigieren
        while s_current_table != self.m_root_table:
            # Parent-Tabelle bestimmen
            if s_current_table not in self.m_child_to_parent:
                return  # Keine Parent-Tabelle

            s_parent_table = self.m_child_to_parent[s_current_table]

            # FK-Spalte bestimmen (Konvention: parent_table + "_id")
            s_fk_column = s_parent_table + "_id"

            # FK-Wert aus aktuellem Datensatz holen
            o_current_iterator = self.m_database.get_by_primary_key(s_current_table, v_current_pk)

            if o_current_iterator is None:
                log_and_raise(f"Datensatz nicht gefunden: {s_current_table} PK={v_current_pk}")
                return

            # FK-Wert lesen
            if not o_current_iterator.field_exists(s_fk_column):
                log_and_raise(f"FK-Spalte '{s_fk_column}' nicht in Tabelle '{s_current_table}' gefunden")
                return

            v_parent_pk = o_current_iterator.value(s_fk_column)

            # Parent-Mapping hinzufügen
            o_mapping: Dict[str, Any] = {
                "table": s_parent_table,
                "pk": v_parent_pk
            }
            o_table_mappings.append(o_mapping)

            # Für nächste Iteration
            s_current_table = s_parent_table
            v_current_pk = v_parent_pk

            # Wenn wir bei bpmn_element angekommen sind, ID speichern
            if s_current_table == self.m_root_table:
                s_bpmn_element_id = str(v_current_pk)

        # Element-Info aktualisieren (nicht neu erstellen)
        if s_bpmn_element_id != "":
            self._update_element_mapping_specialization(s_bpmn_element_id, o_table_mappings, s_specific_type)

    def _update_element_mapping_specialization(self, s_bpmn_element_id: str, o_table_mappings: List[Dict[str, Any]], s_specific_type: str) -> None:
        """Aktualisiert das Element-Mapping mit Spezialisierungsinformationen."""
        # Element muss bereits existieren (wurde in ProcessRootTable erstellt)
        if s_bpmn_element_id not in self.m_element_mapping:
            log_and_raise(f"Element '{s_bpmn_element_id}' nicht in Basis-Mapping gefunden")
            return

        o_element_info = self.m_element_mapping[s_bpmn_element_id]

        # Spezialisierte Informationen aktualisieren
        o_element_info["specific_type"] = s_specific_type
        o_element_info["hierarchy_depth"] = len(o_table_mappings)
        o_element_info["category"] = self._get_table_category(s_specific_type)

        # Alle Mappings hinzufügen (überschreibt die Basis-Collection)
        o_element_info["mappings"] = o_table_mappings

    def _build_process_element_mapping(self) -> None:
        """Baut das Process-Element Mapping auf."""
        # process_element Tabelle abrufen
        # get_table() raises via log_and_raise() if table doesn't exist
        o_process_element_table = self.m_database.get_table("process_element")

        # Durch alle Einträge iterieren
        o_iterator = o_process_element_table.create_iterator()

        while not o_iterator.is_empty():
            s_process_id = o_iterator.value("bpmn_process_id")
            s_element_id = o_iterator.value("bpmn_element_id")

            # Sicherstellen, dass die Element-ID im Element-Mapping existiert
            if s_element_id in self.m_element_mapping:
                # Process -> Elements mapping
                if s_process_id not in self.m_process_elements:
                    self.m_process_elements[s_process_id] = []
                self.m_process_elements[s_process_id].append(s_element_id)
            else:
                log_msg(f"Warnung: Element-ID '{s_element_id}' in process_element gefunden, aber nicht im Element-Mapping")

            o_iterator.pp()

        # log_msg(f"Process-Element Mapping aufgebaut: {len(self.m_process_elements)} Prozesse")

    def _get_primary_key_column(self, s_table_name: str) -> str:
        """Bestimmt die Primary Key Spalte für eine Tabelle."""
        # Konvention: tabellenname + "_id"
        return s_table_name + "_id"

    def _is_leaf_table(self, s_table_name: str) -> bool:
        """Prüft ob eine Tabelle ein Blatt ist."""
        return s_table_name in self.m_leaf_tables

    def get_process_elements(self, s_process_id: str) -> List[str]:
        """Gibt alle Element-IDs für einen Prozess zurück."""
        if s_process_id in self.m_process_elements:
            return self.m_process_elements[s_process_id]
        else:
            return []

    def get_process_name(self, process_id: Union[str, int]) -> str:
        """
        Gibt den Namen eines BPMN-Prozesses zurück.

        Args:
            process_id: ID des Prozesses (int oder str).
                        Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")

        Returns:
            Name des Prozesses

        Raises:
            ValueError: Wenn der Prozess nicht gefunden wird
        """
        s_process_id = self._format_db_internal_id(process_id)

        iterator = self.m_database.get_by_primary_key("bpmn_process", s_process_id)

        if iterator is None:
            log_and_raise(ValueError(f"Prozess '{s_process_id}' nicht gefunden"))

        return iterator.value("name")

    def _format_db_internal_id(self, db_id: Union[str, int]) -> str:
        """
        Formatiert eine interne DB-ID als dreistelligen String mit fuehrenden Nullen.

        Diese Methode wird intern verwendet um Integer-IDs in das
        String-Format zu konvertieren, das in den Mappings verwendet wird.

        Beispiele:
            1 -> "001"
            20 -> "020"
            123 -> "123"
            "001" -> "001" (unveraendert)

        Args:
            db_id: Interne DB-ID als int oder str

        Returns:
            Formatierter String mit 3 Stellen und fuehrenden Nullen
        """
        if isinstance(db_id, int):
            return f"{db_id:03d}"
        return str(db_id)

    def get_all_start_events(self, process_id: Union[str, int]) -> List[str]:
        """
        Gibt alle Start-Event-IDs für einen Prozess zurück.

        Filtert alle Elemente eines Prozesses und gibt nur die Start-Events zurück.
        Ein Element ist ein Start-Event wenn:
        - element_type == "event" UND
        - event_type == "start"

        Args:
            process_id: ID des Prozesses (int oder str).
                        Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")

        Returns:
            Liste von Start-Event-IDs (leer wenn keine Start-Events gefunden)

        Raises:
            Exception: Bei Fehlern in get_element_attribute (propagiert mit Kontext)
        """
        s_process_id: str = self._format_db_internal_id(process_id)
        return_list: List[str] = []
        bpmn_element_ids = self.get_process_elements(s_process_id)

        for bpmn_element_id in bpmn_element_ids:
            element_type = self.get_element_attribute(bpmn_element_id, "element_type")
            if element_type == "event":
                event_type = self.get_element_attribute(bpmn_element_id, "event_type")
                if event_type == "start":
                    return_list.append(bpmn_element_id)

        return return_list

    def next_elements_in_flow(self, bpmn_element_id: str) -> Optional[List[str]]:
        """
        Gibt die Liste der im Sequenzfluss folgenden BPMN-Element-IDs zurück.

        Durchsucht die sequence_flow Tabelle nach allen Flows, die vom gegebenen
        Element ausgehen (source_bpmn_element_id) und gibt die Ziel-Element-IDs zurück.

        Args:
            bpmn_element_id: ID des Quell-Elements

        Returns:
            Liste der folgenden Element-IDs (target_bpmn_element_id)
            None wenn keine ausgehenden Sequenzflüsse vorhanden sind

        Raises:
            Exception: Wenn sequence_flow Tabelle nicht verfügbar ist
        """
        # get_table() raises via log_and_raise() if table doesn't exist
        sequence_flow_table = self.m_database.get_table("sequence_flow")

        # Filter auf source_bpmn_element_id
        condition = ConditionEquals("source_bpmn_element_id", bpmn_element_id)
        iterator = sequence_flow_table.create_iterator(True, condition)

        # Target-IDs sammeln
        target_ids: List[str] = []
        while not iterator.is_empty():
            target_id = iterator.value("target_bpmn_element_id")
            target_ids.append(target_id)
            iterator.pp()

        # None zurückgeben wenn keine Elemente gefunden
        if len(target_ids) == 0:
            return None

        return target_ids

    def get_outgoing_sequence_flows(self, bpmn_element_id: Union[str, int]) -> List[OutgoingSequenceFlowInfo]:
        """
        Gibt alle ausgehenden Sequence Flows eines Elements zurueck.

        Durchsucht die sequence_flow Tabelle nach allen Flows, die vom gegebenen
        Element ausgehen (source_bpmn_element_id) und gibt vollstaendige Flow-Informationen zurueck.

        Args:
            bpmn_element_id: ID des Quell-Elements (int oder str).
                             Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")

        Returns:
            Liste von OutgoingSequenceFlowInfo mit:
            - sequence_flow_id: ID des Sequence Flows
            - target_element_id: ID des Ziel-Elements
            - condition_expression: Bedingungsausdruck (kann None sein)
            Leere Liste wenn keine ausgehenden Sequence Flows vorhanden sind

        Raises:
            Exception: Wenn sequence_flow Tabelle nicht verfuegbar ist
        """
        bpmn_element_id = self._format_db_internal_id(bpmn_element_id)
        # get_table() raises via log_and_raise() if table doesn't exist
        sequence_flow_table = self.m_database.get_table("sequence_flow")

        # Filter auf source_bpmn_element_id
        condition = ConditionEquals("source_bpmn_element_id", bpmn_element_id)
        iterator = sequence_flow_table.create_iterator(True, condition)

        # Flow-Daten sammeln
        outgoing_flows: List[OutgoingSequenceFlowInfo] = []
        while not iterator.is_empty():
            flow_info = OutgoingSequenceFlowInfo(
                sequence_flow_id=iterator.value("bpmn_element_id"),
                target_element_id=iterator.value("target_bpmn_element_id"),
                condition_expression=iterator.value("condition_expression"),
            )
            outgoing_flows.append(flow_info)
            iterator.pp()

        return outgoing_flows

    def get_incoming_sequence_flows(self, bpmn_element_id: Union[str, int]) -> List[IncomingSequenceFlowInfo]:
        """
        Gibt alle eingehenden Sequence Flows eines Elements zurueck.

        Durchsucht die sequence_flow Tabelle nach allen Flows, die zum gegebenen
        Element hinfuehren (target_bpmn_element_id) und gibt vollstaendige Flow-Informationen zurueck.

        Args:
            bpmn_element_id: ID des Ziel-Elements (int oder str).
                             Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")

        Returns:
            Liste von IncomingSequenceFlowInfo mit:
            - sequence_flow_id: ID des Sequence Flows
            - source_element_id: ID des Quell-Elements
            - condition_expression: Bedingungsausdruck (kann None sein)
            Leere Liste wenn keine eingehenden Sequence Flows vorhanden sind

        Raises:
            Exception: Wenn sequence_flow Tabelle nicht verfuegbar ist
        """
        bpmn_element_id = self._format_db_internal_id(bpmn_element_id)
        # get_table() raises via log_and_raise() if table doesn't exist
        sequence_flow_table = self.m_database.get_table("sequence_flow")

        # Filter auf target_bpmn_element_id
        condition = ConditionEquals("target_bpmn_element_id", bpmn_element_id)
        iterator = sequence_flow_table.create_iterator(True, condition)

        # Flow-Daten sammeln
        incoming_flows: List[IncomingSequenceFlowInfo] = []
        while not iterator.is_empty():
            flow_info = IncomingSequenceFlowInfo(
                sequence_flow_id=iterator.value("bpmn_element_id"),
                source_element_id=iterator.value("source_bpmn_element_id"),
                condition_expression=iterator.value("condition_expression"),
            )
            incoming_flows.append(flow_info)
            iterator.pp()

        return incoming_flows

    def previous_elements_in_flow(self, bpmn_element_id: str) -> Optional[List[str]]:
        """
        Gibt die Liste der im Sequenzfluss vorhergehenden BPMN-Element-IDs zurueck.

        Durchsucht die sequence_flow Tabelle nach allen Flows, die zum gegebenen
        Element hinfuehren (target_bpmn_element_id) und gibt die Quell-Element-IDs zurueck.

        Args:
            bpmn_element_id: ID des Ziel-Elements

        Returns:
            Liste der vorhergehenden Element-IDs (source_bpmn_element_id)
            None wenn keine eingehenden Sequenzfluesse vorhanden sind

        Raises:
            Exception: Wenn sequence_flow Tabelle nicht verfuegbar ist
        """
        # get_table() raises via log_and_raise() if table doesn't exist
        sequence_flow_table = self.m_database.get_table("sequence_flow")

        # Filter auf target_bpmn_element_id
        condition = ConditionEquals("target_bpmn_element_id", bpmn_element_id)
        iterator = sequence_flow_table.create_iterator(True, condition)

        # Source-IDs sammeln
        source_ids: List[str] = []
        while not iterator.is_empty():
            source_id = iterator.value("source_bpmn_element_id")
            source_ids.append(source_id)
            iterator.pp()

        # None zurueckgeben wenn keine Elemente gefunden
        if len(source_ids) == 0:
            return None

        return source_ids

    def get_data_inputs(self, bpmn_element_id: Union[str, int]) -> Optional[List[str]]:
        """
        Gibt die Liste der Data Object IDs zurück, die als Input für das Element dienen.

        Durchsucht die data_association Tabelle nach allen Associations,
        die auf das gegebene Element zeigen (target_bpmn_element_id).

        Args:
            bpmn_element_id: ID des Ziel-Elements (int oder str).
                             Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")

        Returns:
            Liste der Source-Element-IDs als int (typischerweise Data Objects)
            None wenn keine Data Input Associations vorhanden sind

        Raises:
            Exception: Wenn data_association Tabelle nicht verfügbar ist
        """
        bpmn_element_id = self._format_db_internal_id(bpmn_element_id)
        # get_table() raises via log_and_raise() if table doesn't exist
        data_table = self.m_database.get_table(TBL_DATA_ASSOCIATION)

        # Filter auf target_bpmn_element_id
        condition = ConditionEquals("target_bpmn_element_id", bpmn_element_id)
        iterator = data_table.create_iterator(True, condition)

        # Source-IDs sammeln
        source_ids: List[str] = []
        while not iterator.is_empty():
            source_id = iterator.value("source_bpmn_element_id")
            source_ids.append(source_id)
            iterator.pp()

        # None zurückgeben wenn keine Elemente gefunden
        if len(source_ids) == 0:
            return None

        return source_ids

    def get_data_outputs(self, bpmn_element_id: str) -> Optional[List[str]]:
        """
        Gibt die Liste der Data Object IDs zurück, die als Output vom Element erzeugt werden.

        Durchsucht die data_association Tabelle nach allen Associations,
        die vom gegebenen Element ausgehen (source_bpmn_element_id).

        Args:
            bpmn_element_id: ID des Quell-Elements

        Returns:
            Liste der Target-Element-IDs (typischerweise Data Objects)
            None wenn keine Data Output Associations vorhanden sind

        Raises:
            Exception: Wenn data_association Tabelle nicht verfügbar ist
        """
        # get_table() raises via log_and_raise() if table doesn't exist
        data_table = self.m_database.get_table(TBL_DATA_ASSOCIATION)

        # Filter auf source_bpmn_element_id
        condition = ConditionEquals("source_bpmn_element_id", bpmn_element_id)
        iterator = data_table.create_iterator(True, condition)

        # Target-IDs sammeln
        target_ids: List[str] = []
        while not iterator.is_empty():
            target_id = iterator.value("target_bpmn_element_id")
            target_ids.append(target_id)
            iterator.pp()

        # None zurückgeben wenn keine Elemente gefunden
        if len(target_ids) == 0:
            return None

        return target_ids

    def get_element_info(self, s_bpmn_element_id: str) -> Optional[Dict[str, Any]]:
        """Gibt Element-Info für eine Element-ID zurück."""
        if s_bpmn_element_id in self.m_element_mapping:
            return self.m_element_mapping[s_bpmn_element_id]
        else:
            return None

    def get_element_attribute(self, s_bpmn_element_id: Union[str, int], s_attribute_name: str) -> Any:
        """
        Gibt Attributwert mit Hierarchie-Navigation zurück.

        Args:
            s_bpmn_element_id: Die Element-ID (int oder str).
                               Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")
            s_attribute_name: Der Attributname

        Returns:
            Der Attributwert oder None wenn nicht gefunden
        """
        # ID formatieren (int -> "001")
        s_bpmn_element_id = self._format_db_internal_id(s_bpmn_element_id)

        # Element-Info abrufen
        o_element_info = self.get_element_info(s_bpmn_element_id)

        if o_element_info is None:
            log_and_raise(f"Element '{s_bpmn_element_id}' nicht im Mapping gefunden")
            return None

        # Durch alle Tabellen-Mappings iterieren (von speziell zu allgemein)
        o_mappings = o_element_info["mappings"]

        for o_mapping in o_mappings:
            s_table_name = o_mapping["table"]
            v_pk = o_mapping["pk"]

            # Versuche Attribut aus dieser Tabelle zu lesen
            # get_table() raises via log_and_raise() if table doesn't exist
            o_table = self.m_database.get_table(s_table_name)

            # Prüfe ob Spalte existiert
            if o_table.field_exists(s_attribute_name):
                # Wert über PK abrufen
                o_iterator = self.m_database.get_by_primary_key(s_table_name, v_pk)

                if o_iterator is not None:
                    return o_iterator.value(s_attribute_name)

        # Attribut nicht gefunden
        log_msg(f"Attribut '{s_attribute_name}' für Element '{s_bpmn_element_id}' nicht gefunden")
        return None

    def get_element_attribute_typed(self, s_bpmn_element_id: Union[str, int], s_attribute_name: str) -> Union[str, int, bool, datetime, None]:
        """
        Gibt Attributwert im korrekten Python-Datentyp zurück.

        Konvertiert den Rohwert basierend auf dem Schema-Datentyp:
        - VARCHAR, TEXT, CHAR -> str
        - INTEGER, INT, BIGINT -> int
        - BOOLEAN, BOOL -> bool
        - TIMESTAMP, DATETIME, DATE -> datetime

        Args:
            s_bpmn_element_id: Die Element-ID (int oder str).
                               Bei int wird automatisch auf 3 Stellen formatiert (1 -> "001")
            s_attribute_name: Der Attributname

        Returns:
            Der Wert im korrekten Python-Typ oder None
        """
        # ID formatieren (int -> "001")
        s_bpmn_element_id = self._format_db_internal_id(s_bpmn_element_id)

        # Element-Info abrufen
        o_element_info = self.get_element_info(s_bpmn_element_id)

        if o_element_info is None:
            log_and_raise(f"Element '{s_bpmn_element_id}' nicht im Mapping gefunden")
            return None

        # Durch alle Tabellen-Mappings iterieren (von speziell zu allgemein)
        o_mappings = o_element_info["mappings"]

        for o_mapping in o_mappings:
            s_table_name = o_mapping["table"]
            v_pk = o_mapping["pk"]

            # Versuche Attribut aus dieser Tabelle zu lesen
            # get_table() raises via log_and_raise() if table doesn't exist
            o_table = self.m_database.get_table(s_table_name)

            # Prüfe ob Spalte existiert
            if o_table.field_exists(s_attribute_name):
                # Wert über PK abrufen
                o_iterator = self.m_database.get_by_primary_key(s_table_name, v_pk)

                if o_iterator is not None:
                    raw_value = o_iterator.value(s_attribute_name)

                    # Datentyp aus Schema holen
                    o_schema = self.m_database.get_schema()
                    o_table_def = o_schema.get_table_definition(s_table_name)
                    o_col_def = o_table_def.get_column(s_attribute_name)

                    if o_col_def is not None:
                        return self._convert_to_python_type(raw_value, o_col_def)
                    else:
                        # Fallback: Rohwert zurückgeben
                        return raw_value

        # Attribut nicht gefunden
        log_msg(f"Attribut '{s_attribute_name}' für Element '{s_bpmn_element_id}' nicht gefunden")
        return None

    def _convert_to_python_type(self, raw_value: Any, o_col_def: ColumnDefinition) -> Union[str, int, bool, datetime, None]:
        """
        Konvertiert einen Rohwert in den korrekten Python-Datentyp.

        Args:
            raw_value: Der Rohwert (meist String)
            o_col_def: Die ColumnDefinition mit Typ- und Nullable-Information

        Returns:
            Der konvertierte Wert im korrekten Python-Typ

        Raises:
            ValueError: Wenn Wert NULL ist aber Spalte NOT NULL ist
        """
        s_data_type = o_col_def.get_data_type().upper()
        b_is_nullable = o_col_def.is_nullable()
        s_column_name = o_col_def.get_column_name()

        # Prüfe auf echten NULL-Wert (None oder ##!empty!## Marker)
        is_null = raw_value is None

        if is_null:
            if b_is_nullable:
                return None
            else:
                log_and_raise(ValueError(
                    f"Spalte '{s_column_name}' ist NOT NULL, aber Wert ist NULL"
                ))

        str_value = str(raw_value)

        # String-Typen (VARCHAR, TEXT, CHAR) - leerer String ist gültiger Wert!
        if s_data_type in ["VARCHAR", "TEXT", "CHAR"]:
            return str_value

        # Für nicht-String-Typen: Leerer String wird wie NULL behandelt
        if str_value.strip() == "":
            if b_is_nullable:
                return None
            else:
                log_and_raise(ValueError(
                    f"Spalte '{s_column_name}' ist NOT NULL, aber Wert ist leer"
                ))

        str_value = str_value.strip()

        # Integer-Typen
        if s_data_type in ["INTEGER", "INT", "BIGINT"]:
            try:
                return int(str_value)
            except ValueError:
                log_and_raise(ValueError(
                    f"Konvertierung zu int fehlgeschlagen für Spalte '{s_column_name}', Wert: '{str_value}'"
                ))

        # Boolean-Typen
        if s_data_type in ["BOOLEAN", "BOOL"]:
            upper_val = str_value.upper()
            if upper_val in ["TRUE", "1", "-1"]:
                return True
            elif upper_val in ["FALSE", "0"]:
                return False
            else:
                log_and_raise(ValueError(
                    f"Konvertierung zu bool fehlgeschlagen für Spalte '{s_column_name}', Wert: '{str_value}'"
                ))

        # Datum/Zeit-Typen
        if s_data_type in ["TIMESTAMP", "DATETIME", "DATE"]:
            try:
                return datetime.fromisoformat(str_value)
            except ValueError:
                log_and_raise(ValueError(
                    f"Konvertierung zu datetime fehlgeschlagen für Spalte '{s_column_name}', Wert: '{str_value}'"
                ))

        # Unbekannte Typen -> als String zurückgeben
        return str_value

    def is_element_descendant_of(self, s_bpmn_element_id: str, s_ancestor_table: str) -> bool:
        """Prüft ob ein Element von einer bestimmten Tabelle abstammt."""
        try:
            # Element-Info abrufen
            o_element_info = self.get_element_info(s_bpmn_element_id)

            # Wenn Element nicht existiert, False zurückgeben
            if o_element_info is None:
                log_msg(f"IsElementDescendantOf: Element '{s_bpmn_element_id}' nicht gefunden")
                return False

            # Durch alle Tabellen in der Hierarchie iterieren
            o_mappings = o_element_info["mappings"]

            # Von speziell zu allgemein durchsuchen
            for o_mapping in o_mappings:
                s_table_name = o_mapping["table"]

                # Prüfen ob dies die gesuchte Ancestor-Tabelle ist
                if s_table_name.lower() == s_ancestor_table.lower():
                    log_msg(f"IsElementDescendantOf: Element '{s_bpmn_element_id}' stammt von '{s_ancestor_table}' ab")
                    return True

            # Ancestor-Tabelle nicht in der Hierarchie gefunden
            return False
        except Exception as e:
            log_msg(f"Fehler in IsElementDescendantOf für Element '{s_bpmn_element_id}': {str(e)}")
            return False

    def _validate_element_type_specificity(self, o_report: List[str]):
        """Validiert dass element_type immer den spezifischsten Typ in der Hierarchie enthält."""
        # log_msg("Starte Validierung der element_type Spezifität...")

        # Hole bpmn_element Tabelle
        o_bpmn_element_table = self.get_table("bpmn_element")

        # Iterator für alle bpmn_element Datensätze erstellen
        o_it = o_bpmn_element_table.create_iterator()

        # Durch alle Datensätze iterieren
        while not o_it.is_empty():
            s_bpmn_element_id = o_it.value("bpmn_element_id")
            s_element_type = o_it.value("element_type")

            # Validierung für diesen Datensatz durchführen
            self._validate_single_record(s_bpmn_element_id, s_element_type)

            # Fortschrittsanzeige alle 1000 Datensätze
            if o_it.position() % 1000 == 0:
                o_it.write_pp_message(f"element_type Validierung: {o_it.position()} Datensätze geprüft, {self.m_val_result.count()} Fehler gefunden")

            o_it.pp()

    def _validate_single_record(self, s_bpmn_element_id: str, s_element_type: str):
        """Validiert einen einzelnen Datensatz."""

        # Phase A: Konsistenz-Prüfung
        error_msg = self._validate_inheritance_chain(s_bpmn_element_id, s_element_type)
        if error_msg!="":
            # Fehler protokollieren
            #log_msg(error_msg)
            self.add_validation_error(error_msg)
            # Bei Konsistenzfehler Phase B überspringen
            return

        # Phase B: Spezifität-Prüfung
        error_msg = self._validate_type_specificity(s_bpmn_element_id, s_element_type)
        if error_msg!="":
            # Fehler protokollieren
            #log_msg(s_specificity_error)
            self.add_validation_error(error_msg)
            return

        #return True

    def add_validation_error(self, s_msg: str):
        """Fügt einen Validierungsfehler hinzu."""
        self.m_database.add_validation_error(s_msg)

    def get_table(self, s_name: str) -> ContainerInMemory:
        """Gibt eine Tabelle aus der Datenbank zurück."""
        return self.m_database.get_table(s_name)

    def _validate_inheritance_chain(self, s_bpmn_element_id: str, s_element_type: str) -> str:
        """Phase A: Validiert die Konsistenz der Vererbungskette."""


        # Hierarchie-Pfad ermitteln
        o_path = self._get_hierarchy_path("bpmn_element", s_element_type)

        if o_path is None or len(o_path) == 0:
            s_error_message = f"Validierungsfehler für bpmn_element_id={s_bpmn_element_id}: " \
                            f"element_type '{s_element_type}' nicht in Hierarchie gefunden"
            return s_error_message

        # Starte bei bpmn_element
        s_current_id = s_bpmn_element_id
        s_pfad_string = f"bpmn_element(id={s_current_id})"

        # Durch den Pfad navigieren (Start bei 2, da 1 = bpmn_element)
        for i in range(1, len(o_path)):
            s_current_table = o_path[i]
            s_previous_table = o_path[i - 1]

            # FK-Spaltenname bilden
            s_fk_column = s_previous_table + "_id"

            # In aktueller Tabelle nach Datensatz suchen
            o_current_table = self.get_table(s_current_table)

            # Iterator mit Condition für FK-Suche erstellen
            o_condition = ConditionEquals(s_fk_column, s_current_id)

            o_search_it = o_current_table.create_iterator(True, o_condition)

            # Prüfen ob genau ein Datensatz gefunden wurde
            n_found_count = 0
            s_found_id = ""
            s_first_found_id = ""

            while not o_search_it.is_empty():
                n_found_count += 1

                if n_found_count == 1:
                    # Ersten gefundenen Datensatz merken
                    s_first_found_id = o_search_it.value(s_current_table + "_id")
                    s_found_id = s_first_found_id
                elif n_found_count == 2:
                    # Bei zweitem Datensatz direkt Fehler
                    s_pfad_string += f" > {s_current_table}(MEHRERE GEFUNDEN: ids={s_first_found_id}"
                else:
                    # Weitere IDs anhängen
                    s_pfad_string += f">{o_search_it.value(s_current_table + '_id')}"

                o_search_it.pp()

            # Validierung der Ergebnisse
            if n_found_count == 0:
                # Kein Datensatz gefunden
                s_pfad_string += f" > {s_current_table}(id=UNBEKANNT)"
                s_error_message = f"Hierarchiefehler für bpmn_element_id={s_bpmn_element_id}: " \
                                f"element_type='{s_element_type}': " \
                                f"Vererbungspfad: {s_pfad_string}, " \
                                f"kein entsprechender Datensatz in Tabelle '{s_current_table}' gefunden."
                return s_error_message

            elif n_found_count > 1:
                # Mehrere Datensätze gefunden
                s_pfad_string += ")"
                s_error_message = f"Validierungsfehler für bpmn_element_id={s_bpmn_element_id}: " \
                                f"Duplizierte Vererbung in Tabelle '{s_current_table}'\n" \
                                f"Pfad: {s_pfad_string}"
                return s_error_message

            # Genau ein Datensatz gefunden - weiter navigieren
            s_current_id = s_found_id
            s_pfad_string += f" > {s_current_table}(id={s_current_id})"

        # Wenn wir hier ankommen, ist die Vererbungskette vollständig und konsistent
        return ""

    def _validate_type_specificity(self, s_bpmn_element_id: str, s_element_type: str) -> str:
        """Phase B: Validiert dass der element_type der spezifischste ist."""

        # Kinder des element_type aus Hierarchie ermitteln
        o_children = self._find_children_in_hierarchy(s_element_type)

        # Wenn keine Kinder, dann ist es bereits der spezifischste Typ
        if len(o_children) == 0:
            return ""

        # Aktuellen Datensatz in der element_type Tabelle finden um dessen PK zu ermitteln
        s_current_table_id = self._get_record_id_in_table(s_bpmn_element_id, s_element_type)

        if s_current_table_id == "":
            # Sollte nicht passieren wenn Phase A erfolgreich war
            s_error_message = f"INTERNER FEHLER: Konnte Datensatz nicht in Tabelle '{s_element_type}' finden"
            return s_error_message

        # Pfad für Fehlermeldung vorbereiten
        s_current_path = self._build_path_string(s_bpmn_element_id, s_element_type)

        # Für jedes Kind prüfen ob ein Datensatz existiert
        for v_child in o_children:
            s_child_table = v_child

            # FK-Spalte bilden
            s_fk_column = s_element_type + "_id"

            # In Kind-Tabelle nach Datensatz suchen
            s_found_child_id = self._find_record_in_child_table(s_child_table, s_fk_column, s_current_table_id)

            if s_found_child_id != "":
                # Spezifischerer Typ gefunden - das ist ein Fehler
                # Aber wir müssen noch tiefer suchen um den SPEZIFISCHSTEN zu finden
                s_found_specific_type = self._find_most_specific_type(s_child_table, s_found_child_id)

                # Fehlermeldung erstellen
                s_error_message = f"Validierungsfehler für bpmn_element_id={s_bpmn_element_id}:" \
                                f"element_type nicht spezifisch genug." \
                                f"Aktueller Pfad: {s_current_path}" \
                                f". Spezifischerer Typ {s_found_specific_type} als element_type in bpmn_element hinterlegt," \
                                f" aber in der Hierarchie wurde ein Kind vom Typ '{s_element_type}' gefunden, dies ist ein Widerspruch. Bitte fachlich prüfen und anschliessend korrigieren."
                return s_error_message

        # Wenn wir hier ankommen, wurde kein spezifischerer Typ gefunden, der Eingangstyp war bereits spezifisch
        return ""

    def _get_record_id_in_table(self, s_bpmn_element_id: str, s_table_name: str) -> str:
        """Ermittelt die ID eines Datensatzes in einer bestimmten Tabelle."""
        # Pfad von bpmn_element zur Zieltabelle ermitteln
        o_path = self._get_hierarchy_path("bpmn_element", s_table_name)

        if o_path is None:
            return ""

        # Navigation durch den Pfad
        s_current_id = s_bpmn_element_id

        # Bei bpmn_element starten, bis zur Zieltabelle navigieren
        for i in range(1, len(o_path)):
            s_current_table = o_path[i]
            s_previous_table = o_path[i - 1]

            # FK-Spalte bilden
            s_fk_column = s_previous_table + "_id"

            # Nächsten Datensatz finden
            s_current_id = self._navigate_to_child(s_current_table, s_fk_column, s_current_id)

            if s_current_id == "":
                # Navigation fehlgeschlagen
                return ""

        return s_current_id

    def _navigate_to_child(self, s_child_table: str, s_fk_column: str, s_parent_id: str) -> str:
        """Navigiert von Parent zu Child über FK."""
        o_table = self.get_table(s_child_table)
        o_condition = ConditionEquals(s_fk_column, s_parent_id)
        o_it = o_table.create_iterator(True, o_condition)

        if not o_it.is_empty():
            # PK-Spalte ist immer <tablename>_id
            return o_it.value(s_child_table + "_id")
        else:
            return ""

    def _find_record_in_child_table(self, s_child_table: str, s_fk_column: str, s_parent_id: str) -> str:
        """Sucht Datensatz in Kind-Tabelle."""
        # Nutzt NavigateToChild, da die Logik identisch ist
        return self._navigate_to_child(s_child_table, s_fk_column, s_parent_id)

    def _find_most_specific_type(self, s_start_table: str, s_start_id: str) -> str:
        """Findet den spezifischsten Typ durch rekursive Suche."""
        s_current_table = s_start_table
        s_current_id = s_start_id

        # Rekursiv nach unten suchen bis keine Kinder mehr gefunden werden
        while True:
            o_children = self._find_children_in_hierarchy(s_current_table)

            if len(o_children) == 0:
                # Keine Kinder mehr - das ist der spezifischste Typ
                return s_current_table

            # In allen Kindern suchen
            b_found_child = False

            for v_child in o_children:
                s_child_table = v_child

                # FK-Spalte bilden
                s_fk_column = s_current_table + "_id"

                # In Kind suchen
                s_child_id = self._navigate_to_child(s_child_table, s_fk_column, s_current_id)

                if s_child_id != "":
                    # Kind gefunden - weiter nach unten
                    s_current_table = s_child_table
                    s_current_id = s_child_id
                    b_found_child = True
                    break

            # Wenn kein Kind gefunden wurde, sind wir am Ende
            if not b_found_child:
                return s_current_table

    def _get_hierarchy_path(self, s_start_table: str, s_end_table: str) -> Optional[List[str]]:
        """Ermittelt den Hierarchie-Pfad von einer Start- zu einer Zieltabelle."""
        o_path: List[str] = []
        o_temp_path: List[str] = []

        # Spezialfall: Start und Ziel sind gleich
        if s_start_table == s_end_table:
            o_path.append(s_start_table)
            return o_path

        # Von der Zieltabelle rückwärts zum Start navigieren
        s_current_table = s_end_table
        b_found = False

        # Maximale Tiefe als Sicherheit gegen Endlosschleifen
        n_max_depth = 20
        n_depth = 0

        # Zieltabelle zum temporären Pfad hinzufügen
        o_temp_path.append(s_current_table)

        # Rückwärts navigieren bis zur Starttabelle
        while s_current_table != s_start_table and n_depth < n_max_depth and not b_found:
            n_depth += 1

            # Parent für aktuelle Tabelle suchen
            s_parent_table = self._find_parent_in_hierarchy(s_current_table)

            if s_parent_table == "":
                # Kein Parent gefunden - Pfad existiert nicht
                return None

            # Parent zum temporären Pfad hinzufügen
            o_temp_path.append(s_parent_table)
            s_current_table = s_parent_table

            # Prüfen ob wir beim Start angekommen sind
            if s_current_table == s_start_table:
                b_found = True

        # Wenn Pfad nicht gefunden wurde
        if not b_found:
            return None

        # Temporären Pfad umkehren (da wir rückwärts navigiert sind)
        for i in range(len(o_temp_path) - 1, -1, -1):
            o_path.append(o_temp_path[i])

        return o_path

    def _find_parent_in_hierarchy(self, s_child_table: str) -> str:
        """Findet die Elterntabelle für eine gegebene Tabelle."""
        # Nach Einträgen suchen wo child = s_child_table
        o_condition = ConditionEquals("child", s_child_table)
        o_it = self._hierarchy_container.create_iterator(True, o_condition)

        # Sollte genau einen Eintrag geben (außer bei Root)
        if not o_it.is_empty():
            return o_it.value("parent")
        else:
            # Kein Parent gefunden (könnte Root sein oder Tabelle nicht in Hierarchie)
            return ""

    def _find_children_in_hierarchy(self, s_parent_table: str) -> List[str]:
        """Findet alle direkten Kinder einer Tabelle in der Hierarchie."""
        o_children: List[str] = []

        # Nach Einträgen suchen wo parent = s_parent_table
        o_condition = ConditionEquals("parent", s_parent_table)
        o_it = self._hierarchy_container.create_iterator(True, o_condition)

        # Alle gefundenen Kinder sammeln
        while not o_it.is_empty():
            o_children.append(o_it.value("child"))
            o_it.pp()

        return o_children

    def _build_path_string(self, s_bpmn_element_id: str, s_element_type: str) -> str:
        """Erstellt einen Pfad-String für Fehlermeldungen."""
        # Pfad ermitteln
        o_path = self._get_hierarchy_path("bpmn_element", s_element_type)

        # String aufbauen
        s_path = f"bpmn_element(id={s_bpmn_element_id}, element_type='{s_element_type}')"

        if o_path is not None and len(o_path) > 1:
            # Restlichen Pfad hinzufügen
            for i in range(1, len(o_path)):
                s_path += f" > {o_path[i]}"

        return s_path

    def _get_path_string(self, o_path: List[str]) -> str:
        """Erstellt einen String aus dem Pfad-Collection."""
        s_result = ""
        for i in range(len(o_path)):
            if i > 0:
                s_result += " > "
            s_result += o_path[i]

        return s_result

    def find_element_id_by_name(self, name: str, process_id: str) -> Optional[str]:
        """
        Sucht Element-ID anhand des Namens innerhalb eines Prozesses.

        Args:
            name: Name des Elements (z.B. "Solution Designer")
            process_id: Prozess-ID als Scope-Einschränkung

        Returns:
            Element-ID oder None wenn nicht gefunden
        """
        bpmn_element_table = self.m_database.get_table("bpmn_element")
        process_element_table = self.m_database.get_table("process_element")

        # Nach name in bpmn_element filtern
        name_condition = ConditionEquals("name", name)
        name_iterator = bpmn_element_table.create_iterator(True, name_condition)

        while not name_iterator.is_empty():
            element_id = name_iterator.value("bpmn_element_id")

            # Prüfen ob Element zum Prozess gehört
            process_condition = ConditionEquals("bpmn_element_id", element_id)
            process_iterator = process_element_table.create_iterator(True, process_condition)

            while not process_iterator.is_empty():
                if process_iterator.value("bpmn_process_id") == process_id:
                    return element_id
                process_iterator.pp()

            name_iterator.pp()

        return None
