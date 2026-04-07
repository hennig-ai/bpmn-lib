"""
DatabaseSchema - Zentrale Verwaltung des gesamten Datenbankschemas.
"""

from typing import Any, Dict, List
from basic_framework.proc_frame import log_msg, log_and_raise
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship


class DatabaseSchema:
    """Zentrale Verwaltung des gesamten Datenbankschemas."""

    def __init__(self, oValResult: ValidationResult, sSchemaName: str) -> None:
        """Initialisiert das DatabaseSchema."""
        self.m_SchemaName = sSchemaName
        self.m_TableDefinitions: Dict[str, Any] = {}
        self.m_Relationships: List[Any] = []
        self.m_oResult = oValResult

        # log_msg(f"DatabaseSchema '{self.m_SchemaName}' initialisiert.")

    def add_table_definition(self, oTableDef: TableDefinition) -> None:
        """Fügt eine Tabellendefinition zum Schema hinzu."""
        sTableName = oTableDef.get_table_name()

        # Prüfen ob Tabelle bereits existiert
        if sTableName in self.m_TableDefinitions:
            self.m_oResult.add_error(f"Tabelle '{sTableName}' existiert bereits im Schema.")
            return

        # Tabelle hinzufügen
        self.m_TableDefinitions[sTableName] = oTableDef

    def add_relationship(self, oRelationship: ForeignKeyRelationship) -> None:
        """Fügt eine Foreign Key Beziehung hinzu."""
        self.m_Relationships.append(oRelationship)

    def validate_schema(self) -> bool:
        """Validiert das gesamte Schema auf Konsistenz."""
        log_msg(f"Starte Schema-Validierung fuer '{self.m_SchemaName}'...")

        # 1. Prüfe ob alle FK-Referenzen existieren
        self.validate_foreign_key_references()

        # 2. Prüfe ob alle Tabellen mindestens einen PK haben
        self.validate_primary_keys()

        # 3. Prüfe Datentyp-Kompatibilität bei FK-Beziehungen
        self.validate_data_type_compatibility()

        # 4. Prüfe zirkuläre Abhängigkeiten NOCH NICHT IMPLEMENTIERT!
        self.validate_circular_dependencies()

        # Ergebnis
        if self.m_oResult.count() == 0:
            log_msg("Schema-Validierung erfolgreich abgeschlossen.")
            return True
        else:
            log_msg(f"Schema-Validierung fehlgeschlagen mit {self.m_oResult.count()} Fehlern.")
            return False

    def validate_foreign_key_references(self) -> None:
        """Validiert Foreign Key Referenzen."""
        for oFKRel in self.m_Relationships:
            # Prüfe ob Quelltabelle existiert
            if oFKRel.get_source_table() not in self.m_TableDefinitions:
                self.m_oResult.add_error(f"FK-Beziehung: Quelltabelle '{oFKRel.get_source_table()}' existiert nicht.")

            # Prüfe ob Zieltabelle existiert
            if oFKRel.get_target_table() not in self.m_TableDefinitions:
                self.m_oResult.add_error(f"FK-Beziehung: Zieltabelle '{oFKRel.get_target_table()}' existiert nicht.")

            # Prüfe ob Spalten existieren
            if oFKRel.get_source_table() in self.m_TableDefinitions:
                oSourceTable = self.m_TableDefinitions[oFKRel.get_source_table()]
                if not oSourceTable.has_column(oFKRel.get_source_column()):
                    self.m_oResult.add_error(f"FK-Beziehung: Spalte '{oFKRel.get_source_column()}' "
                                          f"existiert nicht in Tabelle '{oFKRel.get_source_table()}'.")

            if oFKRel.get_target_table() in self.m_TableDefinitions:
                oTargetTable = self.m_TableDefinitions[oFKRel.get_target_table()]
                if not oTargetTable.has_column(oFKRel.get_target_column()):
                    self.m_oResult.add_error(f"FK-Beziehung: Spalte '{oFKRel.get_target_column()}' "
                                          f"existiert nicht in Tabelle '{oFKRel.get_target_table()}'.")

    def validate_primary_keys(self) -> None:
        """Validiert Primary Keys."""
        for vKey in self.m_TableDefinitions.keys():
            oTableDef = self.m_TableDefinitions[vKey]

            if len(oTableDef.get_primary_key_columns()) == 0:
                # Warnung statt Fehler - manche Tabellen könnten ohne PK valide sein
                log_msg(f"WARNUNG: Tabelle '{vKey}' hat keinen Primary Key definiert.")

    def validate_data_type_compatibility(self) -> None:
        """Validiert Datentyp-Kompatibilität."""
        for oFKRel in self.m_Relationships:
            # Nur prüfen wenn beide Tabellen existieren
            if (oFKRel.get_source_table() in self.m_TableDefinitions and
                oFKRel.get_target_table() in self.m_TableDefinitions):

                oSourceTable = self.m_TableDefinitions[oFKRel.get_source_table()]
                oTargetTable = self.m_TableDefinitions[oFKRel.get_target_table()]

                # Spalten-Definitionen holen
                oSourceCol = oSourceTable.get_column(oFKRel.get_source_column())
                oTargetCol = oTargetTable.get_column(oFKRel.get_target_column())

                if oSourceCol is not None and oTargetCol is not None:
                    # Datentypen vergleichen
                    if oSourceCol.get_data_type() != oTargetCol.get_data_type():
                        self.m_oResult.add_error(f"FK-Beziehung: Inkompatible Datentypen zwischen '"
                                              f"{oFKRel.get_source_table()}.{oFKRel.get_source_column()}' "
                                              f"({oSourceCol.get_data_type()}) und '"
                                              f"{oFKRel.get_target_table()}.{oFKRel.get_target_column()}' "
                                              f"({oTargetCol.get_data_type()}).")

    def validate_circular_dependencies(self) -> None:
        """Validiert zirkuläre Abhängigkeiten (vereinfachte Version)."""
        # Implementierung würde Graphen-Traversierung erfordern
        # Für diese Version nur als Platzhalter
        log_msg("Pruefung auf zirkulaere Abhaengigkeiten uebersprungen (noch nicht implementiert).")

    def has_table(self, sTableName: str) -> bool:
        """Prüft ob eine Tabelle mit dem Namen existiert."""
        return sTableName in self.m_TableDefinitions

    def get_table_definition(self, sTableName: str) -> TableDefinition:
        """Gibt eine spezifische Tabellendefinition zurück."""
        if sTableName in self.m_TableDefinitions:
            return self.m_TableDefinitions[sTableName]
        else:
            log_and_raise(f"Tabelle '{sTableName}' existiert nicht im Schema.")

    def get_table_names(self) -> List[str]:
        """Gibt alle Tabellennamen zurück."""
        return list(self.m_TableDefinitions.keys())

    def get_relationships(self) -> List[ForeignKeyRelationship]:
        """Gibt alle Beziehungen zurück."""
        return self.m_Relationships

    def get_schema_name(self) -> str:
        """Gibt den Schema-Namen zurück."""
        return self.m_SchemaName

    def add_validation_error(self, sError: str) -> None:
        """Fügt einen Validierungsfehler hinzu."""
        log_msg(sError)
        self.m_oResult.add_error(sError)

    def get_statistics(self) -> str:
        """Gibt Statistiken über das Schema zurück."""
        sStats = f"Schema: {self.m_SchemaName}\n"
        sStats += f"Anzahl Tabellen: {len(self.m_TableDefinitions)}\n"
        sStats += f"Anzahl Beziehungen: {len(self.m_Relationships)}\n"

        # Spalten zählen
        nTotalColumns = 0
        for vKey in self.m_TableDefinitions.keys():
            oTableDef = self.m_TableDefinitions[vKey]
            nTotalColumns += oTableDef.get_column_count()
        sStats += f"Gesamtanzahl Spalten: {nTotalColumns}\n"

        return sStats
