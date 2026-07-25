"""
DatabaseSchemaParser - Erstellt DatabaseSchema aus dem TableDictionary.
"""

from typing import Dict, List, Optional, Tuple
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework.container_utils.container_in_memory import ContainerInMemory
from basic_framework.container_utils.abstract_container import AbstractContainer
from basic_framework.container_utils.abstract_iterator import AbstractIterator
from basic_framework.container_utils.knot_object import KnotObject
from basic_framework import MarkdownDocument
from bpmn_lib.database.schema.database_schema import DatabaseSchema
from bpmn_lib.database.schema.table_definition import TableDefinition
from bpmn_lib.database.schema.column_definition import ColumnDefinition
from bpmn_lib.database.schema.foreign_key_relationship import ForeignKeyRelationship
from bpmn_lib.utils.validation_result import ValidationResult

# Ueberschrift, die einen Tabellenabschnitt einleitet: "### Table: bpmn_element"
TABLE_HEADING_PREFIX = "Table: "

# Einleitung einer Wertedomaene: "**Value Domain for element_type**: [...]"
VALUE_DOMAIN_MARKER = "Value Domain for"


class DatabaseSchemaParser:
    """Erstellt DatabaseSchema aus dem TableDictionary."""

    def __init__(self):
        """Konstruktor."""
        self.m_TableDict: Dict[str, ContainerInMemory] = {}
        self.m_Schema: Optional[DatabaseSchema] = None
        # (Tabellenname, Spaltenname) -> erlaubte Werte
        self.m_ValueDomains: Dict[Tuple[str, str], List[str]] = {}

    def parse_documents(self, oValResult: ValidationResult, oDoc: MarkdownDocument,
                      sSchemaName: str = "BPMN Schema") -> DatabaseSchema:
        """Parst das Schema aus einem TableDictionary."""
        self.m_TableDict = oDoc.create_table_dictionary()

        self.m_ValueDomains = {}

        # DatabaseSchema erstellen
        self.m_Schema = DatabaseSchema(oValResult, sSchemaName)

        # log_msg(f"SchemaParser: Starte Parsing von {len(self.m_TableDict)} Tabellen...")

        # 1. Alle Tabellendefinitionen erstellen
        self.parse_all_tables()

        # 2. Foreign Key Beziehungen extrahieren
        self.parse_all_relationships()

        # 3. Value Domains aus den Absaetzen zwischen den Tabellen uebernehmen
        self.parse_value_domains(oDoc)

        # 4. Schema validieren
        if self.m_Schema.validate_schema():
            log_msg("SchemaParser: Schema erfolgreich geparst und validiert.")
        else:
            log_msg("SchemaParser: Schema geparst, aber Validierung hat Fehler gefunden.")

        return self.m_Schema

    def build_hierarchy_information(self, oDoc: MarkdownDocument) -> AbstractContainer:
        """Baut Hierarchieinformationen aus dem Dokument."""
        oTableList = oDoc.create_table_dictionary()

        if len(oTableList) != 1:
            log_and_raise("ParseDocument: Fehler beim Einlesen der Hierarchie in der Datei ist mehr als eine Tabelle definiert.")

        oTable = list(oTableList.values())[0]
        return oTable

    def build_hierarchy_information_old(self, oDoc: MarkdownDocument) -> Dict[str, KnotObject]:
        """Alte Version der Hierarchie-Erstellung."""
        oTableList = oDoc.create_table_dictionary()

        if len(oTableList) != 1:
            log_and_raise("ParseDocument: Fehler beim Einlesen der Hierarchie in der Datei ist mehr als eine Tabelle definiert.")

        oTable = list(oTableList.values())[0]

        oResult: Dict[str, KnotObject] = {}
        # Create iterator to go through rows
        oIT = oTable.create_iterator()

        while not oIT.is_empty():
            parentEntity = oIT.value("parent")
            childEntity = oIT.value("child")

            if parentEntity not in oResult:
                # Neuen Knoten erstellen
                ParentNode = KnotObject()
                ParentNode.init("", parentEntity, None)
                oResult[parentEntity] = ParentNode
            else:
                # Existierenden Knoten verwenden
                ParentNode = oResult[parentEntity]

            # Child-Knoten holen oder erstellen
            if childEntity not in oResult:
                # Neuen Knoten erstellen und direkt mit Parent verknüpfen
                childNode = KnotObject()
                childNode.init("", childEntity, ParentNode)
                oResult[childEntity] = childNode
            else:
                log_and_raise(f"das Kind {childEntity} Sollte bei 2 Vaetern als Kind angemeldet werden das geht nicht.")
                # Existierenden Knoten verwenden und mit Parent verknüpfen,
                # aber nur wenn noch kein Parent gesetzt ist
                childNode = oResult[childEntity]
                if childNode.m_oParent is None:
                    childNode.init(childEntity, "", ParentNode)

            oIT.pp()

        return oResult

    def _get_schema(self) -> DatabaseSchema:
        """Gibt das Schema zurück. Wirft Fehler wenn Schema nicht initialisiert."""
        if self.m_Schema is None:
            log_and_raise(ValueError("Schema not initialized - call parse_documents() first"))
        return self.m_Schema

    def parse_all_tables(self) -> None:
        """Parst alle Tabellen."""
        for vTableName in self.m_TableDict.keys():
            self.parse_single_table(vTableName)

    def parse_single_table(self, sTableName: str) -> None:
        """Parst eine einzelne Tabelle."""
        # Container holen
        oContainer = self.m_TableDict[sTableName]

        # TableDefinition erstellen
        # Beschreibung aus ersten Zeile extrahieren (falls vorhanden)
        sDescription = self.extract_description(oContainer)

        oTableDef = TableDefinition(sTableName, sDescription)

        # Spalten parsen
        self.parse_columns(oTableDef, oContainer)

        # Zum Schema hinzufügen
        self._get_schema().add_table_definition(oTableDef)

    def extract_description(self, oContainer: ContainerInMemory) -> str:
        """Extrahiert die Beschreibung aus der ersten Datenzeile."""
        # Prüfen ob Description-Spalte existiert
        if not oContainer.field_exists("Description"):
            raise ValueError(f"keine Description gefunden für den Container {oContainer.get_technical_container_name()}")

        # Erste Zeile lesen
        oIterator = oContainer.create_iterator()

        if not oIterator.is_empty():
            return oIterator.value("Description")
        else:
            raise ValueError(f"ExtractDescription:: Keinen Datensatz gefunden für {oContainer.get_technical_container_name()}")
            return ""

    def parse_columns(self, oTableDef: TableDefinition, oContainer: ContainerInMemory) -> None:
        """Parst die Spalten einer Tabelle."""
        # Durch alle Zeilen iterieren (jede Zeile = eine Spaltendefinition)
        oIterator = oContainer.create_iterator()

        while not oIterator.is_empty():
            self.parse_single_column(oTableDef, oIterator)
            oIterator.pp()

    def parse_single_column(self, oTableDef: TableDefinition, oIterator: AbstractIterator) -> None:
        """Parst eine einzelne Spaltendefinition."""
        # Spaltenname
        sColumnName = oIterator.value("Column")

        if sColumnName == "":
            log_msg(f"WARNUNG: Leere Spalte in Tabelle '{oTableDef.get_table_name()}' uebersprungen.")
            return

        # Datentyp
        sDataType = oIterator.value("Data Type")

        # Beschreibung
        sDescription = oIterator.value("Description")

        # ColumnDefinition erstellen
        oColDef = ColumnDefinition(sColumnName, sDataType, sDescription)

        # Constraints parsen
        self.parse_constraints(oColDef, oIterator.value("Constraints"))

        # References parsen (für FK-Beziehungen)
        sReferences = oIterator.value("References")
        if sReferences != "":
            # FK-Flag setzen
            oColDef.set_foreign_key(True)
            # Beziehung wird später in ParseAllRelationships verarbeitet

        # Zur Tabellendefinition hinzufügen
        oTableDef.add_column(oColDef)

    def parse_constraints(self, oColDef: ColumnDefinition, sConstraints: str) -> None:
        """Parst Constraints aus dem Constraint-String."""
        if sConstraints == "":
            return

        # Constraints in Großbuchstaben für Vergleich
        sUpper = sConstraints.upper()

        # PRIMARY KEY
        if "PK" in sUpper or "PRIMARY KEY" in sUpper:
            oColDef.set_primary_key(True)

        # NOT NULL
        if "NOT NULL" in sUpper:
            oColDef.set_nullable(False)

        # UNIQUE
        if "UNIQUE" in sUpper and "PK" not in sUpper:
            oColDef.set_unique(True)

        # AUTO_INCREMENT
        if "AUTO_INCREMENT" in sUpper or "AUTOINCREMENT" in sUpper:
            oColDef.set_auto_increment(True)

        # FOREIGN KEY
        if "FK" in sUpper or "FOREIGN KEY" in sUpper:
            oColDef.set_foreign_key(True)

    def parse_all_relationships(self) -> None:
        """Parst alle Foreign Key Beziehungen."""
        # log_msg("Parse Foreign Key Beziehungen...")

        for vTableName in self.m_TableDict.keys():
            self.parse_table_relationships(vTableName)

    def parse_table_relationships(self, sTableName: str) -> None:
        """Parst FK-Beziehungen einer Tabelle."""
        # Container holen
        oContainer = self.m_TableDict[sTableName]

        # TableDefinition holen
        oTableDef = self._get_schema().get_table_definition(sTableName)

        # Durch alle Spalten iterieren
        oIterator = oContainer.create_iterator()

        while not oIterator.is_empty():
            # References prüfen
            if oIterator.field_exists("References"):
                sReferences = oIterator.value("References")

                if sReferences != "":
                    # FK-Beziehung parsen
                    self.parse_foreign_key_reference(oTableDef, oIterator.value("Column"), sReferences)

            oIterator.pp()

    def parse_foreign_key_reference(self, oTableDef: TableDefinition, sSourceColumn: str, sReference: str) -> None:
        """Parst eine einzelne FK-Referenz."""
        # Reference Format: "table_name.column_name"
        nDotPos = sReference.find(".")

        if nDotPos > 0:
            sTargetTable = sReference[:nDotPos]
            sTargetColumn = sReference[nDotPos + 1:]

            # ForeignKeyRelationship erstellen
            oFKRel = ForeignKeyRelationship(
                oTableDef.get_table_name(), sSourceColumn, sTargetTable, sTargetColumn
            )

            # Zur Tabelle hinzufügen
            oTableDef.add_foreign_key(oFKRel)

            # Zum Schema hinzufügen
            self._get_schema().add_relationship(oFKRel)

            # log_msg(f"FK-Beziehung gefunden: {oFKRel.get_description()}")
        else:
            log_msg(f"WARNUNG: Ungueltiges Reference-Format in Tabelle '{oTableDef.get_table_name()}', "
                   f"Spalte '{sSourceColumn}': {sReference}")

    def parse_value_domains(self, oMarkdownDoc: MarkdownDocument) -> None:
        """Sucht und parst Value Domains (aus separaten Markdown-Strukturen)."""
        log_msg("Suche Value Domains im Markdown-Dokument...")

        # Rekursiv nach Value Domain Definitionen suchen
        self.search_value_domains_recursive(oMarkdownDoc.get_root_node(), "")

        # Gefundene Value Domains auf Tabellen anwenden
        self.apply_value_domains()

    def search_value_domains_recursive(self, oNode: KnotObject, sTableName: str) -> None:
        """Rekursive Suche nach Value Domains.

        Das Markdown-Modell kennt keinen Absatz-Knoten: Eine Zeile wie
        '**Value Domain for x**: [...]' landet als Listeneintrag im Baum, und ihr
        Text steht im Schluessel, unter dem der Knoten am Elternknoten haengt -
        nicht in einem Leaf.

        Zugeordnet wird die Domaene der Tabelle, in deren Abschnitt sie steht.
        Ueber den Spaltennamen allein ginge es schief: 'implementation' gibt es in
        task, user_task und business_rule_task, aber nur task hat eine Domaene.
        """
        for vKey in oNode.get_children().keys():
            sKey = str(vKey)
            sChildTable = sTableName

            if sKey.startswith(TABLE_HEADING_PREFIX):
                sChildTable = sKey[len(TABLE_HEADING_PREFIX):].strip()
            elif VALUE_DOMAIN_MARKER in sKey:
                self.parse_value_domain_content(sKey, sTableName)

            self.search_value_domains_recursive(oNode.get_child(vKey), sChildTable)

    def parse_value_domain_content(self, sContent: str, sTableName: str) -> None:
        """Parst Value Domain Content."""
        # Format: **Value Domain for column_name**: ["value1", "value2", ...]

        if sTableName == "":
            log_and_raise(ValueError(
                f"Value Domain steht ausserhalb eines Tabellenabschnitts und ist "
                f"keiner Spalte zuzuordnen: '{sContent[:80]}'"
            ))

        # Spaltenname extrahieren
        nStart = sContent.find(VALUE_DOMAIN_MARKER) + len(VALUE_DOMAIN_MARKER)
        nEnd = sContent.find(":", nStart)

        if nEnd <= nStart:
            log_and_raise(ValueError(
                f"Value Domain in Tabelle '{sTableName}' ohne Doppelpunkt nach dem "
                f"Spaltennamen: '{sContent[:80]}'"
            ))

        # ** entfernen falls vorhanden
        sColumnSpec = sContent[nStart:nEnd].strip().replace("**", "").replace("*", "").strip()

        # Werte extrahieren
        nStart = sContent.find("[", nEnd)
        nEnd = sContent.find("]", nStart)

        if nStart < 0 or nEnd <= nStart:
            log_and_raise(ValueError(
                f"Value Domain fuer '{sTableName}.{sColumnSpec}' ohne Werteliste "
                f"in eckigen Klammern: '{sContent[:80]}'"
            ))

        oAllowedValues = self.parse_value_list(sContent[nStart + 1:nEnd])

        if len(oAllowedValues) == 0:
            log_and_raise(ValueError(
                f"Value Domain fuer '{sTableName}.{sColumnSpec}' enthaelt keine Werte"
            ))

        vDomainKey = (sTableName, sColumnSpec)

        if vDomainKey not in self.m_ValueDomains:
            self.m_ValueDomains[vDomainKey] = oAllowedValues
            log_msg(f"Value Domain gefunden fuer '{sTableName}.{sColumnSpec}' mit "
                   f"{len(oAllowedValues)} erlaubten Werten.")

    def parse_value_list(self, sValues: str) -> List[str]:
        """Parst eine Liste von Werten."""
        oValues: List[str] = []

        # Werte durch Komma trennen
        aValues = sValues.split(",")

        for i in range(len(aValues)):
            sValue = aValues[i].strip()

            # Anführungszeichen entfernen
            sValue = sValue.replace('"', '')
            sValue = sValue.replace("'", '')
            sValue = sValue.strip()

            if sValue != "":
                oValues.append(sValue)

        return oValues

    def apply_value_domains(self) -> None:
        """Wendet gefundene Value Domains auf genau ihre Tabelle und Spalte an."""
        if len(self.m_ValueDomains) == 0:
            return

        log_msg(f"Wende {len(self.m_ValueDomains)} Value Domains an...")

        o_schema = self._get_schema()

        for (sTableName, sColumnName), oAllowedValues in self.m_ValueDomains.items():
            if not o_schema.has_table(sTableName):
                log_and_raise(ValueError(
                    f"Value Domain verweist auf die unbekannte Tabelle '{sTableName}'"
                ))

            oTableDef = o_schema.get_table_definition(sTableName)

            if not oTableDef.has_column(sColumnName):
                log_and_raise(ValueError(
                    f"Value Domain verweist auf die Spalte '{sColumnName}', "
                    f"die es in Tabelle '{sTableName}' nicht gibt"
                ))

            oTableDef.add_value_domain(sColumnName, oAllowedValues)
            log_msg(f"Value Domain angewendet auf {sTableName}.{sColumnName}")
