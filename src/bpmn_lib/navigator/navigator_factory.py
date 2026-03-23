"""
Navigator Factory - Erstellt BPMNHierarchyNavigator aus Markdown-Dokumenten.

Factory-Funktion die die komplette Aufbau-Kette kapselt:
1. Schema parsen
2. Daten laden
3. Constraints validieren
4. Indizes erstellen
5. Navigator erstellen
"""

from pathlib import Path
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework import MarkdownDocument
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.schema.database_schema_parser import DatabaseSchemaParser
from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator


def create_navigator(
    schema_file: str,
    data_file: str,
    hierarchy_file: str,
    log_dir: str,
) -> BPMNHierarchyNavigator:
    """
    Factory-Funktion: Erstellt BPMNHierarchyNavigator aus drei Markdown-Dateien.

    Args:
        schema_file: Pfad zur Schema-Definition (Tabellen, Spalten, Constraints)
        data_file: Pfad zu den Daten (BPMN-Elemente)
        hierarchy_file: Pfad zur Vererbungshierarchie (parent-child)
        log_dir: Verzeichnis für Validation-Reports bei Fehlern

    Returns:
        BPMNHierarchyNavigator für Navigation durch die Prozessbeschreibung

    Raises:
        ValueError: Wenn Dateien nicht existieren
        Exception: Bei Validierungsfehlern (nach Schreiben des Reports)
    """
    # log_msg(f"Navigator-Factory: Starte Aufbau aus {schema_file}, {data_file}, {hierarchy_file}")

    # Dateien validieren
    _validate_file_exists(schema_file, "Schema-Datei")
    _validate_file_exists(data_file, "Daten-Datei")
    _validate_file_exists(hierarchy_file, "Hierarchie-Datei")

    # Schema-Name aus Dateiname ableiten
    schema_name = Path(schema_file).stem

    # ValidationResult erstellen
    val_result = ValidationResult()

    # 1. Markdown-Dokumente laden
    log_msg("1. Lade Markdown-Dokumente...")
    schema_doc = MarkdownDocument()
    schema_doc.load_from_file(schema_file)

    data_doc = MarkdownDocument()
    data_doc.load_from_file(data_file)

    hierarchy_doc = MarkdownDocument()
    hierarchy_doc.load_from_file(hierarchy_file)

    # 2. Schema parsen
    log_msg("2. Parse Schema...")
    parser = DatabaseSchemaParser()
    schema = parser.parse_documents(val_result, schema_doc, schema_name)

    # 3. Datenbank aufbauen
    log_msg("3. Baue Datenbank auf...")
    builder = DatabaseBuilder(schema, val_result)

    # 4. Daten laden
    log_msg("4. Lade Daten...")
    data_dict = data_doc.create_table_dictionary()
    builder.load_all_data(data_dict)

    # 5. Constraints validieren
    log_msg("5. Validiere Constraints...")
    builder.validate_all_constraints()

    # 6. Prüfen auf Fehler nach Constraint-Validierung
    if val_result.has_errors():
        filepath = val_result.write_to_file(log_dir, "validation_constraints")
        log_and_raise(
            f"Constraint-Validierung fehlgeschlagen: {val_result.count()} Fehler. "
            f"Details siehe: {filepath}"
        )

    # 7. Indizes erstellen
    log_msg("7. Erstelle Indizes...")
    builder.build_indexes_if_valid()

    # 8. Read-Only Datenbank erstellen
    log_msg("8. Erstelle Read-Only Datenbank...")
    database = builder.create_read_only_database()

    # 9. Navigator erstellen
    log_msg("9. Erstelle Navigator...")
    navigator = BPMNHierarchyNavigator(val_result, database, hierarchy_doc)

    # 10. Finale Fehlerprüfung (Navigator-Validierung)
    if val_result.has_errors():
        filepath = val_result.write_to_file(log_dir, "validation_navigator")
        log_and_raise(
            f"Navigator-Validierung fehlgeschlagen: {val_result.count()} Fehler. "
            f"Details siehe: {filepath}"
        )

    log_msg("Navigator-Factory: Aufbau erfolgreich abgeschlossen.")
    return navigator


def _validate_file_exists(filepath: str, description: str) -> None:
    """Prüft ob eine Datei existiert."""
    path = Path(filepath)
    if not path.exists():
        log_and_raise(f"{description} nicht gefunden: {filepath}")
    if not path.is_file():
        log_and_raise(f"{description} ist keine Datei: {filepath}")
