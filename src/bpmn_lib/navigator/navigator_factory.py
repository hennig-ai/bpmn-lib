"""
Navigator Factory - Erstellt BPMNHierarchyNavigator aus Markdown-Dokumenten.

Factory-Funktion die die komplette Aufbau-Kette kapselt:
1. Schema parsen
2. Daten laden
3. Constraints validieren
4. Indizes erstellen
5. Navigator erstellen
"""

from typing import Optional, TextIO, Union
from pathlib import Path
from basic_framework.proc_frame import log_msg, log_and_raise
from basic_framework import MarkdownDocument
from bpmn_lib.utils.validation_result import ValidationResult
from bpmn_lib.database.schema.database_schema_parser import DatabaseSchemaParser
from bpmn_lib.database.instance.database_builder import DatabaseBuilder
from bpmn_lib.navigator.bpmn_hierarchy_navigator import BPMNHierarchyNavigator
from bpmn_lib.validation.rule_store import build_rule_store
from bpmn_lib.validation.rule_engine import BPMNRuleEngine
from bpmn_lib.validation.exceptions import BPMNValidationError


def create_navigator(
    schema_file: str,
    data_file: str,
    hierarchy_file: str,
    report_target: Optional[Union[str, TextIO]] = None,
    rules_dir: Optional[str] = None,
    validation_level: Optional[str] = None,
) -> BPMNHierarchyNavigator:
    """
    Factory-Funktion: Erstellt BPMNHierarchyNavigator aus drei Markdown-Dateien.

    Args:
        schema_file: Pfad zur Schema-Definition (Tabellen, Spalten, Constraints)
        data_file: Pfad zu den Daten (BPMN-Elemente)
        hierarchy_file: Pfad zur Vererbungshierarchie (parent-child)
        report_target: Ziel für Validation-Reports bei Fehlern.
            None = kein Report, str = Verzeichnispfad für Dateiausgabe,
            TextIO (z.B. sys.stdout) = Stream-Ausgabe
        rules_dir: Pfad zum Verzeichnis mit BPMN-Validierungsregeln (*.md).
            Muss zusammen mit validation_level angegeben werden.
        validation_level: Validierungsstufe (basic, spec_v2, best_practice, personal).
            Muss zusammen mit rules_dir angegeben werden.

    Returns:
        BPMNHierarchyNavigator für Navigation durch die Prozessbeschreibung

    Raises:
        ValueError: Wenn Dateien nicht existieren oder Parameter inkonsistent
        Exception: Bei Validierungsfehlern (nach Schreiben des Reports)
    """
    # log_msg(f"Navigator-Factory: Starte Aufbau aus {schema_file}, {data_file}, {hierarchy_file}")

    # Parameter-Konsistenzcheck: rules_dir und validation_level muessen beide gesetzt oder beide None sein
    if (rules_dir is None) != (validation_level is None):
        log_and_raise(ValueError(
            "rules_dir and validation_level must both be provided or both be None"
        ))

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
        if report_target is not None:
            filepath = val_result.write_report(report_target, "validation_constraints")
            if filepath is not None:
                log_and_raise(
                    f"Constraint-Validierung fehlgeschlagen: {val_result.count()} Fehler. "
                    f"Details siehe: {filepath}"
                )
        log_and_raise(
            f"Constraint-Validierung fehlgeschlagen: {val_result.count()} Fehler."
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
        if report_target is not None:
            filepath = val_result.write_report(report_target, "validation_navigator")
            if filepath is not None:
                log_and_raise(
                    f"Navigator-Validierung fehlgeschlagen: {val_result.count()} Fehler. "
                    f"Details siehe: {filepath}"
                )
        log_and_raise(
            f"Navigator-Validierung fehlgeschlagen: {val_result.count()} Fehler."
        )

    # BPMN-Regelvalidierung (optional)
    if rules_dir is not None and validation_level is not None:
        # D.8: val_result.clear() vor BPMN-Validierung
        val_result.clear()

        log_msg("11. Lade BPMN-Validierungsregeln...")
        rule_store = build_rule_store(rules_dir, navigator)

        log_msg("12. Fuehre BPMN-Validierung durch...")
        engine = BPMNRuleEngine(navigator, val_result)
        engine.validate(rule_store, validation_level)

        if val_result.has_errors():
            if report_target is not None:
                filepath = val_result.write_report(report_target, "validation_bpmn_rules")
                if filepath is not None:
                    log_and_raise(BPMNValidationError(
                        f"BPMN-Regelvalidierung fehlgeschlagen: {val_result.count()} Fehler. "
                        f"Details siehe: {filepath}"
                    ))
            log_and_raise(BPMNValidationError(
                f"BPMN-Regelvalidierung fehlgeschlagen: {val_result.count()} Fehler."
            ))

    log_msg("Navigator-Factory: Aufbau erfolgreich abgeschlossen.")
    return navigator


def _validate_file_exists(filepath: str, description: str) -> None:
    """Prüft ob eine Datei existiert."""
    path = Path(filepath)
    if not path.exists():
        log_and_raise(f"{description} nicht gefunden: {filepath}")
    if not path.is_file():
        log_and_raise(f"{description} ist keine Datei: {filepath}")
