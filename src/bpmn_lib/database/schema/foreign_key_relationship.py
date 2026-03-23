"""
ForeignKeyRelationship - Definiert eine FK-Beziehung zwischen zwei Tabellen.
"""

from basic_framework.proc_frame import log_and_raise


class ForeignKeyRelationship:
    """Definiert eine FK-Beziehung zwischen zwei Tabellen."""

    def __init__(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        relationship_name: str = ""
    ) -> None:
        """
        Initialisiert die ForeignKeyRelationship.

        Args:
            source_table: Name der Quelltabelle
            source_column: Name der Quellspalte
            target_table: Name der Zieltabelle
            target_column: Name der Zielspalte
            relationship_name: Optionaler Name der Beziehung. Wird automatisch generiert falls leer.

        Raises:
            ValueError: Wenn ein erforderlicher Parameter leer ist
        """
        # Validate required parameters
        if not source_table:
            raise ValueError("source_table cannot be empty")
        if not source_column:
            raise ValueError("source_column cannot be empty")
        if not target_table:
            raise ValueError("target_table cannot be empty")
        if not target_column:
            raise ValueError("target_column cannot be empty")

        self._source_table: str = source_table
        self._source_column: str = source_column
        self._target_table: str = target_table
        self._target_column: str = target_column

        # Automatischen Namen generieren falls nicht angegeben
        if relationship_name == "":
            self._relationship_name = (f"FK_{self._source_table}_{self._source_column}_"
                                      f"{self._target_table}_{self._target_column}")
        else:
            self._relationship_name = relationship_name

        # Standard-Aktionen
        self._on_delete: str = "RESTRICT"
        self._on_update: str = "CASCADE"

    def set_on_delete(self, sAction: str) -> None:
        """Setter für ON DELETE Aktion."""
        # Validiere Aktion
        action_upper = sAction.upper()
        if action_upper in ["CASCADE", "SET NULL", "RESTRICT", "NO ACTION"]:
            self._on_delete = action_upper
        else:
            log_and_raise(f"Ungueltige ON DELETE Aktion: {sAction}")

    def set_on_update(self, sAction: str) -> None:
        """Setter für ON UPDATE Aktion."""
        # Validiere Aktion
        action_upper = sAction.upper()
        if action_upper in ["CASCADE", "SET NULL", "RESTRICT", "NO ACTION"]:
            self._on_update = action_upper
        else:
            log_and_raise(f"Ungueltige ON UPDATE Aktion: {sAction}")

    # Getter-Methoden
    def get_source_table(self) -> str:
        """Gibt den Namen der Quelltabelle zurück."""
        return self._source_table

    def get_source_column(self) -> str:
        """Gibt den Namen der Quellspalte zurück."""
        return self._source_column

    def get_target_table(self) -> str:
        """Gibt den Namen der Zieltabelle zurück."""
        return self._target_table

    def get_target_column(self) -> str:
        """Gibt den Namen der Zielspalte zurück."""
        return self._target_column

    def get_relationship_name(self) -> str:
        """Gibt den Namen der Beziehung zurück."""
        return self._relationship_name

    def get_on_delete(self) -> str:
        """Gibt die ON DELETE Aktion zurück."""
        return self._on_delete

    def get_on_update(self) -> str:
        """Gibt die ON UPDATE Aktion zurück."""
        return self._on_update

    def get_description(self) -> str:
        """Gibt eine textuelle Beschreibung der Beziehung zurück."""
        return f"{self._source_table}.{self._source_column} -> {self._target_table}.{self._target_column}"

    def get_full_definition(self) -> str:
        """Gibt eine vollständige SQL-ähnliche Definition zurück."""
        sDef = (f"CONSTRAINT {self._relationship_name} FOREIGN KEY ({self._source_column}) "
                f"REFERENCES {self._target_table}({self._target_column})")

        if self._on_delete != "":
            sDef += f" ON DELETE {self._on_delete}"

        if self._on_update != "":
            sDef += f" ON UPDATE {self._on_update}"

        return sDef

    def involves_table(self, sTableName: str) -> bool:
        """Prüft ob diese Beziehung eine bestimmte Tabelle betrifft."""
        return self._source_table == sTableName or self._target_table == sTableName

    def is_self_referencing(self) -> bool:
        """Prüft ob dies eine selbstreferenzierende Beziehung ist."""
        return self._source_table == self._target_table

    def create_inverse_relationship(self) -> 'ForeignKeyRelationship':
        """Erstellt eine inverse Beziehung (für Navigation)."""
        # Inverse Beziehung hat umgekehrte Richtung
        oInverse = ForeignKeyRelationship(
            self._target_table, self._target_column,
            self._source_table, self._source_column,
            f"INV_{self._relationship_name}"
        )

        return oInverse
