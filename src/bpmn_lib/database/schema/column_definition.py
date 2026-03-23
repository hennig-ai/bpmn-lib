"""
ColumnDefinition - Defines a single column with all properties and constraints.
"""

from typing import Optional, Any, List
import re


class ColumnDefinition:
    """Defines a single column with all properties and constraints."""

    def __init__(self, s_column_name: str, s_data_type: str, s_description: str = ""):
        """Initialize the ColumnDefinition."""
        self.m_column_name = s_column_name
        self.m_data_type = s_data_type
        self.m_description = s_description
        self.m_length = 0
        self.m_is_nullable = True  # Standard: nullable
        self.m_is_primary_key = False
        self.m_is_foreign_key = False
        self.m_is_unique = False
        self.m_is_auto_increment = False
        self.m_default_value = None
        self.m_value_domain: Optional[List[Any]] = None

        # Laenge aus Datentyp extrahieren (z.B. VARCHAR(50))
        self._extract_length_from_data_type()

    def _extract_length_from_data_type(self):
        """Extract length from data type string."""
        # Pruefen ob Laenge angegeben (z.B. VARCHAR(50))
        match = re.match(r'^(\w+)\((\d+)\)', self.m_data_type)
        if match:
            self.m_data_type = match.group(1)
            self.m_length = int(match.group(2))

    def set_nullable(self, b_nullable: bool):
        """Set nullable constraint."""
        self.m_is_nullable = b_nullable

    def set_primary_key(self, b_is_primary_key: bool):
        """Set primary key constraint."""
        self.m_is_primary_key = b_is_primary_key
        # PK ist automatisch NOT NULL und UNIQUE
        if b_is_primary_key:
            self.m_is_nullable = False
            self.m_is_unique = True

    def set_foreign_key(self, b_is_foreign_key: bool):
        """Set foreign key flag."""
        self.m_is_foreign_key = b_is_foreign_key

    def set_unique(self, b_is_unique: bool):
        """Set unique constraint."""
        self.m_is_unique = b_is_unique

    def set_auto_increment(self, b_is_auto_increment: bool):
        """Set auto increment flag."""
        self.m_is_auto_increment = b_is_auto_increment
        # Auto-Increment impliziert NOT NULL
        if b_is_auto_increment:
            self.m_is_nullable = False

    def set_default_value(self, v_default_value: Any):
        """Set default value."""
        self.m_default_value = v_default_value

    def set_value_domain(self, o_allowed_values: List[Any]):
        """Set allowed values."""
        self.m_value_domain = o_allowed_values

    def get_column_name(self) -> str:
        """Get column name."""
        return self.m_column_name

    def get_data_type(self) -> str:
        """Get data type."""
        return self.m_data_type

    def get_data_type_with_length(self) -> str:
        """Get data type with length."""
        # Gibt Datentyp mit Laenge zurueck (z.B. VARCHAR(50))
        if self.m_length > 0 and self.m_data_type.upper() in ["VARCHAR", "CHAR"]:
            return f"{self.m_data_type}({self.m_length})"
        else:
            return self.m_data_type

    def get_length(self) -> int:
        """Get column length."""
        return self.m_length

    def is_nullable(self) -> bool:
        """Check if column is nullable."""
        return self.m_is_nullable

    def is_primary_key(self) -> bool:
        """Check if column is primary key."""
        return self.m_is_primary_key

    def is_foreign_key(self) -> bool:
        """Check if column is foreign key."""
        return self.m_is_foreign_key

    def is_unique(self) -> bool:
        """Check if column has unique constraint."""
        return self.m_is_unique

    def is_auto_increment(self) -> bool:
        """Check if column is auto increment."""
        return self.m_is_auto_increment

    def get_default_value(self) -> Optional[Any]:
        """Get default value."""
        return self.m_default_value

    def get_description(self) -> str:
        """Get column description."""
        return self.m_description

    def has_value_domain(self) -> bool:
        """Check if column has value domain."""
        return self.m_value_domain is not None

    def get_value_domain(self) -> Optional[List[Any]]:
        """Get allowed values."""
        return self.m_value_domain

    def get_full_description(self) -> str:
        """Get full textual description."""
        s_desc = f"{self.m_column_name} {self.get_data_type_with_length()}"

        # Constraints hinzufuegen
        s_constraints = ""

        if self.m_is_primary_key:
            s_constraints += "PRIMARY KEY "

        if not self.m_is_nullable:
            s_constraints += "NOT NULL "

        if self.m_is_unique and not self.m_is_primary_key:
            s_constraints += "UNIQUE "

        if self.m_is_auto_increment:
            s_constraints += "AUTO_INCREMENT "

        if self.m_is_foreign_key:
            s_constraints += "FOREIGN KEY "

        if s_constraints.strip():
            s_desc += " " + s_constraints.strip()

        # Default-Wert
        if self.m_default_value is not None:
            s_desc += f" DEFAULT {str(self.m_default_value)}"

        # Beschreibung
        if self.m_description:
            s_desc += f" -- {self.m_description}"

        return s_desc

    def validate_value(self, v_value: Any, s_error: Optional[List[str]] = None) -> bool:
        """Validate value against column definition."""
        if s_error is None:
            s_error = []
        else:
            s_error.clear()

        # NULL-Wert pruefen
        if v_value is None or str(v_value) == "":
            if not self.m_is_nullable:
                s_error.append(f"NULL-Wert nicht erlaubt fuer Spalte '{self.m_column_name}'")
                return False
            else:
                return True

        # Datentyp pruefen
        data_type_upper = self.m_data_type.upper()

        if data_type_upper in ["INTEGER", "INT", "BIGINT"]:
            try:
                int(v_value)
            except (ValueError, TypeError):
                s_error.append(f"Wert muss numerisch sein fuer Spalte '{self.m_column_name}'")
                return False

        elif data_type_upper in ["VARCHAR", "CHAR", "TEXT"]:
            # Laengenpruefung
            if self.m_length > 0 and len(str(v_value)) > self.m_length:
                s_error.append(f"Wert ueberschreitet maximale Laenge ({self.m_length}) fuer Spalte '{self.m_column_name}'")
                return False

        elif data_type_upper in ["BOOLEAN", "BOOL"]:
            # Boolean-Validierung
            s_val = str(v_value).upper()
            if s_val not in ["TRUE", "FALSE", "1", "0", "-1"]:
                s_error.append(f"Wert muss Boolean sein fuer Spalte '{self.m_column_name}'")
                return False

        elif data_type_upper in ["TIMESTAMP", "DATETIME", "DATE"]:
            # In Python pruefen wir ob es ein datetime-Objekt ist oder parse-bar
            from datetime import datetime
            if not isinstance(v_value, datetime):
                try:
                    datetime.fromisoformat(str(v_value))
                except (ValueError, AttributeError):
                    s_error.append(f"Wert muss Datum sein fuer Spalte '{self.m_column_name}'")
                    return False

        # Value Domain pruefen
        if self.m_value_domain is not None:
            b_found = False
            for v_allowed in self.m_value_domain:
                if str(v_value) == str(v_allowed):
                    b_found = True
                    break

            if not b_found:
                s_error.append(f"Wert '{str(v_value)}' ist nicht in der Liste erlaubter Werte fuer Spalte '{self.m_column_name}'")
                return False

        return True
