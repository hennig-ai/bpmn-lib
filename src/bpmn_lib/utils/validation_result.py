"""
ValidationResult - Light-Version für bpmn_lib

Manages validation messages (errors and warnings) without connector-specific dependencies.
Uses Protocol/Hook pattern for extensibility.
"""

from datetime import datetime
from pathlib import Path
from typing import List, Protocol, Optional, runtime_checkable
from basic_framework.proc_frame import log_msg


@runtime_checkable
class ValidationResultHook(Protocol):
    """Protocol for extending ValidationResult behavior."""

    def on_error_added(self, message: str) -> None:
        """Called when an error is added."""
        ...

    def on_warning_added(self, message: str) -> None:
        """Called when a warning is added."""
        ...

    def on_check_validation(self, result: 'ValidationResult') -> None:
        """Called during check_validation."""
        ...


class ValidationResult:
    """
    Manages validation messages.
    Light version for bpmn_lib - no file operations or connector-specific dependencies.
    """

    def __init__(self, hook: Optional[ValidationResultHook] = None):
        """
        Initialize the ValidationResult.

        Args:
            hook: Optional hook for extending behavior (e.g., file writing, beeps)
        """
        self.m_validation_messages: List[str] = []
        self._hook: Optional[ValidationResultHook] = hook

    def add_error(self, s_message: str) -> None:
        """Add an error message."""
        self.m_validation_messages.append(s_message)
        log_msg("Schwerer Fehler in der DB: " + s_message)

        if self._hook is not None:
            self._hook.on_error_added(s_message)

    def add_warning(self, s_message: str) -> None:
        """Add a warning message."""
        self.m_validation_messages.append(s_message)

        if self._hook is not None:
            self._hook.on_warning_added(s_message)

    def count(self) -> int:
        """Return count of validation messages."""
        return len(self.m_validation_messages)

    def check_validation(self) -> None:
        """
        Check validation and handle results.
        Delegates to hook if present, otherwise just logs.
        """
        if self.count() == 0:
            # log_msg("Soweit keine Fehler in der Validierung gefunden", self)
            return

        # Report found via hook if present
        if self._hook is not None:
            self._hook.on_check_validation(self)
        else:
            # Default behavior: raise exception with all errors
            report = self.generate_validation_report()
            raise ValueError(report)

    def generate_validation_report(self) -> str:
        """Generate validation report as string."""
        s_content = "Das DB-Modell enthält folgende Fehler:\n"

        for message in self.m_validation_messages:
            s_content += str(message) + ";\n"

        return s_content

    def write_to_file(self, log_dir: str, prefix: str = "validation_errors") -> Path:
        """
        Schreibt den Validation-Report in eine Datei im Log-Verzeichnis.

        Args:
            log_dir: Pfad zum Log-Verzeichnis
            prefix: Präfix für den Dateinamen

        Returns:
            Path zur geschriebenen Datei
        """
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.txt"
        filepath = log_dir_path / filename

        report = self.generate_validation_report()
        filepath.write_text(report, encoding='utf-8')

        log_msg(f"Validation-Report geschrieben nach: {filepath}")
        return filepath

    def get_messages(self) -> List[str]:
        """Return all validation messages."""
        return self.m_validation_messages.copy()

    def has_errors(self) -> bool:
        """Check if there are any validation errors."""
        return len(self.m_validation_messages) > 0

    def clear(self) -> None:
        """Clear all validation messages."""
        self.m_validation_messages.clear()
