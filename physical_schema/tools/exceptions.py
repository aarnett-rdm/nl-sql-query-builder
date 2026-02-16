"""
Centralized exception hierarchy for the NL-SQL pipeline.

All custom exceptions inherit from NLSQLError so callers can catch
broad categories or specific error types as needed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


class NLSQLError(Exception):
    """Base exception for the NL-SQL pipeline."""

    def to_dict(self) -> Dict[str, Any]:
        """Structured representation for API error responses."""
        return {"error_type": type(self).__name__, "message": str(self)}


class MetricResolutionError(NLSQLError):
    """Metric lookup, validation, or grain/platform incompatibility."""

    def __init__(
        self,
        message: str,
        *,
        metric_name: Optional[str] = None,
        suggestions: Optional[List[str]] = None,
    ):
        self.metric_name = metric_name
        self.suggestions = suggestions or []
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if self.metric_name:
            d["metric_name"] = self.metric_name
        if self.suggestions:
            d["suggestions"] = self.suggestions
        return d


class AmbiguousDimensionError(NLSQLError):
    """A dimension column matches multiple tables and cannot be auto-resolved."""

    def __init__(
        self,
        column: str,
        candidates: List[str],
        question: str = "",
    ):
        self.column = column
        self.candidates = candidates
        self.question = (
            question
            or f"Column '{column}' exists in multiple tables. Which one do you mean?"
        )
        super().__init__(self.question)

    def to_dict(self) -> Dict[str, Any]:
        return {
            **super().to_dict(),
            "column": self.column,
            "candidates": self.candidates,
        }


class LLMBackendError(NLSQLError):
    """Base exception for any LLM backend failure (timeout, HTTP error, auth, etc.)."""
    pass


class OllamaError(LLMBackendError):
    """Ollama communication failure (HTTP error, timeout, etc.)."""
    pass


class SpecValidationError(NLSQLError):
    """The Spec dict is structurally invalid or contains unsupported values."""

    def __init__(
        self,
        message: str,
        *,
        field: Optional[str] = None,
        value: Any = None,
    ):
        self.field = field
        self.value = value
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if self.field:
            d["field"] = self.field
        if self.value is not None:
            d["value"] = str(self.value)
        return d


class ConfigError(NLSQLError):
    """Missing, unreadable, or invalid configuration file."""

    def __init__(self, message: str, *, config_path: Optional[Path] = None):
        self.config_path = config_path
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if self.config_path:
            d["config_path"] = str(self.config_path)
        return d


class DateFilterError(NLSQLError):
    """Invalid or conflicting date filter specification."""

    def __init__(
        self,
        message: str,
        *,
        filter_type: Optional[str] = None,
        raw_value: Any = None,
    ):
        self.filter_type = filter_type
        self.raw_value = raw_value
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if self.filter_type:
            d["filter_type"] = self.filter_type
        if self.raw_value is not None:
            d["raw_value"] = str(self.raw_value)
        return d
