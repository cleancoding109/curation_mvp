"""
Structured Logging Utilities

Provides consistent logging across the framework.
"""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically module name)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


class PipelineLogger:
    """
    Context-aware logger for pipeline execution.
    
    Tracks run_id, table_name, and step for structured logging.
    """
    
    def __init__(
        self,
        name: str,
        run_id: Optional[str] = None,
        table_name: Optional[str] = None,
        level: str = "INFO"
    ):
        self._logger = get_logger(name, level)
        self.run_id = run_id
        self.table_name = table_name
        self._step = None
    
    def set_context(self, run_id: str = None, table_name: str = None, step: str = None):
        """Update logging context."""
        if run_id:
            self.run_id = run_id
        if table_name:
            self.table_name = table_name
        if step:
            self._step = step
    
    def _format_message(self, message: str) -> str:
        """Format message with context."""
        parts = []
        if self.run_id:
            parts.append(f"run={self.run_id}")
        if self.table_name:
            parts.append(f"table={self.table_name}")
        if self._step:
            parts.append(f"step={self._step}")
        
        context = " | ".join(parts)
        return f"[{context}] {message}" if context else message
    
    def info(self, message: str):
        self._logger.info(self._format_message(message))
    
    def debug(self, message: str):
        self._logger.debug(self._format_message(message))
    
    def warning(self, message: str):
        self._logger.warning(self._format_message(message))
    
    def error(self, message: str):
        self._logger.error(self._format_message(message))
    
    def exception(self, message: str):
        self._logger.exception(self._format_message(message))
