"""
Structured logging setup for Retail Risk Intelligence Platform.

Provides JSON-formatted logging with CloudWatch integration and correlation ID tracking.
Implements requirements 11.1, 11.2, 11.3, 11.7, 11.8.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from contextvars import ContextVar
import traceback

# Context variable for correlation ID tracking across async requests
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    
    Formats log records as JSON with timestamp, level, message, and correlation_id.
    Includes exception information when present.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON string.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add correlation ID if present
        correlation_id = correlation_id_var.get()
        if correlation_id:
            log_data["correlation_id"] = correlation_id
        
        # Add exception information if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "stacktrace": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields from record
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


class CloudWatchLogHandler(logging.StreamHandler):
    """
    Log handler configured for AWS CloudWatch integration.
    
    Writes structured JSON logs to stdout for CloudWatch Logs capture.
    """
    
    def __init__(self):
        """Initialize CloudWatch log handler with stdout stream."""
        super().__init__(stream=sys.stdout)
        self.setFormatter(JSONFormatter())


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Get a configured logger instance with structured JSON logging.
    
    Creates a logger with CloudWatch-compatible JSON formatting and
    correlation ID tracking for request tracing.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing request", extra={"extra_fields": {"user_id": "123"}})
    """
    logger = logging.getLogger(name)
    
    # Avoid adding duplicate handlers
    if not logger.handlers:
        logger.setLevel(level)
        
        # Add CloudWatch handler
        cloudwatch_handler = CloudWatchLogHandler()
        logger.addHandler(cloudwatch_handler)
        
        # Prevent propagation to root logger to avoid duplicate logs
        logger.propagate = False
    
    return logger


def set_correlation_id(correlation_id: str) -> None:
    """
    Set correlation ID for the current context.
    
    The correlation ID will be included in all log entries within
    the current async context for request tracing.
    
    Args:
        correlation_id: Unique identifier for request correlation
        
    Example:
        >>> set_correlation_id("req-123-456")
        >>> logger.info("Processing request")  # Will include correlation_id
    """
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """
    Get the current correlation ID from context.
    
    Returns:
        Current correlation ID or None if not set
    """
    return correlation_id_var.get()


def clear_correlation_id() -> None:
    """
    Clear the correlation ID from the current context.
    
    Useful for cleanup after request processing.
    """
    correlation_id_var.set(None)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically includes extra fields in log records.
    
    Useful for adding consistent context to all logs from a specific component.
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process log message to include extra fields.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments for logging call
            
        Returns:
            Tuple of (message, kwargs) with extra fields added
        """
        # Merge extra fields from adapter with call-specific extra
        extra_fields = self.extra.copy()
        if "extra" in kwargs and "extra_fields" in kwargs["extra"]:
            extra_fields.update(kwargs["extra"]["extra_fields"])
        
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["extra_fields"] = extra_fields
        
        return msg, kwargs


def get_logger_with_context(name: str, context: Dict[str, Any], level: int = logging.INFO) -> LoggerAdapter:
    """
    Get a logger adapter with persistent context fields.
    
    All log entries from this logger will include the provided context fields.
    
    Args:
        name: Logger name
        context: Dictionary of context fields to include in all logs
        level: Logging level (default: INFO)
        
    Returns:
        Logger adapter with context
        
    Example:
        >>> logger = get_logger_with_context(__name__, {"service": "risk_detection"})
        >>> logger.info("Analysis complete")  # Will include service field
    """
    base_logger = get_logger(name, level)
    return LoggerAdapter(base_logger, context)
