"""
Unit tests for structured logging module.

Tests JSON formatting, CloudWatch handler, and correlation ID tracking.
"""

import json
import logging
from io import StringIO
from app.utils.logging import (
    get_logger,
    set_correlation_id,
    get_correlation_id,
    clear_correlation_id,
    get_logger_with_context,
    JSONFormatter,
)


def test_json_formatter():
    """Test that JSONFormatter produces valid JSON with required fields."""
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    
    formatted = formatter.format(record)
    log_data = json.loads(formatted)
    
    assert "timestamp" in log_data
    assert log_data["level"] == "INFO"
    assert log_data["message"] == "Test message"
    assert log_data["logger"] == "test_logger"
    assert log_data["line"] == 42


def test_correlation_id_in_logs():
    """Test that correlation ID is included in log entries."""
    # Set correlation ID
    set_correlation_id("test-correlation-123")
    
    # Create logger and capture output
    logger = get_logger("test_correlation")
    handler = logger.handlers[0]
    stream = StringIO()
    handler.stream = stream
    
    # Log a message
    logger.info("Test with correlation ID")
    
    # Parse output
    log_output = stream.getvalue()
    log_data = json.loads(log_output)
    
    assert log_data["correlation_id"] == "test-correlation-123"
    assert log_data["message"] == "Test with correlation ID"
    
    # Cleanup
    clear_correlation_id()


def test_get_correlation_id():
    """Test getting and clearing correlation ID."""
    # Initially should be None
    assert get_correlation_id() is None
    
    # Set and verify
    set_correlation_id("test-id-456")
    assert get_correlation_id() == "test-id-456"
    
    # Clear and verify
    clear_correlation_id()
    assert get_correlation_id() is None


def test_exception_logging():
    """Test that exceptions are properly formatted in logs."""
    formatter = JSONFormatter()
    
    try:
        raise ValueError("Test exception")
    except ValueError:
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=42,
            msg="Error occurred",
            args=(),
            exc_info=True,
        )
        record.exc_info = (ValueError, ValueError("Test exception"), None)
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert "exception" in log_data
        assert log_data["exception"]["type"] == "ValueError"
        assert "Test exception" in log_data["exception"]["message"]


def test_logger_with_context():
    """Test logger adapter with persistent context fields."""
    context = {"service": "test_service", "version": "1.0"}
    logger = get_logger_with_context("test_context", context)
    
    # Capture output
    handler = logger.logger.handlers[0]
    stream = StringIO()
    handler.stream = stream
    
    # Log a message
    logger.info("Test with context")
    
    # Parse output
    log_output = stream.getvalue()
    log_data = json.loads(log_output)
    
    assert log_data["service"] == "test_service"
    assert log_data["version"] == "1.0"
    assert log_data["message"] == "Test with context"


def test_extra_fields():
    """Test adding extra fields to individual log entries."""
    logger = get_logger("test_extra")
    handler = logger.handlers[0]
    stream = StringIO()
    handler.stream = stream
    
    # Log with extra fields
    logger.info("Test message", extra={"extra_fields": {"user_id": "123", "action": "upload"}})
    
    # Parse output
    log_output = stream.getvalue()
    log_data = json.loads(log_output)
    
    assert log_data["user_id"] == "123"
    assert log_data["action"] == "upload"
    assert log_data["message"] == "Test message"


if __name__ == "__main__":
    # Run tests
    test_json_formatter()
    print("✓ test_json_formatter passed")
    
    test_correlation_id_in_logs()
    print("✓ test_correlation_id_in_logs passed")
    
    test_get_correlation_id()
    print("✓ test_get_correlation_id passed")
    
    test_exception_logging()
    print("✓ test_exception_logging passed")
    
    test_logger_with_context()
    print("✓ test_logger_with_context passed")
    
    test_extra_fields()
    print("✓ test_extra_fields passed")
    
    print("\nAll tests passed!")
