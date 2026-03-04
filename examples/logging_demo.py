"""
Demonstration of structured logging capabilities.

Shows JSON formatting, correlation ID tracking, and CloudWatch integration.
"""

from app.utils.logging import (
    get_logger,
    get_logger_with_context,
    set_correlation_id,
    clear_correlation_id,
)

# Example 1: Basic logging
print("=== Example 1: Basic Structured Logging ===")
logger = get_logger(__name__)
logger.info("Application started")
logger.warning("This is a warning message")
print()

# Example 2: Logging with correlation ID
print("=== Example 2: Correlation ID Tracking ===")
set_correlation_id("req-12345-abcde")
logger.info("Processing user request")
logger.info("Fetching data from S3")
clear_correlation_id()
print()

# Example 3: Logging with extra fields
print("=== Example 3: Extra Fields ===")
logger.info(
    "File uploaded successfully",
    extra={"extra_fields": {"file_size": 1024000, "file_type": "csv", "user_id": "user-789"}}
)
print()

# Example 4: Logger with persistent context
print("=== Example 4: Logger with Context ===")
service_logger = get_logger_with_context(
    "risk_detection_service",
    {"service": "risk_detection", "version": "1.0.0"}
)
service_logger.info("Risk analysis started")
service_logger.info("Analyzing 5000 products")
print()

# Example 5: Exception logging
print("=== Example 5: Exception Logging ===")
try:
    result = 10 / 0
except ZeroDivisionError as e:
    logger.error("Mathematical error occurred", exc_info=True)
print()

# Example 6: Simulating API request flow
print("=== Example 6: API Request Flow ===")
set_correlation_id("req-api-98765")
api_logger = get_logger_with_context("api", {"endpoint": "/risk/analyze"})
api_logger.info("API request received")
api_logger.info("Validating request payload")
api_logger.info("Invoking risk detection engine")
api_logger.info("Request completed successfully", extra={"extra_fields": {"duration_ms": 245}})
clear_correlation_id()
print()

print("All examples completed. Check the JSON-formatted logs above.")
