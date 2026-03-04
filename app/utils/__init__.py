"""
Utility modules for Retail Risk Intelligence Platform.
"""

from app.utils.aws_clients import (
    get_s3_client,
    get_dynamodb_client,
    get_sagemaker_runtime_client,
    get_bedrock_runtime_client,
    get_lambda_client,
)
from app.utils.logging import (
    get_logger,
    get_logger_with_context,
    set_correlation_id,
    get_correlation_id,
    clear_correlation_id,
)

__all__ = [
    "get_s3_client",
    "get_dynamodb_client",
    "get_sagemaker_runtime_client",
    "get_bedrock_runtime_client",
    "get_lambda_client",
    "get_logger",
    "get_logger_with_context",
    "set_correlation_id",
    "get_correlation_id",
    "clear_correlation_id",
]
