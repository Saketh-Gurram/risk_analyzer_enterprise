"""
AWS client factory functions with IAM role authentication and retry configuration.

This module provides factory functions for creating boto3 clients for various AWS services
used by the Retail Risk Intelligence Platform. All clients are configured with:
- IAM role-based authentication (no explicit credentials required)
- Exponential backoff retry configuration (max 3 attempts)
- Region configuration from application settings

Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8
"""

import boto3
from botocore.config import Config as BotoConfig
from typing import Any

from app.config import get_config


def _get_retry_config() -> BotoConfig:
    """
    Create boto3 configuration with exponential backoff retry logic.
    
    Configures retries with:
    - Maximum 3 retry attempts
    - Exponential backoff strategy
    - Retries on throttling and service errors
    
    Returns:
        BotoConfig: Boto3 configuration object with retry settings
    """
    return BotoConfig(
        retries={
            "max_attempts": 3,
            "mode": "standard",  # Uses exponential backoff
        }
    )


def get_s3_client() -> Any:
    """
    Create and return an S3 client with IAM role authentication.
    
    The client is configured with:
    - IAM role credentials (automatic from execution environment)
    - Exponential backoff retry (max 3 attempts)
    - Region from application configuration
    
    Returns:
        boto3.client: Configured S3 client
        
    Requirements: 10.1, 10.6, 10.7
    """
    config = get_config()
    return boto3.client(
        "s3",
        region_name=config.aws_region,
        config=_get_retry_config()
    )


def get_dynamodb_client() -> Any:
    """
    Create and return a DynamoDB client with IAM role authentication.
    
    The client is configured with:
    - IAM role credentials (automatic from execution environment)
    - Exponential backoff retry (max 3 attempts)
    - Region from application configuration
    
    Returns:
        boto3.client: Configured DynamoDB client
        
    Requirements: 10.2, 10.6, 10.7
    """
    config = get_config()
    return boto3.client(
        "dynamodb",
        region_name=config.aws_region,
        config=_get_retry_config()
    )


def get_sagemaker_runtime_client() -> Any:
    """
    Create and return a SageMaker Runtime client with IAM role authentication.
    
    The client is configured with:
    - IAM role credentials (automatic from execution environment)
    - Exponential backoff retry (max 3 attempts)
    - Region from application configuration
    
    Used for invoking SageMaker endpoints for ML inference.
    
    Returns:
        boto3.client: Configured SageMaker Runtime client
        
    Requirements: 10.3, 10.6, 10.7
    """
    config = get_config()
    return boto3.client(
        "sagemaker-runtime",
        region_name=config.aws_region,
        config=_get_retry_config()
    )


def get_bedrock_runtime_client() -> Any:
    """
    Create and return a Bedrock Runtime client with IAM role authentication.
    
    The client is configured with:
    - IAM role credentials (automatic from execution environment)
    - Exponential backoff retry (max 3 attempts)
    - Region from application configuration
    
    Used for invoking Amazon Bedrock models (Claude 3.5 Sonnet) for AI reasoning.
    
    Returns:
        boto3.client: Configured Bedrock Runtime client
        
    Requirements: 10.4, 10.6, 10.7
    """
    config = get_config()
    return boto3.client(
        "bedrock-runtime",
        region_name=config.aws_region,
        config=_get_retry_config()
    )


def get_lambda_client() -> Any:
    """
    Create and return a Lambda client with IAM role authentication.
    
    The client is configured with:
    - IAM role credentials (automatic from execution environment)
    - Exponential backoff retry (max 3 attempts)
    - Region from application configuration
    
    Used for invoking Lambda functions for failure simulation execution.
    
    Returns:
        boto3.client: Configured Lambda client
        
    Requirements: 10.5, 10.6, 10.7
    """
    config = get_config()
    return boto3.client(
        "lambda",
        region_name=config.aws_region,
        config=_get_retry_config()
    )
