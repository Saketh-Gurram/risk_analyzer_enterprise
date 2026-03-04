"""
Unit tests for AWS client factory functions.

Tests verify that client factory functions create properly configured boto3 clients
with IAM role authentication and exponential backoff retry configuration.
"""

import pytest
from unittest.mock import patch, MagicMock
from botocore.config import Config as BotoConfig

from app.utils.aws_clients import (
    get_s3_client,
    get_dynamodb_client,
    get_sagemaker_runtime_client,
    get_bedrock_runtime_client,
    get_lambda_client,
    _get_retry_config,
)


class TestRetryConfig:
    """Tests for retry configuration helper."""
    
    def test_retry_config_has_max_attempts(self):
        """Test that retry config sets max_attempts to 3."""
        config = _get_retry_config()
        assert isinstance(config, BotoConfig)
        assert config.retries["max_attempts"] == 3
    
    def test_retry_config_uses_standard_mode(self):
        """Test that retry config uses standard mode (exponential backoff)."""
        config = _get_retry_config()
        assert config.retries["mode"] == "standard"


class TestS3Client:
    """Tests for S3 client factory."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_s3_client_creates_client_with_correct_service(
        self, mock_get_config, mock_boto_client
    ):
        """Test that get_s3_client creates an S3 client."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_s3_client()
        
        mock_boto_client.assert_called_once()
        assert mock_boto_client.call_args[0][0] == "s3"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_s3_client_uses_configured_region(
        self, mock_get_config, mock_boto_client
    ):
        """Test that S3 client uses region from configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-west-2"
        mock_get_config.return_value = mock_config
        
        get_s3_client()
        
        assert mock_boto_client.call_args[1]["region_name"] == "us-west-2"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_s3_client_includes_retry_config(
        self, mock_get_config, mock_boto_client
    ):
        """Test that S3 client includes retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_s3_client()
        
        config_arg = mock_boto_client.call_args[1]["config"]
        assert isinstance(config_arg, BotoConfig)
        assert config_arg.retries["max_attempts"] == 3


class TestDynamoDBClient:
    """Tests for DynamoDB client factory."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_dynamodb_client_creates_client_with_correct_service(
        self, mock_get_config, mock_boto_client
    ):
        """Test that get_dynamodb_client creates a DynamoDB client."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_dynamodb_client()
        
        mock_boto_client.assert_called_once()
        assert mock_boto_client.call_args[0][0] == "dynamodb"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_dynamodb_client_uses_configured_region(
        self, mock_get_config, mock_boto_client
    ):
        """Test that DynamoDB client uses region from configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "eu-west-1"
        mock_get_config.return_value = mock_config
        
        get_dynamodb_client()
        
        assert mock_boto_client.call_args[1]["region_name"] == "eu-west-1"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_dynamodb_client_includes_retry_config(
        self, mock_get_config, mock_boto_client
    ):
        """Test that DynamoDB client includes retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_dynamodb_client()
        
        config_arg = mock_boto_client.call_args[1]["config"]
        assert isinstance(config_arg, BotoConfig)
        assert config_arg.retries["max_attempts"] == 3


class TestSageMakerRuntimeClient:
    """Tests for SageMaker Runtime client factory."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_sagemaker_runtime_client_creates_client_with_correct_service(
        self, mock_get_config, mock_boto_client
    ):
        """Test that get_sagemaker_runtime_client creates a SageMaker Runtime client."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_sagemaker_runtime_client()
        
        mock_boto_client.assert_called_once()
        assert mock_boto_client.call_args[0][0] == "sagemaker-runtime"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_sagemaker_runtime_client_uses_configured_region(
        self, mock_get_config, mock_boto_client
    ):
        """Test that SageMaker Runtime client uses region from configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "ap-southeast-1"
        mock_get_config.return_value = mock_config
        
        get_sagemaker_runtime_client()
        
        assert mock_boto_client.call_args[1]["region_name"] == "ap-southeast-1"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_sagemaker_runtime_client_includes_retry_config(
        self, mock_get_config, mock_boto_client
    ):
        """Test that SageMaker Runtime client includes retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_sagemaker_runtime_client()
        
        config_arg = mock_boto_client.call_args[1]["config"]
        assert isinstance(config_arg, BotoConfig)
        assert config_arg.retries["max_attempts"] == 3


class TestBedrockRuntimeClient:
    """Tests for Bedrock Runtime client factory."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_bedrock_runtime_client_creates_client_with_correct_service(
        self, mock_get_config, mock_boto_client
    ):
        """Test that get_bedrock_runtime_client creates a Bedrock Runtime client."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_bedrock_runtime_client()
        
        mock_boto_client.assert_called_once()
        assert mock_boto_client.call_args[0][0] == "bedrock-runtime"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_bedrock_runtime_client_uses_configured_region(
        self, mock_get_config, mock_boto_client
    ):
        """Test that Bedrock Runtime client uses region from configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-west-2"
        mock_get_config.return_value = mock_config
        
        get_bedrock_runtime_client()
        
        assert mock_boto_client.call_args[1]["region_name"] == "us-west-2"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_bedrock_runtime_client_includes_retry_config(
        self, mock_get_config, mock_boto_client
    ):
        """Test that Bedrock Runtime client includes retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_bedrock_runtime_client()
        
        config_arg = mock_boto_client.call_args[1]["config"]
        assert isinstance(config_arg, BotoConfig)
        assert config_arg.retries["max_attempts"] == 3


class TestLambdaClient:
    """Tests for Lambda client factory."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_lambda_client_creates_client_with_correct_service(
        self, mock_get_config, mock_boto_client
    ):
        """Test that get_lambda_client creates a Lambda client."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_lambda_client()
        
        mock_boto_client.assert_called_once()
        assert mock_boto_client.call_args[0][0] == "lambda"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_lambda_client_uses_configured_region(
        self, mock_get_config, mock_boto_client
    ):
        """Test that Lambda client uses region from configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "eu-central-1"
        mock_get_config.return_value = mock_config
        
        get_lambda_client()
        
        assert mock_boto_client.call_args[1]["region_name"] == "eu-central-1"
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_get_lambda_client_includes_retry_config(
        self, mock_get_config, mock_boto_client
    ):
        """Test that Lambda client includes retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        get_lambda_client()
        
        config_arg = mock_boto_client.call_args[1]["config"]
        assert isinstance(config_arg, BotoConfig)
        assert config_arg.retries["max_attempts"] == 3


class TestClientFactoryIntegration:
    """Integration tests for all client factories."""
    
    @patch("app.utils.aws_clients.boto3.client")
    @patch("app.utils.aws_clients.get_config")
    def test_all_clients_use_same_retry_configuration(
        self, mock_get_config, mock_boto_client
    ):
        """Test that all client factories use consistent retry configuration."""
        mock_config = MagicMock()
        mock_config.aws_region = "us-east-1"
        mock_get_config.return_value = mock_config
        
        # Call all client factories
        get_s3_client()
        get_dynamodb_client()
        get_sagemaker_runtime_client()
        get_bedrock_runtime_client()
        get_lambda_client()
        
        # Verify all calls included retry config with max_attempts=3
        assert mock_boto_client.call_count == 5
        for call in mock_boto_client.call_args_list:
            config_arg = call[1]["config"]
            assert config_arg.retries["max_attempts"] == 3
            assert config_arg.retries["mode"] == "standard"
