"""
Unit tests for DataIngestionService.

Tests verify file upload, schema validation, S3 storage, DynamoDB metadata creation,
file size limits, and error handling for the data ingestion service.

Requirements: 1.1, 1.2, 1.3, 1.5
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from app.services.data_ingestion import DataIngestionService
from app.models.upload import UploadResponse, ValidationResult


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    client = MagicMock()
    client.put_object = MagicMock()
    return client


@pytest.fixture
def mock_dynamodb_client():
    """Create a mock DynamoDB client."""
    client = MagicMock()
    client.put_item = MagicMock()
    return client


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    config = MagicMock()
    config.s3_bucket_name = "test-bucket"
    config.dynamodb_table_name = "test-table"
    config.max_upload_size_bytes = 100 * 1024 * 1024  # 100MB
    return config


@pytest.fixture
def data_ingestion_service(mock_s3_client, mock_dynamodb_client, mock_config):
    """Create a DataIngestionService instance with mocked dependencies."""
    with patch("app.services.data_ingestion.get_config", return_value=mock_config):
        service = DataIngestionService(mock_s3_client, mock_dynamodb_client)
    return service


@pytest.fixture
def valid_csv_content():
    """Create valid CSV file content."""
    return b"""product_id,product_name,date,quantity,price
P001,Widget A,2024-01-01,100,19.99
P002,Widget B,2024-01-02,150,29.99
P003,Widget C,2024-01-03,200,39.99"""


@pytest.fixture
def valid_json_content():
    """Create valid JSON file content."""
    return b"""{
    "products": [
        {"product_id": "P001", "product_name": "Widget A", "date": "2024-01-01", "quantity": 100, "price": 19.99},
        {"product_id": "P002", "product_name": "Widget B", "date": "2024-01-02", "quantity": 150, "price": 29.99}
    ]
}"""


class TestSuccessfulCSVUpload:
    """Tests for successful CSV file upload (Requirement 1.1)."""
    
    @pytest.mark.asyncio
    async def test_upload_csv_file_returns_upload_response(
        self, data_ingestion_service, valid_csv_content
    ):
        """Test that uploading a valid CSV file returns an UploadResponse."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_csv_content,
                filename="test.csv",
                file_type="csv"
            )
        
        assert isinstance(result, UploadResponse)
        assert result.status == "success"
        assert result.upload_id is not None
        assert result.file_size_bytes == len(valid_csv_content)
        assert isinstance(result.timestamp, datetime)
    
    @pytest.mark.asyncio
    async def test_upload_csv_file_stores_in_s3_with_correct_prefix(
        self, data_ingestion_service, valid_csv_content, mock_s3_client
    ):
        """Test that CSV file is stored in S3 with correct prefix structure."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_csv_content,
                filename="test.csv",
                file_type="csv"
            )
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]
        
        # Verify bucket name
        assert call_kwargs["Bucket"] == "test-bucket"
        
        # Verify key follows prefix structure: raw/{upload_id}/{filename}
        assert call_kwargs["Key"].startswith("raw/")
        assert call_kwargs["Key"].endswith("/test.csv")
        assert result.upload_id in call_kwargs["Key"]
        
        # Verify file content
        assert call_kwargs["Body"] == valid_csv_content
        
        # Verify metadata
        assert call_kwargs["Metadata"]["upload_id"] == result.upload_id
        assert call_kwargs["Metadata"]["original_filename"] == "test.csv"
        assert call_kwargs["Metadata"]["file_type"] == "csv"
    
    @pytest.mark.asyncio
    async def test_upload_csv_file_creates_dynamodb_metadata(
        self, data_ingestion_service, valid_csv_content, mock_dynamodb_client
    ):
        """Test that CSV upload creates metadata record in DynamoDB."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_csv_content,
                filename="test.csv",
                file_type="csv"
            )
        
        # Verify DynamoDB put_item was called
        mock_dynamodb_client.put_item.assert_called_once()
        call_kwargs = mock_dynamodb_client.put_item.call_args[1]
        
        # Verify table name
        assert call_kwargs["TableName"] == "test-table"
        
        # Verify item structure
        item = call_kwargs["Item"]
        assert item["scenario_id"]["S"] == result.upload_id
        assert item["record_type"]["S"] == "upload"
        assert item["upload_id"]["S"] == result.upload_id
        assert item["status"]["S"] == "success"
        
        # Verify data field
        data = item["data"]["M"]
        assert data["filename"]["S"] == "test.csv"
        assert data["file_type"]["S"] == "csv"
        assert data["file_size_bytes"]["N"] == str(len(valid_csv_content))
        assert data["s3_bucket"]["S"] == "test-bucket"


class TestSuccessfulJSONUpload:
    """Tests for successful JSON file upload (Requirement 1.2)."""
    
    @pytest.mark.asyncio
    async def test_upload_json_file_returns_upload_response(
        self, data_ingestion_service, valid_json_content
    ):
        """Test that uploading a valid JSON file returns an UploadResponse."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_json_content,
                filename="test.json",
                file_type="json"
            )
        
        assert isinstance(result, UploadResponse)
        assert result.status == "success"
        assert result.upload_id is not None
        assert result.file_size_bytes == len(valid_json_content)
    
    @pytest.mark.asyncio
    async def test_upload_json_file_stores_in_s3_with_correct_prefix(
        self, data_ingestion_service, valid_json_content, mock_s3_client
    ):
        """Test that JSON file is stored in S3 with correct prefix structure."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_json_content,
                filename="test.json",
                file_type="json"
            )
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]
        
        # Verify key follows prefix structure
        assert call_kwargs["Key"].startswith("raw/")
        assert call_kwargs["Key"].endswith("/test.json")
        
        # Verify metadata
        assert call_kwargs["Metadata"]["file_type"] == "json"
    
    @pytest.mark.asyncio
    async def test_upload_json_file_creates_dynamodb_metadata(
        self, data_ingestion_service, valid_json_content, mock_dynamodb_client
    ):
        """Test that JSON upload creates metadata record in DynamoDB."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=valid_json_content,
                filename="test.json",
                file_type="json"
            )
        
        # Verify DynamoDB put_item was called
        mock_dynamodb_client.put_item.assert_called_once()
        call_kwargs = mock_dynamodb_client.put_item.call_args[1]
        
        # Verify data field
        data = call_kwargs["Item"]["data"]["M"]
        assert data["file_type"]["S"] == "json"


class TestSchemaValidationFailure:
    """Tests for schema validation failure (Requirement 1.3)."""
    
    @pytest.mark.asyncio
    async def test_upload_with_invalid_schema_raises_value_error(
        self, data_ingestion_service, valid_csv_content
    ):
        """Test that uploading a file with invalid schema raises ValueError."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(
                is_valid=False,
                errors=["Missing required field: product_id", "Invalid data type for quantity"],
                warnings=[]
            )
        ):
            with pytest.raises(ValueError) as exc_info:
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="invalid.csv",
                    file_type="csv"
                )
        
        # Verify error message contains validation errors
        error_message = str(exc_info.value)
        assert "Schema validation failed" in error_message
        assert "Missing required field: product_id" in error_message
        assert "Invalid data type for quantity" in error_message
    
    @pytest.mark.asyncio
    async def test_upload_with_invalid_schema_does_not_store_in_s3(
        self, data_ingestion_service, valid_csv_content, mock_s3_client
    ):
        """Test that files with invalid schema are not stored in S3."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(
                is_valid=False,
                errors=["Invalid schema"],
                warnings=[]
            )
        ):
            with pytest.raises(ValueError):
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="invalid.csv",
                    file_type="csv"
                )
        
        # Verify S3 put_object was not called
        mock_s3_client.put_object.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_upload_with_invalid_schema_does_not_create_dynamodb_metadata(
        self, data_ingestion_service, valid_csv_content, mock_dynamodb_client
    ):
        """Test that files with invalid schema do not create DynamoDB metadata."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(
                is_valid=False,
                errors=["Invalid schema"],
                warnings=[]
            )
        ):
            with pytest.raises(ValueError):
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="invalid.csv",
                    file_type="csv"
                )
        
        # Verify DynamoDB put_item was not called
        mock_dynamodb_client.put_item.assert_not_called()


class TestFileSizeLimitEnforcement:
    """Tests for file size limit enforcement (Requirement 1.5)."""
    
    @pytest.mark.asyncio
    async def test_upload_file_exceeding_size_limit_raises_value_error(
        self, data_ingestion_service
    ):
        """Test that uploading a file exceeding 100MB raises ValueError."""
        # Create file content larger than 100MB
        large_file_content = b"x" * (101 * 1024 * 1024)  # 101MB
        
        with pytest.raises(ValueError) as exc_info:
            await data_ingestion_service.upload_file(
                file_content=large_file_content,
                filename="large.csv",
                file_type="csv"
            )
        
        # Verify error message mentions file size limit
        error_message = str(exc_info.value)
        assert "exceeds maximum allowed size" in error_message
        assert "100" in error_message  # Should mention 100MB limit
    
    @pytest.mark.asyncio
    async def test_upload_file_at_size_limit_succeeds(
        self, data_ingestion_service
    ):
        """Test that uploading a file at exactly 100MB succeeds."""
        # Create file content exactly 100MB
        file_content = b"x" * (100 * 1024 * 1024)  # 100MB
        
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            result = await data_ingestion_service.upload_file(
                file_content=file_content,
                filename="max_size.csv",
                file_type="csv"
            )
        
        assert result.status == "success"
        assert result.file_size_bytes == 100 * 1024 * 1024
    
    @pytest.mark.asyncio
    async def test_upload_oversized_file_does_not_store_in_s3(
        self, data_ingestion_service, mock_s3_client
    ):
        """Test that oversized files are not stored in S3."""
        large_file_content = b"x" * (101 * 1024 * 1024)  # 101MB
        
        with pytest.raises(ValueError):
            await data_ingestion_service.upload_file(
                file_content=large_file_content,
                filename="large.csv",
                file_type="csv"
            )
        
        # Verify S3 put_object was not called
        mock_s3_client.put_object.assert_not_called()


class TestS3UploadErrorHandling:
    """Tests for S3 upload error handling."""
    
    @pytest.mark.asyncio
    async def test_s3_upload_failure_raises_runtime_error(
        self, data_ingestion_service, valid_csv_content, mock_s3_client
    ):
        """Test that S3 upload failure raises RuntimeError with descriptive message."""
        # Configure S3 client to raise ClientError
        mock_s3_client.put_object.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchBucket",
                    "Message": "The specified bucket does not exist"
                }
            },
            operation_name="PutObject"
        )
        
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            with pytest.raises(RuntimeError) as exc_info:
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="test.csv",
                    file_type="csv"
                )
        
        # Verify error message contains S3 error details
        error_message = str(exc_info.value)
        assert "Failed to upload file to S3" in error_message
        assert "NoSuchBucket" in error_message
    
    @pytest.mark.asyncio
    async def test_s3_upload_failure_does_not_create_dynamodb_metadata(
        self, data_ingestion_service, valid_csv_content, mock_s3_client, mock_dynamodb_client
    ):
        """Test that S3 upload failure prevents DynamoDB metadata creation."""
        # Configure S3 client to raise ClientError
        mock_s3_client.put_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            operation_name="PutObject"
        )
        
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            with pytest.raises(RuntimeError):
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="test.csv",
                    file_type="csv"
                )
        
        # Verify DynamoDB put_item was not called
        mock_dynamodb_client.put_item.assert_not_called()


class TestDynamoDBMetadataCreation:
    """Tests for DynamoDB metadata creation."""
    
    @pytest.mark.asyncio
    async def test_dynamodb_creation_failure_raises_runtime_error(
        self, data_ingestion_service, valid_csv_content, mock_dynamodb_client
    ):
        """Test that DynamoDB metadata creation failure raises RuntimeError."""
        # Configure DynamoDB client to raise ClientError
        mock_dynamodb_client.put_item.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Requested resource not found"
                }
            },
            operation_name="PutItem"
        )
        
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            with pytest.raises(RuntimeError) as exc_info:
                await data_ingestion_service.upload_file(
                    file_content=valid_csv_content,
                    filename="test.csv",
                    file_type="csv"
                )
        
        # Verify error message contains DynamoDB error details
        error_message = str(exc_info.value)
        assert "Failed to create DynamoDB metadata" in error_message
        assert "ResourceNotFoundException" in error_message
    
    @pytest.mark.asyncio
    async def test_dynamodb_metadata_includes_execution_time(
        self, data_ingestion_service, valid_csv_content, mock_dynamodb_client
    ):
        """Test that DynamoDB metadata includes execution time tracking."""
        with patch.object(
            data_ingestion_service,
            "validate_schema",
            return_value=ValidationResult(is_valid=True, errors=[], warnings=[])
        ):
            await data_ingestion_service.upload_file(
                file_content=valid_csv_content,
                filename="test.csv",
                file_type="csv"
            )
        
        # Verify DynamoDB put_item was called
        call_kwargs = mock_dynamodb_client.put_item.call_args[1]
        item = call_kwargs["Item"]
        
        # Verify execution_time_seconds field exists and is a number
        assert "execution_time_seconds" in item
        assert "N" in item["execution_time_seconds"]
        execution_time = float(item["execution_time_seconds"]["N"])
        assert execution_time >= 0


class TestValidateSchema:
    """Tests for schema validation method."""
    
    @pytest.mark.asyncio
    async def test_validate_csv_schema_calls_csv_validator(
        self, data_ingestion_service, valid_csv_content
    ):
        """Test that validate_schema calls CSV validator for CSV files."""
        with patch("app.services.data_ingestion.validate_csv_schema") as mock_validator:
            mock_validator.return_value = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }
            
            result = await data_ingestion_service.validate_schema(
                file_content=valid_csv_content,
                file_type="csv"
            )
        
        # Verify CSV validator was called
        mock_validator.assert_called_once_with(valid_csv_content)
        assert result.is_valid is True
    
    @pytest.mark.asyncio
    async def test_validate_json_schema_calls_json_validator(
        self, data_ingestion_service, valid_json_content
    ):
        """Test that validate_schema calls JSON validator for JSON files."""
        with patch("app.services.data_ingestion.validate_json_schema") as mock_validator:
            mock_validator.return_value = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }
            
            result = await data_ingestion_service.validate_schema(
                file_content=valid_json_content,
                file_type="json"
            )
        
        # Verify JSON validator was called
        mock_validator.assert_called_once_with(valid_json_content)
        assert result.is_valid is True
    
    @pytest.mark.asyncio
    async def test_validate_schema_with_unsupported_type_returns_error(
        self, data_ingestion_service
    ):
        """Test that validate_schema returns error for unsupported file types."""
        result = await data_ingestion_service.validate_schema(
            file_content=b"test",
            file_type="xml"  # Unsupported type
        )
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert "Unsupported file type" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_validate_schema_handles_validator_exceptions(
        self, data_ingestion_service, valid_csv_content
    ):
        """Test that validate_schema handles exceptions from validators gracefully."""
        with patch("app.services.data_ingestion.validate_csv_schema") as mock_validator:
            mock_validator.side_effect = Exception("Validator error")
            
            result = await data_ingestion_service.validate_schema(
                file_content=valid_csv_content,
                file_type="csv"
            )
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert "Schema validation error" in result.errors[0]
