"""
Unit tests for DataIngestionService.

Tests cover:
- File size validation
- Schema validation for CSV and JSON
- S3 upload with correct prefix structure
- DynamoDB metadata creation
- Error handling
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
import json

from app.services.data_ingestion import DataIngestionService
from app.models.upload import UploadResponse, ValidationResult


@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    client = Mock()
    client.put_object = Mock(return_value={})
    return client


@pytest.fixture
def mock_dynamodb_client():
    """Mock DynamoDB client."""
    client = Mock()
    client.put_item = Mock(return_value={})
    return client


@pytest.fixture
def mock_config():
    """Mock configuration."""
    config = Mock()
    config.s3_bucket_name = "test-bucket"
    config.dynamodb_table_name = "test-table"
    config.max_upload_size_bytes = 100 * 1024 * 1024  # 100MB
    return config


@pytest.fixture
def data_ingestion_service(mock_s3_client, mock_dynamodb_client, mock_config):
    """Create DataIngestionService with mocked clients."""
    with patch('app.services.data_ingestion.get_config', return_value=mock_config):
        return DataIngestionService(
            s3_client=mock_s3_client,
            dynamodb_client=mock_dynamodb_client
        )


@pytest.mark.asyncio
async def test_upload_csv_file_success(data_ingestion_service, mock_s3_client, mock_dynamodb_client):
    """Test successful CSV file upload."""
    # Valid CSV content
    csv_content = b"product_id,product_name,date,quantity,price\nP001,Widget,2024-01-01,10,99.99"
    filename = "test_data.csv"
    
    result = await data_ingestion_service.upload_file(
        file_content=csv_content,
        filename=filename,
        file_type="csv"
    )
    
    # Verify response
    assert isinstance(result, UploadResponse)
    assert result.status == "success"
    assert result.file_size_bytes == len(csv_content)
    assert result.upload_id is not None
    
    # Verify S3 upload was called with correct prefix
    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args
    assert call_args[1]["Key"].startswith(f"raw/{result.upload_id}/")
    assert call_args[1]["Key"].endswith(filename)
    
    # Verify DynamoDB metadata was created
    mock_dynamodb_client.put_item.assert_called_once()
    dynamo_call = mock_dynamodb_client.put_item.call_args
    assert dynamo_call[1]["Item"]["scenario_id"]["S"] == result.upload_id
    assert dynamo_call[1]["Item"]["record_type"]["S"] == "upload"


@pytest.mark.asyncio
async def test_upload_json_file_success(data_ingestion_service, mock_s3_client, mock_dynamodb_client):
    """Test successful JSON file upload."""
    # Valid JSON content
    json_data = {
        "records": [
            {
                "product_id": "P001",
                "product_name": "Widget",
                "date": "2024-01-01",
                "quantity": 10,
                "price": 99.99
            }
        ]
    }
    json_content = json.dumps(json_data).encode('utf-8')
    filename = "test_data.json"
    
    result = await data_ingestion_service.upload_file(
        file_content=json_content,
        filename=filename,
        file_type="json"
    )
    
    # Verify response
    assert isinstance(result, UploadResponse)
    assert result.status == "success"
    assert result.file_size_bytes == len(json_content)
    
    # Verify S3 and DynamoDB calls
    mock_s3_client.put_object.assert_called_once()
    mock_dynamodb_client.put_item.assert_called_once()


@pytest.mark.asyncio
async def test_upload_file_exceeds_size_limit(data_ingestion_service):
    """Test file size validation - file exceeds 100MB limit."""
    # Create content larger than 100MB
    large_content = b"x" * (101 * 1024 * 1024)
    
    with pytest.raises(ValueError) as exc_info:
        await data_ingestion_service.upload_file(
            file_content=large_content,
            filename="large_file.csv",
            file_type="csv"
        )
    
    assert "exceeds maximum allowed size" in str(exc_info.value)


@pytest.mark.asyncio
async def test_upload_file_invalid_csv_schema(data_ingestion_service):
    """Test CSV schema validation failure."""
    # CSV missing required fields
    invalid_csv = b"product_id,product_name\nP001,Widget"
    
    with pytest.raises(ValueError) as exc_info:
        await data_ingestion_service.upload_file(
            file_content=invalid_csv,
            filename="invalid.csv",
            file_type="csv"
        )
    
    assert "Schema validation failed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_upload_file_invalid_json_schema(data_ingestion_service):
    """Test JSON schema validation failure."""
    # JSON missing required fields
    invalid_json = json.dumps({"records": [{"product_id": "P001"}]}).encode('utf-8')
    
    with pytest.raises(ValueError) as exc_info:
        await data_ingestion_service.upload_file(
            file_content=invalid_json,
            filename="invalid.json",
            file_type="json"
        )
    
    assert "Schema validation failed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_schema_csv(data_ingestion_service):
    """Test CSV schema validation."""
    valid_csv = b"product_id,product_name,date,quantity,price\nP001,Widget,2024-01-01,10,99.99"
    
    result = await data_ingestion_service.validate_schema(valid_csv, "csv")
    
    assert isinstance(result, ValidationResult)
    assert result.is_valid is True
    assert len(result.errors) == 0


@pytest.mark.asyncio
async def test_validate_schema_json(data_ingestion_service):
    """Test JSON schema validation."""
    valid_json = json.dumps({
        "records": [
            {
                "product_id": "P001",
                "product_name": "Widget",
                "date": "2024-01-01",
                "quantity": 10,
                "price": 99.99
            }
        ]
    }).encode('utf-8')
    
    result = await data_ingestion_service.validate_schema(valid_json, "json")
    
    assert isinstance(result, ValidationResult)
    assert result.is_valid is True
    assert len(result.errors) == 0


@pytest.mark.asyncio
async def test_validate_schema_unsupported_type(data_ingestion_service):
    """Test validation with unsupported file type."""
    result = await data_ingestion_service.validate_schema(b"content", "xml")
    
    assert result.is_valid is False
    assert len(result.errors) > 0
    assert "Unsupported file type" in result.errors[0]


@pytest.mark.asyncio
async def test_s3_upload_error_handling(data_ingestion_service, mock_s3_client):
    """Test error handling when S3 upload fails."""
    from botocore.exceptions import ClientError
    
    # Mock S3 client to raise error
    mock_s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "PutObject"
    )
    
    valid_csv = b"product_id,product_name,date,quantity,price\nP001,Widget,2024-01-01,10,99.99"
    
    with pytest.raises(RuntimeError) as exc_info:
        await data_ingestion_service.upload_file(
            file_content=valid_csv,
            filename="test.csv",
            file_type="csv"
        )
    
    assert "Failed to upload file to S3" in str(exc_info.value)
    assert "AccessDenied" in str(exc_info.value)


@pytest.mark.asyncio
async def test_dynamodb_error_handling(data_ingestion_service, mock_dynamodb_client):
    """Test error handling when DynamoDB operation fails."""
    from botocore.exceptions import ClientError
    
    # Mock DynamoDB client to raise error
    mock_dynamodb_client.put_item.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}},
        "PutItem"
    )
    
    valid_csv = b"product_id,product_name,date,quantity,price\nP001,Widget,2024-01-01,10,99.99"
    
    with pytest.raises(RuntimeError) as exc_info:
        await data_ingestion_service.upload_file(
            file_content=valid_csv,
            filename="test.csv",
            file_type="csv"
        )
    
    assert "Failed to create DynamoDB metadata" in str(exc_info.value)
    assert "ResourceNotFoundException" in str(exc_info.value)
