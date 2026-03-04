"""
Data Ingestion Service for the Retail Risk Intelligence Platform.

This service handles file uploads, schema validation, S3 storage, and DynamoDB metadata creation.
Supports CSV and JSON file formats with comprehensive validation.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 8.1, 12.1, 12.2
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Union
from io import BytesIO

import pandas as pd
from botocore.exceptions import ClientError

from app.models.upload import UploadResponse, ValidationResult
from app.utils.validators import validate_csv_schema, validate_json_schema
from app.config import get_config


class DataIngestionService:
    """
    Service for ingesting and validating retail data files.
    
    Handles:
    - File upload validation (size, schema)
    - S3 storage with organized prefix structure
    - DynamoDB metadata creation
    - Error handling with descriptive messages
    - Execution time tracking
    
    Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 8.1, 12.1, 12.2
    """
    
    def __init__(self, s3_client: Any, dynamodb_client: Any):
        """
        Initialize DataIngestionService with dependency injection.
        
        Args:
            s3_client: Boto3 S3 client for file storage
            dynamodb_client: Boto3 DynamoDB client for metadata storage
            
        Requirements: 12.2 - Implement dependency injection for AWS service clients
        """
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        self.config = get_config()
    
    async def upload_file(
        self,
        file_content: bytes,
        filename: str,
        file_type: Literal["csv", "json"]
    ) -> UploadResponse:
        """
        Upload and validate a retail data file.
        
        Process:
        1. Validate file size (max 100MB)
        2. Validate schema using appropriate validator
        3. Upload to S3 with prefix structure: raw/{upload_id}/{filename}
        4. Create metadata record in DynamoDB
        5. Track execution time
        
        Args:
            file_content: Raw file content as bytes
            filename: Original filename
            file_type: File type ("csv" or "json")
            
        Returns:
            UploadResponse: Upload result with upload_id, status, timestamp, and file size
            
        Raises:
            ValueError: If file validation fails or file size exceeds limit
            RuntimeError: If S3 or DynamoDB operations fail
            
        Requirements:
        - 1.1: Validate and store CSV files
        - 1.2: Validate and store JSON files
        - 1.3: Return descriptive error messages within 2 seconds
        - 1.4: Create DynamoDB metadata within 5 seconds
        - 1.5: Support files up to 100MB
        - 1.6: Return upload identifier within 1 second
        - 8.1: Store files with organized prefix structure
        - 12.1: Separate business logic from AWS SDK calls
        """
        start_time = time.time()
        
        try:
            # Validate file size (Requirement 1.5)
            file_size = len(file_content)
            max_size = self.config.max_upload_size_bytes
            
            if file_size > max_size:
                raise ValueError(
                    f"File size ({file_size} bytes) exceeds maximum allowed size "
                    f"({max_size} bytes / {max_size // (1024 * 1024)}MB)"
                )
            
            # Validate schema (Requirements 1.1, 1.2, 1.3)
            validation_result = await self.validate_schema(file_content, file_type)
            
            if not validation_result.is_valid:
                error_message = "; ".join(validation_result.errors)
                raise ValueError(f"Schema validation failed: {error_message}")
            
            # Generate upload ID (Requirement 1.6)
            upload_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc)
            
            # Upload to S3 with prefix structure (Requirement 8.1)
            s3_key = f"raw/{upload_id}/{filename}"
            
            try:
                self.s3_client.put_object(
                    Bucket=self.config.s3_bucket_name,
                    Key=s3_key,
                    Body=file_content,
                    Metadata={
                        "upload_id": upload_id,
                        "original_filename": filename,
                        "file_type": file_type,
                        "upload_timestamp": timestamp.isoformat()
                    }
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", str(e))
                raise RuntimeError(
                    f"Failed to upload file to S3 (Error: {error_code}): {error_message}"
                )
            
            # Create DynamoDB metadata (Requirement 1.4)
            try:
                self.dynamodb_client.put_item(
                    TableName=self.config.dynamodb_table_name,
                    Item={
                        "scenario_id": {"S": upload_id},
                        "record_type": {"S": "upload"},
                        "upload_id": {"S": upload_id},
                        "timestamp": {"S": timestamp.isoformat()},
                        "status": {"S": "success"},
                        "data": {
                            "M": {
                                "filename": {"S": filename},
                                "file_type": {"S": file_type},
                                "file_size_bytes": {"N": str(file_size)},
                                "s3_key": {"S": s3_key},
                                "s3_bucket": {"S": self.config.s3_bucket_name}
                            }
                        },
                        "execution_time_seconds": {"N": str(time.time() - start_time)}
                    }
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", str(e))
                raise RuntimeError(
                    f"Failed to create DynamoDB metadata (Error: {error_code}): {error_message}"
                )
            
            # Track execution time
            execution_time = time.time() - start_time
            
            return UploadResponse(
                upload_id=upload_id,
                status="success",
                timestamp=timestamp,
                file_size_bytes=file_size
            )
            
        except ValueError as e:
            # Validation errors - return descriptive message (Requirement 1.3)
            raise ValueError(str(e))
        except RuntimeError as e:
            # AWS service errors - return descriptive message
            raise RuntimeError(str(e))
        except Exception as e:
            # Unexpected errors - return descriptive message
            raise RuntimeError(f"Unexpected error during file upload: {str(e)}")
    
    async def validate_schema(
        self,
        file_content: bytes,
        file_type: Literal["csv", "json"]
    ) -> ValidationResult:
        """
        Validate file schema using appropriate validator.
        
        Delegates to validators from app.utils.validators module:
        - CSV files: validate_csv_schema()
        - JSON files: validate_json_schema()
        
        Args:
            file_content: Raw file content as bytes
            file_type: File type ("csv" or "json")
            
        Returns:
            ValidationResult: Validation result with is_valid flag, errors, and warnings
            
        Raises:
            ValueError: If file_type is not supported
            
        Requirements:
        - 1.1: Validate CSV schema
        - 1.2: Validate JSON schema
        - 1.3: Return descriptive error messages
        """
        try:
            if file_type == "csv":
                # Use CSV validator from utils
                result = validate_csv_schema(file_content)
            elif file_type == "json":
                # Use JSON validator from utils
                result = validate_json_schema(file_content)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            return ValidationResult(
                is_valid=result["is_valid"],
                errors=result["errors"],
                warnings=result["warnings"]
            )
            
        except Exception as e:
            # Return validation error with descriptive message
            return ValidationResult(
                is_valid=False,
                errors=[f"Schema validation error: {str(e)}"],
                warnings=[]
            )
