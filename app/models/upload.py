"""Upload-related data models for the Retail Risk Intelligence Platform."""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    """Response model for file upload operations.
    
    Attributes:
        upload_id: Unique identifier for the uploaded file
        status: Upload status (e.g., "success", "processing", "failed")
        timestamp: ISO 8601 timestamp of the upload
        file_size_bytes: Size of the uploaded file in bytes
    """
    upload_id: str = Field(..., description="Unique identifier for the uploaded file")
    status: str = Field(..., description="Upload status")
    timestamp: datetime = Field(..., description="Upload timestamp")
    file_size_bytes: int = Field(..., ge=0, description="File size in bytes")


class ValidationResult(BaseModel):
    """Result of data validation operations.
    
    Attributes:
        is_valid: Whether the validation passed
        errors: List of validation error messages (empty if valid)
        warnings: List of validation warning messages (non-blocking issues)
    """
    is_valid: bool = Field(..., description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
