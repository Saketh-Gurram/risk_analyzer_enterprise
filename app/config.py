"""
Configuration management for Retail Risk Intelligence Platform.
Loads and validates environment variables using Pydantic BaseSettings.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from typing import Optional


class Config(BaseSettings):
    """
    Application configuration loaded from environment variables.
    
    Validates required configuration parameters at startup.
    Provides default values for optional parameters.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # AWS Configuration
    aws_region: str = Field(default="us-east-1", description="AWS region for service calls")
    
    # S3 Configuration
    s3_bucket_name: str = Field(..., description="S3 bucket name for data storage")
    
    # DynamoDB Configuration
    dynamodb_table_name: str = Field(..., description="DynamoDB table name for metadata storage")
    
    # SageMaker Configuration
    sagemaker_endpoint_name: str = Field(..., description="SageMaker endpoint name for risk detection")
    
    # Amazon Bedrock Configuration
    bedrock_model_id: str = Field(
        default="anthropic.claude-3-5-sonnet-20241022-v2:0",
        description="Bedrock model ID for AI reasoning"
    )
    
    # Risk Thresholds
    risk_threshold_low_medium: float = Field(
        default=0.15,
        ge=0.0,
        le=1.0,
        description="Coefficient of variation threshold for low-medium risk boundary"
    )
    risk_threshold_medium_high: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Coefficient of variation threshold for medium-high risk boundary"
    )
    
    # Simulation Configuration
    default_simulation_time_horizon_days: int = Field(
        default=90,
        ge=7,
        le=365,
        description="Default simulation time horizon in days"
    )
    default_monte_carlo_iterations: int = Field(
        default=1000,
        ge=100,
        description="Default number of Monte Carlo iterations"
    )
    
    # File Upload Configuration
    max_upload_size_bytes: int = Field(
        default=100 * 1024 * 1024,  # 100MB
        description="Maximum file upload size in bytes"
    )
    
    @field_validator("risk_threshold_low_medium", "risk_threshold_medium_high")
    @classmethod
    def validate_thresholds(cls, v: float) -> float:
        """Validate risk thresholds are between 0 and 1."""
        if not 0.0 <= v <= 1.0:
            raise ValueError("Risk thresholds must be between 0.0 and 1.0")
        return v
    
    @field_validator("risk_threshold_medium_high")
    @classmethod
    def validate_threshold_order(cls, v: float, info) -> float:
        """Validate that medium-high threshold is greater than low-medium threshold."""
        if "risk_threshold_low_medium" in info.data:
            low_medium = info.data["risk_threshold_low_medium"]
            if v <= low_medium:
                raise ValueError(
                    f"risk_threshold_medium_high ({v}) must be greater than "
                    f"risk_threshold_low_medium ({low_medium})"
                )
        return v
    
    def validate_required_config(self) -> None:
        """
        Validate that all required configuration is present.
        Raises ValueError if required configuration is missing.
        """
        required_fields = [
            "s3_bucket_name",
            "dynamodb_table_name",
            "sagemaker_endpoint_name"
        ]
        
        missing_fields = []
        for field_name in required_fields:
            value = getattr(self, field_name, None)
            if not value:
                missing_fields.append(field_name.upper())
        
        if missing_fields:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing_fields)}. "
                "Please set these environment variables."
            )


# Global configuration instance
config: Optional[Config] = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    Creates and validates configuration on first call.
    
    Returns:
        Config: Validated configuration instance
        
    Raises:
        ValueError: If required configuration is missing or invalid
    """
    global config
    if config is None:
        config = Config()
        config.validate_required_config()
    return config


def reset_config() -> None:
    """Reset the global configuration instance. Used for testing."""
    global config
    config = None
