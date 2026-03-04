"""Risk assessment data models for the Retail Risk Intelligence Platform."""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from app.models.enums import RiskLevel


class ProductRisk(BaseModel):
    """Risk assessment for an individual product.
    
    Attributes:
        product_id: Unique identifier for the product
        product_name: Human-readable product name
        coefficient_of_variation: Statistical measure of demand volatility (CV = std/mean)
        risk_level: Classification based on CV thresholds (low/medium/high)
        rolling_avg_deviation: Rolling 30-day average deviation from mean demand
    """
    product_id: str
    product_name: str
    coefficient_of_variation: float = Field(ge=0.0)
    risk_level: RiskLevel
    rolling_avg_deviation: float


class RiskAssessment(BaseModel):
    """Complete risk assessment results for a dataset.
    
    Attributes:
        assessment_id: Unique identifier for this assessment
        upload_id: Reference to the source data upload
        timestamp: When the assessment was completed
        total_products: Total number of products analyzed
        high_risk_count: Number of products classified as high risk
        medium_risk_count: Number of products classified as medium risk
        low_risk_count: Number of products classified as low risk
        product_risks: Detailed risk data for each product
        execution_time_seconds: Time taken to complete the assessment
    """
    assessment_id: str
    upload_id: str
    timestamp: datetime
    total_products: int = Field(ge=0)
    high_risk_count: int = Field(ge=0)
    medium_risk_count: int = Field(ge=0)
    low_risk_count: int = Field(ge=0)
    product_risks: List[ProductRisk]
    execution_time_seconds: float = Field(ge=0.0)
