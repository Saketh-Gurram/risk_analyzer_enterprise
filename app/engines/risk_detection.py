"""
Risk Detection Engine for Retail Risk Intelligence Platform.

Analyzes seasonal volatility and classifies risk levels using ML inference.
Fetches data from S3, calculates statistical metrics, invokes SageMaker for
ML-enhanced scoring, and stores results in DynamoDB.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 8.4, 11.4
"""

import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from app.config import get_config
from app.models.enums import RiskLevel
from app.models.risk import ProductRisk, RiskAssessment
from app.utils.logging import get_logger

logger = get_logger(__name__)


class RiskDetectionEngine:
    """
    Engine for detecting seasonal risk signals in retail data.
    
    Analyzes coefficient of variation for each product, calculates rolling
    average deviation, classifies risk levels, and invokes SageMaker for
    ML-enhanced risk scoring.
    
    Attributes:
        s3_client: Boto3 S3 client for data retrieval
        dynamodb_client: Boto3 DynamoDB client for result storage
        sagemaker_client: Boto3 SageMaker Runtime client for ML inference
        config: Application configuration
    """
    
    def __init__(
        self,
        s3_client: Any,
        dynamodb_client: Any,
        sagemaker_client: Any
    ):
        """
        Initialize RiskDetectionEngine with dependency injection.
        
        Args:
            s3_client: Boto3 S3 client for data retrieval
            dynamodb_client: Boto3 DynamoDB client for result storage
            sagemaker_client: Boto3 SageMaker Runtime client for ML inference
        """
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        self.sagemaker_client = sagemaker_client
        self.config = get_config()
        
        logger.info(
            "RiskDetectionEngine initialized",
            extra={
                "bucket": self.config.s3_bucket_name,
                "table": self.config.dynamodb_table_name,
                "endpoint": self.config.sagemaker_endpoint_name
            }
        )
    
    async def analyze_risk(self, upload_id: str) -> RiskAssessment:
        """
        Analyze risk for uploaded retail data.
        
        Fetches data from S3, calculates coefficient of variation and rolling
        average deviation for each product, invokes SageMaker for ML inference,
        classifies risk levels, and stores results in DynamoDB.
        
        Args:
            upload_id: Unique identifier for the uploaded data
            
        Returns:
            RiskAssessment: Complete risk assessment with per-product risk data
            
        Raises:
            ClientError: If S3, DynamoDB, or SageMaker operations fail
            ValueError: If data format is invalid
            
        Requirements: 2.1, 2.2, 2.6, 2.7, 2.8
        """
        start_time = time.time()
        assessment_id = str(uuid.uuid4())
        
        logger.info(
            "Starting risk analysis",
            extra={
                "assessment_id": assessment_id,
                "upload_id": upload_id
            }
        )
        
        try:
            # Fetch data from S3
            data = await self._fetch_data_from_s3(upload_id)
            
            # Calculate risk metrics for each product
            product_risks = []
            for product_id in data['product_id'].unique():
                product_data = data[data['product_id'] == product_id]
                
                # Calculate coefficient of variation
                cv = self.calculate_coefficient_of_variation(product_data['quantity'])
                
                # Calculate rolling average deviation
                rolling_dev = self.calculate_rolling_average_deviation(product_data['quantity'])
                
                # Invoke SageMaker for ML-enhanced scoring
                ml_score = await self.invoke_sagemaker_endpoint(
                    product_id=product_id,
                    cv=cv,
                    rolling_dev=rolling_dev
                )
                
                # Use ML score if available, otherwise use CV
                final_score = ml_score if ml_score is not None else cv
                
                # Classify risk level
                risk_level = self.classify_risk_level(final_score)
                
                # Get product name
                product_name = product_data['product_name'].iloc[0] if 'product_name' in product_data.columns else f"Product {product_id}"
                
                product_risk = ProductRisk(
                    product_id=product_id,
                    product_name=product_name,
                    coefficient_of_variation=cv,
                    risk_level=risk_level,
                    rolling_avg_deviation=rolling_dev
                )
                product_risks.append(product_risk)
            
            # Count risk levels
            high_risk_count = sum(1 for pr in product_risks if pr.risk_level == RiskLevel.HIGH)
            medium_risk_count = sum(1 for pr in product_risks if pr.risk_level == RiskLevel.MEDIUM)
            low_risk_count = sum(1 for pr in product_risks if pr.risk_level == RiskLevel.LOW)
            
            execution_time = time.time() - start_time
            
            # Create risk assessment
            risk_assessment = RiskAssessment(
                assessment_id=assessment_id,
                upload_id=upload_id,
                timestamp=datetime.utcnow(),
                total_products=len(product_risks),
                high_risk_count=high_risk_count,
                medium_risk_count=medium_risk_count,
                low_risk_count=low_risk_count,
                product_risks=product_risks,
                execution_time_seconds=execution_time
            )
            
            # Store results in DynamoDB
            await self._store_results(risk_assessment)
            
            # Log CloudWatch metrics
            self._log_metrics(risk_assessment)
            
            logger.info(
                "Risk analysis completed",
                extra={
                    "assessment_id": assessment_id,
                    "total_products": len(product_risks),
                    "high_risk": high_risk_count,
                    "medium_risk": medium_risk_count,
                    "low_risk": low_risk_count,
                    "execution_time_seconds": execution_time
                }
            )
            
            return risk_assessment
            
        except Exception as e:
            logger.error(
                "Risk analysis failed",
                extra={
                    "assessment_id": assessment_id,
                    "upload_id": upload_id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise
    
    def calculate_coefficient_of_variation(self, product_data: pd.Series) -> float:
        """
        Calculate coefficient of variation for product demand.
        
        CV = standard_deviation / mean
        Measures relative variability of demand.
        
        Args:
            product_data: Series of demand/quantity values for a product
            
        Returns:
            float: Coefficient of variation (0.0 if mean is zero)
            
        Requirements: 2.1
        """
        mean = product_data.mean()
        if mean == 0:
            return 0.0
        
        std = product_data.std()
        cv = std / mean
        
        logger.debug(
            "Calculated coefficient of variation",
            extra={
                "mean": mean,
                "std": std,
                "cv": cv
            }
        )
        
        return cv
    
    def calculate_rolling_average_deviation(
        self,
        product_data: pd.Series,
        window_days: int = 30
    ) -> float:
        """
        Calculate rolling average deviation over a time window.
        
        Computes the mean of absolute deviations from the rolling average
        over the specified window period.
        
        Args:
            product_data: Series of demand/quantity values for a product
            window_days: Rolling window size in days (default: 30)
            
        Returns:
            float: Rolling average deviation
            
        Requirements: 2.2
        """
        if len(product_data) < window_days:
            # If insufficient data, use simple deviation from mean
            mean = product_data.mean()
            deviation = (product_data - mean).abs().mean()
        else:
            # Calculate rolling average
            rolling_avg = product_data.rolling(window=window_days, min_periods=1).mean()
            # Calculate absolute deviations from rolling average
            deviations = (product_data - rolling_avg).abs()
            # Mean of deviations
            deviation = deviations.mean()
        
        logger.debug(
            "Calculated rolling average deviation",
            extra={
                "window_days": window_days,
                "data_points": len(product_data),
                "deviation": deviation
            }
        )
        
        return deviation
    
    def classify_risk_level(self, cv_score: float) -> RiskLevel:
        """
        Classify risk level based on coefficient of variation thresholds.
        
        Risk levels:
        - LOW: CV < 0.15
        - MEDIUM: 0.15 <= CV < 0.3
        - HIGH: CV >= 0.3
        
        Args:
            cv_score: Coefficient of variation score
            
        Returns:
            RiskLevel: Classified risk level
            
        Requirements: 2.3, 2.4, 2.5
        """
        if cv_score < self.config.risk_threshold_low_medium:
            risk_level = RiskLevel.LOW
        elif cv_score < self.config.risk_threshold_medium_high:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.HIGH
        
        logger.debug(
            "Classified risk level",
            extra={
                "cv_score": cv_score,
                "risk_level": risk_level.value,
                "threshold_low_medium": self.config.risk_threshold_low_medium,
                "threshold_medium_high": self.config.risk_threshold_medium_high
            }
        )
        
        return risk_level
    
    async def invoke_sagemaker_endpoint(
        self,
        product_id: str,
        cv: float,
        rolling_dev: float
    ) -> Optional[float]:
        """
        Invoke SageMaker endpoint for ML-enhanced risk scoring.
        
        Sends product metrics to SageMaker endpoint and receives ML-enhanced
        risk score. Returns None if invocation fails.
        
        Args:
            product_id: Product identifier
            cv: Coefficient of variation
            rolling_dev: Rolling average deviation
            
        Returns:
            Optional[float]: ML-enhanced risk score, or None if invocation fails
            
        Requirements: 2.8
        """
        try:
            # Prepare input payload
            payload = {
                "product_id": product_id,
                "coefficient_of_variation": cv,
                "rolling_avg_deviation": rolling_dev
            }
            
            logger.debug(
                "Invoking SageMaker endpoint",
                extra={
                    "endpoint": self.config.sagemaker_endpoint_name,
                    "product_id": product_id
                }
            )
            
            # Invoke SageMaker endpoint
            response = self.sagemaker_client.invoke_endpoint(
                EndpointName=self.config.sagemaker_endpoint_name,
                ContentType="application/json",
                Body=json.dumps(payload)
            )
            
            # Parse response
            result = json.loads(response['Body'].read().decode())
            ml_score = result.get('risk_score')
            
            logger.debug(
                "SageMaker invocation successful",
                extra={
                    "product_id": product_id,
                    "ml_score": ml_score
                }
            )
            
            return ml_score
            
        except ClientError as e:
            logger.warning(
                "SageMaker invocation failed, using fallback",
                extra={
                    "product_id": product_id,
                    "error": str(e)
                }
            )
            return None
        except Exception as e:
            logger.warning(
                "SageMaker invocation error, using fallback",
                extra={
                    "product_id": product_id,
                    "error": str(e)
                }
            )
            return None
    
    async def _fetch_data_from_s3(self, upload_id: str) -> pd.DataFrame:
        """
        Fetch processed data from S3.
        
        Args:
            upload_id: Unique identifier for the uploaded data
            
        Returns:
            pd.DataFrame: Processed retail data
            
        Raises:
            ClientError: If S3 operation fails
            ValueError: If data format is invalid
        """
        try:
            # Construct S3 key for processed data
            s3_key = f"processed/{upload_id}/processed_data.parquet"
            
            logger.debug(
                "Fetching data from S3",
                extra={
                    "bucket": self.config.s3_bucket_name,
                    "key": s3_key
                }
            )
            
            # Download file from S3
            response = self.s3_client.get_object(
                Bucket=self.config.s3_bucket_name,
                Key=s3_key
            )
            
            # Read parquet data
            data = pd.read_parquet(response['Body'])
            
            logger.debug(
                "Data fetched from S3",
                extra={
                    "rows": len(data),
                    "columns": list(data.columns)
                }
            )
            
            return data
            
        except ClientError as e:
            logger.error(
                "Failed to fetch data from S3",
                extra={
                    "upload_id": upload_id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise
    
    async def _store_results(self, risk_assessment: RiskAssessment) -> None:
        """
        Store risk assessment results in DynamoDB.
        
        Args:
            risk_assessment: Complete risk assessment to store
            
        Raises:
            ClientError: If DynamoDB operation fails
            
        Requirements: 2.7, 8.4
        """
        try:
            # Prepare DynamoDB item
            item = {
                "scenario_id": {"S": risk_assessment.assessment_id},
                "record_type": {"S": "risk_assessment"},
                "upload_id": {"S": risk_assessment.upload_id},
                "timestamp": {"S": risk_assessment.timestamp.isoformat()},
                "data": {"S": risk_assessment.model_dump_json()},
                "status": {"S": "completed"},
                "execution_time_seconds": {"N": str(risk_assessment.execution_time_seconds)}
            }
            
            logger.debug(
                "Storing results in DynamoDB",
                extra={
                    "table": self.config.dynamodb_table_name,
                    "assessment_id": risk_assessment.assessment_id
                }
            )
            
            # Store in DynamoDB
            self.dynamodb_client.put_item(
                TableName=self.config.dynamodb_table_name,
                Item=item
            )
            
            logger.info(
                "Results stored in DynamoDB",
                extra={
                    "assessment_id": risk_assessment.assessment_id
                }
            )
            
        except ClientError as e:
            logger.error(
                "Failed to store results in DynamoDB",
                extra={
                    "assessment_id": risk_assessment.assessment_id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise
    
    def _log_metrics(self, risk_assessment: RiskAssessment) -> None:
        """
        Log execution time and metrics to CloudWatch.
        
        Args:
            risk_assessment: Risk assessment with metrics to log
            
        Requirements: 11.4
        """
        logger.info(
            "Risk assessment metrics",
            extra={
                "metric_name": "RiskAssessmentExecutionTime",
                "metric_value": risk_assessment.execution_time_seconds,
                "metric_unit": "Seconds",
                "assessment_id": risk_assessment.assessment_id,
                "total_products": risk_assessment.total_products,
                "high_risk_count": risk_assessment.high_risk_count,
                "medium_risk_count": risk_assessment.medium_risk_count,
                "low_risk_count": risk_assessment.low_risk_count
            }
        )
