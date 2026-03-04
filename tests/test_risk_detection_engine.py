"""
Unit tests for RiskDetectionEngine.

Tests verify risk analysis, coefficient of variation calculation, rolling average
deviation, risk level classification, SageMaker invocation, S3 data fetching,
DynamoDB result storage, and CloudWatch metrics logging.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 8.4, 11.4
"""

import json
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime
from io import BytesIO
import pandas as pd
from botocore.exceptions import ClientError

from app.engines.risk_detection import RiskDetectionEngine
from app.models.enums import RiskLevel
from app.models.risk import ProductRisk, RiskAssessment


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_dynamodb_client():
    """Create a mock DynamoDB client."""
    client = MagicMock()
    client.put_item = MagicMock()
    return client


@pytest.fixture
def mock_sagemaker_client():
    """Create a mock SageMaker Runtime client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    config = MagicMock()
    config.s3_bucket_name = "test-bucket"
    config.dynamodb_table_name = "test-table"
    config.sagemaker_endpoint_name = "test-endpoint"
    config.risk_threshold_low_medium = 0.15
    config.risk_threshold_medium_high = 0.3
    return config


@pytest.fixture
def risk_detection_engine(mock_s3_client, mock_dynamodb_client, mock_sagemaker_client, mock_config):
    """Create a RiskDetectionEngine instance with mocked dependencies."""
    with patch("app.engines.risk_detection.get_config", return_value=mock_config):
        engine = RiskDetectionEngine(
            mock_s3_client,
            mock_dynamodb_client,
            mock_sagemaker_client
        )
    return engine


@pytest.fixture
def sample_product_data():
    """Create sample product data for testing."""
    return pd.DataFrame({
        'product_id': ['P001', 'P001', 'P001', 'P002', 'P002', 'P002'],
        'product_name': ['Widget A', 'Widget A', 'Widget A', 'Widget B', 'Widget B', 'Widget B'],
        'quantity': [100, 110, 105, 200, 250, 220],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-01', '2024-01-02', '2024-01-03']
    })


class TestCoefficientOfVariation:
    """Tests for coefficient of variation calculation (Requirement 2.1)."""
    
    def test_calculate_cv_with_normal_data(self, risk_detection_engine):
        """Test CV calculation with normal product data."""
        data = pd.Series([100, 110, 105, 95, 102])
        cv = risk_detection_engine.calculate_coefficient_of_variation(data)
        
        # CV = std / mean
        expected_cv = data.std() / data.mean()
        assert abs(cv - expected_cv) < 0.001
    
    def test_calculate_cv_with_zero_mean(self, risk_detection_engine):
        """Test CV calculation when mean is zero."""
        data = pd.Series([0, 0, 0, 0])
        cv = risk_detection_engine.calculate_coefficient_of_variation(data)
        
        # Should return 0.0 when mean is zero
        assert cv == 0.0
    
    def test_calculate_cv_with_high_volatility(self, risk_detection_engine):
        """Test CV calculation with high volatility data."""
        data = pd.Series([10, 100, 20, 90, 15])
        cv = risk_detection_engine.calculate_coefficient_of_variation(data)
        
        # High volatility should result in high CV
        assert cv > 0.5
    
    def test_calculate_cv_with_low_volatility(self, risk_detection_engine):
        """Test CV calculation with low volatility data."""
        data = pd.Series([100, 101, 99, 100, 102])
        cv = risk_detection_engine.calculate_coefficient_of_variation(data)
        
        # Low volatility should result in low CV
        assert cv < 0.05


class TestRollingAverageDeviation:
    """Tests for rolling average deviation calculation (Requirement 2.2)."""
    
    def test_calculate_rolling_deviation_with_sufficient_data(self, risk_detection_engine):
        """Test rolling deviation with sufficient data points."""
        # Create 40 days of data
        data = pd.Series([100 + i % 10 for i in range(40)])
        deviation = risk_detection_engine.calculate_rolling_average_deviation(data, window_days=30)
        
        assert deviation >= 0
        assert isinstance(deviation, float)
    
    def test_calculate_rolling_deviation_with_insufficient_data(self, risk_detection_engine):
        """Test rolling deviation with insufficient data points."""
        # Create only 10 days of data (less than 30-day window)
        data = pd.Series([100, 105, 110, 95, 102, 108, 97, 103, 106, 99])
        deviation = risk_detection_engine.calculate_rolling_average_deviation(data, window_days=30)
        
        # Should still calculate deviation using simple mean
        assert deviation >= 0
        assert isinstance(deviation, float)
    
    def test_calculate_rolling_deviation_default_window(self, risk_detection_engine):
        """Test rolling deviation uses 30-day default window."""
        data = pd.Series([100 + i for i in range(35)])
        deviation = risk_detection_engine.calculate_rolling_average_deviation(data)
        
        assert deviation >= 0
    
    def test_calculate_rolling_deviation_custom_window(self, risk_detection_engine):
        """Test rolling deviation with custom window size."""
        data = pd.Series([100 + i for i in range(50)])
        deviation_7day = risk_detection_engine.calculate_rolling_average_deviation(data, window_days=7)
        deviation_30day = risk_detection_engine.calculate_rolling_average_deviation(data, window_days=30)
        
        # Different windows should produce different results
        assert deviation_7day != deviation_30day


class TestRiskLevelClassification:
    """Tests for risk level classification (Requirements 2.3, 2.4, 2.5)."""
    
    def test_classify_low_risk(self, risk_detection_engine):
        """Test classification of low risk (CV < 0.15)."""
        cv_score = 0.10
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.LOW
    
    def test_classify_medium_risk_lower_bound(self, risk_detection_engine):
        """Test classification of medium risk at lower boundary (CV = 0.15)."""
        cv_score = 0.15
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.MEDIUM
    
    def test_classify_medium_risk_upper_bound(self, risk_detection_engine):
        """Test classification of medium risk at upper boundary (CV < 0.3)."""
        cv_score = 0.29
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.MEDIUM
    
    def test_classify_high_risk(self, risk_detection_engine):
        """Test classification of high risk (CV >= 0.3)."""
        cv_score = 0.35
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.HIGH
    
    def test_classify_high_risk_at_threshold(self, risk_detection_engine):
        """Test classification of high risk at threshold (CV = 0.3)."""
        cv_score = 0.3
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.HIGH
    
    def test_classify_zero_risk(self, risk_detection_engine):
        """Test classification with zero CV."""
        cv_score = 0.0
        risk_level = risk_detection_engine.classify_risk_level(cv_score)
        
        assert risk_level == RiskLevel.LOW


class TestSageMakerInvocation:
    """Tests for SageMaker endpoint invocation (Requirement 2.8)."""
    
    @pytest.mark.asyncio
    async def test_invoke_sagemaker_success(self, risk_detection_engine, mock_sagemaker_client):
        """Test successful SageMaker endpoint invocation."""
        # Mock successful response
        mock_response = {
            'Body': BytesIO(json.dumps({'risk_score': 0.25}).encode())
        }
        mock_sagemaker_client.invoke_endpoint.return_value = mock_response
        
        ml_score = await risk_detection_engine.invoke_sagemaker_endpoint(
            product_id='P001',
            cv=0.20,
            rolling_dev=5.5
        )
        
        assert ml_score == 0.25
        mock_sagemaker_client.invoke_endpoint.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_invoke_sagemaker_with_correct_payload(self, risk_detection_engine, mock_sagemaker_client):
        """Test SageMaker invocation sends correct payload."""
        mock_response = {
            'Body': BytesIO(json.dumps({'risk_score': 0.25}).encode())
        }
        mock_sagemaker_client.invoke_endpoint.return_value = mock_response
        
        await risk_detection_engine.invoke_sagemaker_endpoint(
            product_id='P001',
            cv=0.20,
            rolling_dev=5.5
        )
        
        call_kwargs = mock_sagemaker_client.invoke_endpoint.call_args[1]
        assert call_kwargs['EndpointName'] == 'test-endpoint'
        assert call_kwargs['ContentType'] == 'application/json'
        
        payload = json.loads(call_kwargs['Body'])
        assert payload['product_id'] == 'P001'
        assert payload['coefficient_of_variation'] == 0.20
        assert payload['rolling_avg_deviation'] == 5.5
    
    @pytest.mark.asyncio
    async def test_invoke_sagemaker_failure_returns_none(self, risk_detection_engine, mock_sagemaker_client):
        """Test SageMaker invocation failure returns None."""
        # Mock ClientError
        mock_sagemaker_client.invoke_endpoint.side_effect = ClientError(
            error_response={'Error': {'Code': 'ModelError', 'Message': 'Model error'}},
            operation_name='InvokeEndpoint'
        )
        
        ml_score = await risk_detection_engine.invoke_sagemaker_endpoint(
            product_id='P001',
            cv=0.20,
            rolling_dev=5.5
        )
        
        assert ml_score is None
    
    @pytest.mark.asyncio
    async def test_invoke_sagemaker_exception_returns_none(self, risk_detection_engine, mock_sagemaker_client):
        """Test SageMaker invocation exception returns None."""
        # Mock generic exception
        mock_sagemaker_client.invoke_endpoint.side_effect = Exception('Network error')
        
        ml_score = await risk_detection_engine.invoke_sagemaker_endpoint(
            product_id='P001',
            cv=0.20,
            rolling_dev=5.5
        )
        
        assert ml_score is None


class TestFetchDataFromS3:
    """Tests for fetching data from S3."""
    
    @pytest.mark.asyncio
    async def test_fetch_data_success(self, risk_detection_engine, mock_s3_client, sample_product_data):
        """Test successful data fetch from S3."""
        # Create parquet data in memory
        parquet_buffer = BytesIO()
        sample_product_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        # Mock S3 response
        mock_s3_client.get_object.return_value = {
            'Body': parquet_buffer
        }
        
        data = await risk_detection_engine._fetch_data_from_s3('test-upload-id')
        
        assert isinstance(data, pd.DataFrame)
        assert len(data) == len(sample_product_data)
        assert 'product_id' in data.columns
    
    @pytest.mark.asyncio
    async def test_fetch_data_correct_s3_key(self, risk_detection_engine, mock_s3_client, sample_product_data):
        """Test data fetch uses correct S3 key structure."""
        parquet_buffer = BytesIO()
        sample_product_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        mock_s3_client.get_object.return_value = {
            'Body': parquet_buffer
        }
        
        await risk_detection_engine._fetch_data_from_s3('test-upload-id')
        
        call_kwargs = mock_s3_client.get_object.call_args[1]
        assert call_kwargs['Bucket'] == 'test-bucket'
        assert call_kwargs['Key'] == 'processed/test-upload-id/processed_data.parquet'
    
    @pytest.mark.asyncio
    async def test_fetch_data_s3_error(self, risk_detection_engine, mock_s3_client):
        """Test data fetch handles S3 errors."""
        mock_s3_client.get_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
            operation_name='GetObject'
        )
        
        with pytest.raises(ClientError):
            await risk_detection_engine._fetch_data_from_s3('test-upload-id')


class TestStoreResults:
    """Tests for storing results in DynamoDB (Requirements 2.7, 8.4)."""
    
    @pytest.mark.asyncio
    async def test_store_results_success(self, risk_detection_engine, mock_dynamodb_client):
        """Test successful result storage in DynamoDB."""
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=3,
            high_risk_count=1,
            medium_risk_count=1,
            low_risk_count=1,
            product_risks=[],
            execution_time_seconds=5.5
        )
        
        await risk_detection_engine._store_results(risk_assessment)
        
        mock_dynamodb_client.put_item.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_store_results_correct_structure(self, risk_detection_engine, mock_dynamodb_client):
        """Test result storage uses correct DynamoDB structure."""
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=3,
            high_risk_count=1,
            medium_risk_count=1,
            low_risk_count=1,
            product_risks=[],
            execution_time_seconds=5.5
        )
        
        await risk_detection_engine._store_results(risk_assessment)
        
        call_kwargs = mock_dynamodb_client.put_item.call_args[1]
        assert call_kwargs['TableName'] == 'test-table'
        
        item = call_kwargs['Item']
        assert item['scenario_id']['S'] == 'test-assessment-id'
        assert item['record_type']['S'] == 'risk_assessment'
        assert item['upload_id']['S'] == 'test-upload-id'
        assert item['status']['S'] == 'completed'
        assert 'execution_time_seconds' in item
    
    @pytest.mark.asyncio
    async def test_store_results_uses_scenario_id_partition_key(self, risk_detection_engine, mock_dynamodb_client):
        """Test result storage uses scenario_id as partition key (Requirement 8.4)."""
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=3,
            high_risk_count=1,
            medium_risk_count=1,
            low_risk_count=1,
            product_risks=[],
            execution_time_seconds=5.5
        )
        
        await risk_detection_engine._store_results(risk_assessment)
        
        call_kwargs = mock_dynamodb_client.put_item.call_args[1]
        item = call_kwargs['Item']
        
        # Verify scenario_id is used as partition key
        assert 'scenario_id' in item
        assert item['scenario_id']['S'] == risk_assessment.assessment_id
    
    @pytest.mark.asyncio
    async def test_store_results_dynamodb_error(self, risk_detection_engine, mock_dynamodb_client):
        """Test result storage handles DynamoDB errors."""
        mock_dynamodb_client.put_item.side_effect = ClientError(
            error_response={'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}},
            operation_name='PutItem'
        )
        
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=3,
            high_risk_count=1,
            medium_risk_count=1,
            low_risk_count=1,
            product_risks=[],
            execution_time_seconds=5.5
        )
        
        with pytest.raises(ClientError):
            await risk_detection_engine._store_results(risk_assessment)


class TestAnalyzeRisk:
    """Tests for complete risk analysis workflow (Requirements 2.6, 2.7)."""
    
    @pytest.mark.asyncio
    async def test_analyze_risk_complete_workflow(self, risk_detection_engine, mock_s3_client, mock_dynamodb_client, mock_sagemaker_client, sample_product_data):
        """Test complete risk analysis workflow."""
        # Mock S3 data fetch
        parquet_buffer = BytesIO()
        sample_product_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        mock_s3_client.get_object.return_value = {'Body': parquet_buffer}
        
        # Mock SageMaker response
        mock_sagemaker_client.invoke_endpoint.return_value = {
            'Body': BytesIO(json.dumps({'risk_score': 0.25}).encode())
        }
        
        result = await risk_detection_engine.analyze_risk('test-upload-id')
        
        assert isinstance(result, RiskAssessment)
        assert result.upload_id == 'test-upload-id'
        assert result.total_products == 2  # P001 and P002
        assert len(result.product_risks) == 2
        assert result.execution_time_seconds > 0
    
    @pytest.mark.asyncio
    async def test_analyze_risk_counts_risk_levels(self, risk_detection_engine, mock_s3_client, mock_dynamodb_client, mock_sagemaker_client):
        """Test risk analysis correctly counts risk levels."""
        # Create data with known risk levels
        test_data = pd.DataFrame({
            'product_id': ['P001', 'P001', 'P002', 'P002', 'P003', 'P003'],
            'product_name': ['Low Risk', 'Low Risk', 'Medium Risk', 'Medium Risk', 'High Risk', 'High Risk'],
            'quantity': [100, 101, 100, 120, 100, 200]  # Different volatilities
        })
        
        parquet_buffer = BytesIO()
        test_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        mock_s3_client.get_object.return_value = {'Body': parquet_buffer}
        
        # Mock SageMaker to return None (use CV directly)
        mock_sagemaker_client.invoke_endpoint.return_value = {
            'Body': BytesIO(json.dumps({}).encode())
        }
        
        result = await risk_detection_engine.analyze_risk('test-upload-id')
        
        assert result.total_products == 3
        assert result.high_risk_count + result.medium_risk_count + result.low_risk_count == 3
    
    @pytest.mark.asyncio
    async def test_analyze_risk_stores_results(self, risk_detection_engine, mock_s3_client, mock_dynamodb_client, mock_sagemaker_client, sample_product_data):
        """Test risk analysis stores results in DynamoDB."""
        parquet_buffer = BytesIO()
        sample_product_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        mock_s3_client.get_object.return_value = {'Body': parquet_buffer}
        
        mock_sagemaker_client.invoke_endpoint.return_value = {
            'Body': BytesIO(json.dumps({'risk_score': 0.25}).encode())
        }
        
        await risk_detection_engine.analyze_risk('test-upload-id')
        
        # Verify DynamoDB put_item was called
        mock_dynamodb_client.put_item.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_analyze_risk_execution_time_tracking(self, risk_detection_engine, mock_s3_client, mock_dynamodb_client, mock_sagemaker_client, sample_product_data):
        """Test risk analysis tracks execution time (Requirement 11.4)."""
        parquet_buffer = BytesIO()
        sample_product_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        mock_s3_client.get_object.return_value = {'Body': parquet_buffer}
        
        mock_sagemaker_client.invoke_endpoint.return_value = {
            'Body': BytesIO(json.dumps({'risk_score': 0.25}).encode())
        }
        
        result = await risk_detection_engine.analyze_risk('test-upload-id')
        
        assert result.execution_time_seconds > 0
        assert isinstance(result.execution_time_seconds, float)


class TestLogMetrics:
    """Tests for CloudWatch metrics logging (Requirement 11.4)."""
    
    def test_log_metrics_called(self, risk_detection_engine):
        """Test metrics logging is called with risk assessment."""
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=10,
            high_risk_count=2,
            medium_risk_count=3,
            low_risk_count=5,
            product_risks=[],
            execution_time_seconds=15.5
        )
        
        # Should not raise any exceptions
        risk_detection_engine._log_metrics(risk_assessment)
    
    def test_log_metrics_includes_execution_time(self, risk_detection_engine):
        """Test metrics logging includes execution time."""
        risk_assessment = RiskAssessment(
            assessment_id='test-assessment-id',
            upload_id='test-upload-id',
            timestamp=datetime.utcnow(),
            total_products=10,
            high_risk_count=2,
            medium_risk_count=3,
            low_risk_count=5,
            product_risks=[],
            execution_time_seconds=15.5
        )
        
        with patch('app.engines.risk_detection.logger') as mock_logger:
            risk_detection_engine._log_metrics(risk_assessment)
            
            # Verify logger.info was called
            mock_logger.info.assert_called_once()
            call_kwargs = mock_logger.info.call_args[1]
            
            # Verify execution time is in extra data
            assert 'extra' in call_kwargs
            assert 'metric_value' in call_kwargs['extra']
            assert call_kwargs['extra']['metric_value'] == 15.5
