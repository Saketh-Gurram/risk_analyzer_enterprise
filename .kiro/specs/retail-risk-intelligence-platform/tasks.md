# Implementation Plan: Retail Risk Intelligence Platform

## Overview

This implementation plan breaks down the AWS-Native AI Bharat Retail Risk Intelligence Platform into discrete, testable coding tasks. The platform combines FastAPI, AWS services (S3, DynamoDB, SageMaker, Bedrock, Lambda), and AI-powered reasoning to deliver failure-first intelligence for retail operations. Tasks are organized to build incrementally, with early validation through testing and checkpoints.

## Tasks

- [x] 1. Project setup and core infrastructure
  - Create project directory structure (ai_bharat/, app/, lambda_functions/, sagemaker/, infrastructure/)
  - Initialize requirements.txt with core dependencies (fastapi, uvicorn, boto3, pydantic, pandas, networkx, python-dotenv)
  - Create .env.example file with required environment variables (AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME, SAGEMAKER_ENDPOINT_NAME, BEDROCK_MODEL_ID)
  - Create app/config.py with environment variable loading and validation using Pydantic BaseSettings
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.6_

- [ ] 2. Define Pydantic data models
  - [x] 2.1 Create app/models/enums.py with RiskLevel, ScenarioType, Complexity enums
    - Define RiskLevel (LOW, MEDIUM, HIGH)
    - Define ScenarioType (STOCKOUT, OVERSTOCK, SEASONAL_MISMATCH, PRICING_FAILURE, FULFILLMENT_FAILURE)
    - Define Complexity (LOW, MEDIUM, HIGH)
    - _Requirements: 2.3, 2.4, 2.5, 3.1, 3.2, 3.3, 3.4, 3.5, 6.3_


  - [x] 2.2 Create app/models/upload.py with UploadResponse and ValidationResult models
    - Define UploadResponse with upload_id, status, timestamp, file_size_bytes
    - Define ValidationResult with is_valid, errors, warnings
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 13.1, 13.2_

  - [x] 2.3 Create app/models/risk.py with ProductRisk and RiskAssessment models
    - Define ProductRisk with product_id, product_name, coefficient_of_variation, risk_level, rolling_avg_deviation
    - Define RiskAssessment with assessment_id, upload_id, timestamp, product counts by risk level, product_risks list, execution_time_seconds
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [x] 2.4 Create app/models/simulation.py with simulation-related models
    - Define SimulationParameters with time_horizon_days, monte_carlo_iterations, confidence_level, optional inventory/demand fields
    - Define SimulationRequest with scenario_type, parameters, upload_id
    - Define RevenueImpact with expected_loss, confidence intervals, currency
    - Define InventoryImpact with units_affected, holding_cost, markdown_required
    - Define SimulationResult with scenario_id, scenario_type, timestamp, impacts, timeline, probability, execution_time
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.9_

  - [x] 2.5 Create app/models/propagation.py with PropagationScore and PropagationResponse models
    - Define PropagationScore with domain, impact_score (0-10), propagation_order, affected_by list
    - Define PropagationResponse with scenario_id, scores list, total_organizational_impact
    - _Requirements: 4.1, 4.2, 4.4, 4.8_

  - [x] 2.6 Create app/models/executive.py with ExecutiveSummary model
    - Define ExecutiveSummary with scenario_id, timestamp, revenue_risk_quantification, market_context, urgency_level, recommended_actions, trade_offs_analysis, generated_by
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.8, 5.9_

  - [x] 2.7 Create app/models/mitigation.py with MitigationStrategy and MitigationResponse models
    - Define MitigationStrategy with strategy_id, title, description, effectiveness_score, complexity, timeline_days, cost range, prerequisites, risks
    - Define MitigationResponse with scenario_id, strategies list, timestamp
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [ ] 3. Implement AWS service clients and utilities
  - [x] 3.1 Create app/utils/aws_clients.py with boto3 client factory functions
    - Implement get_s3_client() with IAM role authentication
    - Implement get_dynamodb_client() with IAM role authentication
    - Implement get_sagemaker_runtime_client() with IAM role authentication
    - Implement get_bedrock_runtime_client() with IAM role authentication
    - Implement get_lambda_client() with IAM role authentication
    - Add exponential backoff retry configuration (max 3 attempts)
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8_

  - [x] 3.2 Create app/utils/logging.py with structured logging setup
    - Configure JSON logging format with timestamp, level, message, correlation_id
    - Implement get_logger() function that returns configured logger
    - Add CloudWatch log handler configuration
    - _Requirements: 11.1, 11.2, 11.3, 11.7, 11.8_

  - [x] 3.3 Create app/utils/validators.py with data validation functions
    - Implement validate_csv_schema() for CSV file validation
    - Implement validate_json_schema() for JSON file validation
    - Implement validate_required_fields() for field presence checks
    - Implement validate_data_types() for type validation
    - Implement validate_date_format() for ISO 8601 date validation
    - Implement validate_numeric_fields() for non-negative value checks
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.6, 13.7_

- [x] 4. Checkpoint - Verify project structure and models
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement Data Ingestion Service
  - [x] 5.1 Create app/services/data_ingestion.py with DataIngestionService class
    - Implement __init__ with dependency injection for S3 and DynamoDB clients
    - Implement async upload_file() method with file type parameter
    - Implement async validate_schema() method using validators from utils
    - Implement file size validation (max 100MB)
    - Implement S3 upload with prefix structure: raw/{upload_id}/{filename}
    - Implement DynamoDB metadata creation with upload_id as partition key
    - Add error handling with descriptive messages
    - Add execution time tracking
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 8.1, 12.1, 12.2_

  - [x] 5.2 Write unit tests for DataIngestionService
    - Test successful CSV upload
    - Test successful JSON upload
    - Test schema validation failure
    - Test file size limit enforcement
    - Test S3 upload error handling
    - Test DynamoDB metadata creation
    - _Requirements: 1.1, 1.2, 1.3, 1.5_

- [ ] 6. Implement Risk Detection Engine
  - [x] 6.1 Create app/engines/risk_detection.py with RiskDetectionEngine class
    - Implement __init__ with dependency injection for S3, DynamoDB, SageMaker clients
    - Implement async analyze_risk() method that fetches data from S3
    - Implement calculate_coefficient_of_variation() for per-product CV calculation
    - Implement calculate_rolling_average_deviation() with 30-day window
    - Implement classify_risk_level() with thresholds (low <0.15, medium 0.15-0.3, high >0.3)
    - Implement async invoke_sagemaker_endpoint() for ML inference
    - Implement DynamoDB result storage with scenario_id partition key
    - Add execution time tracking and CloudWatch metrics
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 8.4, 11.4_

  - [x] 6.2 Write unit tests for RiskDetectionEngine
    - Test coefficient of variation calculation
    - Test rolling average deviation calculation
    - Test risk level classification (low, medium, high)
    - Test SageMaker endpoint invocation
    - Test DynamoDB result storage
    - Test error handling for SageMaker unavailability
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 15.3_

- [ ] 7. Implement Failure Simulation Engine
  - [x] 7.1 Create lambda_functions/simulation_lambda.py with Lambda handler
    - Implement lambda_handler() function with event parsing
    - Implement monte_carlo_simulation() for configurable iterations
    - Implement simulate_stockout() scenario logic
    - Implement simulate_overstock() scenario logic
    - Implement simulate_seasonal_mismatch() scenario logic
    - Implement simulate_pricing_failure() scenario logic
    - Implement simulate_fulfillment_failure() scenario logic
    - Calculate revenue impact with confidence intervals
    - Calculate inventory impact metrics
    - Add timeout handling for 15-minute Lambda limit
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 15.5_

  - [x] 7.2 Create app/engines/failure_simulation.py with FailureSimulationEngine class
    - Implement __init__ with dependency injection for Lambda, S3, DynamoDB clients
    - Implement async simulate_scenario() method that invokes Lambda
    - Implement async invoke_lambda() with payload construction
    - Implement S3 result storage: simulations/{scenario_id}/results.json
    - Implement DynamoDB metadata storage with scenario_id partition key
    - Add execution time tracking and CloudWatch metrics
    - _Requirements: 3.6, 3.7, 3.8, 3.9, 8.3, 8.6, 11.5_

  - [x] 7.3 Write unit tests for FailureSimulationEngine
    - Test Lambda invocation with correct payload
    - Test S3 result storage
    - Test DynamoDB metadata storage
    - Test timeout error handling
    - _Requirements: 3.6, 3.7, 3.9, 15.5_

- [x] 8. Checkpoint - Verify core engines
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement Impact Propagation Engine
  - [x] 9.1 Create app/engines/impact_propagation.py with ImpactPropagationEngine class
    - Implement __init__ with dependency injection for DynamoDB client
    - Implement build_domain_graph() using NetworkX DiGraph
    - Add nodes for business domains (inventory, pricing, revenue, fulfillment, customer_satisfaction)
    - Add weighted edges for domain relationships (0.0-1.0 weights)
    - Implement async calculate_propagation() for first-order effects
    - Implement async calculate_propagation() for second-order effects
    - Implement normalize_score() to convert to 0-10 scale
    - Implement DynamoDB result storage with scenario_id partition key
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 8.7_

  - [ ] 9.2 Write unit tests for ImpactPropagationEngine
    - Test domain graph construction
    - Test first-order propagation calculation
    - Test second-order propagation calculation
    - Test score normalization (0-10 scale)
    - Test inventory → pricing → revenue propagation path
    - Test fulfillment → customer satisfaction propagation path
    - _Requirements: 4.1, 4.2, 4.4, 4.5, 4.6, 4.7_

- [ ] 10. Implement AI Reasoning Engine
  - [~] 10.1 Create app/engines/ai_reasoning.py with AIReasoningEngine class
    - Implement __init__ with dependency injection for Bedrock and DynamoDB clients
    - Implement async generate_executive_summary() method
    - Implement construct_prompt() with simulation results and propagation scores
    - Implement async invoke_bedrock() with Claude 3.5 Sonnet model (temperature=0.7)
    - Implement parse_bedrock_response() to extract JSON structure
    - Implement fallback_summary() with rule-based logic for Bedrock unavailability
    - Include revenue risk quantification, market context, urgency level, recommended actions, trade-offs analysis
    - Implement DynamoDB result storage with scenario_id partition key
    - Add execution time tracking and CloudWatch metrics
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 8.8, 11.6, 15.4_

  - [~] 10.2 Write unit tests for AIReasoningEngine
    - Test Bedrock invocation with correct prompt structure
    - Test JSON response parsing
    - Test fallback logic when Bedrock unavailable
    - Test executive summary structure validation
    - Test DynamoDB result storage
    - _Requirements: 5.1, 5.7, 5.9, 15.4_

- [ ] 11. Implement Mitigation Strategy Engine
  - [~] 11.1 Create app/engines/mitigation_strategy.py with MitigationStrategyEngine class
    - Implement __init__ with dependency injection for DynamoDB client
    - Implement async generate_strategies() method
    - Implement scenario-specific strategy generation for each ScenarioType
    - Assign effectiveness scores (0-100 scale)
    - Assess complexity (low/medium/high)
    - Estimate timeline in days
    - Estimate cost range (min/max)
    - Implement rank_strategies() by effectiveness score descending
    - Implement DynamoDB result storage with scenario_id partition key
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.9_

  - [~] 11.2 Write unit tests for MitigationStrategyEngine
    - Test strategy generation for each scenario type
    - Test effectiveness score assignment
    - Test complexity assessment
    - Test timeline estimation
    - Test cost range estimation
    - Test ranking by effectiveness score
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [~] 12. Checkpoint - Verify all engines
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 13. Implement service layer orchestration
  - [~] 13.1 Create app/services/risk_service.py with RiskService class
    - Implement __init__ with dependency injection for RiskDetectionEngine
    - Implement async analyze_risk() orchestration method
    - Add error handling and logging
    - _Requirements: 2.6, 2.7, 12.1, 12.2_

  - [~] 13.2 Create app/services/simulation_service.py with SimulationService class
    - Implement __init__ with dependency injection for FailureSimulationEngine, ImpactPropagationEngine, AIReasoningEngine, MitigationStrategyEngine
    - Implement async run_full_simulation() orchestration method that chains all engines
    - Add error handling and logging
    - _Requirements: 3.7, 4.8, 5.8, 6.7, 12.1, 12.2_

- [ ] 14. Implement FastAPI routes
  - [~] 14.1 Create app/routes/upload.py with upload endpoint
    - Implement POST /upload route handler
    - Add multipart/form-data file handling
    - Add Pydantic request validation
    - Inject DataIngestionService dependency
    - Return UploadResponse with 200 status
    - Add error handling (400 for validation errors)
    - _Requirements: 7.1, 7.7, 7.9, 7.10, 12.1_

  - [~] 14.2 Create app/routes/risk.py with risk analysis endpoint
    - Implement POST /risk/analyze route handler
    - Add Pydantic request validation
    - Inject RiskService dependency
    - Return RiskAssessment with 200 status
    - Add error handling (404 for missing upload_id)
    - _Requirements: 7.2, 7.7, 7.8, 7.9, 7.10, 12.1_

  - [~] 14.3 Create app/routes/simulation.py with simulation endpoint
    - Implement POST /simulate route handler
    - Add Pydantic request validation
    - Inject SimulationService dependency
    - Return SimulationResponse with 200 status
    - Add error handling (400 for invalid parameters)
    - _Requirements: 7.3, 7.7, 7.9, 7.10, 12.1_

  - [~] 14.4 Create app/routes/results.py with result retrieval endpoints
    - Implement GET /propagation/{scenario_id} route handler
    - Implement GET /executive-summary/{scenario_id} route handler
    - Implement GET /mitigation/{scenario_id} route handler
    - Add DynamoDB query logic for each endpoint
    - Return appropriate response models with 200 status
    - Add error handling (404 for missing scenario_id)
    - _Requirements: 7.4, 7.5, 7.6, 7.8, 7.9, 12.1_

  - [~] 14.5 Create app/routes/health.py with health check endpoint
    - Implement GET /health route handler
    - Return HealthResponse with status and timestamp
    - Ensure response time under 500ms
    - _Requirements: 14.1_

- [ ] 15. Implement main FastAPI application
  - [~] 15.1 Create app/main.py with FastAPI application setup
    - Initialize FastAPI app with title, description, version
    - Include all route modules (upload, risk, simulation, results, health)
    - Add startup event handler for configuration validation
    - Add shutdown event handler for cleanup
    - Configure CORS middleware if needed
    - Add request logging middleware
    - Add error handling middleware
    - _Requirements: 9.5, 11.1, 12.1, 12.4, 15.7_

  - [~] 15.2 Write integration tests for API endpoints
    - Test POST /upload with valid CSV file
    - Test POST /upload with valid JSON file
    - Test POST /risk/analyze with valid upload_id
    - Test POST /simulate with valid parameters
    - Test GET /propagation/{scenario_id}
    - Test GET /executive-summary/{scenario_id}
    - Test GET /mitigation/{scenario_id}
    - Test GET /health
    - Test error responses (400, 404, 429, 500)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 14.1_

- [~] 16. Checkpoint - Verify API layer
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 17. Create SageMaker model code
  - [~] 17.1 Create sagemaker/risk_model.py with ML model training script
    - Implement data preprocessing for coefficient of variation features
    - Implement model training logic (e.g., XGBoost or scikit-learn)
    - Implement model serialization for SageMaker deployment
    - Add hyperparameter configuration
    - _Requirements: 2.8, 10.3_

  - [~] 17.2 Create sagemaker/inference.py with SageMaker inference handler
    - Implement model_fn() to load trained model
    - Implement input_fn() to parse inference requests
    - Implement predict_fn() to generate risk scores
    - Implement output_fn() to format inference responses
    - _Requirements: 2.8, 10.3_

- [ ] 18. Create infrastructure documentation
  - [~] 18.1 Create infrastructure/iam_roles.md with IAM role definitions
    - Document FastAPI application IAM role with S3, DynamoDB, SageMaker, Bedrock, Lambda permissions
    - Document Lambda execution role with S3, DynamoDB permissions
    - Document SageMaker execution role with S3 permissions
    - Include trust relationships and policy documents
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [~] 18.2 Create infrastructure/deployment_guide.md with deployment instructions
    - Document AWS resource provisioning (S3 bucket, DynamoDB table, SageMaker endpoint, Lambda function)
    - Document environment variable configuration
    - Document FastAPI application deployment (EC2, ECS, or Lambda)
    - Document SageMaker model deployment steps
    - Document Lambda function deployment steps
    - Include CloudWatch logging setup
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 9.1, 9.2, 11.1, 11.2_

- [ ] 19. Create project documentation
  - [~] 19.1 Create README.md with project overview and setup instructions
    - Document project purpose and architecture
    - Document installation steps (pip install -r requirements.txt)
    - Document environment variable configuration
    - Document local development setup
    - Document API endpoint usage examples
    - Document testing instructions
    - _Requirements: 9.1, 9.2, 12.4_

  - [~] 19.2 Update requirements.txt with all dependencies
    - Add fastapi with version constraint
    - Add uvicorn with version constraint
    - Add boto3 with version constraint
    - Add pydantic with version constraint
    - Add pandas with version constraint
    - Add networkx with version constraint
    - Add python-dotenv with version constraint
    - Add pytest and pytest-asyncio for testing
    - _Requirements: 10.6, 12.4_

- [~] 20. Final checkpoint - End-to-end validation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation at key milestones
- All AWS service interactions use boto3 with IAM role authentication
- All data models use Pydantic for validation
- All engines implement dependency injection for testability
- Configuration is environment-based for multi-environment deployment
