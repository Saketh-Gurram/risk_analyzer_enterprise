"""
Failure Simulation Engine for orchestrating Lambda-based Monte Carlo simulations.

This engine coordinates the execution of retail failure scenario simulations by:
- Invoking AWS Lambda functions with simulation parameters
- Storing simulation results in S3
- Storing metadata in DynamoDB
- Tracking execution time and CloudWatch metrics

Requirements: 3.6, 3.7, 3.8, 3.9, 8.3, 8.6, 11.5
"""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.models.simulation import SimulationRequest, SimulationResult
from app.utils.logging import get_logger

logger = get_logger(__name__)


class FailureSimulationEngine:
    """
    Engine for orchestrating Lambda-based failure scenario simulations.
    
    This engine acts as the coordinator between the FastAPI application and
    the Lambda function that executes Monte Carlo simulations. It handles:
    - Lambda invocation with proper payload construction
    - S3 storage of detailed simulation results
    - DynamoDB metadata storage for quick retrieval
    - Execution time tracking and CloudWatch metrics
    
    Attributes:
        lambda_client: Boto3 Lambda client for function invocation
        s3_client: Boto3 S3 client for result storage
        dynamodb_client: Boto3 DynamoDB client for metadata storage
        lambda_function_name: Name of the Lambda function to invoke
        s3_bucket_name: S3 bucket for storing simulation results
        dynamodb_table_name: DynamoDB table for storing metadata
    """
    
    def __init__(
        self,
        lambda_client: Any,
        s3_client: Any,
        dynamodb_client: Any,
        lambda_function_name: str,
        s3_bucket_name: str,
        dynamodb_table_name: str
    ):
        """
        Initialize the FailureSimulationEngine with AWS service clients.
        
        Args:
            lambda_client: Configured boto3 Lambda client
            s3_client: Configured boto3 S3 client
            dynamodb_client: Configured boto3 DynamoDB client
            lambda_function_name: Name of the Lambda function for simulations
            s3_bucket_name: S3 bucket name for result storage
            dynamodb_table_name: DynamoDB table name for metadata storage
        """
        self.lambda_client = lambda_client
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        self.lambda_function_name = lambda_function_name
        self.s3_bucket_name = s3_bucket_name
        self.dynamodb_table_name = dynamodb_table_name
        
        logger.info(
            "FailureSimulationEngine initialized",
            extra={
                "extra_fields": {
                    "lambda_function": lambda_function_name,
                    "s3_bucket": s3_bucket_name,
                    "dynamodb_table": dynamodb_table_name
                }
            }
        )

    async def simulate_scenario(
        self,
        simulation_request: SimulationRequest
    ) -> SimulationResult:
        """
        Execute a failure scenario simulation by invoking Lambda function.
        
        This method orchestrates the complete simulation workflow:
        1. Generate unique scenario_id
        2. Construct Lambda payload from simulation request
        3. Invoke Lambda function and wait for results
        4. Parse simulation results
        5. Store results in S3
        6. Store metadata in DynamoDB
        7. Track execution time and log metrics
        
        Args:
            simulation_request: Simulation request with scenario type and parameters
            
        Returns:
            SimulationResult: Complete simulation results with revenue and inventory impacts
            
        Raises:
            Exception: If Lambda invocation fails or result parsing fails
            
        Requirements: 3.6, 3.7, 3.9, 11.5
        """
        start_time = time.time()
        scenario_id = str(uuid.uuid4())
        
        logger.info(
            f"Starting simulation for scenario type: {simulation_request.scenario_type}",
            extra={
                "extra_fields": {
                    "scenario_id": scenario_id,
                    "scenario_type": simulation_request.scenario_type,
                    "upload_id": simulation_request.upload_id,
                    "time_horizon_days": simulation_request.parameters.time_horizon_days
                }
            }
        )
        
        try:
            # Invoke Lambda function with simulation parameters
            lambda_response = await self.invoke_lambda(
                scenario_id=scenario_id,
                simulation_request=simulation_request
            )
            
            # Parse Lambda response
            simulation_result = self._parse_lambda_response(
                lambda_response,
                scenario_id
            )
            
            # Store results in S3
            await self._store_results_in_s3(
                scenario_id=scenario_id,
                simulation_result=simulation_result,
                lambda_response=lambda_response
            )
            
            # Store metadata in DynamoDB
            await self._store_metadata_in_dynamodb(
                scenario_id=scenario_id,
                simulation_result=simulation_result,
                upload_id=simulation_request.upload_id
            )
            
            # Calculate total execution time
            execution_time = time.time() - start_time
            
            # Log execution metrics to CloudWatch
            logger.info(
                f"Simulation completed successfully for scenario {scenario_id}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "scenario_type": simulation_request.scenario_type,
                        "execution_time_seconds": round(execution_time, 2),
                        "lambda_execution_time_seconds": simulation_result.execution_time_seconds,
                        "expected_revenue_loss": simulation_result.revenue_impact.expected_loss
                    }
                }
            )
            
            return simulation_result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Simulation failed for scenario {scenario_id}: {str(e)}",
                exc_info=True,
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "scenario_type": simulation_request.scenario_type,
                        "execution_time_seconds": round(execution_time, 2),
                        "error": str(e)
                    }
                }
            )
            raise

    async def invoke_lambda(
        self,
        scenario_id: str,
        simulation_request: SimulationRequest
    ) -> Dict[str, Any]:
        """
        Invoke Lambda function with constructed payload.
        
        Constructs the Lambda payload from the simulation request and invokes
        the Lambda function synchronously (RequestResponse invocation type).
        
        Args:
            scenario_id: Unique identifier for this simulation scenario
            simulation_request: Simulation request with parameters
            
        Returns:
            Dict containing the Lambda response payload
            
        Raises:
            Exception: If Lambda invocation fails or returns an error
            
        Requirements: 3.6, 3.8
        """
        # Construct Lambda payload
        payload = {
            "scenario_id": scenario_id,
            "scenario_type": simulation_request.scenario_type.value,
            "parameters": {
                "time_horizon_days": simulation_request.parameters.time_horizon_days,
                "monte_carlo_iterations": simulation_request.parameters.monte_carlo_iterations,
                "confidence_level": simulation_request.parameters.confidence_level,
            }
        }
        
        # Add optional parameters if provided
        if simulation_request.parameters.initial_inventory is not None:
            payload["parameters"]["initial_inventory"] = simulation_request.parameters.initial_inventory
        
        if simulation_request.parameters.daily_demand_mean is not None:
            payload["parameters"]["daily_demand_mean"] = simulation_request.parameters.daily_demand_mean
        
        if simulation_request.parameters.daily_demand_std is not None:
            payload["parameters"]["daily_demand_std"] = simulation_request.parameters.daily_demand_std
        
        logger.info(
            f"Invoking Lambda function: {self.lambda_function_name}",
            extra={
                "extra_fields": {
                    "scenario_id": scenario_id,
                    "lambda_function": self.lambda_function_name,
                    "payload_size_bytes": len(json.dumps(payload))
                }
            }
        )
        
        try:
            # Invoke Lambda function synchronously
            response = self.lambda_client.invoke(
                FunctionName=self.lambda_function_name,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(payload)
            )
            
            # Read response payload
            response_payload = json.loads(response["Payload"].read())
            
            # Check for Lambda function errors
            if response.get("FunctionError"):
                error_message = response_payload.get("errorMessage", "Unknown Lambda error")
                logger.error(
                    f"Lambda function error: {error_message}",
                    extra={
                        "extra_fields": {
                            "scenario_id": scenario_id,
                            "function_error": response.get("FunctionError"),
                            "error_message": error_message
                        }
                    }
                )
                raise Exception(f"Lambda function error: {error_message}")
            
            # Check for application-level errors (statusCode != 200)
            status_code = response_payload.get("statusCode", 500)
            if status_code != 200:
                error_body = json.loads(response_payload.get("body", "{}"))
                error_message = error_body.get("error", "Unknown error")
                logger.error(
                    f"Simulation error: {error_message}",
                    extra={
                        "extra_fields": {
                            "scenario_id": scenario_id,
                            "status_code": status_code,
                            "error": error_message
                        }
                    }
                )
                raise Exception(f"Simulation failed: {error_message}")
            
            logger.info(
                f"Lambda invocation successful for scenario {scenario_id}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "status_code": status_code
                    }
                }
            )
            
            return response_payload
            
        except Exception as e:
            logger.error(
                f"Failed to invoke Lambda function: {str(e)}",
                exc_info=True,
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "lambda_function": self.lambda_function_name,
                        "error": str(e)
                    }
                }
            )
            raise

    def _parse_lambda_response(
        self,
        lambda_response: Dict[str, Any],
        scenario_id: str
    ) -> SimulationResult:
        """
        Parse Lambda response into SimulationResult model.
        
        Args:
            lambda_response: Raw Lambda response payload
            scenario_id: Scenario identifier for validation
            
        Returns:
            SimulationResult: Parsed and validated simulation result
            
        Raises:
            ValueError: If response parsing fails or validation fails
        """
        try:
            # Extract body from Lambda response
            body = json.loads(lambda_response["body"])
            
            # Validate scenario_id matches
            if body.get("scenario_id") != scenario_id:
                raise ValueError(
                    f"Scenario ID mismatch: expected {scenario_id}, got {body.get('scenario_id')}"
                )
            
            # Parse into SimulationResult model (Pydantic will validate)
            simulation_result = SimulationResult(**body)
            
            return simulation_result
            
        except Exception as e:
            logger.error(
                f"Failed to parse Lambda response: {str(e)}",
                exc_info=True,
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "error": str(e)
                    }
                }
            )
            raise ValueError(f"Failed to parse simulation result: {str(e)}")

    async def _store_results_in_s3(
        self,
        scenario_id: str,
        simulation_result: SimulationResult,
        lambda_response: Dict[str, Any]
    ) -> None:
        """
        Store simulation results in S3 with organized prefix structure.
        
        Stores results at: simulations/{scenario_id}/results.json
        
        Args:
            scenario_id: Unique scenario identifier
            simulation_result: Parsed simulation result
            lambda_response: Raw Lambda response for complete record
            
        Raises:
            Exception: If S3 upload fails
            
        Requirements: 3.9, 8.3
        """
        s3_key = f"simulations/{scenario_id}/results.json"
        
        # Prepare result data for storage
        result_data = {
            "scenario_id": scenario_id,
            "simulation_result": simulation_result.model_dump(mode="json"),
            "raw_lambda_response": lambda_response,
            "stored_at": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            logger.info(
                f"Storing simulation results in S3: {s3_key}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "s3_bucket": self.s3_bucket_name,
                        "s3_key": s3_key
                    }
                }
            )
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key,
                Body=json.dumps(result_data, indent=2),
                ContentType="application/json"
            )
            
            logger.info(
                f"Successfully stored results in S3 for scenario {scenario_id}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "s3_key": s3_key
                    }
                }
            )
            
        except Exception as e:
            logger.error(
                f"Failed to store results in S3: {str(e)}",
                exc_info=True,
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "s3_bucket": self.s3_bucket_name,
                        "s3_key": s3_key,
                        "error": str(e)
                    }
                }
            )
            raise

    async def _store_metadata_in_dynamodb(
        self,
        scenario_id: str,
        simulation_result: SimulationResult,
        upload_id: str
    ) -> None:
        """
        Store simulation metadata in DynamoDB with scenario_id partition key.
        
        Stores metadata for quick retrieval without reading full S3 results.
        Uses scenario_id as partition key and "simulation" as sort key.
        
        Args:
            scenario_id: Unique scenario identifier (partition key)
            simulation_result: Simulation result with metadata
            upload_id: Reference to source data upload
            
        Raises:
            Exception: If DynamoDB put operation fails
            
        Requirements: 3.9, 8.6
        """
        try:
            logger.info(
                f"Storing simulation metadata in DynamoDB for scenario {scenario_id}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "dynamodb_table": self.dynamodb_table_name
                    }
                }
            )
            
            # Prepare DynamoDB item
            item = {
                "scenario_id": {"S": scenario_id},
                "record_type": {"S": "simulation"},
                "upload_id": {"S": upload_id},
                "timestamp": {"S": simulation_result.timestamp.isoformat()},
                "scenario_type": {"S": simulation_result.scenario_type.value},
                "status": {"S": "completed"},
                "execution_time_seconds": {"N": str(simulation_result.execution_time_seconds)},
                "data": {
                    "M": {
                        "revenue_impact": {
                            "M": {
                                "expected_loss": {"N": str(simulation_result.revenue_impact.expected_loss)},
                                "confidence_interval_lower": {"N": str(simulation_result.revenue_impact.confidence_interval_lower)},
                                "confidence_interval_upper": {"N": str(simulation_result.revenue_impact.confidence_interval_upper)},
                                "currency": {"S": simulation_result.revenue_impact.currency}
                            }
                        },
                        "inventory_impact": {
                            "M": {
                                "units_affected": {"N": str(simulation_result.inventory_impact.units_affected)}
                            }
                        },
                        "timeline_days": {"N": str(simulation_result.timeline_days)},
                        "probability_of_occurrence": {"N": str(simulation_result.probability_of_occurrence)}
                    }
                }
            }
            
            # Add optional inventory impact fields if present
            if simulation_result.inventory_impact.holding_cost is not None:
                item["data"]["M"]["inventory_impact"]["M"]["holding_cost"] = {
                    "N": str(simulation_result.inventory_impact.holding_cost)
                }
            
            if simulation_result.inventory_impact.markdown_required is not None:
                item["data"]["M"]["inventory_impact"]["M"]["markdown_required"] = {
                    "N": str(simulation_result.inventory_impact.markdown_required)
                }
            
            # Put item in DynamoDB
            self.dynamodb_client.put_item(
                TableName=self.dynamodb_table_name,
                Item=item
            )
            
            logger.info(
                f"Successfully stored metadata in DynamoDB for scenario {scenario_id}",
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "record_type": "simulation"
                    }
                }
            )
            
        except Exception as e:
            logger.error(
                f"Failed to store metadata in DynamoDB: {str(e)}",
                exc_info=True,
                extra={
                    "extra_fields": {
                        "scenario_id": scenario_id,
                        "dynamodb_table": self.dynamodb_table_name,
                        "error": str(e)
                    }
                }
            )
            raise
