"""
Unit tests for simulation Lambda function.

Tests verify Lambda handler, Monte Carlo simulation, scenario-specific logic,
timeout handling, and error handling for the simulation Lambda function.

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 15.5
"""

import json
import pytest
from unittest.mock import MagicMock, patch
import numpy as np

from lambda_functions.simulation_lambda import (
    lambda_handler,
    monte_carlo_simulation,
    simulate_stockout,
    simulate_overstock,
    simulate_seasonal_mismatch,
    simulate_pricing_failure,
    simulate_fulfillment_failure,
    calculate_probability_of_occurrence,
    build_inventory_impact
)


@pytest.fixture
def mock_context():
    """Create a mock Lambda context."""
    context = MagicMock()
    context.get_remaining_time_in_millis = MagicMock(return_value=900000)  # 15 minutes
    return context


@pytest.fixture
def valid_event():
    """Create a valid Lambda event."""
    return {
        "scenario_type": "stockout",
        "scenario_id": "test-scenario-123",
        "parameters": {
            "time_horizon_days": 30,
            "monte_carlo_iterations": 100,
            "confidence_level": 0.95,
            "initial_inventory": 1000,
            "daily_demand_mean": 50.0,
            "daily_demand_std": 15.0
        }
    }


class TestLambdaHandler:
    """Test suite for lambda_handler function."""
    
    def test_successful_stockout_simulation(self, valid_event, mock_context):
        """Test successful execution of stockout simulation.
        
        **Validates: Requirements 3.1, 3.6, 3.7**
        """
        response = lambda_handler(valid_event, mock_context)
        
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        
        assert body["scenario_id"] == "test-scenario-123"
        assert body["scenario_type"] == "stockout"
        assert "timestamp" in body
        assert "revenue_impact" in body
        assert "inventory_impact" in body
        assert body["timeline_days"] == 30
        assert 0.0 <= body["probability_of_occurrence"] <= 1.0
        assert body["execution_time_seconds"] >= 0
    
    def test_missing_scenario_type(self, mock_context):
        """Test error handling when scenario_type is missing.
        
        **Validates: Requirements 3.7, 15.5**
        """
        event = {"scenario_id": "test-123", "parameters": {}}
        response = lambda_handler(event, mock_context)
        
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
    
    def test_missing_scenario_id(self, mock_context):
        """Test error handling when scenario_id is missing.
        
        **Validates: Requirements 3.7, 15.5**
        """
        event = {"scenario_type": "stockout", "parameters": {}}
        response = lambda_handler(event, mock_context)
        
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
    
    def test_invalid_time_horizon(self, valid_event, mock_context):
        """Test validation of time_horizon_days parameter.
        
        **Validates: Requirements 3.6, 15.5**
        """
        valid_event["parameters"]["time_horizon_days"] = 500  # Exceeds 365 limit
        response = lambda_handler(valid_event, mock_context)
        
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
    
    def test_invalid_monte_carlo_iterations(self, valid_event, mock_context):
        """Test validation of monte_carlo_iterations parameter.
        
        **Validates: Requirements 3.6, 15.5**
        """
        valid_event["parameters"]["monte_carlo_iterations"] = 50  # Below 100 minimum
        response = lambda_handler(valid_event, mock_context)
        
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
    
    def test_default_parameters(self, mock_context):
        """Test that default parameters are applied when not provided.
        
        **Validates: Requirements 3.6**
        """
        event = {
            "scenario_type": "overstock",
            "scenario_id": "test-456",
            "parameters": {}
        }
        response = lambda_handler(event, mock_context)
        
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["timeline_days"] == 90  # Default value


class TestMonteCarloSimulation:
    """Test suite for monte_carlo_simulation function."""
    
    def test_stockout_simulation_execution(self, mock_context):
        """Test Monte Carlo simulation for stockout scenario.
        
        **Validates: Requirements 3.1, 3.6, 3.7**
        """
        result = monte_carlo_simulation(
            scenario_type="stockout",
            time_horizon_days=30,
            iterations=100,
            confidence_level=0.95,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0,
            start_time=0,
            context=mock_context
        )
        
        assert "revenue_impact" in result
        assert "inventory_impact" in result
        assert "probability_of_occurrence" in result
        
        revenue = result["revenue_impact"]
        assert revenue["expected_loss"] >= 0
        assert revenue["confidence_interval_lower"] >= 0
        assert revenue["confidence_interval_upper"] >= revenue["expected_loss"]
        assert revenue["currency"] == "USD"
    
    def test_overstock_simulation_execution(self, mock_context):
        """Test Monte Carlo simulation for overstock scenario.
        
        **Validates: Requirements 3.2, 3.6, 3.7**
        """
        result = monte_carlo_simulation(
            scenario_type="overstock",
            time_horizon_days=30,
            iterations=100,
            confidence_level=0.95,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0,
            start_time=0,
            context=mock_context
        )
        
        assert result["inventory_impact"]["units_affected"] > 0
        assert "holding_cost" in result["inventory_impact"]
        assert "markdown_required" in result["inventory_impact"]
    
    def test_confidence_interval_calculation(self, mock_context):
        """Test confidence interval calculation at different levels.
        
        **Validates: Requirements 3.7**
        """
        result_95 = monte_carlo_simulation(
            scenario_type="stockout",
            time_horizon_days=30,
            iterations=100,
            confidence_level=0.95,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0,
            start_time=0,
            context=mock_context
        )
        
        result_90 = monte_carlo_simulation(
            scenario_type="stockout",
            time_horizon_days=30,
            iterations=100,
            confidence_level=0.90,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0,
            start_time=0,
            context=mock_context
        )
        
        # 95% CI should be wider than 90% CI
        width_95 = result_95["revenue_impact"]["confidence_interval_upper"] - result_95["revenue_impact"]["confidence_interval_lower"]
        width_90 = result_90["revenue_impact"]["confidence_interval_upper"] - result_90["revenue_impact"]["confidence_interval_lower"]
        
        assert width_95 >= width_90
    
    def test_unknown_scenario_type(self, mock_context):
        """Test error handling for unknown scenario type.
        
        **Validates: Requirements 15.5**
        """
        with pytest.raises(ValueError, match="Unknown scenario type"):
            monte_carlo_simulation(
                scenario_type="invalid_scenario",
                time_horizon_days=30,
                iterations=100,
                confidence_level=0.95,
                initial_inventory=1000,
                daily_demand_mean=50.0,
                daily_demand_std=15.0,
                start_time=0,
                context=mock_context
            )


class TestStockoutSimulation:
    """Test suite for simulate_stockout function."""
    
    def test_stockout_basic_execution(self):
        """Test basic stockout simulation execution.
        
        **Validates: Requirements 3.1**
        """
        revenue_loss, units_affected = simulate_stockout(
            time_horizon_days=30,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        assert revenue_loss >= 0
        assert units_affected >= 0
        assert isinstance(revenue_loss, float)
        assert isinstance(units_affected, int)
    
    def test_stockout_with_high_demand(self):
        """Test stockout with high demand scenario.
        
        **Validates: Requirements 3.1**
        """
        revenue_loss, units_affected = simulate_stockout(
            time_horizon_days=30,
            initial_inventory=500,
            daily_demand_mean=100.0,  # High demand
            daily_demand_std=20.0
        )
        
        # High demand should result in significant stockout
        assert revenue_loss > 0
        assert units_affected > 0


class TestOverstockSimulation:
    """Test suite for simulate_overstock function."""
    
    def test_overstock_basic_execution(self):
        """Test basic overstock simulation execution.
        
        **Validates: Requirements 3.2**
        """
        revenue_loss, units_affected = simulate_overstock(
            time_horizon_days=30,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        assert revenue_loss >= 0
        assert units_affected > 0  # Should have excess inventory
        assert isinstance(revenue_loss, float)
        assert isinstance(units_affected, int)
    
    def test_overstock_holding_costs(self):
        """Test that overstock incurs holding costs.
        
        **Validates: Requirements 3.2**
        """
        revenue_loss, units_affected = simulate_overstock(
            time_horizon_days=60,  # Longer period
            initial_inventory=1000,
            daily_demand_mean=20.0,  # Low demand
            daily_demand_std=5.0
        )
        
        # Longer period with low demand should result in higher costs
        assert revenue_loss > 0


class TestSeasonalMismatchSimulation:
    """Test suite for simulate_seasonal_mismatch function."""
    
    def test_seasonal_mismatch_basic_execution(self):
        """Test basic seasonal mismatch simulation execution.
        
        **Validates: Requirements 3.3**
        """
        revenue_loss, units_affected = simulate_seasonal_mismatch(
            time_horizon_days=90,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        assert revenue_loss >= 0
        assert units_affected >= 0
        assert isinstance(revenue_loss, float)
        assert isinstance(units_affected, int)
    
    def test_seasonal_demand_pattern(self):
        """Test that seasonal mismatch models demand peaks.
        
        **Validates: Requirements 3.3**
        """
        # Run multiple times to verify pattern
        losses = []
        for _ in range(5):
            revenue_loss, _ = simulate_seasonal_mismatch(
                time_horizon_days=90,
                initial_inventory=500,
                daily_demand_mean=50.0,
                daily_demand_std=10.0
            )
            losses.append(revenue_loss)
        
        # Should have some revenue loss due to timing mismatch
        assert np.mean(losses) > 0


class TestPricingFailureSimulation:
    """Test suite for simulate_pricing_failure function."""
    
    def test_pricing_failure_basic_execution(self):
        """Test basic pricing failure simulation execution.
        
        **Validates: Requirements 3.4**
        """
        revenue_loss, units_affected = simulate_pricing_failure(
            time_horizon_days=30,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        assert revenue_loss >= 0
        assert units_affected >= 0
        assert isinstance(revenue_loss, float)
        assert isinstance(units_affected, int)
    
    def test_pricing_failure_margin_erosion(self):
        """Test that pricing failure results in margin impact.
        
        **Validates: Requirements 3.4**
        """
        # Run multiple simulations
        losses = []
        for _ in range(10):
            revenue_loss, _ = simulate_pricing_failure(
                time_horizon_days=30,
                initial_inventory=1000,
                daily_demand_mean=50.0,
                daily_demand_std=15.0
            )
            losses.append(revenue_loss)
        
        # Should have revenue loss from pricing errors
        assert np.mean(losses) > 0


class TestFulfillmentFailureSimulation:
    """Test suite for simulate_fulfillment_failure function."""
    
    def test_fulfillment_failure_basic_execution(self):
        """Test basic fulfillment failure simulation execution.
        
        **Validates: Requirements 3.5**
        """
        revenue_loss, units_affected = simulate_fulfillment_failure(
            time_horizon_days=30,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        assert revenue_loss >= 0
        assert units_affected >= 0
        assert isinstance(revenue_loss, float)
        assert isinstance(units_affected, int)
    
    def test_fulfillment_failure_customer_churn(self):
        """Test that fulfillment failure models customer churn.
        
        **Validates: Requirements 3.5**
        """
        revenue_loss, units_affected = simulate_fulfillment_failure(
            time_horizon_days=60,
            initial_inventory=2000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0
        )
        
        # Should have some revenue loss from churn
        assert revenue_loss > 0
        assert units_affected > 0


class TestProbabilityCalculation:
    """Test suite for calculate_probability_of_occurrence function."""
    
    def test_probability_range(self):
        """Test that probability is within valid range.
        
        **Validates: Requirements 3.7**
        """
        revenue_losses = np.array([1000, 1500, 2000, 1200, 1800])
        
        for scenario_type in ["stockout", "overstock", "seasonal_mismatch", "pricing_failure", "fulfillment_failure"]:
            prob = calculate_probability_of_occurrence(scenario_type, revenue_losses)
            assert 0.0 <= prob <= 1.0
    
    def test_probability_by_scenario_type(self):
        """Test that different scenarios have different base probabilities.
        
        **Validates: Requirements 3.7**
        """
        revenue_losses = np.array([1000, 1500, 2000, 1200, 1800])
        
        prob_stockout = calculate_probability_of_occurrence("stockout", revenue_losses)
        prob_pricing = calculate_probability_of_occurrence("pricing_failure", revenue_losses)
        
        # Different scenarios should have different probabilities
        assert prob_stockout != prob_pricing


class TestInventoryImpactBuilder:
    """Test suite for build_inventory_impact function."""
    
    def test_overstock_inventory_impact(self):
        """Test inventory impact for overstock scenario.
        
        **Validates: Requirements 3.2, 3.7**
        """
        impact = build_inventory_impact("overstock", 1000, 50.0)
        
        assert "units_affected" in impact
        assert "holding_cost" in impact
        assert "markdown_required" in impact
        assert impact["units_affected"] == 1000
        assert impact["holding_cost"] > 0
        assert 0 < impact["markdown_required"] <= 1.0
    
    def test_seasonal_mismatch_inventory_impact(self):
        """Test inventory impact for seasonal mismatch scenario.
        
        **Validates: Requirements 3.3, 3.7**
        """
        impact = build_inventory_impact("seasonal_mismatch", 500, 50.0)
        
        assert "units_affected" in impact
        assert "markdown_required" in impact
        assert impact["units_affected"] == 500
        assert impact["markdown_required"] == 0.40  # 40% for seasonal
    
    def test_stockout_inventory_impact(self):
        """Test inventory impact for stockout scenario.
        
        **Validates: Requirements 3.1, 3.7**
        """
        impact = build_inventory_impact("stockout", 200, 50.0)
        
        assert "units_affected" in impact
        assert impact["units_affected"] == 200
        # Stockout doesn't have holding_cost or markdown_required
        assert "holding_cost" not in impact
        assert "markdown_required" not in impact


class TestTimeoutHandling:
    """Test suite for timeout handling."""
    
    @patch('lambda_functions.simulation_lambda.time.time')
    def test_timeout_detection(self, mock_time, mock_context):
        """Test that simulation detects approaching timeout.
        
        **Validates: Requirements 3.8, 15.5**
        """
        # Simulate time progression that approaches timeout
        mock_time.side_effect = [0, 870, 871, 872]  # Exceeds MAX_EXECUTION_TIME
        
        # Should complete with partial results rather than timing out
        result = monte_carlo_simulation(
            scenario_type="stockout",
            time_horizon_days=30,
            iterations=1000,  # Large number
            confidence_level=0.95,
            initial_inventory=1000,
            daily_demand_mean=50.0,
            daily_demand_std=15.0,
            start_time=0,
            context=mock_context
        )
        
        # Should return results even with timeout
        assert "revenue_impact" in result
        assert "inventory_impact" in result
