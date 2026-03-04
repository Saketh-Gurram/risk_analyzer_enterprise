"""AWS Lambda function for Monte Carlo simulation of retail failure scenarios.

This Lambda function executes Monte Carlo simulations for five retail failure scenarios:
- Stockout: Inventory depletion and lost sales
- Overstock: Holding cost accumulation and markdown requirements
- Seasonal Mismatch: Demand-supply timing gaps and revenue loss
- Pricing Failure: Margin erosion and competitive positioning impact
- Fulfillment Failure: Delivery delays and customer churn

The function is designed to run within AWS Lambda's 15-minute execution limit.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import numpy as np

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lambda timeout buffer (seconds) - stop processing before Lambda times out
TIMEOUT_BUFFER = 30
MAX_EXECUTION_TIME = 900 - TIMEOUT_BUFFER  # 15 minutes - buffer


def lambda_handler(event: Dict, context) -> Dict:
    """AWS Lambda handler for Monte Carlo simulation execution.
    
    Args:
        event: Lambda event containing:
            - scenario_type: Type of failure scenario (stockout, overstock, etc.)
            - parameters: Simulation parameters (time_horizon_days, monte_carlo_iterations, etc.)
            - scenario_id: Unique identifier for this simulation
        context: Lambda context object with runtime information
    
    Returns:
        Dict containing simulation results with revenue and inventory impacts
    
    Raises:
        ValueError: If required parameters are missing or invalid
        TimeoutError: If execution approaches Lambda timeout limit
    """
    start_time = time.time()
    
    try:
        # Parse and validate event
        logger.info(f"Received simulation request: {json.dumps(event)}")
        
        scenario_type = event.get("scenario_type")
        parameters = event.get("parameters", {})
        scenario_id = event.get("scenario_id")
        
        if not scenario_type or not scenario_id:
            raise ValueError("Missing required fields: scenario_type and scenario_id")
        
        # Extract simulation parameters with defaults
        time_horizon_days = parameters.get("time_horizon_days", 90)
        monte_carlo_iterations = parameters.get("monte_carlo_iterations", 1000)
        confidence_level = parameters.get("confidence_level", 0.95)
        initial_inventory = parameters.get("initial_inventory", 1000)
        daily_demand_mean = parameters.get("daily_demand_mean", 50.0)
        daily_demand_std = parameters.get("daily_demand_std", 15.0)
        
        # Validate parameters
        if not (7 <= time_horizon_days <= 365):
            raise ValueError("time_horizon_days must be between 7 and 365")
        if monte_carlo_iterations < 100:
            raise ValueError("monte_carlo_iterations must be at least 100")
        if not (0.8 <= confidence_level <= 0.99):
            raise ValueError("confidence_level must be between 0.8 and 0.99")
        
        logger.info(f"Starting {scenario_type} simulation with {monte_carlo_iterations} iterations")
        
        # Execute Monte Carlo simulation
        simulation_results = monte_carlo_simulation(
            scenario_type=scenario_type,
            time_horizon_days=time_horizon_days,
            iterations=monte_carlo_iterations,
            confidence_level=confidence_level,
            initial_inventory=initial_inventory,
            daily_demand_mean=daily_demand_mean,
            daily_demand_std=daily_demand_std,
            start_time=start_time,
            context=context
        )
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        # Build response
        response = {
            "scenario_id": scenario_id,
            "scenario_type": scenario_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "revenue_impact": simulation_results["revenue_impact"],
            "inventory_impact": simulation_results["inventory_impact"],
            "timeline_days": time_horizon_days,
            "probability_of_occurrence": simulation_results["probability_of_occurrence"],
            "execution_time_seconds": round(execution_time, 2)
        }
        
        logger.info(f"Simulation completed successfully in {execution_time:.2f} seconds")
        
        return {
            "statusCode": 200,
            "body": json.dumps(response)
        }
        
    except Exception as e:
        logger.error(f"Simulation failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "scenario_id": event.get("scenario_id"),
                "execution_time_seconds": round(time.time() - start_time, 2)
            })
        }


def monte_carlo_simulation(
    scenario_type: str,
    time_horizon_days: int,
    iterations: int,
    confidence_level: float,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float,
    start_time: float,
    context
) -> Dict:
    """Execute Monte Carlo simulation for the specified scenario.
    
    Args:
        scenario_type: Type of failure scenario to simulate
        time_horizon_days: Number of days to simulate
        iterations: Number of Monte Carlo iterations
        confidence_level: Statistical confidence level (e.g., 0.95 for 95%)
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
        start_time: Simulation start timestamp for timeout tracking
        context: Lambda context for timeout checking
    
    Returns:
        Dict containing revenue_impact, inventory_impact, and probability_of_occurrence
    """
    revenue_losses = []
    inventory_impacts = []
    
    # Calculate confidence interval bounds
    alpha = 1 - confidence_level
    lower_percentile = (alpha / 2) * 100
    upper_percentile = (1 - alpha / 2) * 100
    
    logger.info(f"Running {iterations} Monte Carlo iterations for {scenario_type}")
    
    for i in range(iterations):
        # Check for timeout every 100 iterations
        if i % 100 == 0 and i > 0:
            elapsed_time = time.time() - start_time
            if elapsed_time > MAX_EXECUTION_TIME:
                logger.warning(f"Approaching timeout limit. Completed {i}/{iterations} iterations")
                # Return partial results
                if revenue_losses:
                    break
                else:
                    raise TimeoutError("Simulation timeout before completing minimum iterations")
        
        # Execute scenario-specific simulation
        if scenario_type == "stockout":
            revenue_loss, inventory_impact = simulate_stockout(
                time_horizon_days, initial_inventory, daily_demand_mean, daily_demand_std
            )
        elif scenario_type == "overstock":
            revenue_loss, inventory_impact = simulate_overstock(
                time_horizon_days, initial_inventory, daily_demand_mean, daily_demand_std
            )
        elif scenario_type == "seasonal_mismatch":
            revenue_loss, inventory_impact = simulate_seasonal_mismatch(
                time_horizon_days, initial_inventory, daily_demand_mean, daily_demand_std
            )
        elif scenario_type == "pricing_failure":
            revenue_loss, inventory_impact = simulate_pricing_failure(
                time_horizon_days, initial_inventory, daily_demand_mean, daily_demand_std
            )
        elif scenario_type == "fulfillment_failure":
            revenue_loss, inventory_impact = simulate_fulfillment_failure(
                time_horizon_days, initial_inventory, daily_demand_mean, daily_demand_std
            )
        else:
            raise ValueError(f"Unknown scenario type: {scenario_type}")
        
        revenue_losses.append(revenue_loss)
        inventory_impacts.append(inventory_impact)
    
    # Calculate statistics
    revenue_losses_array = np.array(revenue_losses)
    inventory_impacts_array = np.array(inventory_impacts)
    
    expected_revenue_loss = float(np.mean(revenue_losses_array))
    revenue_ci_lower = float(np.percentile(revenue_losses_array, lower_percentile))
    revenue_ci_upper = float(np.percentile(revenue_losses_array, upper_percentile))
    
    expected_inventory_impact = int(np.mean(inventory_impacts_array))
    
    # Calculate probability of occurrence based on scenario type
    probability = calculate_probability_of_occurrence(scenario_type, revenue_losses_array)
    
    # Build scenario-specific inventory impact details
    inventory_impact_details = build_inventory_impact(
        scenario_type, expected_inventory_impact, daily_demand_mean
    )
    
    return {
        "revenue_impact": {
            "expected_loss": round(expected_revenue_loss, 2),
            "confidence_interval_lower": round(revenue_ci_lower, 2),
            "confidence_interval_upper": round(revenue_ci_upper, 2),
            "currency": "USD"
        },
        "inventory_impact": inventory_impact_details,
        "probability_of_occurrence": round(probability, 3)
    }


def simulate_stockout(
    time_horizon_days: int,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float
) -> Tuple[float, int]:
    """Simulate stockout scenario: inventory depletion and lost sales.
    
    Models the scenario where inventory runs out before replenishment,
    resulting in lost sales opportunities.
    
    Args:
        time_horizon_days: Simulation period in days
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
    
    Returns:
        Tuple of (revenue_loss, units_affected)
    """
    inventory = initial_inventory
    lost_sales = 0
    unit_price = 50.0  # Average unit price
    
    for day in range(time_horizon_days):
        # Generate daily demand with randomness
        daily_demand = max(0, np.random.normal(daily_demand_mean, daily_demand_std))
        
        if inventory >= daily_demand:
            inventory -= daily_demand
        else:
            # Stockout: lost sales = unmet demand
            lost_sales += (daily_demand - inventory)
            inventory = 0
        
        # Simulate replenishment (every 7 days, but may be insufficient)
        if day % 7 == 0 and day > 0:
            replenishment = int(daily_demand_mean * 5)  # 5 days of average demand
            inventory += replenishment
    
    revenue_loss = lost_sales * unit_price
    units_affected = int(lost_sales)
    
    return revenue_loss, units_affected


def simulate_overstock(
    time_horizon_days: int,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float
) -> Tuple[float, int]:
    """Simulate overstock scenario: holding cost accumulation and markdown requirements.
    
    Models the scenario where excess inventory accumulates, incurring holding costs
    and requiring markdowns to clear.
    
    Args:
        time_horizon_days: Simulation period in days
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
    
    Returns:
        Tuple of (revenue_loss, units_affected)
    """
    # Overstock: initial inventory is 3x normal demand
    inventory = initial_inventory * 3
    daily_holding_cost_per_unit = 0.10  # $0.10 per unit per day
    unit_price = 50.0
    
    total_holding_cost = 0
    
    for day in range(time_horizon_days):
        # Generate daily demand (lower than expected due to overstock)
        daily_demand = max(0, np.random.normal(daily_demand_mean * 0.7, daily_demand_std))
        
        # Calculate holding cost for current inventory
        total_holding_cost += inventory * daily_holding_cost_per_unit
        
        # Reduce inventory by sales
        inventory = max(0, inventory - daily_demand)
    
    # Remaining inventory requires markdown to clear
    markdown_percentage = 0.30  # 30% markdown
    markdown_loss = inventory * unit_price * markdown_percentage
    
    revenue_loss = total_holding_cost + markdown_loss
    units_affected = initial_inventory * 3
    
    return revenue_loss, units_affected


def simulate_seasonal_mismatch(
    time_horizon_days: int,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float
) -> Tuple[float, int]:
    """Simulate seasonal mismatch scenario: demand-supply timing gaps and revenue loss.
    
    Models the scenario where inventory arrives too early or too late for seasonal demand,
    resulting in lost sales and excess inventory.
    
    Args:
        time_horizon_days: Simulation period in days
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
    
    Returns:
        Tuple of (revenue_loss, units_affected)
    """
    inventory = initial_inventory
    unit_price = 50.0
    lost_sales = 0
    excess_inventory = 0
    
    # Seasonal demand pattern: peaks in middle of time horizon
    peak_day = time_horizon_days // 2
    
    for day in range(time_horizon_days):
        # Calculate seasonal demand multiplier (bell curve)
        days_from_peak = abs(day - peak_day)
        seasonal_multiplier = np.exp(-0.01 * days_from_peak ** 2) + 0.3
        
        daily_demand = max(0, np.random.normal(
            daily_demand_mean * seasonal_multiplier, 
            daily_demand_std
        ))
        
        if inventory >= daily_demand:
            inventory -= daily_demand
        else:
            # Lost sales during peak season
            lost_sales += (daily_demand - inventory)
            inventory = 0
    
    # Remaining inventory after season ends
    excess_inventory = inventory
    markdown_loss = excess_inventory * unit_price * 0.40  # 40% markdown for seasonal items
    
    revenue_loss = (lost_sales * unit_price) + markdown_loss
    units_affected = int(lost_sales + excess_inventory)
    
    return revenue_loss, units_affected


def simulate_pricing_failure(
    time_horizon_days: int,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float
) -> Tuple[float, int]:
    """Simulate pricing failure scenario: margin erosion and competitive positioning impact.
    
    Models the scenario where incorrect pricing leads to margin erosion or lost sales
    due to competitive disadvantage.
    
    Args:
        time_horizon_days: Simulation period in days
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
    
    Returns:
        Tuple of (revenue_loss, units_affected)
    """
    inventory = initial_inventory
    optimal_price = 50.0
    
    # Pricing failure: price is either too high or too low
    pricing_error = np.random.choice([-0.20, 0.20])  # ±20% pricing error
    actual_price = optimal_price * (1 + pricing_error)
    
    total_revenue_loss = 0
    units_sold = 0
    
    for day in range(time_horizon_days):
        # Demand is affected by pricing error
        if pricing_error > 0:
            # Price too high: reduced demand
            demand_multiplier = 0.6
        else:
            # Price too low: increased demand but margin erosion
            demand_multiplier = 1.3
        
        daily_demand = max(0, np.random.normal(
            daily_demand_mean * demand_multiplier, 
            daily_demand_std
        ))
        
        # Calculate sales
        units_sold_today = min(inventory, daily_demand)
        units_sold += units_sold_today
        inventory -= units_sold_today
        
        # Calculate margin loss per unit
        if pricing_error > 0:
            # High price: lost sales opportunity
            lost_sales = daily_demand - units_sold_today
            total_revenue_loss += lost_sales * optimal_price
        else:
            # Low price: margin erosion on each sale
            margin_loss_per_unit = optimal_price - actual_price
            total_revenue_loss += units_sold_today * margin_loss_per_unit
    
    units_affected = int(units_sold)
    
    return total_revenue_loss, units_affected


def simulate_fulfillment_failure(
    time_horizon_days: int,
    initial_inventory: int,
    daily_demand_mean: float,
    daily_demand_std: float
) -> Tuple[float, int]:
    """Simulate fulfillment failure scenario: delivery delays and customer churn.
    
    Models the scenario where fulfillment issues lead to delivery delays,
    resulting in customer churn and lost future revenue.
    
    Args:
        time_horizon_days: Simulation period in days
        initial_inventory: Starting inventory level
        daily_demand_mean: Mean daily demand
        daily_demand_std: Standard deviation of daily demand
    
    Returns:
        Tuple of (revenue_loss, units_affected)
    """
    inventory = initial_inventory
    unit_price = 50.0
    
    # Fulfillment failure parameters
    delay_probability = 0.25  # 25% of orders delayed
    churn_rate_per_delay = 0.15  # 15% of delayed customers churn
    customer_lifetime_value = 500.0  # Average CLV
    
    total_orders = 0
    delayed_orders = 0
    churned_customers = 0
    
    for day in range(time_horizon_days):
        daily_demand = max(0, np.random.normal(daily_demand_mean, daily_demand_std))
        
        # Process orders
        orders_today = min(inventory, daily_demand)
        total_orders += orders_today
        inventory -= orders_today
        
        # Simulate fulfillment delays
        if np.random.random() < delay_probability:
            delayed_orders += orders_today
            # Some delayed customers churn
            churned_customers += orders_today * churn_rate_per_delay
        
        # Replenish inventory weekly
        if day % 7 == 0 and day > 0:
            inventory += int(daily_demand_mean * 7)
    
    # Calculate revenue loss from churned customers
    revenue_loss = churned_customers * customer_lifetime_value
    units_affected = int(delayed_orders)
    
    return revenue_loss, units_affected


def calculate_probability_of_occurrence(
    scenario_type: str,
    revenue_losses: np.ndarray
) -> float:
    """Calculate probability of occurrence based on scenario type and simulation results.
    
    Args:
        scenario_type: Type of failure scenario
        revenue_losses: Array of revenue losses from Monte Carlo iterations
    
    Returns:
        Probability of occurrence (0.0 to 1.0)
    """
    # Base probabilities by scenario type (industry estimates)
    base_probabilities = {
        "stockout": 0.35,
        "overstock": 0.25,
        "seasonal_mismatch": 0.40,
        "pricing_failure": 0.20,
        "fulfillment_failure": 0.30
    }
    
    base_prob = base_probabilities.get(scenario_type, 0.25)
    
    # Adjust probability based on severity of losses
    mean_loss = np.mean(revenue_losses)
    std_loss = np.std(revenue_losses)
    
    # Higher variance increases probability (more unpredictable)
    coefficient_of_variation = std_loss / mean_loss if mean_loss > 0 else 0
    adjustment = min(0.15, coefficient_of_variation * 0.5)
    
    probability = min(0.95, base_prob + adjustment)
    
    return probability


def build_inventory_impact(
    scenario_type: str,
    units_affected: int,
    daily_demand_mean: float
) -> Dict:
    """Build inventory impact details based on scenario type.
    
    Args:
        scenario_type: Type of failure scenario
        units_affected: Number of inventory units affected
        daily_demand_mean: Mean daily demand for calculations
    
    Returns:
        Dict with units_affected and scenario-specific metrics
    """
    impact = {
        "units_affected": units_affected
    }
    
    if scenario_type == "overstock":
        # Calculate holding cost and markdown
        holding_cost = units_affected * 0.10 * 30  # 30 days average
        markdown_required = 0.30  # 30% markdown
        impact["holding_cost"] = round(holding_cost, 2)
        impact["markdown_required"] = markdown_required
    
    elif scenario_type == "seasonal_mismatch":
        # Higher markdown for seasonal items
        markdown_required = 0.40  # 40% markdown
        impact["markdown_required"] = markdown_required
    
    return impact
