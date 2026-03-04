"""Simulation-related data models for the Retail Risk Intelligence Platform."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from app.models.enums import ScenarioType


class SimulationParameters(BaseModel):
    """Parameters for Monte Carlo simulation execution.
    
    Attributes:
        time_horizon_days: Simulation time horizon (7-365 days)
        monte_carlo_iterations: Number of Monte Carlo iterations (minimum 100)
        confidence_level: Statistical confidence level (0.8-0.99)
        initial_inventory: Optional starting inventory level
        daily_demand_mean: Optional mean daily demand for simulation
        daily_demand_std: Optional standard deviation of daily demand
    """
    time_horizon_days: int = Field(..., ge=7, le=365, description="Simulation time horizon in days")
    monte_carlo_iterations: int = Field(default=1000, ge=100, description="Number of Monte Carlo iterations")
    confidence_level: float = Field(default=0.95, ge=0.8, le=0.99, description="Statistical confidence level")
    initial_inventory: Optional[int] = Field(default=None, ge=0, description="Starting inventory level")
    daily_demand_mean: Optional[float] = Field(default=None, ge=0.0, description="Mean daily demand")
    daily_demand_std: Optional[float] = Field(default=None, ge=0.0, description="Standard deviation of daily demand")


class SimulationRequest(BaseModel):
    """Request model for initiating a failure scenario simulation.
    
    Attributes:
        scenario_type: Type of retail failure scenario to simulate
        parameters: Simulation configuration parameters
        upload_id: Reference to the source data upload
    """
    scenario_type: ScenarioType = Field(..., description="Type of failure scenario to simulate")
    parameters: SimulationParameters = Field(..., description="Simulation configuration")
    upload_id: str = Field(..., description="Reference to source data upload")


class RevenueImpact(BaseModel):
    """Revenue impact projections from simulation.
    
    Attributes:
        expected_loss: Expected revenue loss (mean of Monte Carlo iterations)
        confidence_interval_lower: Lower bound of confidence interval
        confidence_interval_upper: Upper bound of confidence interval
        currency: Currency code for monetary values
    """
    expected_loss: float = Field(..., ge=0.0, description="Expected revenue loss")
    confidence_interval_lower: float = Field(..., ge=0.0, description="Lower confidence interval bound")
    confidence_interval_upper: float = Field(..., ge=0.0, description="Upper confidence interval bound")
    currency: str = Field(default="USD", description="Currency code")


class InventoryImpact(BaseModel):
    """Inventory impact projections from simulation.
    
    Attributes:
        units_affected: Number of inventory units affected by the scenario
        holding_cost: Optional holding cost for overstock scenarios
        markdown_required: Optional markdown percentage required to clear excess inventory
    """
    units_affected: int = Field(..., ge=0, description="Number of units affected")
    holding_cost: Optional[float] = Field(default=None, ge=0.0, description="Holding cost for overstock")
    markdown_required: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Markdown percentage (0-1)")


class SimulationResult(BaseModel):
    """Complete simulation results for a failure scenario.
    
    Attributes:
        scenario_id: Unique identifier for this simulation scenario
        scenario_type: Type of failure scenario simulated
        timestamp: When the simulation was completed
        revenue_impact: Revenue loss projections with confidence intervals
        inventory_impact: Inventory-related impacts
        timeline_days: Actual timeline simulated
        probability_of_occurrence: Estimated probability of this scenario occurring
        execution_time_seconds: Time taken to complete the simulation
    """
    scenario_id: str = Field(..., description="Unique scenario identifier")
    scenario_type: ScenarioType = Field(..., description="Type of failure scenario")
    timestamp: datetime = Field(..., description="Simulation completion timestamp")
    revenue_impact: RevenueImpact = Field(..., description="Revenue impact projections")
    inventory_impact: InventoryImpact = Field(..., description="Inventory impact projections")
    timeline_days: int = Field(..., ge=1, description="Simulated timeline in days")
    probability_of_occurrence: float = Field(..., ge=0.0, le=1.0, description="Probability of occurrence")
    execution_time_seconds: float = Field(..., ge=0.0, description="Simulation execution time")
