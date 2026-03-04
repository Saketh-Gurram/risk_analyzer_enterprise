"""Mitigation strategy models for the Retail Risk Intelligence Platform."""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from app.models.enums import Complexity


class MitigationStrategy(BaseModel):
    """Mitigation strategy recommendation with implementation details.
    
    Represents a ranked action plan for risk mitigation with effectiveness scoring,
    complexity assessment, timeline estimation, and cost analysis.
    
    Attributes:
        strategy_id: Unique identifier for the strategy
        title: Short descriptive title of the strategy
        description: Detailed explanation of the mitigation approach
        effectiveness_score: Effectiveness rating on 0-100 scale (higher is better)
        complexity: Implementation complexity (LOW, MEDIUM, HIGH)
        timeline_days: Estimated implementation timeline in days
        estimated_cost_min: Minimum estimated implementation cost in USD
        estimated_cost_max: Maximum estimated implementation cost in USD
        prerequisites: List of required conditions or dependencies
        risks: List of potential risks or challenges in implementation
    """
    strategy_id: str
    title: str
    description: str
    effectiveness_score: int = Field(ge=0, le=100, description="Effectiveness rating (0-100)")
    complexity: Complexity
    timeline_days: int = Field(gt=0, description="Implementation timeline in days")
    estimated_cost_min: float = Field(ge=0.0, description="Minimum cost estimate in USD")
    estimated_cost_max: float = Field(ge=0.0, description="Maximum cost estimate in USD")
    prerequisites: List[str] = Field(default_factory=list, description="Required conditions")
    risks: List[str] = Field(default_factory=list, description="Implementation risks")


class MitigationResponse(BaseModel):
    """Response containing ranked mitigation strategies for a scenario.
    
    Represents the complete set of mitigation recommendations generated for
    a specific failure scenario, ranked by effectiveness.
    
    Attributes:
        scenario_id: Identifier of the scenario these strategies address
        strategies: List of mitigation strategies ranked by effectiveness
        timestamp: When the strategies were generated
    """
    scenario_id: str
    strategies: List[MitigationStrategy]
    timestamp: datetime
