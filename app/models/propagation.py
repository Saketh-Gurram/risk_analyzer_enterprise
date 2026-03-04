"""
Propagation models for the Retail Risk Intelligence Platform.

This module defines data models for impact propagation analysis across business domains.
"""

from pydantic import BaseModel, Field
from typing import List


class PropagationScore(BaseModel):
    """
    Represents the impact score for a single business domain in the propagation analysis.
    
    Attributes:
        domain: Business domain name (e.g., 'inventory', 'pricing', 'revenue', 'fulfillment', 'customer_satisfaction')
        impact_score: Normalized impact score on a 0-10 scale
        propagation_order: Order of propagation (1 for first-order/direct effects, 2 for second-order/indirect effects)
        affected_by: List of upstream domains that affect this domain
    """
    domain: str
    impact_score: float = Field(ge=0.0, le=10.0, description="Normalized impact score (0-10 scale)")
    propagation_order: int = Field(description="1 for first-order, 2 for second-order effects")
    affected_by: List[str] = Field(default_factory=list, description="Upstream domains affecting this domain")


class PropagationResponse(BaseModel):
    """
    Response model containing complete propagation analysis results for a scenario.
    
    Attributes:
        scenario_id: Unique identifier for the simulation scenario
        scores: List of propagation scores for all affected business domains
        total_organizational_impact: Aggregate impact score across all domains
    """
    scenario_id: str
    scores: List[PropagationScore]
    total_organizational_impact: float = Field(description="Aggregate impact across all domains")
