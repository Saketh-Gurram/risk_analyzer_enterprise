"""
Executive summary models for the Retail Risk Intelligence Platform.

This module defines data models for AI-generated executive summaries that provide
business-level insights from risk simulations and impact propagation analysis.
"""

from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import datetime


class ExecutiveSummary(BaseModel):
    """
    AI-generated executive summary providing business-level risk insights.
    
    This model represents structured output from the AI Reasoning Engine (Amazon Bedrock
    Claude 3.5 Sonnet) or fallback rule-based logic. It translates technical simulation
    results into executive-ready insights with actionable recommendations.
    
    Attributes:
        scenario_id: Unique identifier for the simulation scenario
        timestamp: When the executive summary was generated
        revenue_risk_quantification: Quantified revenue risk in business terms
        market_context: Market and business context reasoning for the risk
        urgency_level: Assessment of how urgently action is needed (low, medium, high, critical)
        recommended_actions: List of recommended actions to mitigate the risk
        trade_offs_analysis: Analysis of trade-offs between different mitigation approaches
        generated_by: Source of the summary generation (bedrock or fallback)
    """
    scenario_id: str = Field(description="Unique identifier for the simulation scenario")
    timestamp: datetime = Field(description="Timestamp when the summary was generated")
    revenue_risk_quantification: str = Field(
        description="Quantified revenue risk in business terms"
    )
    market_context: str = Field(
        description="Market and business context reasoning for the risk"
    )
    urgency_level: Literal["low", "medium", "high", "critical"] = Field(
        description="Assessment of how urgently action is needed"
    )
    recommended_actions: List[str] = Field(
        description="List of recommended actions to mitigate the risk"
    )
    trade_offs_analysis: str = Field(
        description="Analysis of trade-offs between different mitigation approaches"
    )
    generated_by: Literal["bedrock", "fallback"] = Field(
        description="Source of summary generation (bedrock for AI, fallback for rule-based)"
    )
