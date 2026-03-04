"""Enumerations for the Retail Risk Intelligence Platform."""

from enum import Enum


class RiskLevel(str, Enum):
    """Risk level classification based on coefficient of variation thresholds.
    
    - LOW: CV < 0.15
    - MEDIUM: 0.15 <= CV < 0.3
    - HIGH: CV >= 0.3
    """
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ScenarioType(str, Enum):
    """Retail failure scenario types for simulation.
    
    - STOCKOUT: Inventory depletion and lost sales
    - OVERSTOCK: Holding cost accumulation and markdown requirements
    - SEASONAL_MISMATCH: Demand-supply timing gaps and revenue loss
    - PRICING_FAILURE: Margin erosion and competitive positioning impact
    - FULFILLMENT_FAILURE: Delivery delays and customer churn
    """
    STOCKOUT = "stockout"
    OVERSTOCK = "overstock"
    SEASONAL_MISMATCH = "seasonal_mismatch"
    PRICING_FAILURE = "pricing_failure"
    FULFILLMENT_FAILURE = "fulfillment_failure"


class Complexity(str, Enum):
    """Implementation complexity assessment for mitigation strategies.
    
    - LOW: Simple to implement
    - MEDIUM: Moderate implementation effort
    - HIGH: Complex implementation requiring significant resources
    """
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
