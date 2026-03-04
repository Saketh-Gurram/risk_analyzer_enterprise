"""
Unit tests for mitigation models.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError
from app.models.mitigation import MitigationStrategy, MitigationResponse
from app.models.enums import Complexity


class TestMitigationStrategy:
    """Tests for MitigationStrategy model."""
    
    def test_valid_mitigation_strategy(self):
        """Test creating a valid MitigationStrategy."""
        strategy = MitigationStrategy(
            strategy_id="strat-001",
            title="Increase Safety Stock",
            description="Implement dynamic safety stock levels based on demand volatility",
            effectiveness_score=85,
            complexity=Complexity.MEDIUM,
            timeline_days=30,
            estimated_cost_min=10000.0,
            estimated_cost_max=25000.0,
            prerequisites=["Historical demand data", "Inventory management system"],
            risks=["Increased holding costs", "Potential overstock"]
        )
        assert strategy.strategy_id == "strat-001"
        assert strategy.title == "Increase Safety Stock"
        assert strategy.effectiveness_score == 85
        assert strategy.complexity == Complexity.MEDIUM
        assert strategy.timeline_days == 30
        assert len(strategy.prerequisites) == 2
        assert len(strategy.risks) == 2
    
    def test_effectiveness_score_min_boundary(self):
        """Test effectiveness_score at minimum boundary (0)."""
        strategy = MitigationStrategy(
            strategy_id="strat-002",
            title="Minimal Strategy",
            description="Low effectiveness strategy",
            effectiveness_score=0,
            complexity=Complexity.LOW,
            timeline_days=1,
            estimated_cost_min=0.0,
            estimated_cost_max=100.0
        )
        assert strategy.effectiveness_score == 0
    
    def test_effectiveness_score_max_boundary(self):
        """Test effectiveness_score at maximum boundary (100)."""
        strategy = MitigationStrategy(
            strategy_id="strat-003",
            title="Optimal Strategy",
            description="Maximum effectiveness strategy",
            effectiveness_score=100,
            complexity=Complexity.HIGH,
            timeline_days=90,
            estimated_cost_min=50000.0,
            estimated_cost_max=100000.0
        )
        assert strategy.effectiveness_score == 100
    
    def test_effectiveness_score_below_min_fails(self):
        """Test that effectiveness_score below 0 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-004",
                title="Invalid Strategy",
                description="Invalid effectiveness score",
                effectiveness_score=-1,
                complexity=Complexity.LOW,
                timeline_days=10,
                estimated_cost_min=1000.0,
                estimated_cost_max=2000.0
            )
        assert "effectiveness_score" in str(exc_info.value)
    
    def test_effectiveness_score_above_max_fails(self):
        """Test that effectiveness_score above 100 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-005",
                title="Invalid Strategy",
                description="Invalid effectiveness score",
                effectiveness_score=101,
                complexity=Complexity.LOW,
                timeline_days=10,
                estimated_cost_min=1000.0,
                estimated_cost_max=2000.0
            )
        assert "effectiveness_score" in str(exc_info.value)
    
    def test_timeline_days_positive(self):
        """Test that timeline_days must be positive."""
        strategy = MitigationStrategy(
            strategy_id="strat-006",
            title="Quick Strategy",
            description="Fast implementation",
            effectiveness_score=70,
            complexity=Complexity.LOW,
            timeline_days=1,
            estimated_cost_min=500.0,
            estimated_cost_max=1000.0
        )
        assert strategy.timeline_days == 1
    
    def test_timeline_days_zero_fails(self):
        """Test that timeline_days of 0 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-007",
                title="Invalid Timeline",
                description="Zero timeline",
                effectiveness_score=50,
                complexity=Complexity.LOW,
                timeline_days=0,
                estimated_cost_min=1000.0,
                estimated_cost_max=2000.0
            )
        assert "timeline_days" in str(exc_info.value)
    
    def test_timeline_days_negative_fails(self):
        """Test that negative timeline_days raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-008",
                title="Invalid Timeline",
                description="Negative timeline",
                effectiveness_score=50,
                complexity=Complexity.LOW,
                timeline_days=-5,
                estimated_cost_min=1000.0,
                estimated_cost_max=2000.0
            )
        assert "timeline_days" in str(exc_info.value)
    
    def test_cost_min_zero(self):
        """Test that estimated_cost_min can be zero."""
        strategy = MitigationStrategy(
            strategy_id="strat-009",
            title="Free Strategy",
            description="No cost implementation",
            effectiveness_score=40,
            complexity=Complexity.LOW,
            timeline_days=5,
            estimated_cost_min=0.0,
            estimated_cost_max=0.0
        )
        assert strategy.estimated_cost_min == 0.0
        assert strategy.estimated_cost_max == 0.0
    
    def test_cost_min_negative_fails(self):
        """Test that negative estimated_cost_min raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-010",
                title="Invalid Cost",
                description="Negative cost",
                effectiveness_score=50,
                complexity=Complexity.LOW,
                timeline_days=10,
                estimated_cost_min=-1000.0,
                estimated_cost_max=2000.0
            )
        assert "estimated_cost_min" in str(exc_info.value)
    
    def test_cost_max_negative_fails(self):
        """Test that negative estimated_cost_max raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            MitigationStrategy(
                strategy_id="strat-011",
                title="Invalid Cost",
                description="Negative cost",
                effectiveness_score=50,
                complexity=Complexity.LOW,
                timeline_days=10,
                estimated_cost_min=1000.0,
                estimated_cost_max=-2000.0
            )
        assert "estimated_cost_max" in str(exc_info.value)
    
    def test_empty_prerequisites_and_risks(self):
        """Test MitigationStrategy with empty prerequisites and risks lists."""
        strategy = MitigationStrategy(
            strategy_id="strat-012",
            title="Simple Strategy",
            description="No prerequisites or risks",
            effectiveness_score=60,
            complexity=Complexity.LOW,
            timeline_days=15,
            estimated_cost_min=5000.0,
            estimated_cost_max=8000.0
        )
        assert strategy.prerequisites == []
        assert strategy.risks == []
    
    def test_complexity_low(self):
        """Test MitigationStrategy with LOW complexity."""
        strategy = MitigationStrategy(
            strategy_id="strat-013",
            title="Low Complexity Strategy",
            description="Easy to implement",
            effectiveness_score=55,
            complexity=Complexity.LOW,
            timeline_days=7,
            estimated_cost_min=2000.0,
            estimated_cost_max=5000.0
        )
        assert strategy.complexity == Complexity.LOW
    
    def test_complexity_medium(self):
        """Test MitigationStrategy with MEDIUM complexity."""
        strategy = MitigationStrategy(
            strategy_id="strat-014",
            title="Medium Complexity Strategy",
            description="Moderate implementation effort",
            effectiveness_score=75,
            complexity=Complexity.MEDIUM,
            timeline_days=30,
            estimated_cost_min=15000.0,
            estimated_cost_max=30000.0
        )
        assert strategy.complexity == Complexity.MEDIUM
    
    def test_complexity_high(self):
        """Test MitigationStrategy with HIGH complexity."""
        strategy = MitigationStrategy(
            strategy_id="strat-015",
            title="High Complexity Strategy",
            description="Complex implementation requiring significant resources",
            effectiveness_score=95,
            complexity=Complexity.HIGH,
            timeline_days=90,
            estimated_cost_min=50000.0,
            estimated_cost_max=150000.0
        )
        assert strategy.complexity == Complexity.HIGH
    
    def test_multiple_prerequisites(self):
        """Test MitigationStrategy with multiple prerequisites."""
        prerequisites = [
            "Data warehouse setup",
            "ML model training",
            "API integration",
            "Staff training"
        ]
        strategy = MitigationStrategy(
            strategy_id="strat-016",
            title="Complex Strategy",
            description="Requires multiple prerequisites",
            effectiveness_score=88,
            complexity=Complexity.HIGH,
            timeline_days=60,
            estimated_cost_min=40000.0,
            estimated_cost_max=80000.0,
            prerequisites=prerequisites
        )
        assert len(strategy.prerequisites) == 4
        assert "ML model training" in strategy.prerequisites
    
    def test_multiple_risks(self):
        """Test MitigationStrategy with multiple risks."""
        risks = [
            "Implementation delays",
            "Budget overruns",
            "Technical challenges",
            "Stakeholder resistance"
        ]
        strategy = MitigationStrategy(
            strategy_id="strat-017",
            title="Risky Strategy",
            description="Multiple implementation risks",
            effectiveness_score=82,
            complexity=Complexity.HIGH,
            timeline_days=45,
            estimated_cost_min=30000.0,
            estimated_cost_max=60000.0,
            risks=risks
        )
        assert len(strategy.risks) == 4
        assert "Budget overruns" in strategy.risks


class TestMitigationResponse:
    """Tests for MitigationResponse model."""
    
    def test_valid_mitigation_response(self):
        """Test creating a valid MitigationResponse."""
        strategies = [
            MitigationStrategy(
                strategy_id="strat-001",
                title="Strategy 1",
                description="First strategy",
                effectiveness_score=90,
                complexity=Complexity.MEDIUM,
                timeline_days=30,
                estimated_cost_min=10000.0,
                estimated_cost_max=20000.0
            ),
            MitigationStrategy(
                strategy_id="strat-002",
                title="Strategy 2",
                description="Second strategy",
                effectiveness_score=75,
                complexity=Complexity.LOW,
                timeline_days=15,
                estimated_cost_min=5000.0,
                estimated_cost_max=10000.0
            )
        ]
        timestamp = datetime.now()
        response = MitigationResponse(
            scenario_id="scenario-123",
            strategies=strategies,
            timestamp=timestamp
        )
        assert response.scenario_id == "scenario-123"
        assert len(response.strategies) == 2
        assert response.timestamp == timestamp
    
    def test_empty_strategies_list(self):
        """Test MitigationResponse with empty strategies list."""
        response = MitigationResponse(
            scenario_id="scenario-456",
            strategies=[],
            timestamp=datetime.now()
        )
        assert len(response.strategies) == 0
    
    def test_single_strategy(self):
        """Test MitigationResponse with single strategy."""
        strategy = MitigationStrategy(
            strategy_id="strat-solo",
            title="Only Strategy",
            description="Single mitigation option",
            effectiveness_score=80,
            complexity=Complexity.MEDIUM,
            timeline_days=20,
            estimated_cost_min=8000.0,
            estimated_cost_max=15000.0
        )
        response = MitigationResponse(
            scenario_id="scenario-789",
            strategies=[strategy],
            timestamp=datetime.now()
        )
        assert len(response.strategies) == 1
        assert response.strategies[0].strategy_id == "strat-solo"
    
    def test_multiple_strategies_ranked(self):
        """Test MitigationResponse with multiple strategies (simulating ranking)."""
        strategies = [
            MitigationStrategy(
                strategy_id="strat-high",
                title="High Effectiveness",
                description="Best strategy",
                effectiveness_score=95,
                complexity=Complexity.HIGH,
                timeline_days=60,
                estimated_cost_min=40000.0,
                estimated_cost_max=80000.0
            ),
            MitigationStrategy(
                strategy_id="strat-medium",
                title="Medium Effectiveness",
                description="Good strategy",
                effectiveness_score=75,
                complexity=Complexity.MEDIUM,
                timeline_days=30,
                estimated_cost_min=15000.0,
                estimated_cost_max=30000.0
            ),
            MitigationStrategy(
                strategy_id="strat-low",
                title="Low Effectiveness",
                description="Basic strategy",
                effectiveness_score=50,
                complexity=Complexity.LOW,
                timeline_days=10,
                estimated_cost_min=3000.0,
                estimated_cost_max=6000.0
            )
        ]
        response = MitigationResponse(
            scenario_id="scenario-ranked",
            strategies=strategies,
            timestamp=datetime.now()
        )
        assert len(response.strategies) == 3
        # Verify strategies are in the list (ranking would be done by engine)
        assert response.strategies[0].effectiveness_score == 95
        assert response.strategies[1].effectiveness_score == 75
        assert response.strategies[2].effectiveness_score == 50
    
    def test_strategies_with_different_complexities(self):
        """Test MitigationResponse with strategies of varying complexity."""
        strategies = [
            MitigationStrategy(
                strategy_id="strat-low-complex",
                title="Low Complexity",
                description="Easy implementation",
                effectiveness_score=60,
                complexity=Complexity.LOW,
                timeline_days=7,
                estimated_cost_min=2000.0,
                estimated_cost_max=4000.0
            ),
            MitigationStrategy(
                strategy_id="strat-med-complex",
                title="Medium Complexity",
                description="Moderate implementation",
                effectiveness_score=80,
                complexity=Complexity.MEDIUM,
                timeline_days=30,
                estimated_cost_min=15000.0,
                estimated_cost_max=25000.0
            ),
            MitigationStrategy(
                strategy_id="strat-high-complex",
                title="High Complexity",
                description="Complex implementation",
                effectiveness_score=92,
                complexity=Complexity.HIGH,
                timeline_days=90,
                estimated_cost_min=60000.0,
                estimated_cost_max=120000.0
            )
        ]
        response = MitigationResponse(
            scenario_id="scenario-complexity",
            strategies=strategies,
            timestamp=datetime.now()
        )
        assert len(response.strategies) == 3
        assert response.strategies[0].complexity == Complexity.LOW
        assert response.strategies[1].complexity == Complexity.MEDIUM
        assert response.strategies[2].complexity == Complexity.HIGH
    
    def test_json_serialization(self):
        """Test that MitigationResponse can be serialized to JSON."""
        strategy = MitigationStrategy(
            strategy_id="strat-json",
            title="JSON Strategy",
            description="Test JSON serialization",
            effectiveness_score=85,
            complexity=Complexity.MEDIUM,
            timeline_days=25,
            estimated_cost_min=12000.0,
            estimated_cost_max=22000.0,
            prerequisites=["Data access"],
            risks=["Time constraints"]
        )
        timestamp = datetime(2024, 1, 15, 10, 30, 0)
        response = MitigationResponse(
            scenario_id="scenario-json",
            strategies=[strategy],
            timestamp=timestamp
        )
        json_data = response.model_dump()
        assert json_data["scenario_id"] == "scenario-json"
        assert len(json_data["strategies"]) == 1
        assert json_data["strategies"][0]["strategy_id"] == "strat-json"
        assert json_data["strategies"][0]["effectiveness_score"] == 85
        assert json_data["strategies"][0]["complexity"] == "medium"
    
    def test_timestamp_format(self):
        """Test that timestamp is properly stored as datetime."""
        timestamp = datetime(2024, 6, 15, 14, 30, 45)
        response = MitigationResponse(
            scenario_id="scenario-time",
            strategies=[],
            timestamp=timestamp
        )
        assert isinstance(response.timestamp, datetime)
        assert response.timestamp.year == 2024
        assert response.timestamp.month == 6
        assert response.timestamp.day == 15
