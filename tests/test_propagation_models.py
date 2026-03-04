"""
Unit tests for propagation models.
"""

import pytest
from pydantic import ValidationError
from app.models.propagation import PropagationScore, PropagationResponse


class TestPropagationScore:
    """Tests for PropagationScore model."""
    
    def test_valid_propagation_score(self):
        """Test creating a valid PropagationScore."""
        score = PropagationScore(
            domain="inventory",
            impact_score=7.5,
            propagation_order=1,
            affected_by=["stockout"]
        )
        assert score.domain == "inventory"
        assert score.impact_score == 7.5
        assert score.propagation_order == 1
        assert score.affected_by == ["stockout"]
    
    def test_impact_score_min_boundary(self):
        """Test impact_score at minimum boundary (0.0)."""
        score = PropagationScore(
            domain="pricing",
            impact_score=0.0,
            propagation_order=2,
            affected_by=[]
        )
        assert score.impact_score == 0.0
    
    def test_impact_score_max_boundary(self):
        """Test impact_score at maximum boundary (10.0)."""
        score = PropagationScore(
            domain="revenue",
            impact_score=10.0,
            propagation_order=1,
            affected_by=["inventory", "pricing"]
        )
        assert score.impact_score == 10.0
    
    def test_impact_score_below_min_fails(self):
        """Test that impact_score below 0.0 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PropagationScore(
                domain="inventory",
                impact_score=-0.1,
                propagation_order=1,
                affected_by=[]
            )
        assert "impact_score" in str(exc_info.value)
    
    def test_impact_score_above_max_fails(self):
        """Test that impact_score above 10.0 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PropagationScore(
                domain="inventory",
                impact_score=10.1,
                propagation_order=1,
                affected_by=[]
            )
        assert "impact_score" in str(exc_info.value)
    
    def test_empty_affected_by_list(self):
        """Test PropagationScore with empty affected_by list."""
        score = PropagationScore(
            domain="inventory",
            impact_score=5.0,
            propagation_order=1,
            affected_by=[]
        )
        assert score.affected_by == []
    
    def test_multiple_affected_by_domains(self):
        """Test PropagationScore with multiple upstream domains."""
        score = PropagationScore(
            domain="customer_satisfaction",
            impact_score=8.2,
            propagation_order=2,
            affected_by=["fulfillment", "pricing", "inventory"]
        )
        assert len(score.affected_by) == 3
        assert "fulfillment" in score.affected_by
    
    def test_first_order_propagation(self):
        """Test first-order propagation (direct effects)."""
        score = PropagationScore(
            domain="pricing",
            impact_score=6.5,
            propagation_order=1,
            affected_by=["inventory"]
        )
        assert score.propagation_order == 1
    
    def test_second_order_propagation(self):
        """Test second-order propagation (indirect effects)."""
        score = PropagationScore(
            domain="revenue",
            impact_score=7.8,
            propagation_order=2,
            affected_by=["pricing"]
        )
        assert score.propagation_order == 2


class TestPropagationResponse:
    """Tests for PropagationResponse model."""
    
    def test_valid_propagation_response(self):
        """Test creating a valid PropagationResponse."""
        scores = [
            PropagationScore(
                domain="inventory",
                impact_score=7.5,
                propagation_order=1,
                affected_by=[]
            ),
            PropagationScore(
                domain="pricing",
                impact_score=6.2,
                propagation_order=2,
                affected_by=["inventory"]
            )
        ]
        response = PropagationResponse(
            scenario_id="scenario-123",
            scores=scores,
            total_organizational_impact=13.7
        )
        assert response.scenario_id == "scenario-123"
        assert len(response.scores) == 2
        assert response.total_organizational_impact == 13.7
    
    def test_empty_scores_list(self):
        """Test PropagationResponse with empty scores list."""
        response = PropagationResponse(
            scenario_id="scenario-456",
            scores=[],
            total_organizational_impact=0.0
        )
        assert len(response.scores) == 0
        assert response.total_organizational_impact == 0.0
    
    def test_multiple_domains_propagation(self):
        """Test PropagationResponse with multiple business domains."""
        scores = [
            PropagationScore(domain="inventory", impact_score=8.0, propagation_order=1, affected_by=[]),
            PropagationScore(domain="pricing", impact_score=7.0, propagation_order=1, affected_by=["inventory"]),
            PropagationScore(domain="revenue", impact_score=6.5, propagation_order=2, affected_by=["pricing"]),
            PropagationScore(domain="fulfillment", impact_score=5.5, propagation_order=1, affected_by=[]),
            PropagationScore(domain="customer_satisfaction", impact_score=4.8, propagation_order=2, affected_by=["fulfillment"])
        ]
        response = PropagationResponse(
            scenario_id="scenario-789",
            scores=scores,
            total_organizational_impact=31.8
        )
        assert len(response.scores) == 5
        assert response.total_organizational_impact == 31.8
    
    def test_json_serialization(self):
        """Test that PropagationResponse can be serialized to JSON."""
        scores = [
            PropagationScore(
                domain="inventory",
                impact_score=7.5,
                propagation_order=1,
                affected_by=["stockout"]
            )
        ]
        response = PropagationResponse(
            scenario_id="scenario-json",
            scores=scores,
            total_organizational_impact=7.5
        )
        json_data = response.model_dump()
        assert json_data["scenario_id"] == "scenario-json"
        assert len(json_data["scores"]) == 1
        assert json_data["scores"][0]["domain"] == "inventory"
