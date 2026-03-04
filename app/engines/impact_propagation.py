"""
Impact Propagation Engine for Retail Risk Intelligence Platform.

Models cascading effects across business domains using a weighted directed graph.
Calculates first-order and second-order propagation effects, normalizes scores
to a 0-10 scale, and stores results in DynamoDB.

Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 8.7
"""

import time
from datetime import datetime
from typing import Any, List

import networkx as nx
from botocore.exceptions import ClientError

from app.config import get_config
from app.models.propagation import PropagationScore, PropagationResponse
from app.utils.logging import get_logger

logger = get_logger(__name__)


class ImpactPropagationEngine:
    """
    Engine for modeling cascading impacts across business domains.
    
    Uses a weighted directed graph to represent business domain relationships
    and calculates first-order (direct) and second-order (indirect) propagation
    effects. Normalizes impact scores to a 0-10 scale and stores results in DynamoDB.
    
    Attributes:
        dynamodb_client: Boto3 DynamoDB client for result storage
        config: Application configuration
        domain_graph: NetworkX DiGraph representing business domain relationships
    """
    
    def __init__(self, dynamodb_client: Any):
        """
        Initialize ImpactPropagationEngine with dependency injection.
        
        Args:
            dynamodb_client: Boto3 DynamoDB client for result storage
        """
        self.dynamodb_client = dynamodb_client
        self.config = get_config()
        self.domain_graph = self.build_domain_graph()
        
        logger.info(
            "ImpactPropagationEngine initialized",
            extra={
                "table": self.config.dynamodb_table_name,
                "nodes": list(self.domain_graph.nodes()),
                "edges": list(self.domain_graph.edges())
            }
        )
    
    def build_domain_graph(self) -> nx.DiGraph:
        """
        Build weighted directed graph of business domain relationships.
        
        Creates a graph with business domains as nodes and weighted edges
        representing the strength of impact propagation between domains.
        Edge weights range from 0.0 to 1.0.
        
        Domain relationships:
        - inventory -> pricing (0.8): Inventory levels strongly affect pricing decisions
        - pricing -> revenue (0.9): Pricing directly impacts revenue outcomes
        - inventory -> revenue (0.7): Inventory affects revenue through stockouts/overstock
        - fulfillment -> customer_satisfaction (0.85): Delivery performance affects satisfaction
        - customer_satisfaction -> revenue (0.75): Customer satisfaction impacts repeat purchases
        - pricing -> customer_satisfaction (0.6): Pricing affects customer perception
        
        Returns:
            nx.DiGraph: Weighted directed graph of business domains
            
        Requirements: 4.3, 4.5, 4.6, 4.7
        """
        graph = nx.DiGraph()
        
        # Add nodes for business domains
        domains = [
            "inventory",
            "pricing",
            "revenue",
            "fulfillment",
            "customer_satisfaction"
        ]
        graph.add_nodes_from(domains)
        
        # Add weighted edges for domain relationships
        # Format: (source, target, weight)
        edges = [
            ("inventory", "pricing", 0.8),
            ("pricing", "revenue", 0.9),
            ("inventory", "revenue", 0.7),
            ("fulfillment", "customer_satisfaction", 0.85),
            ("customer_satisfaction", "revenue", 0.75),
            ("pricing", "customer_satisfaction", 0.6)
        ]
        
        for source, target, weight in edges:
            graph.add_edge(source, target, weight=weight)
        
        logger.debug(
            "Domain graph built",
            extra={
                "nodes": len(graph.nodes()),
                "edges": len(graph.edges()),
                "domains": domains
            }
        )
        
        return graph

    async def calculate_propagation(
        self,
        scenario_id: str,
        initial_impact: float,
        source_domain: str = "inventory"
    ) -> PropagationResponse:
        """
        Calculate cascading impact propagation across business domains.
        
        Computes first-order (direct) and second-order (indirect) effects
        starting from the source domain. Normalizes all scores to 0-10 scale
        and stores results in DynamoDB.
        
        Args:
            scenario_id: Unique identifier for the simulation scenario
            initial_impact: Initial impact score (raw value, will be normalized)
            source_domain: Starting domain for propagation (default: "inventory")
            
        Returns:
            PropagationResponse: Complete propagation analysis with scores for all affected domains
            
        Raises:
            ClientError: If DynamoDB operation fails
            ValueError: If source_domain is not in the graph
            
        Requirements: 4.1, 4.2, 4.4, 4.8
        """
        start_time = time.time()
        
        logger.info(
            "Starting propagation calculation",
            extra={
                "scenario_id": scenario_id,
                "initial_impact": initial_impact,
                "source_domain": source_domain
            }
        )
        
        if source_domain not in self.domain_graph.nodes():
            raise ValueError(f"Source domain '{source_domain}' not found in domain graph")
        
        propagation_scores: List[PropagationScore] = []
        
        # Calculate first-order effects (direct connections)
        first_order_domains = list(self.domain_graph.successors(source_domain))
        
        for domain in first_order_domains:
            edge_weight = self.domain_graph[source_domain][domain]['weight']
            raw_impact = initial_impact * edge_weight
            normalized_impact = self.normalize_score(raw_impact)
            
            score = PropagationScore(
                domain=domain,
                impact_score=normalized_impact,
                propagation_order=1,
                affected_by=[source_domain]
            )
            propagation_scores.append(score)
            
            logger.debug(
                "First-order effect calculated",
                extra={
                    "domain": domain,
                    "edge_weight": edge_weight,
                    "raw_impact": raw_impact,
                    "normalized_impact": normalized_impact
                }
            )
        
        # Calculate second-order effects (indirect connections)
        second_order_impacts = {}
        
        for first_order_domain in first_order_domains:
            # Get the normalized impact for this first-order domain
            first_order_score = next(
                s.impact_score for s in propagation_scores 
                if s.domain == first_order_domain
            )
            
            # Find domains affected by this first-order domain
            second_order_domains = list(self.domain_graph.successors(first_order_domain))
            
            for domain in second_order_domains:
                # Skip if this is the source domain (avoid cycles)
                if domain == source_domain:
                    continue
                
                # Skip if this domain is already a first-order effect
                if domain in first_order_domains:
                    continue
                
                edge_weight = self.domain_graph[first_order_domain][domain]['weight']
                raw_impact = first_order_score * edge_weight
                
                # Accumulate impacts from multiple paths
                if domain not in second_order_impacts:
                    second_order_impacts[domain] = {
                        'total_impact': 0.0,
                        'affected_by': []
                    }
                
                second_order_impacts[domain]['total_impact'] += raw_impact
                second_order_impacts[domain]['affected_by'].append(first_order_domain)
        
        # Create PropagationScore objects for second-order effects
        for domain, impact_data in second_order_impacts.items():
            normalized_impact = self.normalize_score(impact_data['total_impact'])
            
            score = PropagationScore(
                domain=domain,
                impact_score=normalized_impact,
                propagation_order=2,
                affected_by=impact_data['affected_by']
            )
            propagation_scores.append(score)
            
            logger.debug(
                "Second-order effect calculated",
                extra={
                    "domain": domain,
                    "raw_impact": impact_data['total_impact'],
                    "normalized_impact": normalized_impact,
                    "affected_by": impact_data['affected_by']
                }
            )
        
        # Calculate total organizational impact
        total_impact = sum(score.impact_score for score in propagation_scores)
        
        # Create response
        response = PropagationResponse(
            scenario_id=scenario_id,
            scores=propagation_scores,
            total_organizational_impact=total_impact
        )
        
        # Store results in DynamoDB
        await self._store_results(response)
        
        execution_time = time.time() - start_time
        
        logger.info(
            "Propagation calculation completed",
            extra={
                "scenario_id": scenario_id,
                "affected_domains": len(propagation_scores),
                "total_organizational_impact": total_impact,
                "execution_time_seconds": execution_time
            }
        )
        
        return response
    
    def normalize_score(self, raw_score: float) -> float:
        """
        Normalize raw impact score to 0-10 scale.
        
        Uses a logarithmic scaling approach to map raw scores to the 0-10 range,
        ensuring that small impacts are distinguishable while large impacts
        don't exceed the maximum.
        
        Args:
            raw_score: Raw impact score (can be any positive value)
            
        Returns:
            float: Normalized score on 0-10 scale
            
        Requirements: 4.4
        """
        if raw_score <= 0:
            return 0.0
        
        # Use logarithmic scaling for better distribution
        # Formula: 10 * (1 - e^(-raw_score/2))
        # This maps [0, inf) to [0, 10] with good sensitivity for small values
        import math
        normalized = 10.0 * (1 - math.exp(-raw_score / 2.0))
        
        # Ensure we stay within bounds
        normalized = max(0.0, min(10.0, normalized))
        
        logger.debug(
            "Score normalized",
            extra={
                "raw_score": raw_score,
                "normalized_score": normalized
            }
        )
        
        return normalized

    async def _store_results(self, response: PropagationResponse) -> None:
        """
        Store propagation results in DynamoDB.
        
        Stores the complete propagation analysis with scenario_id as partition key
        and 'propagation' as record_type.
        
        Args:
            response: Complete propagation response to store
            
        Raises:
            ClientError: If DynamoDB operation fails
            
        Requirements: 4.8, 8.7
        """
        try:
            # Prepare DynamoDB item
            item = {
                "scenario_id": {"S": response.scenario_id},
                "record_type": {"S": "propagation"},
                "timestamp": {"S": datetime.utcnow().isoformat()},
                "data": {"S": response.model_dump_json()},
                "status": {"S": "completed"},
                "total_organizational_impact": {"N": str(response.total_organizational_impact)}
            }
            
            logger.debug(
                "Storing propagation results in DynamoDB",
                extra={
                    "table": self.config.dynamodb_table_name,
                    "scenario_id": response.scenario_id
                }
            )
            
            # Store in DynamoDB
            self.dynamodb_client.put_item(
                TableName=self.config.dynamodb_table_name,
                Item=item
            )
            
            logger.info(
                "Propagation results stored in DynamoDB",
                extra={
                    "scenario_id": response.scenario_id,
                    "affected_domains": len(response.scores)
                }
            )
            
        except ClientError as e:
            logger.error(
                "Failed to store propagation results in DynamoDB",
                extra={
                    "scenario_id": response.scenario_id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise
