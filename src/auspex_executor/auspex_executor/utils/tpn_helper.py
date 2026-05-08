#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Union
from enum import Enum, auto
from abc import ABC, abstractmethod
import json
import uuid


# ============================================================================
# Constants and Configuration
# ============================================================================

class TPNVersion:
    """TPN format version information"""
    VERSION = "0.4-0"
    DEFAULT_INFINITY = 4611686018427387903  # Large int for unbounded constraints


class DurationType(Enum):
    """Duration type classification (from temporal-networks)"""
    SIMPLE = "simpleDuration"               # Controllable, bounded duration
    CONTINGENT = "simpleContingentDuration"  # Uncontrollable, bounded duration  
    PROBABILISTIC = "probabilisticDuration"  # Probabilistic distribution


class DecisionVariableType(Enum):
    """Types of decision variables (from tpn-defs)"""
    CONTROLLABLE = "controllable"
    UNCONTROLLABLE = "uncontrollable"
    PROBABILISTIC = "probabilistic"


# ============================================================================
# Boolean Expressions (Guards and State Constraints)
# Following: enterprise/tpn/src/tpn-defs.lisp
# ============================================================================

class BooleanExpression(ABC):
    """
    Base class for boolean expressions used in guards and state constraints.
    Maps to: (defclass boolean-expression () ())
    """
    
    @abstractmethod
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        """Evaluate the expression given a context of variable bindings"""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict:
        """Serialize to dictionary format"""
        pass
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BooleanExpression':
        """Deserialize from dictionary"""
        expr_type = data.get("$type", "")
        if expr_type == "booleanConstant":
            return BooleanConstant(value=data.get("value", True))
        elif expr_type == "booleanNegation":
            return BooleanNegation(expression=cls.from_dict(data["expression"]))
        elif expr_type == "booleanConjunction":
            return BooleanConjunction(
                conjuncts=[cls.from_dict(c) for c in data.get("expressions", [])]
            )
        elif expr_type == "booleanDisjunction":
            return BooleanDisjunction(
                disjuncts=[cls.from_dict(d) for d in data.get("expressions", [])]
            )
        elif expr_type == "condition":
            return ConditionExpression(
                condition=data.get("condition", ""),
                values=data.get("values", []),
                when=data.get("when")
            )
        elif expr_type == "decisionVariableAssignment":
            return DecisionVariableAssignment(
                variable_id=data.get("variableId", ""),
                value=data.get("value")
            )
        else:
            # Default to true constant
            return BooleanConstant(value=True)


@dataclass
class BooleanConstant(BooleanExpression):
    """
    Boolean constant (True/False).
    Maps to: (defclass boolean-constant (boolean-expression) ...)
    """
    value: bool = True
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        return self.value
    
    def to_dict(self) -> Dict:
        return {"$type": "booleanConstant", "value": self.value}
    
    def __str__(self) -> str:
        return "True" if self.value else "False"


@dataclass 
class BooleanNegation(BooleanExpression):
    """
    Negation of an expression.
    Maps to: (defclass boolean-negation (boolean-expression) ...)
    """
    expression: BooleanExpression = field(default_factory=lambda: BooleanConstant(True))
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        return not self.expression.evaluate(context)
    
    def to_dict(self) -> Dict:
        return {
            "$type": "booleanNegation",
            "expression": self.expression.to_dict()
        }
    
    def __str__(self) -> str:
        return f"¬({self.expression})"


@dataclass
class BooleanConjunction(BooleanExpression):
    """
    Conjunction (AND) of expressions.
    Maps to: (defclass boolean-conjunction (boolean-expression) ...)
    """
    conjuncts: List[BooleanExpression] = field(default_factory=list)
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        if not self.conjuncts:
            return True
        return all(c.evaluate(context) for c in self.conjuncts)
    
    def to_dict(self) -> Dict:
        return {
            "$type": "booleanConjunction", 
            "expressions": [c.to_dict() for c in self.conjuncts]
        }
    
    def __str__(self) -> str:
        return f"({' ∧ '.join(str(c) for c in self.conjuncts)})"


@dataclass
class BooleanDisjunction(BooleanExpression):
    """
    Disjunction (OR) of expressions.
    Maps to: (defclass boolean-disjunction (boolean-expression) ...)
    """
    disjuncts: List[BooleanExpression] = field(default_factory=list)
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        if not self.disjuncts:
            return False
        return any(d.evaluate(context) for d in self.disjuncts)
    
    def to_dict(self) -> Dict:
        return {
            "$type": "booleanDisjunction",
            "expressions": [d.to_dict() for d in self.disjuncts]
        }
    
    def __str__(self) -> str:
        return f"({' v '.join(str(d) for d in self.disjuncts)})"


@dataclass
class ConditionExpression(BooleanExpression):
    """
    Condition over values (state variable checks, etc.).
    Maps to: (defclass condition-boolean-expression (boolean-expression) ...)
    """
    condition: str = ""  # e.g., "EQUALITY", "ABOVE", "BELOW"
    values: List[Any] = field(default_factory=list)
    when: Optional[Dict] = None  # When to check (at timepoint or between)
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        # Basic implementation - override for specific conditions
        if context is None:
            return True
        # Condition evaluation depends on the specific condition type
        return True
    
    def to_dict(self) -> Dict:
        result = {
            "$type": "condition",
            "condition": self.condition,
            "values": self.values
        }
        if self.when:
            result["when"] = self.when
        return result
    
    def __str__(self) -> str:
        return f"{self.condition}({', '.join(str(v) for v in self.values)})"


@dataclass
class DecisionVariableAssignment(BooleanExpression):
    """
    Assignment to a decision variable.
    Maps to: (defclass decision-variable-assignment (boolean-expression) ...)
    """
    variable_id: str = ""
    value: Any = None
    
    def evaluate(self, context: Dict[str, Any] = None) -> bool:
        if context is None:
            return True
        return context.get(self.variable_id) == self.value
    
    def to_dict(self) -> Dict:
        return {
            "$type": "decisionVariableAssignment",
            "variableId": self.variable_id,
            "value": self.value
        }
    
    def __str__(self) -> str:
        return f"{self.variable_id} = {self.value}"


# ============================================================================
# Duration Classes
# Following: enterprise/temporal-networks/src/constraints.lisp
# ============================================================================

@dataclass
class Duration:
    """
    Base duration with bounds.
    Maps to: (defclass bounded-duration () ...)
    """
    lower_bound: float = 0.0
    upper_bound: float = TPNVersion.DEFAULT_INFINITY
    duration_type: DurationType = DurationType.SIMPLE
    
    def __post_init__(self):
        """Ensure bounds are floats after initialization (handles JSON string values)."""
        try:
            self.lower_bound = float(self.lower_bound) if self.lower_bound is not None else 0.0
        except (ValueError, TypeError):
            self.lower_bound = 0.0
        try:
            self.upper_bound = float(self.upper_bound) if self.upper_bound is not None else float('inf')
        except (ValueError, TypeError):
            self.upper_bound = float('inf')
        if isinstance(self.duration_type, str):
            try:
                self.duration_type = DurationType(self.duration_type)
            except ValueError:
                self.duration_type = DurationType.SIMPLE
    
    def to_dict(self) -> Dict:
        return {
            "$type": self.duration_type.value,
            "lowerBound": self.lower_bound,
            "upperBound": self.upper_bound
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Duration':
        dtype_str = data.get("$type", DurationType.SIMPLE.value)
        try:
            dtype = DurationType(dtype_str)
        except ValueError:
            dtype = DurationType.SIMPLE
        
        # Explicitly convert to float (handles JSON string values from KB)
        try:
            lb = float(data.get("lowerBound", 0.0))
        except (ValueError, TypeError):
            lb = 0.0
        try:
            ub = float(data.get("upperBound", TPNVersion.DEFAULT_INFINITY))
        except (ValueError, TypeError):
            ub = float('inf')
            
        return cls(
            lower_bound=lb,
            upper_bound=ub,
            duration_type=dtype
        )
    
    def is_fixed(self) -> bool:
        """Check if duration is fixed (lb == ub)"""
        return self.lower_bound == self.upper_bound
    
    def is_contingent(self) -> bool:
        """Check if duration is uncontrollable"""
        return self.duration_type == DurationType.CONTINGENT


@dataclass
class ContingentDuration(Duration):
    """
    Contingent (uncontrollable) duration with observation delay.
    Maps to: (defclass simple-contingent-temporal-constraint ...)
    """
    min_observation_delay: float = 0.0
    max_observation_delay: float = 0.0
    
    def __post_init__(self):
        """Ensure all numeric fields are floats (handles JSON string values)."""
        # Call parent __post_init__ for lower_bound and upper_bound
        super().__post_init__()
        self.duration_type = DurationType.CONTINGENT
        # Convert observation delays
        try:
            self.min_observation_delay = float(self.min_observation_delay) if self.min_observation_delay is not None else 0.0
        except (ValueError, TypeError):
            self.min_observation_delay = 0.0
        try:
            self.max_observation_delay = float(self.max_observation_delay) if self.max_observation_delay is not None else 0.0
        except (ValueError, TypeError):
            self.max_observation_delay = 0.0
    
    def to_dict(self) -> Dict:
        base = super().to_dict()
        base["minObservationDelay"] = self.min_observation_delay
        base["maxObservationDelay"] = self.max_observation_delay
        return base
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ContingentDuration':
        # Explicitly convert all values to float (handles JSON string values from KB)
        try:
            lb = float(data.get("lowerBound", 0.0))
        except (ValueError, TypeError):
            lb = 0.0
        try:
            ub = float(data.get("upperBound", TPNVersion.DEFAULT_INFINITY))
        except (ValueError, TypeError):
            ub = float('inf')
        try:
            min_delay = float(data.get("minObservationDelay", 0.0))
        except (ValueError, TypeError):
            min_delay = 0.0
        try:
            max_delay = float(data.get("maxObservationDelay", 0.0))
        except (ValueError, TypeError):
            max_delay = 0.0
            
        return cls(
            lower_bound=lb,
            upper_bound=ub,
            min_observation_delay=min_delay,
            max_observation_delay=max_delay
        )


# ============================================================================
# TPN Event
# Following: enterprise/tpn/src/tpn-defs.lisp (tpn-event class)
# ============================================================================

@dataclass
class TPNEvent:
    """
    TPN Event - A dispatchable, guarded timepoint in the TPN.
    
    Maps to: (defclass tpn-event (temporal-event dispatchable-object guarded-object) ...)
    
    Events are nodes in the TPN graph. They can have:
    - guard: Boolean condition that must be true to execute
    - dispatch: String dispatch command/identifier
    - incoming_constraints: Episodes ending at this event
    - outgoing_constraints: Episodes starting from this event
    """
    id: str
    event_type: str = "event"  # 'start', 'end', 'task_start', 'task_end', 'event'
    
    # From guarded-object
    guard: BooleanExpression = field(default_factory=lambda: BooleanConstant(True))
    
    # From dispatchable-object
    dispatch: str = ""
    
    # Platform assignment (for multi-vehicle)
    platform: Optional[str] = None
    
    # For tracking constraints (populated when added to TPN)
    incoming_constraints: List[str] = field(default_factory=list)  # Episode IDs
    outgoing_constraints: List[str] = field(default_factory=list)  # Episode IDs
    
    # For contingent events - the constraint that makes this event uncontrollable
    contingent_constraint: Optional[str] = None  # Episode ID
    
    # Annotations and metadata
    annotations: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, TPNEvent):
            return self.id == other.id
        return False
    
    def is_controllable(self) -> bool:
        """Check if event is controllable (not the end of a contingent constraint)"""
        return self.contingent_constraint is None
    
    def to_dict(self) -> Dict:
        """Serialize event to dictionary (detailed format)"""
        event_dict = {
            "$id": self.id,
            "$type": self.event_type,
            "domain": {
                "$type": "realDomain",
                "ranges": [{
                    "bounds": [0, TPNVersion.DEFAULT_INFINITY],
                    "maxClosed": True,
                    "minClosed": True
                }]
            }
        }
        
        # Mark end events of contingent constraints as uncontrollable
        # Kirk server reads "uncontrollable": true from the event JSON
        if self.contingent_constraint is not None:
            event_dict["uncontrollable"] = True
        
        # Add guard if not trivially true
        if not (isinstance(self.guard, BooleanConstant) and self.guard.value):
            event_dict["guard"] = self.guard.to_dict()
        
        # Add dispatch if present
        if self.dispatch:
            event_dict["dispatch"] = self.dispatch
        
        # Add platform if present
        if self.platform:
            event_dict["platform"] = self.platform
        
        # Add annotations
        annotations = self.annotations.copy()
        if self.platform:
            annotations["platform"] = self.platform
        if annotations:
            event_dict["$annotations"] = annotations
        
        return event_dict
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TPNEvent':
        """Deserialize from dictionary"""
        annotations = data.get("$annotations", {})
        
        # Parse guard if present
        guard = BooleanConstant(True)
        if "guard" in data:
            guard = BooleanExpression.from_dict(data["guard"])
        
        return cls(
            id=data.get("$id", str(uuid.uuid4())),
            event_type=data.get("$type", "event"),
            guard=guard,
            dispatch=data.get("dispatch", ""),
            platform=data.get("platform") or annotations.get("platform"),
            annotations=annotations
        )


# ============================================================================
# Episode (Temporal Constraint)
# Following: enterprise/tpn/src/tpn-defs.lisp (episode, simple-guarded-episode classes)
# ============================================================================

@dataclass
class Episode:
    """
    Episode - A temporal constraint between two events with a state constraint.
    
    Maps to: (defclass simple-guarded-episode (simple-guarded-temporal-constraint episode) ...)
    
    Episodes are edges in the TPN graph connecting from_event to to_event.
    They have:
    - duration: Temporal bounds on (to_event.time - from_event.time)
    - guard: Boolean condition that must be true
    - state_constraint: Condition that must hold during the episode
    - dispatch: Command to execute
    """
    id: str
    from_event: str  # Event ID (start of episode)
    to_event: str    # Event ID (end of episode)
    duration: Duration = field(default_factory=Duration)
    
    # From guarded-object
    guard: BooleanExpression = field(default_factory=lambda: BooleanConstant(True))
    
    # State constraint (from episode class)
    state_constraint: BooleanExpression = field(default_factory=lambda: BooleanConstant(True))
    
    # From dispatchable-object
    dispatch: str = ""
    
    # Episode type for serialization
    episode_type: str = "episode"
    
    # Platform assignment
    platform: Optional[str] = None
    
    # Asks (preconditions)
    asks: Set[str] = field(default_factory=set)
    
    # Metadata
    annotations: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, Episode):
            return self.id == other.id
        return False
    
    @property
    def task_name(self) -> str:
        """Get task name from annotations (for logging)."""
        return self.annotations.get("task", self.annotations.get("task_name", ""))
    
    @property
    def task_id(self) -> Optional[int]:
        """Get task_id from annotations."""
        return int(self.annotations.get("task_id"))
    
    def add_ask(self, ask: str):
        """Add a precondition (ask) to this episode"""
        self.asks.add(ask)
    
    def is_contingent(self) -> bool:
        """Check if this is a contingent (uncontrollable) episode"""
        return self.duration.is_contingent()
    
    def to_dict(self) -> Dict:
        """Serialize episode to dictionary"""
        episode_dict = {
            "$id": self.id,
            "$type": self.episode_type,
            "startEvent": self.from_event,
            "endEvent": self.to_event,
            "duration": self.duration.to_dict()
        }
        
        # Add guard if not trivially true
        if not (isinstance(self.guard, BooleanConstant) and self.guard.value):
            episode_dict["guard"] = self.guard.to_dict()
        
        # Add state constraint if not trivially true
        if not (isinstance(self.state_constraint, BooleanConstant) and self.state_constraint.value):
            episode_dict["stateConstraint"] = self.state_constraint.to_dict()
        
        # Add dispatch if present
        if self.dispatch:
            episode_dict["dispatch"] = self.dispatch
        
        # Add platform if present
        if self.platform:
            episode_dict["platform"] = self.platform
        
        # Add asks
        if self.asks:
            episode_dict["asks"] = list(self.asks)
        
        # Add annotations
        if self.annotations:
            episode_dict["$annotations"] = self.annotations
        
        return episode_dict
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Episode':
        """Deserialize from dictionary"""
        # Parse duration
        duration_data = data.get("duration", {})
        dtype = duration_data.get("$type", DurationType.SIMPLE.value)
        
        if dtype == DurationType.CONTINGENT.value:
            duration = ContingentDuration.from_dict(duration_data)
        else:
            duration = Duration.from_dict(duration_data)
        
        # Parse guard and state constraint
        guard = BooleanConstant(True)
        if "guard" in data:
            guard = BooleanExpression.from_dict(data["guard"])
        
        state_constraint = BooleanConstant(True)
        if "stateConstraint" in data:
            state_constraint = BooleanExpression.from_dict(data["stateConstraint"])
        
        return cls(
            id=data.get("$id", str(uuid.uuid4())),
            from_event=data.get("startEvent", ""),
            to_event=data.get("endEvent", ""),
            duration=duration,
            guard=guard,
            state_constraint=state_constraint,
            dispatch=data.get("dispatch", ""),
            episode_type=data.get("$type", "episode"),
            platform=data.get("platform"),
            asks=set(data.get("asks", [])),
            annotations=data.get("$annotations", {})
        )


@dataclass
class ContingentEpisode(Episode):
    """
    Contingent Episode - Episode with uncontrollable duration.
    
    Maps to: (defclass simple-guarded-contingent-episode ...)
    
    The to_event of a contingent episode is uncontrollable - it will occur
    at some time within the duration bounds, but we don't control when.
    """
    
    def __post_init__(self):
        self.episode_type = "contingentEpisode"
        if not isinstance(self.duration, ContingentDuration):
            # Convert to contingent duration
            self.duration = ContingentDuration(
                lower_bound=self.duration.lower_bound,
                upper_bound=self.duration.upper_bound
            )


@dataclass
class MacroEpisode(Episode):
    """
    Macro Episode - References another TPN (for hierarchical composition).
    
    Maps to: (defclass macro-episode (simple-guarded-episode) ...)
    """
    macro_tpn_id: Optional[str] = None
    
    def __post_init__(self):
        self.episode_type = "macroEpisode"


# ============================================================================
# Decision Variable
# Following: enterprise/tpn/src/tpn-defs.lisp (decision-variable class)
# ============================================================================

@dataclass
class DecisionVariableValue:
    """
    A value in a decision variable's domain.
    Maps to: (defclass decision-variable-value () ...)
    """
    name: str
    utility: float = 0.0
    probability: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "utility": self.utility,
            "probability": self.probability
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DecisionVariableValue':
        return cls(
            name=data.get("name", ""),
            utility=data.get("utility", 0.0),
            probability=data.get("probability", 0.0)
        )


@dataclass
class DecisionVariable:
    """
    Decision Variable - A choice point in the TPN.
    
    Maps to: (defclass decision-variable (temporal-network-member guarded-object) ...)
    
    Decision variables represent choices that can be:
    - Controllable: We choose the value
    - Uncontrollable: Environment chooses the value
    - Probabilistic: Value is chosen according to probability distribution
    """
    id: str
    domain: List[DecisionVariableValue] = field(default_factory=list)
    variable_type: DecisionVariableType = DecisionVariableType.CONTROLLABLE
    at_event: Optional[str] = None  # Event ID where choice is made
    guard: BooleanExpression = field(default_factory=lambda: BooleanConstant(True))
    
    def to_dict(self) -> Dict:
        result = {
            "$id": self.id,
            "$type": "decisionVariable",
            "domain": [v.to_dict() for v in self.domain],
            "variableType": self.variable_type.value
        }
        if self.at_event:
            result["atEvent"] = self.at_event
        if not (isinstance(self.guard, BooleanConstant) and self.guard.value):
            result["guard"] = self.guard.to_dict()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DecisionVariable':
        var_type_str = data.get("variableType", DecisionVariableType.CONTROLLABLE.value)
        try:
            var_type = DecisionVariableType(var_type_str)
        except ValueError:
            var_type = DecisionVariableType.CONTROLLABLE
        
        guard = BooleanConstant(True)
        if "guard" in data:
            guard = BooleanExpression.from_dict(data["guard"])
        
        return cls(
            id=data.get("$id", str(uuid.uuid4())),
            domain=[DecisionVariableValue.from_dict(v) for v in data.get("domain", [])],
            variable_type=var_type,
            at_event=data.get("atEvent"),
            guard=guard
        )


# ============================================================================
# State Variable
# Following: enterprise/tpn/src/tpn-defs.lisp (state-variable class)
# ============================================================================

@dataclass
class StateVariable:
    """
    State Variable - Observable state value.
    
    Maps to: (defclass state-variable (temporal-network-member) ...)
    """
    id: str
    domain: List[str] = field(default_factory=list)  # Finite domain values
    initial_value: Optional[str] = None
    domain_type: str = "finite"  # "finite" or "continuous"
    continuous_bounds: Optional[tuple] = None  # (lower, upper) for continuous
    
    def to_dict(self) -> Dict:
        result = {
            "$id": self.id,
            "$type": "stateVariable"
        }
        if self.domain_type == "finite":
            result["domain"] = {
                "$type": "finiteVariableDomain",
                "values": self.domain
            }
        else:
            result["domain"] = {
                "$type": "continuousVariableDomain",
                "lowerBound": self.continuous_bounds[0] if self.continuous_bounds else 0,
                "upperBound": self.continuous_bounds[1] if self.continuous_bounds else 1
            }
        if self.initial_value is not None:
            result["initialValue"] = self.initial_value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StateVariable':
        domain_data = data.get("domain", {})
        domain_type = domain_data.get("$type", "finiteVariableDomain")
        
        if "finite" in domain_type.lower():
            return cls(
                id=data.get("$id", str(uuid.uuid4())),
                domain=domain_data.get("values", []),
                initial_value=data.get("initialValue"),
                domain_type="finite"
            )
        else:
            return cls(
                id=data.get("$id", str(uuid.uuid4())),
                domain=[],
                initial_value=data.get("initialValue"),
                domain_type="continuous",
                continuous_bounds=(
                    domain_data.get("lowerBound", 0),
                    domain_data.get("upperBound", 1)
                )
            )


# ============================================================================
# Temporal Plan Network
# Following: enterprise/tpn/src/tpn-defs.lisp (temporal-plan-network class)
# ============================================================================

@dataclass
class TemporalPlanNetwork:
    """
    Temporal Plan Network - The main TPN container.
    
    Maps to: (defclass temporal-plan-network (temporal-network) ...)
    
    A TPN consists of:
    - Events (timepoints) 
    - Episodes (temporal constraints between events)
    - Decision variables (choice points)
    - State variables (observable state)
    - Start and end events
    
    The TPN can be split by platform for multi-vehicle execution.
    """
    id: str = field(default_factory=lambda: f"TPN_{uuid.uuid4().hex[:8]}")
    name: str = ""
    plan_type: str = "statePlan"
    features: List[str] = field(default_factory=list)
    version: str = TPNVersion.VERSION
    
    # Start and end events
    start_event: str = ""
    end_event: str = ""
    
    # Events by ID
    events: Dict[str, TPNEvent] = field(default_factory=dict)
    
    # Episodes by ID  
    episodes: Dict[str, Episode] = field(default_factory=dict)
    
    # Decision variables by ID
    decision_variables: Dict[str, DecisionVariable] = field(default_factory=dict)
    
    # State variables by ID
    state_variables: Dict[str, StateVariable] = field(default_factory=dict)
    
    # Additional constraints (for STN-derived bounds)
    constraints: List[Dict] = field(default_factory=list)
    
    # Value episodes (for utility computation)
    value_episodes: List[Dict] = field(default_factory=list)
    
    # Platforms in this TPN
    platforms: Set[str] = field(default_factory=set)
    
    # Annotations
    annotations: Dict[str, Any] = field(default_factory=dict)
    
    def add_event(self, event: TPNEvent):
        """Add an event to the TPN"""
        if event.id in self.events:
            raise ValueError(f"Duplicate event ID: {event.id}")
        self.events[event.id] = event
        if event.platform:
            self.platforms.add(event.platform)
    
    def add_episode(self, episode: Episode):
        """Add an episode to the TPN and update event constraint lists"""
        if episode.id in self.episodes:
            raise ValueError(f"Duplicate episode ID: {episode.id}")
        
        self.episodes[episode.id] = episode
        
        # Update event constraint lists
        if episode.from_event in self.events:
            self.events[episode.from_event].outgoing_constraints.append(episode.id)
        if episode.to_event in self.events:
            self.events[episode.to_event].incoming_constraints.append(episode.id)
            # Mark contingent events
            if episode.is_contingent():
                self.events[episode.to_event].contingent_constraint = episode.id
        
        if episode.platform:
            self.platforms.add(episode.platform)
    
    def add_decision_variable(self, dv: DecisionVariable):
        """Add a decision variable"""
        if dv.id in self.decision_variables:
            raise ValueError(f"Duplicate decision variable ID: {dv.id}")
        self.decision_variables[dv.id] = dv
    
    def add_state_variable(self, sv: StateVariable):
        """Add a state variable"""
        if sv.id in self.state_variables:
            raise ValueError(f"Duplicate state variable ID: {sv.id}")
        self.state_variables[sv.id] = sv
    
    def add_constraint(self, constraint: Dict):
        """Add a temporal constraint"""
        self.constraints.append(constraint)
    
    def find_event(self, event_spec: Union[str, TPNEvent]) -> Optional[TPNEvent]:
        """Find an event by ID or instance"""
        if isinstance(event_spec, TPNEvent):
            return self.events.get(event_spec.id)
        return self.events.get(event_spec)
    
    def find_episode(self, episode_spec: Union[str, Episode]) -> Optional[Episode]:
        """Find an episode by ID or instance"""
        if isinstance(episode_spec, Episode):
            return self.episodes.get(episode_spec.id)
        return self.episodes.get(episode_spec)
    
    def get_events_for_platform(self, platform: str) -> List[TPNEvent]:
        """Get all events for a specific platform"""
        return [e for e in self.events.values() if e.platform == platform]
    
    def get_episodes_for_platform(self, platform: str) -> List[Episode]:
        """Get all episodes for a specific platform"""
        return [ep for ep in self.episodes.values() if ep.platform == platform]
    
    def get_root_events(self) -> List[str]:
        """
        Get root events (events with no incoming constraints).
        
        Root events are the starting points of the TPN - they have no
        episodes ending at them (no incoming_constraints).
        
        Returns:
            List of event IDs that are root events
        """
        root_events = []
        
        # First check if we have a designated start_event
        if self.start_event and self.start_event in self.events:
            root_events.append(self.start_event)
        
        # Find events with no incoming episodes
        events_with_incoming = set()
        for episode in self.episodes.values():
            events_with_incoming.add(episode.to_event)
        
        for event_id in self.events.keys():
            if event_id not in events_with_incoming and event_id not in root_events:
                root_events.append(event_id)
        
        # If no root events found, return all events (shouldn't happen in valid TPN)
        if not root_events:
            return list(self.events.keys())
        
        return root_events
    
    def get_leaf_events(self) -> List[str]:
        """
        Get leaf events (events with no outgoing constraints).
        
        Leaf events are the ending points of the TPN - they have no
        episodes starting from them (no outgoing_constraints).
        
        Returns:
            List of event IDs that are leaf events
        """
        leaf_events = []
        
        # First check if we have a designated end_event
        if self.end_event and self.end_event in self.events:
            leaf_events.append(self.end_event)
        
        # Find events with no outgoing episodes
        events_with_outgoing = set()
        for episode in self.episodes.values():
            events_with_outgoing.add(episode.from_event)
        
        for event_id in self.events.keys():
            if event_id not in events_with_outgoing and event_id not in leaf_events:
                leaf_events.append(event_id)
        
        return leaf_events
    
    def split_by_platform(self) -> Dict[str, 'TemporalPlanNetwork']:
        """
        Split TPN into per-platform TPNs.
        
        Each platform gets its own TPN with:
        - Its platform-specific events
        - Global events (no platform assignment)
        - Episodes involving its events
        - Relevant constraints
        """
        platform_tpns = {}
        
        # Identify global events
        global_event_ids = {eid for eid, event in self.events.items() 
                          if event.platform is None}
        
        for platform in self.platforms:
            # Create platform-specific TPN
            platform_tpn = TemporalPlanNetwork(
                id=f"{self.id}_{platform}",
                name=f"{self.name}_{platform}" if self.name else platform,
                plan_type=self.plan_type,
                features=self.features.copy(),
                version=self.version,
                start_event=self.start_event,
                end_event=self.end_event
            )
            
            # Get platform event IDs
            platform_event_ids = {eid for eid, event in self.events.items()
                                 if event.platform == platform}
            
            relevant_event_ids = global_event_ids | platform_event_ids
            
            # Add relevant events
            for event_id in relevant_event_ids:
                event = self.events[event_id]
                # Create a copy of the event
                new_event = TPNEvent(
                    id=event.id,
                    event_type=event.event_type,
                    guard=event.guard,
                    dispatch=event.dispatch,
                    platform=event.platform,
                    annotations=event.annotations.copy()
                )
                platform_tpn.add_event(new_event)
            
            # Add relevant episodes
            for episode in self.episodes.values():
                if episode.platform == platform:
                    new_episode = Episode(
                        id=episode.id,
                        from_event=episode.from_event,
                        to_event=episode.to_event,
                        duration=Duration(
                            lower_bound=episode.duration.lower_bound,
                            upper_bound=episode.duration.upper_bound,
                            duration_type=episode.duration.duration_type
                        ),
                        guard=episode.guard,
                        state_constraint=episode.state_constraint,
                        dispatch=episode.dispatch,
                        episode_type=episode.episode_type,
                        platform=episode.platform,
                        asks=episode.asks.copy(),
                        annotations=episode.annotations.copy()
                    )
                    platform_tpn.add_episode(new_episode)
                # Include global episodes whose events are all in this platform's scope
                # (bridges global↔platform events, or purely global like mission bound)
                elif (episode.platform is None and 
                      episode.from_event in relevant_event_ids and 
                      episode.to_event in relevant_event_ids):
                    new_episode = Episode(
                        id=episode.id,
                        from_event=episode.from_event,
                        to_event=episode.to_event,
                        duration=Duration(
                            lower_bound=episode.duration.lower_bound,
                            upper_bound=episode.duration.upper_bound,
                            duration_type=episode.duration.duration_type
                        ),
                        guard=episode.guard,
                        state_constraint=episode.state_constraint,
                        dispatch=episode.dispatch,
                        episode_type=episode.episode_type,
                        platform=platform,
                        asks=episode.asks.copy(),
                        annotations=episode.annotations.copy()
                    )
                    platform_tpn.add_episode(new_episode)
            
            # Add relevant constraints
            for constraint in self.constraints:
                # Check if constraint references only relevant events
                from_ref = constraint.get("expression", {}).get("from", {}).get("ref")
                to_ref = constraint.get("expression", {}).get("to", {}).get("ref")
                if from_ref in relevant_event_ids and to_ref in relevant_event_ids:
                    platform_tpn.add_constraint(constraint.copy())
            
            platform_tpns[platform] = platform_tpn
        
        return platform_tpns
    
    def to_dict(self) -> Dict:
        """Serialize TPN to dictionary (matching enterprise format)"""
        # Build events list
        events_list = [event.to_dict() for event in self.events.values()]
        
        # Build episodes list (goal episodes)
        episodes_list = [episode.to_dict() for episode in self.episodes.values()]
        
        return {
            "goalPlan": {
                "$id": self.id,
                "$type": self.plan_type,
                "$features": self.features,
                "stateSpace": {
                    "$type": "stateSpace",
                    "events": events_list,
                    "stateVariables": [sv.to_dict() for sv in self.state_variables.values()],
                    "variables": [dv.to_dict() for dv in self.decision_variables.values()]
                },
                "startEvent": self.start_event,
                "constraints": self.constraints,
                "goalEpisodes": episodes_list,
                "valueEpisodes": self.value_episodes,
                "$annotations": self.annotations,
                "$version": self.version
            }
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Serialize TPN to JSON string"""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TemporalPlanNetwork':
        """Deserialize TPN from dictionary"""
        goal_plan = data.get("goalPlan", data)
        
        tpn = cls(
            id=goal_plan.get("$id", f"TPN_{uuid.uuid4().hex[:8]}"),
            name=goal_plan.get("name", ""),
            plan_type=goal_plan.get("$type", "statePlan"),
            features=goal_plan.get("$features", []),
            version=goal_plan.get("$version", TPNVersion.VERSION),
            start_event=goal_plan.get("startEvent", "GLOBAL_START"),
            annotations=goal_plan.get("$annotations", {})
        )
        
        # Parse state space
        state_space = goal_plan.get("stateSpace", {})
        
        # Parse events
        for event_data in state_space.get("events", []):
            event = TPNEvent.from_dict(event_data)
            tpn.events[event.id] = event
            if event.platform:
                tpn.platforms.add(event.platform)
        
        # Parse state variables
        for sv_data in state_space.get("stateVariables", []):
            sv = StateVariable.from_dict(sv_data)
            tpn.state_variables[sv.id] = sv
        
        # Parse decision variables
        for dv_data in state_space.get("variables", []):
            dv = DecisionVariable.from_dict(dv_data)
            tpn.decision_variables[dv.id] = dv
        
        # Parse episodes
        for episode_data in goal_plan.get("goalEpisodes", []):
            episode = Episode.from_dict(episode_data)
            tpn.episodes[episode.id] = episode
            if episode.platform:
                tpn.platforms.add(episode.platform)
            # Update event constraint lists
            if episode.from_event in tpn.events:
                tpn.events[episode.from_event].outgoing_constraints.append(episode.id)
            if episode.to_event in tpn.events:
                tpn.events[episode.to_event].incoming_constraints.append(episode.id)
                if episode.is_contingent():
                    tpn.events[episode.to_event].contingent_constraint = episode.id
        
        # Parse constraints
        tpn.constraints = goal_plan.get("constraints", [])
        
        # Parse value episodes
        tpn.value_episodes = goal_plan.get("valueEpisodes", [])
        
        return tpn
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TemporalPlanNetwork':
        """Deserialize TPN from JSON string"""
        return cls.from_dict(json.loads(json_str))
    
    def __str__(self) -> str:
        return (f"TPN({self.id}, events={len(self.events)}, "
                f"episodes={len(self.episodes)}, platforms={self.platforms})")
    
    def __repr__(self) -> str:
        return self.__str__()


# ============================================================================
# TPN Configuration (for backward compatibility with planner)
# ============================================================================

class TPNConfig:
    """Configuration parameters for TPN generation"""
    
    # Version and metadata
    TPN_VERSION = TPNVersion.VERSION
    PLAN_ID = "multi_platform_mission"
    PLAN_TYPE = "statePlan"
    
    # Duration type defaults
    DEFAULT_CONTINGENT_TYPE = DurationType.CONTINGENT.value
    DEFAULT_DURATION_TYPE = DurationType.SIMPLE.value
    
    # State space features
    FEATURES = []
    
    # Contingency tolerance for contingent tasks
    CONTINGENT_TOLERANCE = 0.2  # 20% tolerance
    
    # Default constraint bounds
    DEFAULT_CONSTRAINT_LOWER = 0
    DEFAULT_CONSTRAINT_UPPER = 600 # TPNVersion.DEFAULT_INFINITY


# ============================================================================
# Utility Functions
# ============================================================================

def create_ordering_episode(
    from_event_id: str,
    to_event_id: str,
    platform: Optional[str] = None,
    min_delay: float = 0.0,
    max_delay: float = TPNVersion.DEFAULT_INFINITY
) -> Episode:
    """Create an ordering episode (precedence constraint) between two events"""
    return Episode(
        id=f"ordering_{from_event_id}_to_{to_event_id}",
        from_event=from_event_id,
        to_event=to_event_id,
        duration=Duration(
            lower_bound=min_delay,
            upper_bound=max_delay,
            duration_type=DurationType.SIMPLE
        ),
        platform=platform,
        annotations={"type": "ordering"}
    )


def create_task_episode(
    task_id: str,
    task_name: str,
    platform: str,
    duration: float,
    is_contingent: bool = False,
    params: Optional[Dict] = None
) -> tuple[TPNEvent, TPNEvent, Episode]:
    """
    Create events and episode for a task.
    
    Returns: (start_event, end_event, episode)
    """
    start_event_id = f"{platform}_{task_name}_{task_id}_start"
    end_event_id = f"{platform}_{task_name}_{task_id}_end"
    
    # Flatten params into annotations (no nested objects for Kirk compatibility)
    start_annotations = {
        "task": task_name,
        "task_id": task_id,
        "platform": platform
    }
    if params:
        # Flatten params - convert keys to camelCase for consistency
        if "raw_params" in params:
            start_annotations["rawParams"] = params["raw_params"]
        if "start_time" in params:
            start_annotations["startTime"] = params["start_time"]
    
    end_annotations = start_annotations.copy()
    
    start_event = TPNEvent(
        id=start_event_id,
        event_type="event",  # Use 'event' for Kirk compatibility
        platform=platform,
        annotations=start_annotations
    )
    
    end_event = TPNEvent(
        id=end_event_id,
        event_type="event",  # Use 'event' for Kirk compatibility
        platform=platform,
        annotations=end_annotations
    )
    
    if is_contingent:
        tolerance = TPNConfig.CONTINGENT_TOLERANCE
        episode_duration = ContingentDuration(
            lower_bound=duration * (1 - tolerance),
            upper_bound=duration * (1 + tolerance)
        )
    else:
        episode_duration = Duration(
            lower_bound=duration,
            upper_bound=duration,
            duration_type=DurationType.SIMPLE
        )
    
    episode = Episode(
        id=f"episode_{platform}_{task_name}_{task_id}",
        from_event=start_event_id,
        to_event=end_event_id,
        duration=episode_duration,
        platform=platform,
        annotations={
            "task": task_name,
            "task_id": task_id,
            "rawParams": params.get("raw_params", "") if params else "",
            "startTime": params.get("start_time", 0.0) if params else 0.0
        }
    )
    
    return start_event, end_event, episode
