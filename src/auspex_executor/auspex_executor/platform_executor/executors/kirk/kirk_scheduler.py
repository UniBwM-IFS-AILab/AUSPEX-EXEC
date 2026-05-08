#!/usr/bin/env python3
"""
Kirk Dynamic Scheduler Module

Python translation of MIT enterprise kirk-v2 dynamic scheduler.
Implements the FAST-EX algorithm for real-time temporal execution 
under Variable Delay Controllability (VDC) semantics.

The scheduler maintains a distance graph and computes Real-Time Execution 
Decisions (RTEDs) - which events to dispatch at what time.

Key concepts:
- **Observation Delay (γ)**: Time between when a contingent event actually occurs
  and when we observe it. Configured via `min_observation_delay` and 
  `max_observation_delay` on ContingentDuration.
- **Variable-to-Fixed Delay Conversion**: Transforms variable delays [γ_min, γ_max]
  to a fixed delay γ = γ_max for worst-case scheduling (Bhargava 2022).
- **Default Delay**: When no observation delay is specified, uses `default_delay`
  parameter (default: 0.0 seconds).

Reference: 
- enterprise/kirk-v2/src/scheduler/dynamic/dynamic.lisp
- enterprise/temporal-controllability/src/variable-delay-controllability.lisp
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set, Any
from enum import Enum
import heapq
import math
import logging
import copy

from auspex_executor.utils.tpn_helper import (
    TemporalPlanNetwork, TPNEvent, Episode, ContingentEpisode,
    Duration, ContingentDuration, BooleanExpression, BooleanConstant
)


# ============================================================================
# Constants
# ============================================================================

ZERO_POINT = "__ZERO_POINT__"  # Special event representing time origin
INFINITY = float('inf')


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class RTED:
    """
    Real-Time Execution Decision.
    Represents a decision point: at time `time`, dispatch the events in `events`.
    
    Fields:
        time: Dispatch time
        events: List of (event_id, is_noop) tuples
        anticipated: If True, these events are dispatched via task anticipation
                    (external asks not yet fulfilled). The executor should only
                    dispatch non-critical actions for anticipated tasks.
        unfulfilled_asks: Set of external ask event_ids that are not yet fulfilled
                         for anticipated events. When these arrive, critical actions
                         can proceed.
    
    Maps to: (defstruct rted ...) in dynamic.lisp
    """
    time: float
    events: List[Tuple[str, bool]]  # List of (event_id, is_noop)
    anticipated: bool = False  # True if dispatched via task anticipation
    unfulfilled_asks: Set[str] = field(default_factory=set)
    
    def __repr__(self):
        antic = " [ANTICIPATED]" if self.anticipated else ""
        return f"RTED(time={self.time:.3f}, events={self.events}{antic})"


@dataclass
class EventState:
    """Tracks execution state of an event."""
    event_id: str
    executed: bool = False
    execution_time: Optional[float] = None
    is_contingent: bool = False
    
    def __repr__(self):
        status = f"executed@{self.execution_time:.3f}" if self.executed else "pending"
        ctype = " [contingent]" if self.is_contingent else ""
        return f"EventState({self.event_id}: {status}{ctype})"


class KirkScheduler:
    """
    Dynamic scheduler implementing FAST-EX algorithm for temporal execution.
    
    Maintains:
    - Distance graph: {(from_event, to_event): distance}
    - Commitments: Time windows for each event
    - Event execution status
    - Gamma (y) values: Fixed observation delays per contingent event
    
    The scheduler follows Variable Delay Controllability (VDC) semantics:
    - Contingent events are buffered to their lower bounds
    - Unobserved contingent events are assumed to occur at upper bounds
    - Observation delays are converted from variable [y_min, y_max] to fixed y
    
    Observation Delay Configuration:
    - Set `default_delay` to apply a default observation delay to all contingent
      events that don't have explicit delays specified
    - Explicit delays are set via `min_observation_delay` and `max_observation_delay`
      on ContingentDuration in the TPN
    - The fixed delay y = max_observation_delay (worst-case assumption)
    
    Reference: enterprise/kirk-v2/src/scheduler/dynamic/dynamic.lisp
    """
    
    def __init__(
        self,
        tpn: TemporalPlanNetwork,
        default_delay: float = 0.0,
        reschedule_early_ctg: bool = False,
        use_fixed_delay_conversion: bool = True,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the scheduler with a TPN.
        
        Args:
            tpn: The temporal plan network to schedule
            default_delay: Default observation delay (in seconds) for contingent
                          events without explicit delays. Set this to account for
                          communication latency, sensor processing time, etc.
                          Default: 0.0 (no delay)
            reschedule_early_ctg: Whether to reschedule when contingent events 
                                  are observed early (before lower bound)
            use_fixed_delay_conversion: Whether to convert variable delays to
                                       fixed delays using VDC algorithm.
                                       Default: True
            verbose: Enable verbose logging
            logger: Optional logger instance
        """
        self._tpn = tpn
        self._default_delay = default_delay
        self._reschedule_early_ctg = reschedule_early_ctg
        self._use_fixed_delay_conversion = use_fixed_delay_conversion
        self._verbose = verbose
        self._logger = logger or logging.getLogger(__name__)
        
        # Distance graph: {(from_event, to_event): distance}
        # Negative distances mean from_event must happen before to_event
        self._distances: Dict[Tuple[str, str], float] = {}
        
        # Event states
        self._event_states: Dict[str, EventState] = {}
        
        # Cache for events and episodes
        self._events: Dict[str, TPNEvent] = {}
        self._episodes: Dict[str, Episode] = {}
        
        # Contingent event tracking
        self._contingent_events: Set[str] = set()
        self._contingent_source: Dict[str, str] = {}  # contingent_event -> source_event
        
        # Gamma (y) values: Fixed observation delay per contingent event
        # Maps event_id -> fixed delay (seconds)
        # This is the delay between when the event actually occurs and when we observe it
        self._gamma: Dict[str, float] = {}
        
        # Single-task execution tracking
        # Maps start_event -> end_event for each episode
        self._start_to_end: Dict[str, str] = {}
        # Maps end_event -> start_event for each episode
        self._end_to_start: Dict[str, str] = {}
        # Set of start events (to identify task boundaries)
        self._start_events: Set[str] = set()
        # Currently in-progress episode end events (start dispatched, end not yet executed)
        # When non-empty, no other task start events should be enabled
        self._in_progress_end_events: Set[str] = set()
        
        # External events: events from other platforms that may never arrive
        # Only these events get upper-bound no-ops in collect_rted()
        # Own task events (from TPN) should wait indefinitely for confirmation
        self._external_events: Set[str] = set()
        
        # Fulfilled external events: tracks events that have been received/executed.
        # Maps event_id -> execution_time.  This survives across the entire
        # execution so that late registrations (register_external_event_wait)
        # can detect that the event was already fulfilled.
        self._fulfilled_events: Dict[str, float] = {}
        
        # ═══════════════════════════════════════════════════════════════════════
        # TASK ANTICIPATION
        # ═══════════════════════════════════════════════════════════════════════
        # When enabled, events blocked by unfulfilled external asks can be
        # dispatched early in "anticipation" mode. The executor then only
        # dispatches non-critical actions until the asks are fulfilled.
        self._task_anticipation: bool = False
        
        # Tracks which events are currently dispatched in anticipation mode
        # Maps start_event_id -> set of unfulfilled ask event_ids
        self._anticipated_events: Dict[str, Set[str]] = {}
        
        # Critical action indices per task name.
        # Maps task_name -> list of 0-based action indices that are critical.
        # Critical actions require all external asks to be fulfilled before execution.
        # Non-critical actions can proceed during anticipation.
        # Example: {"deliver": [1, 2, 3]} means for task "deliver", actions at
        # indices 1, 2, 3 are critical (action 0 "fly_to" can proceed early).
        self._critical_action_indices: Dict[str, List[int]] = {}
        
        # Internal: anticipation-eligible events found by get_enabled_events
        # List of (event_id, set_of_blocking_external_asks)
        self._anticipation_eligible: List[Tuple[str, Set[str]]] = []
        
        # Initialize from TPN
        self._init_from_tpn()
    
    @property
    def default_delay(self) -> float:
        """Get the default observation delay."""
        return self._default_delay
    
    @default_delay.setter
    def default_delay(self, value: float):
        """
        Set the default observation delay.
        
        Note: This only affects new gamma computations. Call reinitialize()
        to recompute with new default.
        """
        self._default_delay = value
    
    def get_gamma(self, event_id: str) -> float:
        """
        Get the fixed observation delay (gamma) for an event.
        
        Args:
            event_id: Event ID
            
        Returns:
            Fixed delay in seconds (0.0 for controllable events)
        """
        return self._gamma.get(event_id, 0.0)
    
    def set_gamma(self, event_id: str, delay: float):
        """
        Manually set the fixed observation delay for an event.
        
        Args:
            event_id: Event ID
            delay: Delay in seconds
        """
        self._gamma[event_id] = delay
        
    def _init_from_tpn(self):
        """Initialize scheduler state from the TPN."""
        # Build event and episode lookups
        for event in self._tpn.events.values():
            self._events[event.id] = event
            self._event_states[event.id] = EventState(
                event_id=event.id,
                is_contingent=(event.contingent_constraint is not None)
            )
            if event.contingent_constraint:
                self._contingent_events.add(event.id)
        
        for episode in self._tpn.episodes.values():
            self._episodes[episode.id] = episode
            
            # Build start->end and end->start mappings for single-task execution
            # Only for "task" episodes (those with task_id in annotations)
            if episode.annotations and episode.annotations.get('task_id') is not None:
                self._start_to_end[episode.from_event] = episode.to_event
                self._end_to_start[episode.to_event] = episode.from_event
                self._start_events.add(episode.from_event)
            
            # Track contingent episode relationships
            if episode.is_contingent():
                self._contingent_events.add(episode.to_event)
                self._contingent_source[episode.to_event] = episode.from_event
                self._event_states[episode.to_event].is_contingent = True
                
                # Compute gamma (fixed delay) for this contingent event
                self._compute_gamma_for_episode(episode)
        
        # Initialize distance graph from TPN constraints
        self._init_distance_graph()
    
    def _compute_gamma_for_episode(self, episode: Episode):
        """
        Compute the fixed observation delay (gamma) for a contingent episode.
        
        Implements Bhargava 2022 Algorithm 1 for variable-to-fixed delay conversion.
        Uses max_observation_delay as the fixed delay (worst-case assumption).
        
        Args:
            episode: Contingent episode
        """
        event_id = episode.to_event
        
        # Get observation delays from duration
        if isinstance(episode.duration, ContingentDuration):
            # Defensive conversion to float (handles string values from KB)
            try:
                min_delay = float(episode.duration.min_observation_delay)
            except (ValueError, TypeError):
                min_delay = self._default_delay
            try:
                max_delay = float(episode.duration.max_observation_delay)
            except (ValueError, TypeError):
                max_delay = self._default_delay
        else:
            # No explicit delays - use default
            min_delay = self._default_delay
            max_delay = self._default_delay
        
        # Handle unspecified delays (0.0) - use default
        if max_delay == 0.0 and min_delay == 0.0 and self._default_delay > 0.0:
            max_delay = self._default_delay
            min_delay = self._default_delay
        
        # Variable-to-fixed delay conversion (Bhargava 2022):
        # For worst-case scheduling, use γ = max_observation_delay
        if self._use_fixed_delay_conversion:
            self._gamma[event_id] = max_delay
        else:
            # Use min delay for optimistic scheduling
            self._gamma[event_id] = min_delay
        
        if self._verbose:
            self._log(
                f"Gamma for {event_id}: y={self._gamma[event_id]:.3f}s "
                f"(from [{min_delay:.3f}, {max_delay:.3f}])"
            )
        
    def _init_distance_graph(self):
        """
        Initialize the distance graph from TPN episodes.
        
        The distance d(a, b) represents the maximum time difference (b - a).
        For an episode a->b with duration [lb, ub]:
          - d(a, b) = ub  (b can be at most ub after a)
          - d(b, a) = -lb (b must be at least lb after a, so a is at most -lb after b)
        
        For contingent episodes with observation delay y (fixed-delay conversion):
          - The observed time is shifted by y from actual time
          - Bounds are adjusted: [lb + y, ub + y] for the observation
          - This implements the VDC fixed-delay equivalent STNU
        """
        # First pass: collect all external ask events and create placeholder events
        # Ask events are external events from other platforms that we need to wait for
        external_ask_events: Set[str] = set()
        for episode in self._episodes.values():
            for ask_event_id in episode.asks:
                if ask_event_id not in self._events and ask_event_id != ZERO_POINT:
                    external_ask_events.add(ask_event_id)
        
        # Create placeholder EventStates for external ask events
        for ask_event_id in external_ask_events:
            self._event_states[ask_event_id] = EventState(
                event_id=ask_event_id,
                is_contingent=True  # External events are contingent (we observe them)
            )
            self._contingent_events.add(ask_event_id)
            self._external_events.add(ask_event_id)  # Register as external for no-op handling
            if self._verbose:
                self._log(f"Created placeholder for external ask event: {ask_event_id}")
        
        # Add ZERO_POINT as the reference point
        self._event_states[ZERO_POINT] = EventState(
            event_id=ZERO_POINT,
            executed=True,
            execution_time=0.0
        )
        
        # Initialize distances with infinity (include external ask events)
        all_events = [ZERO_POINT] + list(self._events.keys()) + list(external_ask_events)
        for e1 in all_events:
            for e2 in all_events:
                if e1 == e2:
                    self._distances[(e1, e2)] = 0.0
                else:
                    self._distances[(e1, e2)] = INFINITY
        
        # Set distances from episode constraints
        for episode in self._episodes.values():
            from_evt = episode.from_event
            to_evt = episode.to_event
            
            # Defensive conversion to float (in case source data wasn't properly converted)
            try:
                lb = float(episode.duration.lower_bound)
            except (ValueError, TypeError):
                self._log(f"Warning: Could not convert lower_bound '{episode.duration.lower_bound}' to float, using 0.0")
                lb = 0.0
            try:
                ub = float(episode.duration.upper_bound)
            except (ValueError, TypeError):
                self._log(f"Warning: Could not convert upper_bound '{episode.duration.upper_bound}' to float, using infinity")
                ub = INFINITY
            
            # For contingent episodes, adjust bounds by gamma (fixed delay)
            # This is the key VDC fixed-delay conversion
            if episode.is_contingent() and self._use_fixed_delay_conversion:
                gamma = self._gamma.get(to_evt, 0.0)
                # The observation time is actual_time + gamma
                # So effective bounds become [lb + gamma, ub + gamma]
                lb_adjusted = lb + gamma
                ub_adjusted = ub + gamma
                
                if self._verbose:
                    self._log(
                        f"Episode {episode.id}: [{lb:.3f}, {ub:.3f}] "
                        f"-> [{lb_adjusted:.3f}, {ub_adjusted:.3f}] (γ={gamma:.3f})"
                    )
            else:
                lb_adjusted = lb
                ub_adjusted = ub
            
            # Forward constraint: to - from <= ub
            self._distances[(from_evt, to_evt)] = min(
                self._distances.get((from_evt, to_evt), INFINITY), ub_adjusted
            )
            # Backward constraint: from - to <= -lb (i.e., to - from >= lb)
            self._distances[(to_evt, from_evt)] = min(
                self._distances.get((to_evt, from_evt), INFINITY), -lb_adjusted
            )
            
            # Process asks (preconditions): events that must happen before this episode starts
            # For each ask event, add constraint: ask_event must happen before from_event
            # 
            # Distance semantics: d(A, B) = max(B - A)
            # If d(from_evt, ask_evt) < 0, then ask_evt - from_evt < 0, so ask_evt < from_evt
            # This means ask_evt must happen strictly before from_evt
            #
            # We use a small epsilon to encode "strictly before" as a distance constraint
            # d(from_evt, ask_evt) = -epsilon means ask_evt must be at least epsilon before from_evt
            for ask_event_id in episode.asks:
                # Check if ask event exists (either in our TPN events or as external placeholder)
                if ask_event_id in self._event_states:
                    # Strict precedence: ask_evt must happen before from_evt
                    # Using small epsilon to encode this in the distance graph
                    ASK_EPSILON = 0.001  # 1ms strict precedence
                    self._distances[(from_evt, ask_event_id)] = min(
                        self._distances.get((from_evt, ask_event_id), INFINITY), -ASK_EPSILON
                    )
                    if self._verbose:
                        self._log(f"Ask constraint: {ask_event_id} must happen before {from_evt}")
                else:
                    self._log(f"Warning: Ask event '{ask_event_id}' not found in events or external asks")
        
        # Initialize ZERO_POINT connections to start events
        start_events = self._tpn.get_root_events()
        for evt_id in start_events:
            # Start events can happen at time 0 or later: d(ZERO, start) = inf, d(start, ZERO) = 0
            self._distances[(ZERO_POINT, evt_id)] = INFINITY
            self._distances[(evt_id, ZERO_POINT)] = 0.0
        
        # Initialize ZERO_POINT connections for external ask events
        # External events can happen at any time >= 0
        for ask_evt in external_ask_events:
            self._distances[(ZERO_POINT, ask_evt)] = INFINITY  # Can happen at any time
            self._distances[(ask_evt, ZERO_POINT)] = 0.0       # Must be >= 0
        
        # Run Floyd-Warshall for initial all-pairs shortest paths
        self._floyd_warshall()
        
    def _floyd_warshall(self):
        """Compute all-pairs shortest paths using Floyd-Warshall algorithm."""
        # Include all events in event_states (TPN events + external ask events)
        all_events = [evt for evt in self._event_states.keys()]
        n = len(all_events)
        
        for k_evt in all_events:
            for i_evt in all_events:
                for j_evt in all_events:
                    d_ik = self._distances.get((i_evt, k_evt), INFINITY)
                    d_kj = self._distances.get((k_evt, j_evt), INFINITY)
                    d_ij = self._distances.get((i_evt, j_evt), INFINITY)
                    
                    if d_ik + d_kj < d_ij:
                        self._distances[(i_evt, j_evt)] = d_ik + d_kj
        
    def has_unexecuted_events(self) -> bool:
        """Check if there are still events to execute."""
        for evt_id, state in self._event_states.items():
            if evt_id != ZERO_POINT and not state.executed:
                return True
        return False
    
    def get_commitments(self) -> Dict[Tuple[str, bool], Tuple[float, float]]:
        """
        Get time window commitments for all unexecuted events.
        
        Returns:
            Dict mapping (event_id, is_contingent) to (lower_bound, upper_bound)
            
        Reference: get-commitments in dynamic.lisp
        """
        commitments = {}
        
        for evt_id, state in self._event_states.items():
            if evt_id == ZERO_POINT or state.executed:
                continue
            
            # Lower bound: -d(event, ZERO_POINT) 
            # Upper bound: d(ZERO_POINT, event)
            lb = -self._distances.get((evt_id, ZERO_POINT), 0.0)
            ub = self._distances.get((ZERO_POINT, evt_id), INFINITY)
            
            # Ensure bounds are valid
            lb = max(0.0, lb)
            if ub < lb:
                ub = lb  # Clamp to valid range
            
            is_ctg = evt_id in self._contingent_events
            commitments[(evt_id, is_ctg)] = (lb, ub)
        
        return commitments
    
    def get_enabled_events(self) -> List[str]:
        """
        Get events that are enabled for execution.
        
        An event n1 is enabled if for all OTHER UNEXECUTED events n2:
          d(n1, n2) >= 0
        
        Distance semantics:
          d(A, B) = maximum value of (B - A), i.e., max time B can be after A
          
          If d(n1, n2) < 0, then n2 - n1 <= negative, meaning n1 - n2 >= |d(n1,n2)|
          This means n1 must occur AFTER n2 by at least |d(n1,n2)| time units.
          
          So if d(n1, n2) < 0 and n2 is unexecuted:
            - n1 requires n2 to happen first (n2 is a predecessor of n1)
            - n1 cannot be dispatched until n2 is executed
            - n1 is NOT enabled
        
        In other words: d(X, Y) < 0 means "Y must complete before X can start".
        So for event X to be enabled, all events Y where d(X, Y) < 0 must
        already be executed (their "tell" has been received).
        
        Single-Task Execution:
        When a task is in progress (start event executed, end event not yet),
        only the in-progress task's end event is enabled. No other task start
        events can be enabled until the current task completes.
        
        Task Anticipation:
        When task_anticipation is enabled and the system is idle (no task in
        progress), events that are ONLY blocked by unfulfilled external asks
        can be returned as "anticipation-eligible". These are tracked separately
        and the caller (collect_rted) marks the resulting RTED as anticipated.
        
        Reference: get-enabled-events in dynamic.lisp (Fig 3, Hunsberger 2013)
        """
        enabled = []
        anticipation_eligible = []  # Events blocked only by external asks
        
        # Single-task execution: check if any task is currently in progress
        task_in_progress = len(self._in_progress_end_events) > 0
        
        for n1, state1 in self._event_states.items():
            if n1 == ZERO_POINT or state1.executed:
                continue
            
            # Single-task execution constraint:
            # If a task is in progress, only enable its end event(s), not other start events
            if task_in_progress:
                if n1 in self._start_events:
                    # This is a start event for another task - block it
                    if self._verbose:
                        self._log(f"Event {n1} blocked: task in progress (waiting for {self._in_progress_end_events})")
                    continue
            
            # Check precedence: for all OTHER UNEXECUTED events n2
            is_enabled = True
            blocking_external_asks = set()  # Track which external asks are blocking
            
            for n2, state2 in self._event_states.items():
                if n2 == n1 or n2 == ZERO_POINT:
                    continue
                if state2.executed:
                    # n2 is already executed, no constraint violation possible
                    continue
                
                # Check d(n1, n2):
                # If d(n1, n2) < 0, then n2 - n1 <= negative, meaning n1 > n2
                # This means n2 must happen BEFORE n1 (n2 is a predecessor)
                # Since n2 is unexecuted, n1 cannot be dispatched yet
                d_n1_n2 = self._distances.get((n1, n2), INFINITY)
                if d_n1_n2 < 0:
                    # n2 must happen before n1, but n2 is unexecuted
                    # Check if n2 is an external ask - if so, it might be
                    # eligible for anticipation
                    if (self._task_anticipation 
                        and not task_in_progress
                        and n2 in self._external_events):
                        blocking_external_asks.add(n2)
                    else:
                        # Blocked by a non-external event or anticipation disabled
                        is_enabled = False
                        break
            
            if is_enabled:
                if blocking_external_asks:
                    # This event is only blocked by external asks.
                    # With anticipation enabled, it can be dispatched early.
                    anticipation_eligible.append((n1, blocking_external_asks))
                else:
                    enabled.append(n1)
        
        # Store anticipation-eligible events for collect_rted to use
        self._anticipation_eligible = anticipation_eligible
        
        return enabled
    
    def collect_rted(self, current_time: float) -> RTED:
        """
        Collect events to dispatch at the given time.
        
        Uses the FAST-EX algorithm to determine which free (controllable) events
        should be dispatched together at their lower bound.
        
        The algorithm:
        1. Get enabled events (those whose precedence constraints are satisfied)
        2. Get commitments (time windows) for enabled events
        3. Find the minimum lower bound among enabled FREE events
        4. If no free events, check contingent events at upper bounds (worst-case VDC)
        5. If task_anticipation is enabled and no events found, check
           anticipation-eligible events (blocked only by external asks)
        
        Args:
            current_time: Current execution time (relative to start)
            
        Returns:
            RTED with dispatch time and list of (event_id, is_noop) tuples.
            RTED.anticipated=True if events are dispatched via task anticipation.
            
        Reference: collect-rted in dynamic.lisp
        """
        commitments = self.get_commitments()
        enabled_events = self.get_enabled_events()
        
        if not commitments or not enabled_events:
            # No normally enabled events - check for anticipation-eligible events
            if (self._task_anticipation 
                and hasattr(self, '_anticipation_eligible') 
                and self._anticipation_eligible
                and not self.is_task_in_progress()):
                return self._collect_anticipated_rted(current_time, commitments)
            return RTED(time=current_time, events=[])
        
        # Filter commitments to only enabled events
        enabled_set = set(enabled_events)
        
        # Find the minimum lower bound among enabled, unexecuted FREE events
        min_lb = INFINITY
        min_events = []
        
        for (evt_id, is_ctg), (lb, ub) in commitments.items():
            # Only consider enabled events
            if evt_id not in enabled_set:
                continue
            
            if is_ctg:
                # Skip contingent events in first pass - they're observed, not dispatched
                continue
            
            if lb < min_lb:
                min_lb = lb
                min_events = [(evt_id, False)]  # False = not noop
            elif abs(lb - min_lb) < 1e-9:  # Epsilon comparison for floating point
                min_events.append((evt_id, False))
        
        # If no free events enabled, check EXTERNAL contingent events at their upper bounds
        # This is the worst-case assumption under VDC semantics:
        # If we haven't observed an EXTERNAL event by its upper bound,
        # assume it happened at the upper bound (generate no-op)
        # 
        # IMPORTANT: Own task events (from TPN) do NOT get upper-bound no-ops!
        # Own tasks should wait indefinitely for confirmation, with only max
        # execution time causing cancellation. Only explicitly registered
        # external events (cross-platform tells) get upper-bound no-ops.
        if not min_events:
            for (evt_id, is_ctg), (lb, ub) in commitments.items():
                if evt_id not in enabled_set:
                    continue
                if not is_ctg:
                    continue
                # Only generate no-ops for explicitly registered external events
                # Own contingent events (task completions) wait indefinitely
                if evt_id not in self._external_events:
                    continue
                if ub < min_lb:
                    min_lb = ub
                    min_events = [(evt_id, True)]  # True = noop (observed/assumed)
                elif abs(ub - min_lb) < 1e-9:
                    min_events.append((evt_id, True))
        
        # If still no events and anticipation is enabled, try anticipation
        if (not min_events 
            and self._task_anticipation 
            and hasattr(self, '_anticipation_eligible') 
            and self._anticipation_eligible
            and not self.is_task_in_progress()):
            return self._collect_anticipated_rted(current_time, commitments)
        
        dispatch_time = max(min_lb, current_time) if min_lb < INFINITY else current_time
        return RTED(time=dispatch_time, events=min_events)
    
    def _collect_anticipated_rted(self, current_time: float, commitments: Dict) -> RTED:
        """
        Collect RTED for anticipation-eligible events.
        
        When no normally enabled events exist but there are events only blocked
        by unfulfilled external asks, dispatch them in anticipation mode.
        
        The executor will only dispatch non-critical actions for these tasks.
        
        Args:
            current_time: Current execution time
            commitments: Current commitments dict
            
        Returns:
            RTED with anticipated=True
        """
        min_lb = INFINITY
        min_events = []
        all_unfulfilled = set()
        
        for evt_id, blocking_asks in self._anticipation_eligible:
            # Get commitment bounds for this event
            key_free = (evt_id, False)
            key_ctg = (evt_id, True)
            
            lb = INFINITY
            if key_free in commitments:
                lb = commitments[key_free][0]
            elif key_ctg in commitments:
                lb = commitments[key_ctg][0]
            
            if lb < min_lb:
                min_lb = lb
                min_events = [(evt_id, False)]
                all_unfulfilled = blocking_asks.copy()
            elif abs(lb - min_lb) < 1e-9:
                min_events.append((evt_id, False))
                all_unfulfilled |= blocking_asks
        
        if not min_events:
            return RTED(time=current_time, events=[])
        
        dispatch_time = max(min_lb, current_time) if min_lb < INFINITY else current_time
        
        if self._verbose:
            evt_names = [e[0] for e in min_events]
            self._log(
                f"\u26a1 TASK ANTICIPATION: dispatching {evt_names} at {dispatch_time:.3f} "
                f"(unfulfilled asks: {all_unfulfilled})"
            )
        
        return RTED(
            time=dispatch_time, 
            events=min_events, 
            anticipated=True,
            unfulfilled_asks=all_unfulfilled
        )
    
    def get_next_rted(self, current_time) -> RTED:
        """
        Get the next real-time execution decision.
        
        Returns:
            RTED containing when to dispatch and what events
            
        Reference: get-next-rted in dynamic.lisp
        """
        return self.collect_rted(current_time)
    
    def update_schedule(self, event_id: str, execution_time: float) -> bool:
        """
        Update the schedule after an event has been executed/observed.
        
        For free events: Records execution time, updates distance graph
        For contingent events: Records observation time, subtracts gamma to get
                              actual execution time, then propagates constraints
        
        Args:
            event_id: ID of the executed/observed event
            execution_time: Time of execution/observation (relative to start)
            
        Returns:
            True if update was successful, False otherwise
            
        Reference: update-schedule!, %update-schedule! in dynamic.lisp
        """
        state = self._event_states.get(event_id)
        if state is None:
            self._logger.info(f"Unknown event: {event_id}")
            return False
        
        if state.executed:
            self._logger.info(f"Event already executed: {event_id}")
            return False
        
        # Get current commitment bounds (for logging/debugging only)
        commitments = self.get_commitments()
        key = (event_id, state.is_contingent)
        
        if key in commitments:
            lb, ub = commitments[key]
            
            # Log warnings for bounds violations, but don't reject
            # The Lisp implementation is permissive - it records events as they occur
            if execution_time < lb:
                self._logger.info(
                    f"Warning: Event {event_id} executed early: "
                    f"{execution_time:.3f} < lb={lb:.3f} (delta={lb - execution_time:.3f}s)"
                )
                # For contingent events, may trigger rescheduling
                if state.is_contingent and self._reschedule_early_ctg:
                    self._logger.info(f"Early contingent event - rescheduling may be triggered")
            
            if execution_time > ub:
                self._logger.info(
                    f"Warning: Event {event_id} executed late: "
                    f"{execution_time:.3f} > ub={ub:.3f} (delta={execution_time - ub:.3f}s)"
                )
        else:
            # No commitment found - this can happen for external events
            # or events that have already been processed
            self._logger.info(f"No commitment found for event: {event_id} (may be external)")
        
        # For contingent events with gamma, the recorded execution time is
        # observation_time - gamma (the actual event happened before we observed it)
        # This implements the VDC fixed-delay semantics from dynamic.lisp
        recorded_time = execution_time
        if state.is_contingent and self._use_fixed_delay_conversion:
            gamma = self._gamma.get(event_id, 0.0)
            recorded_time = execution_time - gamma
            if self._verbose:
                self._logger.info(
                    f"Contingent event {event_id}: observed at {execution_time:.3f}, "
                    f"actual at {recorded_time:.3f} (y={gamma:.3f})"
                )
        
        # Mark event as executed
        state.executed = True
        state.execution_time = recorded_time
        
        # Track fulfilled external events so late registrations can detect them
        if event_id in self._external_events or event_id in self._contingent_events:
            self._fulfilled_events[event_id] = recorded_time
        
        # Single-task execution tracking:
        # If this is a start event, mark its corresponding end event as in-progress
        if event_id in self._start_to_end:
            end_event = self._start_to_end[event_id]
            self._in_progress_end_events.add(end_event)
            if self._verbose:
                self._logger.info(f"Task started: {event_id} -> waiting for {end_event}")
        
        # If this is an end event, remove it from in-progress set
        if event_id in self._in_progress_end_events:
            self._in_progress_end_events.discard(event_id)
            if self._verbose:
                self._logger.info(f"Task completed: {event_id} (start was {self._end_to_start.get(event_id)})")
        
        # Update distance graph with Dijkstra
        self._update_distances_after_execution(event_id, recorded_time)
        
        self._logger.info(f"Recorded event {event_id} at time {recorded_time:.3f}")
        return True
    
    def _update_distances_after_execution(self, event_id: str, exec_time: float):
        """
        Update distance graph after event execution using Dijkstra's algorithm.
        
        Sets fixed distances from ZERO_POINT to event and propagates changes.
        
        Reference: %update-schedule!, sink-dijkstra, source-dijkstra in dynamic.lisp
        """
        # Fix the event's time relative to ZERO_POINT
        # d(ZERO, event) = exec_time (event happens at exec_time after ZERO)
        # d(event, ZERO) = -exec_time (ZERO happens at -exec_time after event)
        self._distances[(ZERO_POINT, event_id)] = exec_time
        self._distances[(event_id, ZERO_POINT)] = -exec_time
        
        # Run Dijkstra from event_id (source Dijkstra)
        self._source_dijkstra(event_id)
        
        # Run Dijkstra to event_id (sink Dijkstra)
        self._sink_dijkstra(event_id)
    
    def _source_dijkstra(self, source: str):
        """
        Dijkstra's algorithm from source event.
        Updates d(source, v) for all v.
        
        Reference: source-dijkstra in dynamic.lisp
        """
        # Include all events (TPN events + external ask events)
        all_events = list(self._event_states.keys())
        dist = {e: INFINITY for e in all_events}
        dist[source] = 0.0
        
        # Priority queue: (distance, event_id)
        pq = [(0.0, source)]
        visited = set()
        
        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            
            # Update distances to neighbors
            for v in all_events:
                if v == u:
                    continue
                # Edge weight from u to v
                edge_weight = self._get_edge_weight(u, v)
                if edge_weight < INFINITY:
                    new_dist = d + edge_weight
                    if new_dist < dist[v]:
                        dist[v] = new_dist
                        heapq.heappush(pq, (new_dist, v))
        
        # Update distance graph
        for v, d in dist.items():
            if d < self._distances.get((source, v), INFINITY):
                self._distances[(source, v)] = d
    
    def _sink_dijkstra(self, sink: str):
        """
        Dijkstra's algorithm to sink event.
        Updates d(v, sink) for all v.
        
        Reference: sink-dijkstra in dynamic.lisp
        """
        # Include all events (TPN events + external ask events)
        all_events = list(self._event_states.keys())
        dist = {e: INFINITY for e in all_events}
        dist[sink] = 0.0
        
        # Priority queue (reverse edges)
        pq = [(0.0, sink)]
        visited = set()
        
        while pq:
            d, v = heapq.heappop(pq)
            if v in visited:
                continue
            visited.add(v)
            
            # Update distances from neighbors (reverse edges)
            for u in all_events:
                if u == v:
                    continue
                # Edge weight from u to v (we're going backwards)
                edge_weight = self._get_edge_weight(u, v)
                if edge_weight < INFINITY:
                    new_dist = d + edge_weight
                    if new_dist < dist[u]:
                        dist[u] = new_dist
                        heapq.heappush(pq, (new_dist, u))
        
        # Update distance graph
        for u, d in dist.items():
            if d < self._distances.get((u, sink), INFINITY):
                self._distances[(u, sink)] = d
    
    def _get_edge_weight(self, from_event: str, to_event: str) -> float:
        """Get edge weight between two events from episodes."""
        # Check for direct episode
        for episode in self._episodes.values():
            if episode.from_event == from_event and episode.to_event == to_event:
                return episode.duration.upper_bound
            if episode.from_event == to_event and episode.to_event == from_event:
                return -episode.duration.lower_bound
        
        # Check ZERO_POINT connections
        if from_event == ZERO_POINT or to_event == ZERO_POINT:
            return self._distances.get((from_event, to_event), INFINITY)
        
        return INFINITY
    
    def narrow_bounds(self, event_id: str, lb: float, ub: float) -> bool:
        """
        Narrow the execution bounds for an event.
        
        Args:
            event_id: Event to narrow bounds for
            lb: New lower bound
            ub: New upper bound
            
        Returns:
            True if bounds were successfully narrowed
            
        Reference: narrow-bounds in dynamic.lisp
        """
        # Update distances to reflect narrower bounds
        current_lb = -self._distances.get((event_id, ZERO_POINT), 0.0)
        current_ub = self._distances.get((ZERO_POINT, event_id), INFINITY)
        
        new_lb = max(current_lb, lb)
        new_ub = min(current_ub, ub)
        
        if new_lb > new_ub:
            self._log(f"Cannot narrow bounds for {event_id}: [{new_lb}, {new_ub}] invalid")
            return False
        
        self._distances[(event_id, ZERO_POINT)] = -new_lb
        self._distances[(ZERO_POINT, event_id)] = new_ub
        
        # Propagate changes
        self._source_dijkstra(event_id)
        self._sink_dijkstra(event_id)
        
        return True
    
    def get_event_bounds(self, event_id: str) -> Tuple[float, float]:
        """
        Get the current execution bounds for an event.
        
        Returns:
            (lower_bound, upper_bound) tuple
        """
        lb = -self._distances.get((event_id, ZERO_POINT), 0.0)
        ub = self._distances.get((ZERO_POINT, event_id), INFINITY)
        return (max(0.0, lb), ub)
    
    def get_executed_events(self) -> Dict[str, float]:
        """Get all executed events with their execution times."""
        return {
            evt_id: state.execution_time
            for evt_id, state in self._event_states.items()
            if state.executed and evt_id != ZERO_POINT
        }
    
    def get_pending_events(self) -> List[str]:
        """Get all events that haven't been executed yet."""
        return [
            evt_id for evt_id, state in self._event_states.items()
            if not state.executed and evt_id != ZERO_POINT
        ]
    
    def is_task_in_progress(self) -> bool:
        """
        Check if any task is currently in progress.
        
        A task is in progress if its start event has been executed
        but its end event has not yet been executed/observed.
        
        Returns:
            True if a task is currently executing
        """
        return len(self._in_progress_end_events) > 0
    
    def get_in_progress_end_events(self) -> Set[str]:
        """
        Get the end events of tasks currently in progress.
        
        Returns:
            Set of end event IDs for tasks awaiting completion
        """
        return self._in_progress_end_events.copy()
    
    def get_in_progress_tasks(self) -> List[Tuple[str, str]]:
        """
        Get tasks currently in progress.
        
        Returns:
            List of (start_event_id, end_event_id) tuples for in-progress tasks
        """
        return [
            (self._end_to_start[end_evt], end_evt) 
            for end_evt in self._in_progress_end_events
            if end_evt in self._end_to_start
        ]
    
    def is_event_contingent(self, event_id: str) -> bool:
        """
        Check if an event is contingent (external/uncontrollable).
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if the event is contingent, False if controllable
        """
        return event_id in self._contingent_events
    
    def get_end_event_for_start(self, start_event_id: str) -> Optional[str]:
        """
        Get the end event corresponding to a start event.
        
        Args:
            start_event_id: Start event ID
            
        Returns:
            End event ID if this is a task start event, None otherwise
        """
        return self._start_to_end.get(start_event_id)
    
    def get_episode_for_end_event(self, end_event_id: str) -> Optional['Episode']:
        """
        Get the episode that has this event as its end.
        
        Args:
            end_event_id: End event ID
            
        Returns:
            Episode if found, None otherwise
        """
        for episode in self._episodes.values():
            if episode.to_event == end_event_id:
                return episode
        return None
    
    def get_episode_for_start_event(self, start_event_id: str) -> Optional['Episode']:
        """
        Get the episode that has this event as its start.
        
        Args:
            start_event_id: Start event ID
            
        Returns:
            Episode if found, None otherwise
        """
        for episode in self._episodes.values():
            if episode.from_event == start_event_id:
                return episode
        return None
    
    def get_observation_delay_for_episode(self, episode: 'Episode') -> float:
        """
        Get the per-episode observation delay (gamma) for a specific episode.
        
        Returns the episode-specific delay from ContingentDuration if available,
        otherwise falls back to the default delay.
        
        This is used to set per-task variable delay before dispatch.
        
        Args:
            episode: Episode to get observation delay for
            
        Returns:
            Observation delay in seconds
        """
        if isinstance(episode.duration, ContingentDuration):
            try:
                max_delay = float(episode.duration.max_observation_delay)
            except (ValueError, TypeError):
                max_delay = 0.0
            # If episode has explicit delay, use it; otherwise fall back to default
            if max_delay > 0.0:
                return max_delay
        
        # Check if gamma was already computed for the end event
        gamma = self._gamma.get(episode.to_event, 0.0)
        if gamma > 0.0:
            return gamma
        
        return self._default_delay
    
    def get_episode_expected_duration(self, episode_id: str) -> Tuple[float, float]:
        """
        Get the expected duration bounds for an episode.
        
        Args:
            episode_id: Episode ID
            
        Returns:
            (lower_bound, upper_bound) tuple
        """
        episode = self._episodes.get(episode_id)
        if episode:
            return (episode.duration.lower_bound, episode.duration.upper_bound)
        return (0.0, INFINITY)
    
    # =========================================================================
    # Fulfilled Event Tracking
    # =========================================================================
    
    def is_event_fulfilled(self, event_id: str) -> bool:
        """
        Check if an event has already been fulfilled (received/executed).
        
        This is the authoritative check for whether an external event was
        received, regardless of when it arrived relative to task dispatch.
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if the event was already received and recorded
        """
        # Check the fulfilled events ledger first
        if event_id in self._fulfilled_events:
            return True
        # Fall back to event_states for events recorded via update_schedule
        state = self._event_states.get(event_id)
        return state is not None and state.executed
    
    def get_fulfilled_events(self) -> Dict[str, float]:
        """
        Get all fulfilled external events and their execution times.
        
        Returns:
            Dict mapping event_id -> execution_time
        """
        return self._fulfilled_events.copy()
    
    def record_fulfilled_event(self, event_id: str, execution_time: float):
        """
        Explicitly record an event as fulfilled.
        
        Use this for events that were received before the scheduler was
        initialised (buffered by the executor). The event is recorded in
        the fulfilled ledger and, if it exists in the scheduler's event
        states, it is also marked as executed in the distance graph.
        
        Args:
            event_id: Event ID to record
            execution_time: Time the event was received
            
        Returns:
            True if the event was recorded (even if already known)
        """
        self._fulfilled_events[event_id] = execution_time
        
        # If the event exists in the scheduler, also update the schedule
        state = self._event_states.get(event_id)
        if state is not None and not state.executed:
            self.update_schedule(event_id, execution_time)
            self._logger.info(
                f"Replayed early external event {event_id} at {execution_time:.3f}"
            )
        return True
    
    # =========================================================================
    # External Event Management
    # =========================================================================
    
    def register_external_event(self, event_id: str):
        """
        Register an event as external (cross-platform).
        
        External events get upper-bound no-ops in collect_rted() if not observed.
        Own task events (default) wait indefinitely for confirmation.
        
        Use this for events that come from other platforms that may never arrive,
        such as cross-platform tells or external sensor observations.
        
        Args:
            event_id: Event ID to mark as external
        """
        self._external_events.add(event_id)
        if self._verbose:
            self._log(f"Registered external event: {event_id}")
    
    def unregister_external_event(self, event_id: str):
        """
        Unregister an event as external.
        
        Args:
            event_id: Event ID to unmark as external
        """
        self._external_events.discard(event_id)
    
    def is_external_event(self, event_id: str) -> bool:
        """
        Check if an event is registered as external.
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if event is registered as external
        """
        return event_id in self._external_events
    
    def get_external_events(self) -> Set[str]:
        """
        Get all registered external events.
        
        Returns:
            Set of external event IDs
        """
        return self._external_events.copy()
    
    # =========================================================================
    # Task Anticipation
    # =========================================================================
    
    @property
    def task_anticipation(self) -> bool:
        """Whether task anticipation is enabled."""
        return self._task_anticipation
    
    @task_anticipation.setter
    def task_anticipation(self, value: bool):
        """Enable or disable task anticipation."""
        self._task_anticipation = value
        if self._verbose:
            self._log(f"Task anticipation {'enabled' if value else 'disabled'}")
    
    @property
    def critical_action_indices(self) -> Dict[str, List[int]]:
        """Get the critical action indices per task name."""
        return self._critical_action_indices
    
    @critical_action_indices.setter
    def critical_action_indices(self, value: Dict[str, List[int]]):
        """Set critical action indices per task name."""
        self._critical_action_indices = value
        if self._verbose:
            self._log(f"Critical action indices set: {value}")
    
    def get_unfulfilled_asks_for_event(self, event_id: str) -> Set[str]:
        """
        Get the set of unfulfilled external ask events for a given start event.
        
        An ask is unfulfilled if the corresponding external event has not yet
        been executed (observed/received).
        
        Args:
            event_id: Start event ID of the episode
            
        Returns:
            Set of unfulfilled external ask event IDs
        """
        unfulfilled = set()
        
        # Find the episode that starts at this event
        episode = self.get_episode_for_start_event(event_id)
        if episode is None:
            return unfulfilled
        
        for ask_event_id in episode.asks:
            state = self._event_states.get(ask_event_id)
            if state and not state.executed:
                unfulfilled.add(ask_event_id)
        
        return unfulfilled
    
    def is_event_anticipated(self, event_id: str) -> bool:
        """Check if an event is currently dispatched in anticipation mode."""
        return event_id in self._anticipated_events
    
    def get_anticipated_unfulfilled_asks(self, event_id: str) -> Set[str]:
        """Get unfulfilled asks for an anticipated event."""
        return self._anticipated_events.get(event_id, set()).copy()
    
    def mark_event_anticipated(self, event_id: str, unfulfilled_asks: Set[str]):
        """Mark an event as dispatched in anticipation mode."""
        self._anticipated_events[event_id] = unfulfilled_asks.copy()
        if self._verbose:
            self._log(f"Event {event_id} marked as anticipated (waiting for: {unfulfilled_asks})")
    
    def clear_anticipated_event(self, event_id: str):
        """Clear anticipation state for an event (all asks fulfilled)."""
        self._anticipated_events.pop(event_id, None)
    
    def on_external_ask_fulfilled(self, ask_event_id: str) -> List[str]:
        """
        Notify that an external ask event has been fulfilled.
        
        Removes it from all anticipated events' unfulfilled ask sets.
        Returns list of event_ids whose asks are now all fulfilled
        (i.e., critical actions can now proceed).
        
        Args:
            ask_event_id: The external ask event that was fulfilled
            
        Returns:
            List of start event_ids that are now fully unblocked
        """
        newly_unblocked = []
        
        events_to_clear = []
        for evt_id, unfulfilled in self._anticipated_events.items():
            unfulfilled.discard(ask_event_id)
            if not unfulfilled:
                events_to_clear.append(evt_id)
                newly_unblocked.append(evt_id)
        
        for evt_id in events_to_clear:
            self._anticipated_events.pop(evt_id, None)
            if self._verbose:
                self._log(f"Event {evt_id} fully unblocked (all asks fulfilled)")
        
        return newly_unblocked
    
    def get_task_name_for_event(self, event_id: str) -> Optional[str]:
        """
        Get the task name associated with a start event.
        
        Args:
            event_id: Start event ID
            
        Returns:
            Task name from episode annotations, or None
        """
        episode = self.get_episode_for_start_event(event_id)
        if episode and episode.annotations:
            return episode.annotations.get('task', episode.annotations.get('task_name'))
        return None
    
    def has_critical_actions(self, task_name: str) -> bool:
        """Check if a task has critical action indices defined."""
        return task_name in self._critical_action_indices and len(self._critical_action_indices[task_name]) > 0
    
    def get_critical_action_indices_for_task(self, task_name: str) -> List[int]:
        """Get critical action indices for a task name."""
        return self._critical_action_indices.get(task_name, [])
    
    def _log(self, message: str):
        """Log a message if verbose mode is enabled."""
        if self._verbose:
            self._logger.info(f"[KirkScheduler] {message}")