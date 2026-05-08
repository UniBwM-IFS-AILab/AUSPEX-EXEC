#!/usr/bin/env python3
"""
Execution Timing Guard Module

Timing management for temporal plan execution with:
- Maximum execution time enforcement (hard deadline)
- Own event duration monitoring with epsilon-based warnings
- External event timeout handling with automatic no-op generation

This module implements a comprehensive timing guard that ensures temporal
plan execution completes within specified constraints while gracefully
handling timing violations.

Architecture:
┌─────────────────────────────────────────────────────────────────────┐
│                     ExecutionTimingGuard                            │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ Max Execution   │  │ Own Event       │  │ External Event      │  │
│  │ Time Monitor    │  │ Duration Guard  │  │ Timeout Handler     │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘  │
│           │                   │                      │              │
│           ▼                   ▼                      ▼              │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │           Unified Timing Violation Handler                      ││
│  │  - CANCEL: Max time exceeded                                    ││
│  │  - WARN:   Own event duration missed                            ││
│  │  - NOOP:   External event timeout                               ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘

Usage:
    guard = ExecutionTimingGuard(
        max_execution_time=1200.0,  # 20 minutes
        duration_epsilon=5.0,        # 5 second tolerance for own events
        external_event_delay=30.0,   # 30 second timeout for external events
    )
    
    # Check max time before scheduling
    if guard.would_exceed_max_time(scheduled_time):
        guard.cancel_execution("Schedule exceeds maximum execution time")
    
    # Monitor own event duration
    guard.start_own_event("task_start_1", expected_duration=60.0)
    guard.check_own_event_duration("task_start_1", current_time=75.0)  # Warns
    
    # Handle external event timeout
    guard.await_external_event("cross_platform_tell", start_time=100.0)
    # ... if timeout exceeded, generates no-op
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List, Tuple, Set, Any
from enum import Enum, auto
from datetime import datetime
import threading
import logging
import time


class TimingViolationType(Enum):
    """Types of timing violations that can occur during execution."""
    MAX_TIME_EXCEEDED = auto()     # Execution would exceed max allowed time
    DURATION_MISSED = auto()       # Own event duration exceeded (warning)
    EXTERNAL_TIMEOUT = auto()      # External event not received in time (no-op)
    SCHEDULE_OVERFLOW = auto()     # Scheduler scheduled beyond max time


@dataclass
class TimingViolation:
    """Represents a timing violation event."""
    violation_type: TimingViolationType
    event_id: str
    expected_time: float
    actual_time: float
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __repr__(self) -> str:
        delta = self.actual_time - self.expected_time
        sign = "+" if delta > 0 else ""
        return (
            f"[{self.violation_type.name}] {self.event_id}: "
            f"expected={self.expected_time:.2f}s, actual={self.actual_time:.2f}s "
            f"({sign}{delta:.2f}s) - {self.message}"
        )


@dataclass
class OwnEventTracking:
    """Tracks timing for an own (platform-local) event."""
    event_id: str
    start_time: float
    expected_duration: float
    epsilon: float
    warned: bool = False
    
    @property
    def deadline(self) -> float:
        """Deadline including epsilon tolerance."""
        return self.start_time + self.expected_duration + self.epsilon
    
    def is_overdue(self, current_time: float) -> bool:
        """Check if event has exceeded its expected duration + epsilon."""
        return current_time > self.deadline and not self.warned


@dataclass
class ExternalEventTracking:
    """Tracks timing for an external (cross-platform) event."""
    event_id: str
    wait_start_time: float
    max_delay: float
    source_platform: Optional[str] = None
    generated_noop: bool = False
    
    @property
    def timeout(self) -> float:
        """Timeout time for this external event."""
        return self.wait_start_time + self.max_delay
    
    def is_timed_out(self, current_time: float) -> bool:
        """Check if external event has timed out."""
        return current_time > self.timeout and not self.generated_noop


class ExecutionTimingGuard:
    """
    Comprehensive timing guard for temporal plan execution.
    
    Manages three aspects of execution timing:
    1. **Max Execution Time**: Hard deadline for entire execution
    2. **Own Event Duration**: Soft warnings when platform events exceed expected duration
    3. **External Event Timeout**: Automatic no-op generation for timed-out external events
    
    Thread-safe design for concurrent access from dispatcher and executor threads.
    
    Attributes:
        max_execution_time: Maximum allowed execution time (seconds), None for unlimited
        duration_epsilon: Tolerance for own event duration warnings (seconds)
        external_event_delay: Default delay before generating no-op for external events
    """
    
    # ═══════════════════════════════════════════════════════════════════════════
    # BEAUTIFUL CONSOLE FORMATTING
    # ═══════════════════════════════════════════════════════════════════════════
    
    BANNER_STYLE = """
╔══════════════════════════════════════════════════════════════════════════════╗
║  ⏱️  EXECUTION TIMING GUARD INITIALIZED                                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Max Execution Time : {max_time:>10}                                          ║
║  Duration Epsilon   : {epsilon:>10}                                           ║
║  External Delay     : {ext_delay:>10}                                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

    VIOLATION_BANNER = """
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  ⚠️  TIMING VIOLATION DETECTED                                                ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃  Type    : {violation_type:<66}┃
┃  Event   : {event_id:<66}┃
┃  Message : {message:<66}┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""

    MAX_TIME_EXCEEDED_BANNER = """
╔══════════════════════════════════════════════════════════════════════════════╗
║  🛑  MAXIMUM EXECUTION TIME EXCEEDED - CANCELLING EXECUTION                   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Elapsed Time  : {elapsed:>10.2f}s                                            ║
║  Max Allowed   : {max_time:>10.2f}s                                           ║
║  Overshoot     : {overshoot:>10.2f}s                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    
    def __init__(
        self,
        max_execution_time: Optional[float] = None,
        duration_epsilon: float = 5.0,
        external_event_delay: float = 30.0,
        logger: Optional[logging.Logger] = None,
        verbose: bool = True
    ):
        """
        Initialize the execution timing guard.
        
        Args:
            max_execution_time: Maximum execution time in seconds (None = unlimited)
                               Example: 1200.0 for 20 minutes
            duration_epsilon: Tolerance before warning on own event duration (seconds)
                             The guard warns if actual_duration > expected + epsilon
            external_event_delay: Default delay before generating no-op for external
                                 events that haven't been observed (seconds)
            logger: Optional logger instance
            verbose: Enable verbose logging with fancy formatting
        """
        self._max_execution_time = max_execution_time
        self._duration_epsilon = duration_epsilon
        self._external_event_delay = external_event_delay
        self._logger = logger or logging.getLogger(__name__)
        self._verbose = verbose
        
        # Execution tracking
        self._execution_start_time: Optional[float] = None
        self._cancelled = False
        self._cancel_reason: Optional[str] = None
        
        # Event tracking dictionaries
        self._own_events: Dict[str, OwnEventTracking] = {}
        self._external_events: Dict[str, ExternalEventTracking] = {}
        
        # Violation history
        self._violations: List[TimingViolation] = []
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Callbacks
        self._on_cancel: Optional[Callable[[str], None]] = None
        self._on_noop_generated: Optional[Callable[[str, float], None]] = None
        self._on_duration_warning: Optional[Callable[[str, float, float], None]] = None
        
        if self._verbose:
            self._log_initialization()
    
    def _log_initialization(self):
        """Log initialization with fancy formatting."""
        max_time_str = f"{self._max_execution_time:.1f}s" if self._max_execution_time else "∞ (unlimited)"
        epsilon_str = f"{self._duration_epsilon:.1f}s"
        ext_delay_str = f"{self._external_event_delay:.1f}s"
        
        self._logger.info(self.BANNER_STYLE.format(
            max_time=max_time_str,
            epsilon=epsilon_str,
            ext_delay=ext_delay_str
        ))
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CONFIGURATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def set_callbacks(
        self,
        on_cancel: Optional[Callable[[str], None]] = None,
        on_noop_generated: Optional[Callable[[str, float], None]] = None,
        on_duration_warning: Optional[Callable[[str, float, float], None]] = None
    ):
        """
        Set callback functions for timing events.
        
        Args:
            on_cancel: Called when execution is cancelled (reason)
            on_noop_generated: Called when no-op is generated (event_id, time)
            on_duration_warning: Called on duration warning (event_id, expected, actual)
        """
        with self._lock:
            self._on_cancel = on_cancel
            self._on_noop_generated = on_noop_generated
            self._on_duration_warning = on_duration_warning
    
    @property
    def max_execution_time(self) -> Optional[float]:
        """Get maximum execution time."""
        return self._max_execution_time
    
    @max_execution_time.setter
    def max_execution_time(self, value: Optional[float]):
        """Set maximum execution time."""
        with self._lock:
            self._max_execution_time = value
            if self._verbose and value:
                self._logger.info(f"[TimingGuard] Max execution time set to {value:.1f}s")
    
    @property
    def duration_epsilon(self) -> float:
        """Get duration epsilon tolerance."""
        return self._duration_epsilon
    
    @duration_epsilon.setter  
    def duration_epsilon(self, value: float):
        """Set duration epsilon tolerance."""
        with self._lock:
            self._duration_epsilon = value
    
    @property
    def external_event_delay(self) -> float:
        """Get default external event delay."""
        return self._external_event_delay
    
    @external_event_delay.setter
    def external_event_delay(self, value: float):
        """Set default external event delay."""
        with self._lock:
            self._external_event_delay = value
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EXECUTION LIFECYCLE
    # ═══════════════════════════════════════════════════════════════════════════
    
    def start_execution(self, start_time: Optional[float] = None):
        """
        Mark the start of execution.
        
        Args:
            start_time: Execution start time (default: current time)
        """
        with self._lock:
            self._execution_start_time = start_time if start_time is not None else time.time()
            self._cancelled = False
            self._cancel_reason = None
            self._own_events.clear()
            self._external_events.clear()
            self._violations.clear()
            
            if self._verbose:
                self._logger.info(
                    f"[TimingGuard] ▶ Execution started at t=0"
                    f"{f' (max: {self._max_execution_time:.1f}s)' if self._max_execution_time else ''}"
                )
    
    def reset(self):
        """Reset the timing guard state."""
        with self._lock:
            self._execution_start_time = None
            self._cancelled = False
            self._cancel_reason = None
            self._own_events.clear()
            self._external_events.clear()
            self._violations.clear()
    
    @property
    def is_cancelled(self) -> bool:
        """Check if execution has been cancelled."""
        return self._cancelled
    
    @property
    def cancel_reason(self) -> Optional[str]:
        """Get reason for cancellation."""
        return self._cancel_reason
    
    def get_elapsed_time(self) -> float:
        """Get elapsed time since execution started."""
        if self._execution_start_time is None:
            return 0.0
        return time.time() - self._execution_start_time
    
    def get_remaining_time(self) -> Optional[float]:
        """
        Get remaining time until max execution time.
        
        Returns:
            Remaining time in seconds, None if no max time set
        """
        if self._max_execution_time is None:
            return None
        return max(0.0, self._max_execution_time - self.get_elapsed_time())
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAX EXECUTION TIME HANDLING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def would_exceed_max_time(self, scheduled_time: float) -> bool:
        """
        Check if a scheduled time would exceed max execution time.
        
        Use this BEFORE scheduling an event to ensure the schedule doesn't
        extend beyond the maximum allowed execution time.
        
        Args:
            scheduled_time: Proposed scheduled time (relative to execution start)
            
        Returns:
            True if scheduled_time exceeds max execution time
        """
        if self._max_execution_time is None:
            return False
        return scheduled_time > self._max_execution_time
    
    def check_max_time(self, current_time: Optional[float] = None) -> bool:
        """
        Check if max execution time has been exceeded.
        
        If exceeded, automatically triggers cancellation.
        
        Args:
            current_time: Current time (default: elapsed time)
            
        Returns:
            True if execution should continue, False if cancelled
        """
        if self._max_execution_time is None:
            return True
        
        with self._lock:
            if self._cancelled:
                return False
            
            elapsed = current_time if current_time is not None else self.get_elapsed_time()
            
            if elapsed > self._max_execution_time:
                self._trigger_cancellation(
                    f"Maximum execution time exceeded: {elapsed:.2f}s > {self._max_execution_time:.2f}s",
                    elapsed
                )
                return False
            
            return True
    
    def check_schedule_overflow(self, rted_time: float) -> bool:
        """
        Check if the scheduler is scheduling beyond max time.
        
        This should be called when receiving an RTED to verify the scheduled
        dispatch time doesn't exceed the maximum allowed execution time.
        
        Args:
            rted_time: Time from the RTED (scheduled dispatch time)
            
        Returns:
            True if schedule is valid, False if overflow detected (triggers cancel)
        """
        if self._max_execution_time is None:
            return True
        
        with self._lock:
            if self._cancelled:
                return False
            
            if rted_time > self._max_execution_time:
                violation = TimingViolation(
                    violation_type=TimingViolationType.SCHEDULE_OVERFLOW,
                    event_id="scheduler",
                    expected_time=self._max_execution_time,
                    actual_time=rted_time,
                    message=f"Scheduler RTED time {rted_time:.2f}s exceeds max execution time"
                )
                self._violations.append(violation)
                
                self._trigger_cancellation(
                    f"Schedule overflow: RTED at {rted_time:.2f}s exceeds max time {self._max_execution_time:.2f}s",
                    rted_time
                )
                return False
            
            return True
    
    def _trigger_cancellation(self, reason: str, elapsed_time: float):
        """Trigger execution cancellation."""
        self._cancelled = True
        self._cancel_reason = reason
        
        if self._verbose:
            overshoot = elapsed_time - (self._max_execution_time or 0)
            self._logger.error(self.MAX_TIME_EXCEEDED_BANNER.format(
                elapsed=elapsed_time,
                max_time=self._max_execution_time or 0,
                overshoot=overshoot
            ))
        
        if self._on_cancel:
            self._on_cancel(reason)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # OWN EVENT DURATION MONITORING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def start_own_event(
        self, 
        event_id: str, 
        expected_duration: float,
        start_time: Optional[float] = None,
        epsilon: Optional[float] = None
    ):
        """
        Start tracking duration for an own (platform-local) event.
        
        This is for events/tasks that this platform is executing. If the
        duration exceeds expected_duration + epsilon, a warning is generated
        but execution continues.
        
        Args:
            event_id: Unique event identifier
            expected_duration: Expected duration in seconds
            start_time: Event start time (default: current elapsed time)
            epsilon: Duration tolerance (default: class duration_epsilon)
        """
        with self._lock:
            actual_start = start_time if start_time is not None else self.get_elapsed_time()
            actual_epsilon = epsilon if epsilon is not None else self._duration_epsilon
            
            self._own_events[event_id] = OwnEventTracking(
                event_id=event_id,
                start_time=actual_start,
                expected_duration=expected_duration,
                epsilon=actual_epsilon
            )
            
            if self._verbose:
                deadline = actual_start + expected_duration + actual_epsilon
                self._logger.info(
                    f"[TimingGuard] 📌 Tracking own event '{event_id}': "
                    f"expected={expected_duration:.1f}s, ε={actual_epsilon:.1f}s, deadline={deadline:.1f}s"
                )
    
    def check_own_event(self, event_id: str, current_time: Optional[float] = None) -> bool:
        """
        Check if an own event has exceeded its expected duration.
        
        If overdue, generates a warning but returns True (continue execution).
        
        Args:
            event_id: Event to check
            current_time: Current time (default: elapsed time)
            
        Returns:
            True (always continues, warnings are non-fatal)
        """
        with self._lock:
            tracking = self._own_events.get(event_id)
            if not tracking:
                return True
            
            check_time = current_time if current_time is not None else self.get_elapsed_time()
            
            if tracking.is_overdue(check_time):
                tracking.warned = True
                actual_duration = check_time - tracking.start_time
                expected_deadline = tracking.expected_duration + tracking.epsilon
                
                violation = TimingViolation(
                    violation_type=TimingViolationType.DURATION_MISSED,
                    event_id=event_id,
                    expected_time=tracking.deadline,
                    actual_time=check_time,
                    message=f"Duration {actual_duration:.2f}s exceeded expected {tracking.expected_duration:.2f}s + ε={tracking.epsilon:.2f}s"
                )
                self._violations.append(violation)
                
                self._logger.warning(self.VIOLATION_BANNER.format(
                    violation_type="DURATION_MISSED (WARNING - CONTINUING)",
                    event_id=event_id[:66],
                    message=violation.message[:66]
                ))
                
                if self._on_duration_warning:
                    self._on_duration_warning(
                        event_id, 
                        tracking.expected_duration, 
                        actual_duration
                    )
            
            return True
    
    def complete_own_event(self, event_id: str, completion_time: Optional[float] = None):
        """
        Mark an own event as completed.
        
        Args:
            event_id: Event to complete
            completion_time: Completion time (default: elapsed time)
        """
        with self._lock:
            tracking = self._own_events.pop(event_id, None)
            if tracking and self._verbose:
                actual_duration = (completion_time or self.get_elapsed_time()) - tracking.start_time
                status = "✓" if actual_duration <= tracking.expected_duration + tracking.epsilon else "⚠"
                self._logger.info(
                    f"[TimingGuard] {status} Own event '{event_id}' completed: "
                    f"duration={actual_duration:.2f}s (expected={tracking.expected_duration:.2f}s)"
                )
    
    def check_all_own_events(self, current_time: Optional[float] = None):
        """Check all tracked own events for duration violations."""
        with self._lock:
            for event_id in list(self._own_events.keys()):
                self.check_own_event(event_id, current_time)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EXTERNAL EVENT TIMEOUT HANDLING
    # ═══════════════════════════════════════════════════════════════════════════
    
    def await_external_event(
        self,
        event_id: str,
        wait_start_time: Optional[float] = None,
        max_delay: Optional[float] = None,
        source_platform: Optional[str] = None
    ):
        """
        Start waiting for an external (cross-platform) event.
        
        If the event is not received within max_delay, a no-op event is
        generated to allow execution to continue.
        
        Args:
            event_id: External event identifier
            wait_start_time: Time when wait started (default: elapsed time)
            max_delay: Maximum wait time (default: class external_event_delay)
            source_platform: Optional identifier for the source platform
        """
        with self._lock:
            actual_start = wait_start_time if wait_start_time is not None else self.get_elapsed_time()
            actual_delay = max_delay if max_delay is not None else self._external_event_delay
            
            self._external_events[event_id] = ExternalEventTracking(
                event_id=event_id,
                wait_start_time=actual_start,
                max_delay=actual_delay,
                source_platform=source_platform
            )
            
            if self._verbose:
                timeout = actual_start + actual_delay
                self._logger.info(
                    f"[TimingGuard] ⏳ Awaiting external event '{event_id}'"
                    f"{f' from {source_platform}' if source_platform else ''}: "
                    f"timeout={timeout:.1f}s"
                )
    
    def receive_external_event(self, event_id: str, receive_time: Optional[float] = None):
        """
        Mark that an external event has been received.
        
        Args:
            event_id: Received event identifier
            receive_time: Time of receipt (default: elapsed time)
        """
        with self._lock:
            tracking = self._external_events.pop(event_id, None)
            if tracking and self._verbose:
                wait_duration = (receive_time or self.get_elapsed_time()) - tracking.wait_start_time
                self._logger.info(
                    f"[TimingGuard] ✓ External event '{event_id}' received "
                    f"after {wait_duration:.2f}s"
                )
    
    def check_external_event_timeout(
        self, 
        event_id: str, 
        current_time: Optional[float] = None
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if an external event has timed out.
        
        If timed out, marks the event for no-op generation.
        
        Args:
            event_id: Event to check
            current_time: Current time (default: elapsed time)
            
        Returns:
            Tuple of (timed_out: bool, noop_time: Optional[float])
            If timed_out is True, noop_time is the time for the no-op event
        """
        with self._lock:
            tracking = self._external_events.get(event_id)
            if not tracking:
                return (False, None)
            
            check_time = current_time if current_time is not None else self.get_elapsed_time()
            
            if tracking.is_timed_out(check_time):
                tracking.generated_noop = True
                noop_time = tracking.timeout
                
                violation = TimingViolation(
                    violation_type=TimingViolationType.EXTERNAL_TIMEOUT,
                    event_id=event_id,
                    expected_time=tracking.timeout,
                    actual_time=check_time,
                    message=f"External event not received within {tracking.max_delay:.2f}s - generating no-op"
                )
                self._violations.append(violation)
                
                self._logger.warning(self.VIOLATION_BANNER.format(
                    violation_type="EXTERNAL_TIMEOUT (GENERATING NO-OP)",
                    event_id=event_id[:66],
                    message=violation.message[:66]
                ))
                
                if self._on_noop_generated:
                    self._on_noop_generated(event_id, noop_time)
                
                return (True, noop_time)
            
            return (False, None)
    
    def check_all_external_events(
        self, 
        current_time: Optional[float] = None
    ) -> List[Tuple[str, float]]:
        """
        Check all external events for timeouts.
        
        Args:
            current_time: Current time (default: elapsed time)
            
        Returns:
            List of (event_id, noop_time) for events that timed out
        """
        noops = []
        with self._lock:
            for event_id in list(self._external_events.keys()):
                timed_out, noop_time = self.check_external_event_timeout(event_id, current_time)
                if timed_out and noop_time is not None:
                    noops.append((event_id, noop_time))
        return noops
    
    def get_pending_external_events(self) -> List[str]:
        """Get list of external events still being waited on."""
        with self._lock:
            return [
                eid for eid, tracking in self._external_events.items()
                if not tracking.generated_noop
            ]
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPREHENSIVE TIMING CHECK
    # ═══════════════════════════════════════════════════════════════════════════
    
    def tick(self, current_time: Optional[float] = None) -> Tuple[bool, List[Tuple[str, float]]]:
        """
        Perform comprehensive timing check.
        
        This is the main entry point for timing checks. Should be called
        periodically (e.g., in the dispatcher tick loop).
        
        Args:
            current_time: Current time (default: elapsed time)
            
        Returns:
            Tuple of:
            - continue_execution: False if execution should be cancelled
            - noop_events: List of (event_id, noop_time) for external timeouts
        """
        # Check max execution time first (can trigger cancellation)
        if not self.check_max_time(current_time):
            return (False, [])
        
        # Check own event durations (warnings only)
        self.check_all_own_events(current_time)
        
        # Check external event timeouts (generate no-ops)
        noops = self.check_all_external_events(current_time)
        
        return (True, noops)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # REPORTING & DIAGNOSTICS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_violations(self) -> List[TimingViolation]:
        """Get list of all timing violations."""
        with self._lock:
            return self._violations.copy()
    
    def get_status_report(self) -> str:
        """Generate a detailed status report."""
        with self._lock:
            lines = [
                "╔══════════════════════════════════════════════════════════════════════╗",
                "║  EXECUTION TIMING GUARD STATUS                                        ║",
                "╠══════════════════════════════════════════════════════════════════════╣",
            ]
            
            elapsed = self.get_elapsed_time()
            remaining = self.get_remaining_time()
            
            lines.append(f"║  Elapsed Time      : {elapsed:>10.2f}s                                  ║")
            if remaining is not None:
                lines.append(f"║  Remaining Time    : {remaining:>10.2f}s                                  ║")
            lines.append(f"║  Cancelled         : {'Yes' if self._cancelled else 'No':<10}                                  ║")
            lines.append(f"║  Own Events        : {len(self._own_events):>10}                                  ║")
            lines.append(f"║  External Events   : {len(self._external_events):>10}                                  ║")
            lines.append(f"║  Violations        : {len(self._violations):>10}                                  ║")
            
            lines.append("╚══════════════════════════════════════════════════════════════════════╝")
            
            return "\n".join(lines)
    
    def log_status(self):
        """Log current status."""
        self._logger.info(self.get_status_report())
