#!/usr/bin/env python3
"""
Kirk Dynamic Dispatcher Module

Python translation of MIT enterprise kirk-v2 dynamic dispatcher.
The dispatcher coordinates with a scheduler to send events to drivers
at the correct times, handling both free (controllable) and contingent
(uncontrollable) events.

Key Concepts:
- Free events are dispatched when we're within epsilon of their lower bound
- Contingent events are observed and update the schedule
- The dispatcher uses VDC semantics (Variable Delay Controllability)

Enhanced Features:
- ExecutionTimingGuard integration for max execution time enforcement
- Own event duration monitoring with epsilon-based warnings
- External event timeout handling with automatic no-op generation

Reference: enterprise/kirk-v2/src/dispatcher/dynamic-dispatcher.lisp
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Set, TYPE_CHECKING
from enum import Enum, auto
from collections import deque
import time
import threading
import logging

from auspex_executor.platform_executor.executors.kirk.kirk_scheduler import KirkScheduler, RTED
from auspex_executor.utils.tpn_helper import TemporalPlanNetwork

if TYPE_CHECKING:
    from auspex_executor.platform_executor.executors.kirk.execution_timing_guard import ExecutionTimingGuard


# ============================================================================
# Message Types for Driver Communication
# ============================================================================

class MessageType(Enum):
    """Types of messages exchanged with the driver."""
    EXECUTED = auto()      # Event(s) were executed by driver
    OBSERVED = auto()      # Contingent event was observed
    FAILED = auto()        # Execution failed
    CANCELED = auto()      # Execution was canceled


@dataclass
class DispatchMessage:
    """Message to/from the driver."""
    message_type: MessageType
    event_ids: List[str]
    timestamp: float
    data: Optional[Dict[str, Any]] = None


@dataclass
class DispatchCommand:
    """Command to dispatch events to driver."""
    event_ids: List[str]
    dispatch_time: float
    is_noop: bool = False  # Noop events update schedule but don't execute


@dataclass
class DispatchStatus:
    """Current status of the dispatch loop."""
    message: Optional[DispatchMessage] = None
    next_rted: Optional[RTED] = None


# ============================================================================
# Dispatcher Interface
# ============================================================================

class DispatcherCallback:
    """Interface for dispatcher event callbacks."""
    
    def on_dispatch(self, events: List[str], dispatch_time: float):
        """Called when events should be dispatched to the driver."""
        pass
    
    def on_complete(self, history: Dict[str, float]):
        """Called when all events have been executed."""
        pass
    
    def on_error(self, event_id: str, error: str):
        """Called when an error occurs during dispatch."""
        pass


# ============================================================================
# Kirk Dynamic Dispatcher
# ============================================================================

class KirkDispatcher:
    """
    Dynamic dispatcher for temporal plan execution.
    
    The dispatcher implements a tick-based loop that:
    1. Checks mailbox for execution confirmations from driver
    2. Gets the next RTED from the scheduler
    3. Dispatches events when within epsilon of scheduled time
    
    For contingent events, the dispatcher:
    - Buffers observations to their lower bounds
    - Assumes upper bound execution if no observation received
    
    Enhanced Features:
    - ExecutionTimingGuard integration for max execution time enforcement
    - Automatic cancellation when max time exceeded or schedule overflows
    - External event timeout handling with no-op generation
    
    Reference: enterprise/kirk-v2/src/dispatcher/dynamic-dispatcher.lisp
    """
    
    def __init__(
        self,
        scheduler: KirkScheduler,
        epsilon: float = 0.001,
        reschedule_early_ctg: bool = False,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None,
        timing_guard: Optional['ExecutionTimingGuard'] = None
    ):
        """
        Initialize the dispatcher.
        
        Args:
            scheduler: KirkScheduler instance
            epsilon: Time tolerance for dispatch (seconds). Events are dispatched
                    when within epsilon of their lower bound.
            reschedule_early_ctg: Whether to reschedule when contingent events
                                  are observed early
            verbose: Enable verbose logging
            logger: Optional logger instance
            timing_guard: Optional ExecutionTimingGuard for timing enforcement
        """
        self._scheduler = scheduler
        self._epsilon = epsilon
        self._reschedule_early_ctg = reschedule_early_ctg
        self._verbose = verbose
        self._logger = logger or logging.getLogger(__name__)
        self._timing_guard = timing_guard
        
        # Mailbox for driver communication
        self._mailbox: deque[DispatchMessage] = deque()
        self._mailbox_lock = threading.Lock()
        
        # Dispatch history: {event_id: dispatch_time}
        self._history: Dict[str, float] = {}
        
        # Start timestamp
        self._start_time: Optional[float] = None
        
        # Current RTED (cached)
        self._current_rted: Optional[RTED] = None
        self._rted_invalidated: bool = False
        
        # Running state
        self._running: bool = False
        self._paused: bool = False
        
        # Cancellation state
        self._cancelled: bool = False
        self._cancel_reason: Optional[str] = None
        
        # Callbacks
        self._dispatch_callback: Optional[Callable[[List[str], float], None]] = None
        self._complete_callback: Optional[Callable[[Dict[str, float]], None]] = None
        self._error_callback: Optional[Callable[[str, str], None]] = None
        self._cancel_callback: Optional[Callable[[str], None]] = None
        
        # Task anticipation callback: called with (event_ids, dispatch_time, unfulfilled_asks)
        # when events are dispatched in anticipation mode
        self._anticipation_dispatch_callback: Optional[
            Callable[[List[str], float, Set[str]], None]
        ] = None
        
        # Callback when anticipated task becomes fully unblocked (all asks fulfilled)
        # Called with (event_ids) - list of now-unblocked start events
        self._anticipation_unblocked_callback: Optional[
            Callable[[List[str]], None]
        ] = None
        
        
    def set_callbacks(
        self,
        on_dispatch: Optional[Callable[[List[str], float], None]] = None,
        on_complete: Optional[Callable[[Dict[str, float]], None]] = None,
        on_error: Optional[Callable[[str, str], None]] = None,
        on_cancel: Optional[Callable[[str], None]] = None,
        on_anticipation_dispatch: Optional[Callable[[List[str], float, Set[str]], None]] = None,
        on_anticipation_unblocked: Optional[Callable[[List[str]], None]] = None
    ):
        """
        Set callback functions for dispatch events.
        
        Args:
            on_dispatch: Called with (event_ids, dispatch_time) when events should execute
            on_complete: Called with history dict when all events are done
            on_error: Called with (event_id, error_message) on errors
            on_cancel: Called with (reason) when execution is cancelled due to timing
            on_anticipation_dispatch: Called with (event_ids, dispatch_time, unfulfilled_asks)
                                     when events are dispatched in anticipation mode.
                                     The executor should only dispatch non-critical actions.
            on_anticipation_unblocked: Called with (event_ids) when all asks for an
                                      anticipated task are fulfilled and critical actions
                                      can proceed.
        """
        self._dispatch_callback = on_dispatch
        self._complete_callback = on_complete
        self._error_callback = on_error
        self._cancel_callback = on_cancel
        self._anticipation_dispatch_callback = on_anticipation_dispatch
        self._anticipation_unblocked_callback = on_anticipation_unblocked
    
    def get_elapsed_time(self) -> float:
        """Get time elapsed since dispatch started."""
        if self._start_time is None:
            self._logger.info("[KirkDispatcher] Elapsed time requested before start; returning 0.0")
            return 0.0
        return time.time() - self._start_time
    
    def observe_event(self, event_id: str, observation_time: Optional[float] = None):
        """
        Observe a contingent event and update the schedule.
        
        Args:
            event_id: ID of the observed event
            observation_time: Time of observation (default: current elapsed time)
        """
        if observation_time is None:
            observation_time = self.get_elapsed_time()
        
        with self._mailbox_lock:
            self._mailbox.append(DispatchMessage(
                message_type=MessageType.OBSERVED,
                event_ids=[event_id],
                timestamp=observation_time
            ))
        
        # Invalidate current RTED to force re-evaluation
        self._rted_invalidated = True
    
    def report_executed(self, event_ids: List[str], execution_time: Optional[float] = None):
        """
        Report that events have been executed by the driver.
        
        Args:
            event_ids: List of executed event IDs
            execution_time: Time of execution (default: current elapsed time)
        """
        if execution_time is None:
            execution_time = self.get_elapsed_time()
        
        with self._mailbox_lock:
            self._mailbox.append(DispatchMessage(
                message_type=MessageType.EXECUTED,
                event_ids=event_ids,
                timestamp=execution_time
            ))
    
    def report_failed(self, event_id: str, error: str = ""):
        """Report that event execution failed."""
        with self._mailbox_lock:
            self._mailbox.append(DispatchMessage(
                message_type=MessageType.FAILED,
                event_ids=[event_id],
                timestamp=self.get_elapsed_time(),
                data={"error": error}
            ))
    
    def run(self):
        """
        Run the dispatch loop until all events are executed.
        
        This is a blocking call that runs until completion.
        Use run_async() for non-blocking operation.
        
        The dispatch loop integrates with ExecutionTimingGuard to:
        - Cancel execution when max time is exceeded
        - Cancel when scheduler schedules beyond max time
        - Generate no-ops for timed-out external events
        """
        self._start_time = time.time()
        self._running = True
        self._cancelled = False
        self._cancel_reason = None
        
        # Start timing guard if present
        if self._timing_guard:
            self._timing_guard.start_execution(self._start_time)
        
        status = DispatchStatus()
        
        self._log("Starting dispatch loop")
        
        while self._running and not self._cancelled and self._tick(status):
            # Small sleep to prevent busy-waiting
            time.sleep(0.001)
        
        self._log("Dispatch loop finished")
        
        if self._cancelled:
            self._logger.warning(f"[KirkDispatcher] Execution cancelled: {self._cancel_reason}")
            if self._cancel_callback:
                self._cancel_callback(self._cancel_reason)
        elif self._complete_callback:
            self._complete_callback(self._history)
    
    def run_async(self) -> threading.Thread:
        """
        Run the dispatch loop in a background thread.
        
        Returns:
            Thread object for the dispatch loop
        """
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the dispatch loop."""
        self._running = False
    
    def pause(self):
        """Pause dispatch (events won't be sent to driver)."""
        self._paused = True
    
    def resume(self):
        """Resume dispatch after pause."""
        self._paused = False
    
    def _tick(self, status: DispatchStatus) -> bool:
        """
        Single tick of the dispatch loop.
        
        Args:
            status: Dispatch status object (modified in place)
            
        Returns:
            True if should continue, False if done
            
        Reference: tick method in dynamic-dispatcher.lisp
        
        Enhanced with timing guard integration:
        - Checks max execution time
        - Validates RTED against max time (schedule overflow)
        - Handles external event timeouts with no-op generation
        """
        # ═══════════════════════════════════════════════════════════════════════
        # TIMING GUARD CHECK
        # ═══════════════════════════════════════════════════════════════════════
        if self._timing_guard:
            continue_exec, noop_events = self._timing_guard.tick(self.get_elapsed_time())
            
            if not continue_exec:
                self._cancelled = True
                self._cancel_reason = self._timing_guard.cancel_reason or "Timing guard cancelled execution"
                return False
            
            # Process any no-op events from external timeouts
            for event_id, noop_time in noop_events:
                self._logger.info(f"[KirkDispatcher] Generating no-op for timed-out external event: {event_id}")
                self._scheduler.update_schedule(event_id, noop_time)
                self._history[event_id] = noop_time
                self._rted_invalidated = True
        
        # Check if we still have events to execute
        if not self._scheduler.has_unexecuted_events():
            self._log("All events executed!")
            self._log(f"History: {self._history}")
            return False
        
        # Step 1: Process mailbox messages
        self._process_mailbox(status)
        
        # Handle RTED invalidation (contingent event came in)
        if self._rted_invalidated:
            status.next_rted = None
            self._rted_invalidated = False
        
        # Step 2: Get next RTED if needed
        if status.next_rted is None:
            status.next_rted = self._scheduler.get_next_rted(self.get_elapsed_time())
        
        if not status.next_rted.events:
            # No events to dispatch
            return True
        
        # ═══════════════════════════════════════════════════════════════════════
        # SCHEDULE OVERFLOW CHECK
        # ═══════════════════════════════════════════════════════════════════════
        if self._timing_guard:
            if not self._timing_guard.check_schedule_overflow(status.next_rted.time):
                self._cancelled = True
                self._cancel_reason = (
                    f"Schedule overflow: RTED time {status.next_rted.time:.2f}s "
                    f"exceeds max execution time"
                )
                return False
        
        # Check if we've already dispatched these events
        all_dispatched = all(
            event_id in self._history 
            for event_id, _ in status.next_rted.events
        )
        if all_dispatched:
            self._logger.debug(f"Already dispatched {status.next_rted.events}, waiting for confirmation")
            return True
        
        # Step 3: Check timing for dispatch
        elapsed_time = self.get_elapsed_time()
        event_time = status.next_rted.time
        time_until_dispatch = event_time - elapsed_time
        
        if time_until_dispatch > self._epsilon:
            # Too early to dispatch
            if self._verbose:
                self._logger.info(f"Too early to dispatch (wait {time_until_dispatch:.3f}s)")
            return True
        
        if self._paused:
            return True
        
        # Step 4: Dispatch events
        self._dispatch_events(status.next_rted, elapsed_time)
        
        # Clear RTED after dispatch
        status.next_rted = None
        
        return True
    
    def _process_mailbox(self, status: DispatchStatus):
        """Process messages from the mailbox."""
        with self._mailbox_lock:
            if not self._mailbox:
                return
            message = self._mailbox.popleft()
        
        status.message = message
        
        if message.message_type == MessageType.EXECUTED:
            for event_id in message.event_ids:
                success = self._scheduler.update_schedule(event_id, message.timestamp)
                if not success:
                    self._logger.info(f"Failed to record execution for {event_id}")
                # Mark event as completed in timing guard
                if self._timing_guard:
                    self._timing_guard.complete_own_event(event_id, message.timestamp)
            # Invalidate RTED after execution confirmation
            status.next_rted = None
            
        elif message.message_type == MessageType.OBSERVED:
            for event_id in message.event_ids:
                success = self._scheduler.update_schedule(event_id, message.timestamp)
                if success:
                    self._logger.info(f"Observed contingent event {event_id} at {message.timestamp:.3f}")
                    # Mark external event as received in timing guard
                    if self._timing_guard:
                        self._timing_guard.receive_external_event(event_id, message.timestamp)
                else:
                    # update_schedule can fail if the executor already called it
                    # directly (e.g., in _handle_external_completion). This is
                    # expected and harmless - the event is already recorded in
                    # the scheduler's fulfilled events ledger.
                    self._logger.info(
                        f"Event {event_id} already recorded in schedule "
                        f"(fulfilled={self._scheduler.is_event_fulfilled(event_id)})"
                    )
                
                # ═══════════════════════════════════════════════════════════
                # TASK ANTICIPATION: Check if any anticipated tasks are now
                # fully unblocked by this ask being fulfilled.
                # This runs regardless of update_schedule success because
                # the executor may have already updated the schedule directly
                # via _handle_external_completion before this mailbox message
                # is processed.
                # ═══════════════════════════════════════════════════════════
                if self._scheduler.task_anticipation:
                    newly_unblocked = self._scheduler.on_external_ask_fulfilled(event_id)
                    if newly_unblocked and self._anticipation_unblocked_callback:
                        self._anticipation_unblocked_callback(newly_unblocked)
                        self._log(
                            f"Ask {event_id} fulfilled - unblocked events: {newly_unblocked}"
                        )
            # Invalidate RTED after observation
            status.next_rted = None
            
        elif message.message_type == MessageType.FAILED:
            for event_id in message.event_ids:
                error = message.data.get("error", "Unknown error") if message.data else "Unknown error"
                self._logger.info(f"Event {event_id} failed: {error}")
                if self._error_callback:
                    self._error_callback(event_id, error)
    
    def _dispatch_events(self, rted: RTED, current_time: float):
        """
        Dispatch events from an RTED.
        
        Separates real events (sent to driver) from noop events (just update schedule).
        
        For anticipated RTEDs, uses the anticipation dispatch callback instead of
        the normal dispatch callback. The executor will only dispatch non-critical
        actions for anticipated tasks.
        
        For start events, registers the corresponding end event with the timing guard:
        - Contingent (external) end events: await_external_event() for timeout handling
        - Controllable (own) end events: start_own_event() for duration monitoring
        """
        real_events = []
        noop_events = []
        
        for event_id, is_noop in rted.events:
            if is_noop:
                noop_events.append(event_id)
            else:
                real_events.append(event_id)
        
        # Send real events to driver
        if real_events:
            if rted.anticipated and self._anticipation_dispatch_callback:
                # Anticipated dispatch: notify executor with unfulfilled asks info
                self._anticipation_dispatch_callback(
                    real_events, current_time, rted.unfulfilled_asks
                )
                # Mark events as anticipated in the scheduler
                for event_id in real_events:
                    self._scheduler.mark_event_anticipated(
                        event_id, rted.unfulfilled_asks
                    )
                self._log(
                    f"\u26a1 Anticipated dispatch: {real_events} "
                    f"(unfulfilled: {rted.unfulfilled_asks})"
                )
            elif self._dispatch_callback:
                self._dispatch_callback(real_events, current_time)
        
        # Register dispatched start events with timing guard for duration/timeout tracking
        if self._timing_guard:
            for event_id in real_events:
                self._register_event_with_timing_guard(event_id, current_time)
        
        # Update schedule with noop events immediately
        for event_id in noop_events:
            self._scheduler.update_schedule(event_id, rted.time)
        
        # Record in history
        for event_id in real_events + noop_events:
            self._history[event_id] = current_time
    
    def _register_event_with_timing_guard(self, event_id: str, dispatch_time: float):
        """
        Register an event with the timing guard for duration monitoring.
        
        When a start event is dispatched, we register its corresponding end event
        for duration monitoring. This applies to OWN tasks only.
        
        Before registering, the per-episode observation delay (gamma) is applied
        to the scheduler. This ensures that each task uses its own variable delay
        from the TPN's ContingentDuration, rather than a single global default.
        
        External events (cross-platform) must be explicitly registered via
        register_external_event_wait() - they are NOT auto-detected from contingency.
        
        Own tasks (even with contingent end events) get:
        - Duration monitoring with warnings when expected_duration + epsilon exceeded
        - NO automatic no-ops - they wait indefinitely for confirmation
        - Only max execution time can cancel
        
        Args:
            event_id: Event ID being dispatched
            dispatch_time: Time of dispatch
        """
        if not self._timing_guard:
            return
        
        # Check if this is a start event with a corresponding end event
        end_event_id = self._scheduler.get_end_event_for_start(event_id)
        if not end_event_id:
            return
        
        # Get the episode to check duration bounds
        episode = self._scheduler.get_episode_for_end_event(end_event_id)
        if not episode:
            return
        
        # ═══════════════════════════════════════════════════════════════════
        # SET PER-EPISODE VARIABLE DELAY (GAMMA) BEFORE DISPATCH
        # ═══════════════════════════════════════════════════════════════════
        # Each episode may have its own observation delay specified in its
        # ContingentDuration. Apply it now so the scheduler uses the correct
        # gamma for this specific task when processing its completion.
        episode_delay = self._scheduler.get_observation_delay_for_episode(episode)
        current_gamma = self._scheduler.get_gamma(end_event_id)
        if abs(episode_delay - current_gamma) > 1e-6:
            self._scheduler.set_gamma(end_event_id, episode_delay)
            self._log(
                f"Updated gamma for {end_event_id}: {current_gamma:.3f}s -> {episode_delay:.3f}s "
                f"(per-episode variable delay)"
            )
        
        # ALL task end events from TPN are OWN events
        # Register for duration monitoring (warnings only, no no-ops)
        # External events are handled separately via register_external_event_wait()
        expected_duration = episode.duration.upper_bound
        self._timing_guard.start_own_event(
            event_id=end_event_id,
            expected_duration=expected_duration,
            start_time=dispatch_time,
            epsilon=episode_delay  # Use per-episode delay as epsilon for duration warning
        )
        self._log(
            f"Registered own event {end_event_id} for duration monitoring "
            f"(expected={expected_duration:.2f}s, delay={episode_delay:.3f}s)"
        )
    
    def register_external_event_wait(
        self, 
        event_id: str, 
        wait_start_time: Optional[float] = None,
        max_delay: Optional[float] = None
    ):
        """
        Explicitly register an external event for timeout handling.
        
        Use this for cross-platform events that may never arrive. If the event
        is not observed within max_delay, a no-op is generated.
        
        If the event has already been fulfilled (checked via the scheduler's
        fulfilled events ledger), no timeout is registered.
        
        This also registers the event with the scheduler so that collect_rted()
        can generate upper-bound no-ops.
        
        Args:
            event_id: External event ID
            wait_start_time: When the wait started (default: current elapsed time)
            max_delay: Timeout delay (default: timing guard's external_event_delay)
        """
        # Check if the event is already fulfilled — no need to wait or timeout
        if self._scheduler.is_event_fulfilled(event_id):
            self._log(
                f"External event {event_id} already fulfilled — skipping timeout registration"
            )
            return
        
        actual_start = wait_start_time if wait_start_time is not None else self.get_elapsed_time()
        
        # Register with scheduler for upper-bound no-op in collect_rted()
        self._scheduler.register_external_event(event_id)
        
        # Register with timing guard for timeout tracking
        if self._timing_guard:
            actual_delay = max_delay if max_delay is not None else self._timing_guard.external_event_delay
            self._timing_guard.await_external_event(
                event_id=event_id,
                wait_start_time=actual_start,
                max_delay=actual_delay
            )
        
        self._log(f"Registered EXTERNAL event {event_id} for timeout (start={actual_start:.2f}s)")
    
    def _log(self, message: str):
        """Log a message if verbose mode is enabled."""
        if self._verbose:
            self._logger.info(f"[KirkDispatcher] {message}")
    
    # =========================================================================
    # State Query Methods
    # =========================================================================
    
    def get_history(self) -> Dict[str, float]:
        """Get dispatch history."""
        return self._history.copy()
    
    def get_pending_events(self) -> List[str]:
        """Get events that haven't been dispatched yet."""
        return self._scheduler.get_pending_events()
    
    def get_commitments(self) -> Dict[Tuple[str, bool], Tuple[float, float]]:
        """Get current time commitments for all events."""
        return self._scheduler.get_commitments()
    
    def is_running(self) -> bool:
        """Check if dispatcher is running."""
        return self._running
    
    def is_paused(self) -> bool:
        """Check if dispatcher is paused."""
        return self._paused
    
    def is_cancelled(self) -> bool:
        """Check if dispatcher was cancelled."""
        return self._cancelled
    
    def get_cancel_reason(self) -> Optional[str]:
        """Get the reason for cancellation."""
        return self._cancel_reason
    
    # =========================================================================
    # Timing Guard Integration
    # =========================================================================
    
    def set_timing_guard(self, timing_guard: 'ExecutionTimingGuard'):
        """
        Set or replace the timing guard.
        
        Args:
            timing_guard: ExecutionTimingGuard instance
        """
        self._timing_guard = timing_guard
    
    def get_timing_guard(self) -> Optional['ExecutionTimingGuard']:
        """Get the current timing guard."""
        return self._timing_guard
    
    def cancel(self, reason: str = "Manual cancellation"):
        """
        Cancel the dispatch loop.
        
        Args:
            reason: Reason for cancellation
        """
        self._cancelled = True
        self._cancel_reason = reason
        self._running = False