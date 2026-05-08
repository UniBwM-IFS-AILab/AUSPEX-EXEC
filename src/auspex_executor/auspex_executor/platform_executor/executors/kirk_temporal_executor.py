#!/usr/bin/env python3
"""
Kirk Temporal Executor Module

This module provides a temporal executor that uses the Kirk scheduler and dispatcher
for executing TPNs (Temporal Plan Networks) with real-time constraints.

The Kirk executor implements the FAST-EX algorithm for temporal execution with
Variable Delay Controllability (VDC) semantics, properly handling both controllable
and uncontrollable (contingent) events.

Key features:
- Task-centric execution: works with Tasks throughout, only maps to Actions at dispatch
- Real-time dispatch of controllable events at their lower bounds
- Observation handling for contingent events
- Multi-platform coordination via TaskEventCommunication
- Integration with the AUSPEX executor framework
- Task anticipation: early dispatch of non-critical actions for tasks with pending asks

Advanced Timing Features:
- Maximum execution time enforcement with automatic cancellation
- Own event duration monitoring with epsilon-based warnings
- External event timeout handling with automatic no-op generation

Task Anticipation:
- When enabled, tasks blocked by unfulfilled external asks dispatch non-critical
  actions early, while holding back critical actions until asks are fulfilled.
- Controlled by ``task_anticipation`` flag and ``critical_action_indices`` mapping.

Matching Strategy:
- task_id is the sole key: action.task_id == task.id == episode.annotations['task_id']
- Episodes link to Tasks via annotations['task_id']
- Actions are looked up from Tasks only when dispatching to the driver

Reference: enterprise/kirk-v2
"""

from typing import List, Dict, Optional, Any, Tuple
import time
import threading
import json
import traceback

from auspex_executor.platform_executor.executors.base_executor import BaseExecutor
from auspex_executor.platform_executor.executors.task_event_communication import (
    TaskEventData, TaskEventType
)
from auspex_executor.platform_executor.executors.kirk import (
    KirkScheduler, KirkDispatcher, RTED, ExecutionTimingGuard, TimingViolationType
)

from auspex_executor.utils.knowledge_client_ros import KnowledgeClientROS
from auspex_executor.utils.tpn_helper import (
    TemporalPlanNetwork, TPNEvent, Episode
)
from auspex_executor.utils.utils import enum_to_str

from auspex_msgs.msg import (
    ExecutorState,
    ExecutionInfo,
    PlannerCommand,
    PlanStatus,
    ActionStatus,
    Action,
    Task,
    TaskEvent,
)


class KirkTemporalExecutor(BaseExecutor):
    """
    Task-centric temporal executor using Kirk scheduler/dispatcher for TPN execution.
    
    This executor works with Tasks as the primary unit throughout execution.
    Actions are only looked up from tasks when dispatching to the driver.
    The sole matching key is task_id:
    - action.task_id == task.id == episode.annotations['task_id']
    
    Unlike the StandardExecutor which executes actions sequentially, this executor:
    - Respects temporal windows from the TPN
    - Waits until the appropriate time to dispatch tasks
    - Handles contingent events via observations
    - Coordinates with other platforms on shared constraints
    
    Advanced Timing Features:
    - **Max Execution Time**: Hard deadline for entire execution (e.g., 20 minutes)
      When exceeded or when scheduler schedules beyond max time, execution cancels
    - **Own Event Duration Monitoring**: Warns when platform tasks exceed expected
      duration + epsilon, but continues execution
    - **External Event Timeout**: Generates no-op events for cross-platform events
      that aren't received within (event_start_time + default_delay)
    
    Task Anticipation:
    - When ``task_anticipation`` is enabled, tasks blocked by unfulfilled external
      asks can begin executing non-critical actions early.
    - Critical actions (defined by ``critical_action_indices``) are held back until
      all external asks are fulfilled.
    - Example: For a "deliver" task with 4 actions where actions 1,2,3 are critical,
      the executor dispatches action 0 (fly_to) immediately in anticipation mode,
      then dispatches actions 1,2,3 when all asks arrive.
    - When disabled (default), the executor waits for all external asks and
      introduces a no-op if exceeded bounds.
    
    Configuration:
    - `max_execution_time`: Maximum allowed execution time (seconds), None for unlimited
    - `duration_epsilon`: Tolerance for own event duration warnings (seconds)
    - `default_delay`: Unified delay used for both:
        - Observation delay for contingent events
        - External event timeout (event_start_time + default_delay)
    - `task_anticipation`: Enable/disable task anticipation (default: False)
    - `critical_action_indices`: Dict mapping task names to lists of 0-based
      critical action indices
    
    Example:
        executor = KirkTemporalExecutor(platform_id=\"drone_1\")
        executor.max_execution_time = 1200.0  # 20 minutes
        executor.duration_epsilon = 10.0       # 10 second tolerance
        executor.set_default_delay(30.0)       # 30 second delay
        executor.task_anticipation = True      # Enable anticipation
        executor.critical_action_indices = {"deliver": [1, 2, 3]}
    """
    
    EXECUTOR_TYPE = "kirk"
    
    def __init__(self, platform_id: str, knowledge_client_ros: Optional[KnowledgeClientROS] = None):
        """
        Initialize the Kirk temporal executor.
        """
        super().__init__(platform_id, knowledge_client_ros)
        
        self._verbose = False
        self._epsilon = 0.01
        self._max_execution_time = 999999999.0
        self._duration_epsilon = 10.0
        self._default_delay = 5.0
        
        # Timing guard instance (created in _init_kirk_components)
        self._timing_guard: Optional[ExecutionTimingGuard] = None
        
        # Kirk components
        self._scheduler: Optional[KirkScheduler] = None
        self._dispatcher: Optional[KirkDispatcher] = None
        self._dispatcher_thread: Optional[threading.Thread] = None
        
        # TPN and task mappings (task_id is the sole matching key)
        self._tpn: Optional[TemporalPlanNetwork] = None
        
        # task_id -> Episode (episode.annotations['task_id'] == task.id)
        self._task_to_episode: Dict[int, Episode] = {}
        
        # event_id -> Task (the task whose episode starts at this event)
        self._event_to_task: Dict[str, Task] = {}
        
        # episode_id -> (start_event_id, end_event_id)
        self._episode_to_events: Dict[str, Tuple[str, str]] = {}
        
        # task_id -> List[Action] (lookup cache, populated lazily from _current_actions)
        # Multiple actions can relate to one task
        self._task_to_actions: Dict[int, List[Action]] = {}
        
        # Execution tracking
        self._events_in_progress: Dict[str, float] = {}  # event_id -> start_time
        self._completed_actions_per_task: Dict[int, int] = {}  # task_id -> count of completed actions
        self._lock = threading.Lock()
        
        # Expected durations for own events (computed from TPN)
        self._expected_durations: Dict[str, float] = {}  # event_id -> expected_duration
        
        # External event tracking (for timeout handling)
        self._external_events_pending: Dict[str, str] = {}  # event_id -> source_platform
        
        # Buffer for external events received before the scheduler/dispatcher
        # were initialised.  These are replayed once Kirk components are ready.
        # Maps event_id -> (timestamp, source_platform)
        self._early_fulfilled_events: Dict[str, Tuple[float, str]] = {}
        
        # ═══════════════════════════════════════════════════════════════════════
        # TASK ANTICIPATION
        # ═══════════════════════════════════════════════════════════════════════
        # When enabled, tasks blocked by unfulfilled external asks can begin
        # executing non-critical actions early. Critical actions are held back
        # until all asks are fulfilled.
        self._task_anticipation: bool = True
        
        # Critical action indices per task name (0-based).
        # Actions at these indices require all external asks to be fulfilled.
        # Actions at other indices can execute during anticipation.
        # Example: {"deliver": [1, 2, 3]} means the 2nd, 3rd, 4th actions of
        # "deliver" are critical. The 1st action (e.g. "fly_to") can proceed.
        self._critical_action_indices: Dict[str, List[int]] = {}
        
        # Tasks currently executing in anticipation mode
        # Maps task_id -> list of critical Action objects held back
        self._anticipated_tasks: Dict[int, List[Action]] = {}
        
        # Maps task_id -> start_event_id for anticipated tasks (for tracking)
        self._anticipated_task_events: Dict[int, str] = {}
        
        # Critical actions deferred until non-critical actions finish.
        # When _on_anticipation_unblocked fires while the OBC is still busy
        # executing non-critical actions, critical actions are parked here
        # and dispatched from _handle_actions_succeeded once the OBC is free.
        # Maps task_id -> list of critical Action objects
        self._pending_critical_actions: Dict[int, List[Action]] = {}
        
        # Sequential fallback: when True, the executor runs actions
        # sequentially like the StandardExecutor (no TPN available).
        self._sequential_fallback: bool = False
    
    def _init_executor_specifics(self):
        """Initialize Kirk-specific components."""
        self.get_logger().info(f"[KirkTemporalExecutor] Initialized for platform {self._platform_id}")
        
        # Log timing configuration
        max_time_str = f"{self._max_execution_time:.1f}s" if self._max_execution_time else "∞ (unlimited)"
        self.get_logger().info(
            f"[KirkTemporalExecutor] ⏱️  Timing Configuration:\n"
            f"  ├── Max Execution Time  : {max_time_str}\n"
            f"  ├── Duration Epsilon    : {self._duration_epsilon:.1f}s\n"
            f"  ├── Default Delay       : {self._default_delay:.1f}s (observation + external timeout)\n"
            f"  └── Task Anticipation   : {'enabled' if self._task_anticipation else 'disabled'}"
        )
        
        if self._verbose:
            self.get_logger().info(f"[KirkTemporalExecutor] Epsilon: {self._epsilon}s")
    
    def _reset_executor_specifics(self):
        """Reset Kirk-specific state."""
        # Stop dispatcher if running
        if self._dispatcher is not None:
            self._dispatcher.stop()
        
        # Reset timing guard
        if self._timing_guard is not None:
            self._timing_guard.reset()
        
        self._scheduler = None
        self._dispatcher = None
        self._dispatcher_thread = None
        self._timing_guard = None
        self._tpn = None
        self._task_to_episode.clear()
        self._event_to_task.clear()
        self._episode_to_events.clear()
        self._task_to_actions.clear()
        self._events_in_progress.clear()
        self._completed_actions_per_task.clear()
        self._expected_durations.clear()
        self._external_events_pending.clear()
        self._early_fulfilled_events.clear()
        self._anticipated_tasks.clear()
        self._anticipated_task_events.clear()
        self._pending_critical_actions.clear()
        self._sequential_fallback = False
    
    def _check_pending_execution(self):
        """
        Check for pending executions.
        
        The Kirk executor uses a dispatcher thread, so this method
        checks if we need to start or restart the dispatcher.
        """
        if self._sequence_client._result_mutex.acquire(blocking=False):
            try:
                if self._pending_execution:
                    # Do not dispatch new work while paused. This preserves
                    # pending work until an explicit CONTINUE is received.
                    if self._is_vhcl_paused:
                        return

                    if self._sequential_fallback:
                        # Sequential fallback: execute one action at a time
                        if len(self._current_actions) > 0:
                            self._pending_execution = False
                            self.get_logger().info("[KirkTemporalExecutor] Sequential fallback: dispatching next action")
                            self.execute_sequence(sequence_length=1)
                        else:
                            self._pending_execution = False
                    elif self._tpn is not None:
                        self._pending_execution = False
                        
                        # Start the dispatcher if not already running
                        if self._dispatcher is None:
                            self._init_kirk_components()
                        
                        if self._dispatcher is not None and not self._dispatcher.is_running():
                            self._start_dispatcher()
            finally:
                self._sequence_client._result_mutex.release()
    
    def _on_plan_ready(self):
        """
        Called when a plan is ready for execution.
        Initialize Kirk scheduler and dispatcher from the TPN.
        """
        self.get_logger().info(f"[KirkTemporalExecutor] Plan ready with {len(self._current_tasks)} tasks")
        
        if self._current_tpn is None:
            self.get_logger().warn("[KirkTemporalExecutor] No TPN data present - falling back to sequential execution")
            self._sequential_fallback = True
            return True
        
        # Parse TPN
        try:
            self._tpn = TemporalPlanNetwork.from_dict(self._current_tpn)
            self.get_logger().info(f"[KirkTemporalExecutor] Parsed TPN with {len(self._tpn.events)} events, {len(self._tpn.episodes)} episodes")           
        except Exception as e:
            self.get_logger().error(f"[KirkTemporalExecutor] Failed to parse TPN: {e}")
            traceback.print_exc()
            return False
        
        self._build_task_mapping()
        return True
    
    def _init_kirk_components(self):
        """Initialize the Kirk scheduler and dispatcher."""
        if self._tpn is None:
            return
        
        try:
            # ═══════════════════════════════════════════════════════════════════
            # CREATE TIMING GUARD
            # ═══════════════════════════════════════════════════════════════════
            self._timing_guard = ExecutionTimingGuard(
                max_execution_time=self._max_execution_time,
                duration_epsilon=self._duration_epsilon,
                external_event_delay=self._default_delay,  # Use default_delay for external event timeout
                logger=self.get_logger(),
                verbose=self._verbose
            )
            
            # Set up timing guard callbacks
            self._timing_guard.set_callbacks(
                on_cancel=self._on_timing_cancel,
                on_noop_generated=self._on_external_event_noop,
                on_duration_warning=self._on_duration_warning
            )
            
            # ═══════════════════════════════════════════════════════════════════
            # CREATE SCHEDULER
            # ═══════════════════════════════════════════════════════════════════
            self._scheduler = KirkScheduler(
                tpn=self._tpn,
                default_delay=self._default_delay,
                reschedule_early_ctg=True,
                use_fixed_delay_conversion=True,
                verbose=self._verbose,
                logger=self.get_logger()
            )
            
            # ═══════════════════════════════════════════════════════════════════
            # CONFIGURE TASK ANTICIPATION ON SCHEDULER
            # ═══════════════════════════════════════════════════════════════════
            self._scheduler.task_anticipation = self._task_anticipation
            self._scheduler.critical_action_indices = self._critical_action_indices
            
            if self._task_anticipation:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Task anticipation enabled\n"
                    f"  └── Critical action indices: {self._critical_action_indices}"
                )
            
            if self._verbose:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Scheduler initialized with "
                    f"default_delay={self._default_delay}s"
                )
            
            # ═══════════════════════════════════════════════════════════════════
            # CREATE DISPATCHER WITH TIMING GUARD
            # ═══════════════════════════════════════════════════════════════════
            self._dispatcher = KirkDispatcher(
                scheduler=self._scheduler,
                epsilon=self._epsilon,
                verbose=self._verbose,
                logger=self.get_logger(),
                timing_guard=self._timing_guard
            )
            
            # Set up dispatch callback
            self._dispatcher.set_callbacks(
                on_dispatch=self._on_events_dispatch,
                on_complete=self._on_dispatch_complete,
                on_error=self._on_dispatch_error,
                on_cancel=self._on_dispatch_cancelled,
                on_anticipation_dispatch=self._on_anticipation_dispatch,
                on_anticipation_unblocked=self._on_anticipation_unblocked
            )
            
            # Compute expected durations for timing monitoring
            self._compute_expected_durations()
            
            self.get_logger().info("[KirkTemporalExecutor] Kirk components initialized with timing guard")
            
        except Exception as e:
            self.get_logger().error(f"[KirkTemporalExecutor] Failed to initialize Kirk: {e}")
            traceback.print_exc()
            self._scheduler = None
            self._dispatcher = None
            self._timing_guard = None
    
    def _start_dispatcher(self):
        """Start the dispatcher in a background thread."""
        if self._dispatcher is None:
            return
        
        self.change_executor_state(ExecutorState.STATE_EXECUTING)
        
        # Replay any external events that arrived before the scheduler was ready
        self._replay_early_fulfilled_events()
        
        self._dispatcher_thread = self._dispatcher.run_async()
        self.get_logger().info("[KirkTemporalExecutor] Dispatcher started")
    
    def _replay_early_fulfilled_events(self):
        """
        Replay external events that were buffered before the scheduler was ready.
        
        Events received via _handle_external_completion before _init_kirk_components
        are stored in _early_fulfilled_events.  Now that the scheduler exists we
        feed them through so the distance graph is updated and the events are
        marked as fulfilled.
        """
        if not self._early_fulfilled_events:
            return
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] Replaying {len(self._early_fulfilled_events)} "
            f"early external event(s): {list(self._early_fulfilled_events.keys())}"
        )
        
        for event_id, (timestamp, source_platform) in self._early_fulfilled_events.items():
            # Record in the scheduler's fulfilled ledger and update the schedule
            if self._scheduler is not None:
                self._scheduler.record_fulfilled_event(event_id, timestamp)
            
            # Also notify the timing guard if it's tracking this event
            if self._timing_guard:
                self._timing_guard.receive_external_event(event_id, timestamp)
            
            # Remove from pending external events (may have been registered already)
            self._external_events_pending.pop(event_id, None)
        
        self._early_fulfilled_events.clear()
    
    # =========================================================================
    # Timing Guard Callbacks
    # =========================================================================
    
    def _on_timing_cancel(self, reason: str):
        """
        Callback when timing guard cancels execution.
        
        Args:
            reason: Reason for cancellation
        """
        self.get_logger().error(
            f"[KirkTemporalExecutor] EXECUTION CANCELLED BY TIMING GUARD\n"
            f"  └── Reason: {reason}"
        )
        
        # Abort all current tasks
        for task in self._current_tasks:
            self._update_task_status(task.id, ActionStatus.ABORTED)
        
        # Update plan status
        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
        
        # Publish cancellation
        self.publish_controllerCommand(
            command=PlannerCommand.ABORTED,
            info_msg=ExecutionInfo(
                platform_id=self._platform_id, 
                success=False,
            )
        )
        
        self.reset_execution()
    
    def _on_external_event_noop(self, event_id: str, noop_time: float):
        """
        Callback when a no-op is generated for a timed-out external event.
        
        Args:
            event_id: The external event that timed out
            noop_time: Time at which the no-op was generated
        """
        self.get_logger().warning(
            f"[KirkTemporalExecutor] NO-OP generated for external event\n"
            f"  ├── Event: {event_id}\n"
            f"  ├── Time: {noop_time:.2f}s\n"
            f"  └── Continuing execution with assumed event completion"
        )
        
        # Remove from pending external events
        self._external_events_pending.pop(event_id, None)
    
    def _on_duration_warning(self, event_id: str, expected: float, actual: float):
        """
        Callback when an own event exceeds its expected duration.
        
        Args:
            event_id: Event that exceeded duration
            expected: Expected duration
            actual: Actual duration
        """
        overshoot = actual - expected
        self.get_logger().warning(
            f"[KirkTemporalExecutor] DURATION WARNING - OWN EVENT OVERDUE\n"
            f"  ├── Event: {event_id}\n"
            f"  ├── Expected Duration: {expected:.2f}s\n"
            f"  ├── Actual Duration: {actual:.2f}s\n"
            f"  ├── Overshoot: +{overshoot:.2f}s\n"
            f"  └── Continuing execution..."
        )
    
    def _on_dispatch_cancelled(self, reason: str):
        """
        Callback when dispatcher cancels due to timing.
        
        Args:
            reason: Cancellation reason
        """
        self.get_logger().error(f"[KirkTemporalExecutor] Dispatch cancelled: {reason}")
        # The timing guard callback will handle the actual cancellation
    
    def _compute_expected_durations(self):
        """
        Compute expected durations for all episodes in the TPN.
        
        These are used for own event duration monitoring.
        """
        if self._tpn is None:
            return
        
        self._expected_durations.clear()
        
        for episode in self._tpn.episodes.values():
            # Use the upper bound as expected duration (worst case)
            # For monitoring, we'll warn when this + epsilon is exceeded
            try:
                expected = float(episode.duration.upper_bound)
                self._expected_durations[episode.from_event] = expected
                
                if self._verbose:
                    self.get_logger().info(
                        f"[KirkTemporalExecutor] Expected duration for {episode.id}: {expected:.2f}s"
                    )
            except (ValueError, TypeError):
                # Use a default if conversion fails
                self._expected_durations[episode.from_event] = 60.0
    
    def _is_external_event(self, event_id: str) -> bool:
        """
        Check if an event is an external (cross-platform) event.
        
        External events are those from asks/tells with other platforms.
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if this is an external event
        """
        if self._tpn is None:
            return False
        
        event = self._tpn.events.get(event_id)
        if event is None:
            return False
        
        # Check if event has platform annotation different from ours
        if event.annotations:
            event_platform = event.annotations.get('platform_id')
            if event_platform and event_platform != self._platform_id:
                return True
        
        return False
    
    def _get_event_expected_duration(self, event_id: str) -> Optional[float]:
        """Get the expected duration for an event."""
        return self._expected_durations.get(event_id)
    
    def _apply_episode_delay_for_task(self, task: Task, event_id: str):
        """
        Apply the per-episode variable delay (gamma) for a task before dispatch.
        
        Reads the observation delay from the episode's ContingentDuration and
        updates the scheduler's gamma for the task's end event. This ensures
        that each task dispatched uses its own delay value rather than the
        global default.
        
        This is called right before a task is dispatched, so the scheduler
        has the correct gamma when it later processes the completion of
        this specific task.
        
        Args:
            task: Task being dispatched
            event_id: Start event ID for the task
        """
        if self._scheduler is None:
            return
        
        episode = self._task_to_episode.get(task.id)
        if episode is None:
            return
        
        # Get the per-episode observation delay
        episode_delay = self._scheduler.get_observation_delay_for_episode(episode)
        
        # Apply to the end event's gamma in the scheduler
        end_event_id = self._scheduler.get_end_event_for_start(event_id)
        if end_event_id:
            current_gamma = self._scheduler.get_gamma(end_event_id)
            if abs(episode_delay - current_gamma) > 1e-6:
                self._scheduler.set_gamma(end_event_id, episode_delay)
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Set variable delay for task '{task.name}' "
                    f"(event {end_event_id}): {current_gamma:.3f}s -> {episode_delay:.3f}s"
                )
            elif self._verbose:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Variable delay for task '{task.name}' "
                    f"(event {end_event_id}): {episode_delay:.3f}s (unchanged)"
                )
    
    def _build_task_mapping(self):
        """
        Build mapping between TPN events/episodes and tasks.
        
        Matching strategy: task_id is the sole key
        - episode.annotations['task_id'] == task.id
        - event -> episode -> task
        """
        self._task_to_episode.clear()
        self._event_to_task.clear()
        self._episode_to_events.clear()
        self._task_to_actions.clear()
        
        if self._tpn is None:
            return
        
        # Build episode to event mapping
        for episode in self._tpn.episodes.values():
            self._episode_to_events[episode.id] = (episode.from_event, episode.to_event)
        
        # Build task_id -> List[Action] lookup cache from _current_actions
        # Multiple actions can map to the same task
        for action in self._current_actions:
            if action.task_id not in self._task_to_actions:
                self._task_to_actions[action.task_id] = []
            self._task_to_actions[action.task_id].append(action)
        
        # Match tasks to episodes by task_id
        for task in self._current_tasks:
            episode = self._find_episode_for_task(task)
            if episode is not None:
                self._task_to_episode[task.id] = episode
                
                # Map the start event to the task
                if episode.from_event:
                    self._event_to_task[episode.from_event] = task
                
                if self._verbose:
                    self.get_logger().info(
                        f"[KirkTemporalExecutor] Mapped task '{task.name}' (id={task.id}) "
                        f"to episode '{episode.id}'"
                    )
            else:
                self.get_logger().warn(
                    f"[KirkTemporalExecutor] No episode found for task: {task.name} (id={task.id})"
                )
    
    def _find_episode_for_task(self, task: Task) -> Optional[Episode]:
        """
        Find the TPN episode corresponding to a task.
        
        Match by task_id in episode annotations.
        episode.annotations['task_id'] == task.id
        
        Args:
            task: Task to find episode for
            
        Returns:
            Episode if found, None otherwise
        """
        if self._tpn is None:
            return None
        for episode in self._tpn.episodes.values():
            if int(episode.annotations.get('task_id')) == task.id:
                return episode
        return None
    
    def _get_actions_for_task(self, task: Task) -> List[Action]:
        """
        Get all Actions for a given Task.
        
        Multiple actions can relate to one task.
        Match by task_id: action.task_id == task.id
        
        Args:
            task: Task to get actions for
            
        Returns:
            List of Actions (empty if none found)
        """
        # First check cache
        if task.id in self._task_to_actions:
            return self._task_to_actions[task.id]
        
        # Search in _current_actions and build cache
        actions = [action for action in self._current_actions if action.task_id == task.id]
        if actions:
            self._task_to_actions[task.id] = actions
        
        return actions
    
    def _get_task_for_action(self, action: Action) -> Optional[Task]:
        """
        Get the Task for a given Action.
        
        Match by task_id: action.task_id == task.id
        
        Args:
            action: Action to get task for
            
        Returns:
            Task if found, None otherwise
        """
        for task in self._current_tasks:
            if task.id == action.task_id:
                return task
        return None
    
    def _on_events_dispatch(self, event_ids: List[str], dispatch_time: float):
        """
        Callback when events should be dispatched to the driver.
        
        This is called by the Kirk dispatcher when events are ready to execute.
        Maps event -> task -> action only at dispatch time.
        
        Also registers own events with the timing guard for duration monitoring.
        
        Args:
            event_ids: List of event IDs to dispatch
            dispatch_time: Current time relative to start
        """
        self.get_logger().info(f"[KirkTemporalExecutor] Dispatching events: {event_ids} at time {dispatch_time:.3f}")
        
        # Find tasks for these events, then get actions to execute
        actions_to_execute: List[Action] = []
        
        for event_id in event_ids:
            if event_id in self._event_to_task:
                task = self._event_to_task[event_id]
                
                # ═══════════════════════════════════════════════════════════════
                # SET PER-TASK VARIABLE DELAY BEFORE DISPATCH
                # ═══════════════════════════════════════════════════════════════
                # Each task's episode may have its own observation delay.
                # Apply it to the scheduler now, before dispatch, so the correct
                # gamma is used when processing completion of this specific task.
                self._apply_episode_delay_for_task(task, event_id)
                
                # Look up all actions for this task (only at dispatch time)
                task_actions = self._get_actions_for_task(task)
                if task_actions:
                    actions_to_execute.extend(task_actions)
                else:
                    self.get_logger().warn(f"[KirkTemporalExecutor] No actions found for task {task.name} (id={task.id})")
                
                # Track that this task/event is in progress
                with self._lock:
                    self._events_in_progress[event_id] = dispatch_time
                
                # Record wall-clock task start time for overrun calculation
                self._task_start_times[task.id] = time.time()
                
                # ═══════════════════════════════════════════════════════════════
                # TIMING GUARD: Track own event for duration monitoring
                # ═══════════════════════════════════════════════════════════════
                if self._timing_guard:
                    expected_duration = self._get_event_expected_duration(event_id)
                    episode = self._task_to_episode.get(task.id)
                    episode_delay = (
                        self._scheduler.get_observation_delay_for_episode(episode)
                        if self._scheduler and episode else self._default_delay
                    )
                    if expected_duration is not None:
                        self._timing_guard.start_own_event(
                            event_id=event_id,
                            expected_duration=expected_duration,
                            start_time=dispatch_time,
                            epsilon=episode_delay  # Per-episode delay as tolerance
                        )
                        if self._verbose:
                            self.get_logger().info(
                                f"[KirkTemporalExecutor] Tracking own event {event_id}: "
                                f"expected_duration={expected_duration:.2f}s, "
                                f"delay={episode_delay:.3f}s"
                            )
                
                # Publish task started event
                self._publish_task_started(event_id, dispatch_time)

                # This confirms to the dispatcher that the start event has begun
                if self._dispatcher is not None:
                    self._dispatcher.report_executed([event_id], dispatch_time)
                    self.get_logger().info(f"[KirkTemporalExecutor] Reported start event {event_id} as executed at {dispatch_time:.3f}")
            else:
                if self._verbose:
                    self.get_logger().info(f"[KirkTemporalExecutor] No task for event {event_id} (might be ordering constraint or end event)")
        
        if actions_to_execute:
            self._dispatch_actions(actions_to_execute)
    
    def _dispatch_actions(self, actions: List[Action]):
        """
        Send actions to be executed by the platform.
        
        Args:
            actions: Actions to execute
        """
        # Remove from current_actions list
        for action in actions:
            if action in self._current_actions:
                self._current_actions.remove(action)
        
        # Update KB status for actions
        for action in actions:
            self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.ACTIVE))
        
        # Update task status to ACTIVE for all tasks that have actions being dispatched
        task_ids_dispatched = set(action.task_id for action in actions)
        for task_id in task_ids_dispatched:
            self._update_task_status(task_id, ActionStatus.ACTIVE)
        
        # Send to sequence client
        self._sequence_client.send_action_goal(actions)
    
    # =========================================================================
    # Task Anticipation Dispatch
    # =========================================================================
    
    def _on_anticipation_dispatch(self, event_ids: List[str], dispatch_time: float, unfulfilled_asks: set):
        """
        Callback when events are dispatched in anticipation mode.
        
        Only dispatches non-critical actions for the task. Critical actions are
        held back until all external asks are fulfilled.
        
        Args:
            event_ids: List of event IDs to dispatch
            dispatch_time: Current time relative to start
            unfulfilled_asks: Set of external ask event IDs not yet fulfilled
        """
        self.get_logger().info(
            f"[KirkTemporalExecutor] ANTICIPATION DISPATCH: events={event_ids} "
            f"at {dispatch_time:.3f} (unfulfilled asks: {unfulfilled_asks})"
        )
        
        actions_to_execute: List[Action] = []
        
        for event_id in event_ids:
            if event_id not in self._event_to_task:
                if self._verbose:
                    self.get_logger().info(
                        f"[KirkTemporalExecutor] No task for event {event_id} (ordering constraint)"
                    )
                continue
            
            task = self._event_to_task[event_id]
            
            # Apply per-task variable delay
            self._apply_episode_delay_for_task(task, event_id)
            
            # Look up all actions for this task
            task_actions = self._get_actions_for_task(task)
            if not task_actions:
                self.get_logger().warn(
                    f"[KirkTemporalExecutor] No actions found for anticipated task "
                    f"{task.name} (id={task.id})"
                )
                continue
            
            # Determine which task name to look up critical indices for
            task_name = task.name.lower() if hasattr(task, 'name') else ""
            critical_indices = self._critical_action_indices.get(task_name, [])
            
            if not critical_indices:
                # No critical actions defined for this task type - dispatch all normally
                self.get_logger().info(
                    f"[KirkTemporalExecutor] No critical actions defined for '{task_name}' "
                    f"- dispatching all {len(task_actions)} actions"
                )
                actions_to_execute.extend(task_actions)
            else:
                # Split actions into non-critical (dispatch now) and critical (hold back)
                non_critical = []
                critical = []
                
                for idx, action in enumerate(task_actions):
                    if idx in critical_indices:
                        critical.append(action)
                    else:
                        non_critical.append(action)
                
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Task '{task_name}' anticipation split:\n"
                    f"  ├── Non-critical (dispatch now): {[a.name for a in non_critical]}\n"
                    f"  └── Critical (held back):        {[a.name for a in critical]}"
                )
                
                actions_to_execute.extend(non_critical)
                
                # Store critical actions for later dispatch
                if critical:
                    with self._lock:
                        self._anticipated_tasks[task.id] = critical
                        self._anticipated_task_events[task.id] = event_id
                    
                    # NOTE: Do NOT start idle wait here. The UAV is actively
                    # executing non-critical actions, so it is NOT idle.
                    # Idle wait is started later in _handle_actions_succeeded
                    # (only if non-critical actions finish before external
                    # asks are fulfilled).
            
            # Track that this task/event is in progress
            with self._lock:
                self._events_in_progress[event_id] = dispatch_time
            
            # Record wall-clock task start time for overrun calculation
            self._task_start_times[task.id] = time.time()
            
            # Timing guard: track own event
            if self._timing_guard:
                expected_duration = self._get_event_expected_duration(event_id)
                episode = self._task_to_episode.get(task.id)
                episode_delay = (
                    self._scheduler.get_observation_delay_for_episode(episode)
                    if self._scheduler and episode else self._default_delay
                )
                if expected_duration is not None:
                    self._timing_guard.start_own_event(
                        event_id=event_id,
                        expected_duration=expected_duration,
                        start_time=dispatch_time,
                        epsilon=episode_delay
                    )
            
            # Publish task started event
            self._publish_task_started(event_id, dispatch_time)
            
            # Confirm start event to dispatcher
            if self._dispatcher is not None:
                self._dispatcher.report_executed([event_id], dispatch_time)
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Reported anticipated start event "
                    f"{event_id} as executed at {dispatch_time:.3f}"
                )
        
        if actions_to_execute:
            self._dispatch_actions(actions_to_execute)
    
    def _on_anticipation_unblocked(self, event_ids: List[str]):
        """
        Callback when all external asks for an anticipated task are fulfilled.
        
        Dispatches the previously held-back critical actions — but only if
        the non-critical actions for the same task have already completed.
        If non-critical actions are still running on the OBC, the critical
        actions are deferred to ``_pending_critical_actions`` and dispatched
        from ``_handle_actions_succeeded`` once the OBC is free.
        
        Args:
            event_ids: List of start event IDs that are now fully unblocked
        """
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] ANTICIPATION UNBLOCKED: events={event_ids} "
            f"- checking readiness for critical actions"
        )
        
        critical_actions_to_dispatch: List[Action] = []
        
        for event_id in event_ids:
            if event_id not in self._event_to_task:
                continue
            
            task = self._event_to_task[event_id]
            
            with self._lock:
                critical_actions = self._anticipated_tasks.pop(task.id, [])
                self._anticipated_task_events.pop(task.id, None)
            
            if not critical_actions:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] No held-back critical actions for "
                    f"task '{task.name}' (id={task.id})"
                )
                continue
            
            # ── Check whether non-critical actions are still executing ────
            total_actions = len(self._task_to_actions.get(task.id, []))
            non_critical_count = total_actions - len(critical_actions)
            with self._lock:
                completed_count = self._completed_actions_per_task.get(task.id, 0)
            
            if completed_count >= non_critical_count:
                # Non-critical actions done → dispatch critical now
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Dispatching critical actions for "
                    f"task '{task.name}': {[a.name for a in critical_actions]}"
                )
                critical_actions_to_dispatch.extend(critical_actions)
            else:
                # Non-critical still running → defer until OBC is free
                with self._lock:
                    self._pending_critical_actions[task.id] = critical_actions
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Deferring {len(critical_actions)} critical "
                    f"actions for task '{task.name}' (id={task.id}) — OBC busy "
                    f"({completed_count}/{non_critical_count} non-critical done)"
                )
        
        if critical_actions_to_dispatch:
            self._dispatch_actions(critical_actions_to_dispatch)
    
    def _on_dispatch_complete(self, history: Dict[str, float]):
        """
        Callback when all events have been dispatched.
        
        Args:
            history: Dictionary of event_id -> dispatch_time
        """
        self.get_logger().info(f"[KirkTemporalExecutor] Dispatch complete. History: {history}")
        
        # Check if there are pending tasks
        if len(self._current_tasks) == 0:
            self._finish_plan()
    
    def _on_dispatch_error(self, event_id: str, error: str):
        """
        Callback when a dispatch error occurs.
        
        Args:
            event_id: Event that failed
            error: Error message
        """
        self.get_logger().error(f"[KirkTemporalExecutor] Dispatch error for {event_id}: {error}")
        
        # Find associated task and actions
        if event_id in self._event_to_task:
            task = self._event_to_task[event_id]
            task_actions = self._get_actions_for_task(task)
            for action in task_actions:
                self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id,action_id=action.id,status=enum_to_str(ActionStatus, ActionStatus.ABORTED))
    
    def _publish_task_started(self, event_id: str, dispatch_time: float):
        """Publish a task started event."""
        
        self._publish_task_event(
            event_type=TaskEvent.EVENT_STARTED,
            event_id=event_id,
        )
    
    def _handle_external_task_event(self, event: TaskEventData):
        """
        Handle task events from other platforms.
        
        Used for multi-platform coordination. When another platform completes
        a task that we're waiting on (via asks/tells), we update our schedule.
        
        Also notifies the timing guard to cancel external event timeout tracking.
        
        Args:
            event: Received task event data
        """
        if event.platform_id == self._platform_id:
            return  # Ignore our own events
        
        if self._sequential_fallback:
            return  # Sequential fallback doesn't participate in coordination
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] Received external event: {event.event_type.name} "
            f"from {event.platform_id} (event_id: {event.event_id})"
        )
        
        if event.event_type == TaskEventType.COMPLETED:
            self._handle_external_completion(event)
    
    def _handle_external_completion(self, event: TaskEventData):
        """
        Handle task completion from another platform.
        
        When another platform completes a task (sends a "tell"), we check if
        that event exists in our scheduler. If so, we report it as executed,
        which updates the distance graph and may enable waiting events.
        
        Also notifies the timing guard that the external event was received.
        
        Cross-platform asks work because:
        - If our TPN has an ask for an external event, that event is included
          in our TPN's event states (as a shared/external event)
        - When we receive the tell, we mark it as executed in our scheduler
        - This updates distances and may enable our waiting episodes
        
        Args:
            event: Completed task event data containing the "tell" event_id
        """
        if self._dispatcher is None or self._scheduler is None:
            # Scheduler/dispatcher not ready yet — buffer the events so they
            # can be replayed once Kirk components are initialised.
            for tell_event_id in event.tells:
                self._early_fulfilled_events[tell_event_id] = (
                    0.0,  # timestamp relative to start; will be replayed at 0
                    event.platform_id
                )
            self.get_logger().info(
                f"[KirkTemporalExecutor] Buffered {len(event.tells)} early external "
                f"event(s) from {event.platform_id} (scheduler not ready): {event.tells}"
            )
            return
        
        current_time = self.get_elapsed_execution_time()
        
        for tell_event_id in event.tells:
            if tell_event_id not in self._scheduler._event_states:
                continue
            
            # ═══════════════════════════════════════════════════════════════════
            # TIMING GUARD: Mark external event as received
            # ═══════════════════════════════════════════════════════════════════
            if self._timing_guard:
                self._timing_guard.receive_external_event(tell_event_id, current_time)
            
            # Remove from pending external events
            self._external_events_pending.pop(tell_event_id, None)
                
            success = self._scheduler.update_schedule(tell_event_id, current_time)
            if success:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] External tell '{tell_event_id}' received - "
                    f"updated schedule at {current_time:.3f}"
                )
                # Invalidate current RTED to force re-evaluation
                self._dispatcher.observe_event(tell_event_id, current_time)
            else:
                self.get_logger().warn(
                    f"[KirkTemporalExecutor] Failed to update schedule with "
                    f"external event '{tell_event_id}'"
                )
    
    def register_external_event_wait(
        self, 
        event_id: str, 
        source_platform: Optional[str] = None,
        max_delay: Optional[float] = None
    ):
        """
        Register that we're waiting for an external event.
        
        This should be called when the scheduler indicates we need to wait
        for an event from another platform (an "ask"). The timing guard will
        track this and generate a no-op if the event isn't received in time.
        
        If the event has already been fulfilled (received before we started
        waiting), no timeout is registered and the scheduler's distance graph
        is already up-to-date.
        
        Args:
            event_id: External event we're waiting for
            source_platform: Platform that should send this event
            max_delay: Maximum time to wait (default: external_event_delay)
        """
        # ─── Check if already fulfilled (early arrival) ───────────────────
        if self._scheduler is not None and self._scheduler.is_event_fulfilled(event_id):
            fulfilled_time = self._scheduler.get_fulfilled_events().get(event_id, 0.0)
            self.get_logger().info(
                f"[KirkTemporalExecutor] External event '{event_id}' already fulfilled "
                f"at {fulfilled_time:.3f}s — skipping timeout registration"
            )
            # Make sure pending tracking is clean
            self._external_events_pending.pop(event_id, None)
            return
        
        if self._timing_guard is None:
            return
        
        current_time = self.get_elapsed_execution_time()
        
        self._external_events_pending[event_id] = source_platform or "unknown"
        
        self._timing_guard.await_external_event(
            event_id=event_id,
            wait_start_time=current_time,
            max_delay=max_delay,
            source_platform=source_platform
        )
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] Registered wait for external event '{event_id}'"
            f"{f' from {source_platform}' if source_platform else ''}"
        )

    def _handle_actions_succeeded(self, transmitted_actions: List[Action], result):
        """
        Handle successful action completion.
        
        Maps action -> task -> episode -> event to report completion.
        Updates task status when all actions for a task complete.
        Also completes own event tracking in the timing guard.
        
        In sequential fallback mode, delegates to StandardExecutor-style handling.
        """
        if self._sequential_fallback:
            self._handle_actions_succeeded_sequential(transmitted_actions, result)
            return
        
        current_time = self.get_elapsed_execution_time()
        
        # Track which tasks had actions complete
        tasks_with_completed_actions: Dict[int, Task] = {}
        task = None

        for action in transmitted_actions:
            # Map action -> task via task_id
            task = self._get_task_for_action(action)
            if task is None:
                self.get_logger().warn(f"[KirkTemporalExecutor] No task found for completed action: {action.name}")
                continue
            
            tasks_with_completed_actions[task.id] = task
            
            # Track completed action count for this task
            with self._lock:
                if task.id not in self._completed_actions_per_task:
                    self._completed_actions_per_task[task.id] = 0
                self._completed_actions_per_task[task.id] += 1
        
        for task_id, task in tasks_with_completed_actions.items():
            with self._lock:
                pending = self._pending_critical_actions.get(task_id)
            if pending:
                total_actions = len(self._task_to_actions.get(task_id, []))
                non_critical_count = total_actions - len(pending)
                with self._lock:
                    completed_count = self._completed_actions_per_task.get(task_id, 0)
                if completed_count >= non_critical_count:
                    with self._lock:
                        actions_to_send = self._pending_critical_actions.pop(task_id, [])
                    if actions_to_send:
                        self.get_logger().info(
                            f"[KirkTemporalExecutor] OBC free — dispatching deferred "
                            f"critical actions for task '{task.name}' (id={task_id}): "
                            f"{[a.name for a in actions_to_send]}"
                        )
                        self._dispatch_actions(actions_to_send)
        
        for task_id in tasks_with_completed_actions:
            with self._lock:
                has_anticipated_critical = task_id in self._anticipated_tasks
            if has_anticipated_critical:
                self.get_logger().info(
                    f"[KirkTemporalExecutor] Non-critical actions done for "
                    f"anticipated task (id={task_id}) — critical actions still "
                    f"held back, UAV is idle waiting for external asks"
                )
                break 
        
        # Check if any tasks have all their actions completed and report if true
        for task_id, task in tasks_with_completed_actions.items():
            if self._are_all_task_actions_completed(task_id):
                self._update_task_status(task_id, ActionStatus.COMPLETED)

                # Remove task from current tasks
                if task in self._current_tasks:
                    self._current_tasks.remove(task)

                episode = self._task_to_episode.get(task.id)
                start_event_id, end_event_id = self._episode_to_events[episode.id]
                
                if self._overrun >= 0:
                    task_start = self._task_start_times.get(task_id)
                    if task_start is not None:
                        task_duration = time.time() - task_start
                        overrun_delay = (self._overrun / 100.0) * task_duration
                    else:
                        overrun_delay = 0.0
                    if overrun_delay > 0:
                        self.get_logger().info(
                            f"[KirkTemporalExecutor] Overrun delay: {self._overrun:.0f}% "
                            f"of {task_duration:.1f}s = {overrun_delay:.1f}s before "
                            f"completing task '{task.name}'"
                        )
                        time.sleep(overrun_delay)
                    current_time = self.get_elapsed_execution_time()
                
                if self._timing_guard and start_event_id:
                    self._timing_guard.complete_own_event(start_event_id, current_time)
                    
                # Report execution to dispatcher
                self._report_task_completed(start_event_id, end_event_id, current_time)

                # Publish completion event
                self._publish_task_event(
                    event_type=TaskEvent.EVENT_COMPLETED,
                    event_id=end_event_id,
                )
        
        # Check if plan is complete
        if len(self._current_tasks) == 0 and (self._scheduler is None or not self._scheduler.has_unexecuted_events()):
            self._finish_plan()
        else:
            # Continue with pending tasks
            self._pending_execution = True
    
    def _report_task_completed(self, start_event_id: str, end_event_id: str, completion_time: float):
        """
        Report task completion to the Kirk dispatcher.
        """
        if self._dispatcher is None:
            return
        
        # Get the end event for this episode
        if end_event_id:
            self.get_logger().info(f"[KirkTemporalExecutor] Reporting executed end event: {end_event_id}")
            self._dispatcher.report_executed([end_event_id], completion_time)
            
            with self._lock:
                self._events_in_progress.pop(start_event_id, None)
    
    def _are_all_task_actions_completed(self, task_id: int) -> bool:
        """
        Check if all actions for a task have completed.
        
        Args:
            task_id: Task ID to check
            
        Returns:
            True if all actions for the task have completed
        """
        # Get total number of actions for this task
        total_actions = len(self._task_to_actions.get(task_id, []))
        if total_actions == 0:
            return True
        
        # Get completed count
        with self._lock:
            completed_count = self._completed_actions_per_task.get(task_id, 0)
        
        return completed_count >= total_actions
    
    def _update_task_status(self, task_id: int, status: int):
        """
        Update the status of a task in the knowledge base.
        
        Args:
            task_id: Task ID to update
            status: New status (ActionStatus enum value)
        """
        status_str = enum_to_str(ActionStatus, status)
        
        self._knowledge_client_ros.updateTaskStatus(plan_id=self._plan_id, task_id=task_id, status=status_str)
        
        if self._verbose:
            self.get_logger().info(f"[KirkTemporalExecutor] Updated task {task_id} status to {status_str}")
    
    def _handle_actions_canceled(self, transmitted_actions: List[Action], result):
        """Handle canceled actions."""
        if self._sequential_fallback:
            self._handle_actions_canceled_sequential(transmitted_actions, result)
            return
        # Track which tasks had actions canceled
        canceled_task_ids = set()
        
        for action in transmitted_actions:
            task = self._get_task_for_action(action)
            episode = self._task_to_episode.get(task.id) if task else None

            if task is None:
                self.get_logger().warning(f"[KirkTemporalExecutor] Canceled action {action.id} is not mapped to any task; skipping event publication")
                continue

            end_event_id = self._episode_to_events.get(episode.id, (None, None))[1] if episode else None

            if not end_event_id:
                episode_id = episode.id if episode else "<none>"
                self.get_logger().warning(f"[KirkTemporalExecutor] No end event found for canceled action {action.id} (task {task.id}, episode {episode_id}); skipping")
                continue
            
            self.get_logger().info(f"[KirkTemporalExecutor] Reporting canceled end event: {end_event_id}")

            if task:
                canceled_task_ids.add(task.id)
            
            self._publish_task_event(
                event_type=TaskEvent.EVENT_CANCELLED,
                event_id=end_event_id,
            )
        
        # Update task status to CANCELED for all affected tasks
        for task_id in canceled_task_ids:
            self._update_task_status(task_id, ActionStatus.CANCELED)

        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.CANCELED))
        
        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Goal canceled.")
        self.publish_controllerCommand(
            command=PlannerCommand.CANCEL_DONE,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )
    
    def _handle_actions_aborted(self, transmitted_actions: List[Action], result):
        """Handle aborted actions."""
        if self._sequential_fallback:
            self._handle_actions_aborted_sequential(transmitted_actions, result)
            return
        # Track which tasks had actions aborted
        aborted_task_ids = set()
        
        for action in transmitted_actions:
            task = self._get_task_for_action(action)
            episode = self._task_to_episode.get(task.id) if task else None
            end_event_id = self._episode_to_events.get(episode.id, (None, None))[1] if episode else None
            if task:
                aborted_task_ids.add(task.id)
            
            self._publish_task_event(
                event_type=TaskEvent.EVENT_FAILED,
                event_id=end_event_id,
            )
        
        # Update task status to ABORTED for all affected tasks
        for task_id in aborted_task_ids:
            self._update_task_status(task_id, ActionStatus.ABORTED)
        
        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
   
        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Goal Aborted.")
        self.publish_controllerCommand(
            command=PlannerCommand.ABORTED,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )

    def _handle_actions_failed(self, transmitted_actions: List[Action], result):
        """Handle failed actions."""
        if self._sequential_fallback:
            self._handle_actions_failed_sequential(transmitted_actions, result)
            return
        # Track which tasks had actions fail
        failed_task_ids = set()
        
        for action in transmitted_actions:
            task = self._get_task_for_action(action)
            episode = self._task_to_episode.get(task.id) if task else None
            end_event_id = self._episode_to_events.get(episode.id, (None, None))[1] if episode else None
            
            if task:
                failed_task_ids.add(task.id)
            
            self._publish_task_event(
                event_type=TaskEvent.EVENT_FAILED,
                event_id=end_event_id,
            )
        
        # Update task status to ABORTED for all affected tasks
        for task_id in failed_task_ids:
            self._update_task_status(task_id, ActionStatus.ABORTED)
        
        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))

        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Goal FAILED.")
        self.publish_controllerCommand(
            command=PlannerCommand.FAILED,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )
    
    def _finish_plan(self):
        """Complete the plan execution."""
        self.change_executor_state(ExecutorState.STATE_IDLE)
        self.get_logger().info(f"[KirkTemporalExecutor] Plan finished for {self._platform_id}")
        
        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.COMPLETED))

        self.publish_controllerCommand(
            command=PlannerCommand.FINISHED,
            info_msg=ExecutionInfo(platform_id=self._platform_id, success=True)
        )
        self.reset_execution()
    
    def _execute_sequential(self):
        """Execute actions sequentially (fallback when no TPN is available)."""
        if len(self._current_actions) > 0:
            self.execute_sequence(sequence_length=1)

    # =========================================================================
    # Sequential Fallback Handlers (no TPN)
    # =========================================================================

    def _handle_actions_succeeded_sequential(self, transmitted_actions: List[Action], result):
        """Handle successful action completion in sequential fallback mode."""
        if len(self._current_actions) == 0:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.get_logger().info(
                f"[KirkTemporalExecutor] Sequential fallback: Plan finished for {self._platform_id}"
            )
            try:
                self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.COMPLETED))
            except Exception as e:
                self.get_logger().error(f"Failed to update plan status to COMPLETED in KB: {e}")
            self.publish_controllerCommand(command=PlannerCommand.FINISHED,info_msg=ExecutionInfo(platform_id=self._platform_id, success=True))
            self.reset_execution()
        else:
            self._pending_execution = True

    def _handle_actions_canceled_sequential(self, transmitted_actions: List[Action], result):
        """Handle canceled actions in sequential fallback mode."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.CANCELED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to CANCELED in KB: {e}")
        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Sequential fallback: Goal canceled for {self._platform_id}.")
        self.publish_controllerCommand(
            command=PlannerCommand.CANCEL_DONE,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )

    def _handle_actions_aborted_sequential(self, transmitted_actions: List[Action], result):
        """Handle aborted actions in sequential fallback mode."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to ABORTED in KB: {e}")
        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Sequential fallback: Goal aborted for {self._platform_id}.")
        self.publish_controllerCommand(
            command=PlannerCommand.ABORTED,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )

    def _handle_actions_failed_sequential(self, transmitted_actions: List[Action], result):
        """Handle failed actions in sequential fallback mode."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to ABORTED in KB: {e}")
        self.reset_execution()
        self.get_logger().info(f"[KirkTemporalExecutor] Sequential fallback: Goal FAILED for {self._platform_id}.")
        self.publish_controllerCommand(
            command=PlannerCommand.FAILED,
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )
        
    # =========================================================================
    # Observation Delay Configuration
    # =========================================================================
    
    def get_default_delay(self) -> float:
        """
        Get the default delay.
        
        This delay is used for:
        - Observation delay for contingent events
        - External event timeout (event_start_time + default_delay)
        
        Returns:
            Default delay in seconds
        """
        return self._default_delay
    
    def set_default_delay(self, delay: float):
        """
        Set the default delay.
        
        This delay is used for:
        - Observation delay for contingent events  
        - External event timeout (event_start_time + default_delay)
        
        Args:
            delay: Delay in seconds (e.g., 30.0 for 30 seconds)
        """
        self._default_delay = delay
        if self._scheduler is not None:
            self._scheduler.default_delay = delay
        if self._timing_guard is not None:
            self._timing_guard.external_event_delay = delay
        
        self.get_logger().info(f"[KirkTemporalExecutor] Default delay set to: {delay:.1f}s")
    
    def get_event_gamma(self, event_id: str) -> float:
        """
        Get the fixed observation delay (gamma) for a specific event.
        
        Args:
            event_id: Event ID
            
        Returns:
            Delay in seconds (0.0 for controllable events)
        """
        if self._scheduler is None:
            return 0.0
        return self._scheduler.get_gamma(event_id)
    
    def set_event_gamma(self, event_id: str, delay: float):
        """
        Set the fixed observation delay (gamma) for a specific event.
        
        Use this to override the default or TPN-specified delay for
        a specific contingent event.
        
        Args:
            event_id: Event ID
            delay: Delay in seconds
        """
        if self._scheduler is not None:
            self._scheduler.set_gamma(event_id, delay)
    
    # =========================================================================
    # Observation API for External Systems
    # =========================================================================
    
    def observe_contingent_event(self, event_id: str, observation_time: Optional[float] = None):
        """
        Report observation of a contingent event.
        
        External systems (e.g., sensors, other ROS nodes) can call this
        to report when an uncontrollable event has been observed.
        
        Args:
            event_id: ID of the observed event
            observation_time: Time of observation (default: current time)
        """
        if self._dispatcher is None:
            self.get_logger().warn("[KirkTemporalExecutor] Cannot observe event - dispatcher not initialized")
            return
        
        if observation_time is None:
            observation_time = self.get_elapsed_execution_time()
        
        self.get_logger().info(f"[KirkTemporalExecutor] Observed contingent event: {event_id} at {observation_time:.3f}")
        
        self._dispatcher.observe_event(event_id, observation_time)
    
    def get_current_commitments(self) -> Dict[str, Tuple[float, float]]:
        """
        Get current time commitments for all unexecuted events.
        
        Returns:
            Dictionary mapping event_id to (lower_bound, upper_bound)
        """
        if self._scheduler is None:
            return {}
        
        commitments = self._scheduler.get_commitments()
        return {
            evt_id: bounds
            for (evt_id, _), bounds in commitments.items()
        }
    
    def get_pending_events(self) -> List[str]:
        """Get list of events that haven't been executed yet."""
        if self._scheduler is None:
            return []
        return self._scheduler.get_pending_events()
    
    def get_execution_history(self) -> Dict[str, float]:
        """Get history of executed events with their execution times."""
        if self._dispatcher is None:
            return {}
        return self._dispatcher.get_history()
    
    # =========================================================================
    # Task Query API
    # =========================================================================
    def get_episode_for_task(self, task_id: int) -> Optional[Episode]:
        """
        Get the TPN episode for a given task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Episode if found, None otherwise
        """
        return self._task_to_episode.get(task_id)
    
    def get_event_bounds_for_task(self, task_id: int) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
        """
        Get the time bounds for a task's start and end events.
        
        Args:
            task_id: Task ID
            
        Returns:
            Tuple of ((start_lb, start_ub), (end_lb, end_ub)) or None
        """
        episode = self._task_to_episode.get(task_id)
        if episode is None or self._scheduler is None:
            return None
        
        start_event, end_event = self._episode_to_events.get(episode.id, (None, None))
        if not start_event or not end_event:
            return None
        
        start_bounds = self._scheduler.get_event_bounds(start_event)
        end_bounds = self._scheduler.get_event_bounds(end_event)
        
        if start_bounds is None or end_bounds is None:
            return None
        
        return (start_bounds, end_bounds)
    
    # =========================================================================
    # Timing Configuration API
    # =========================================================================
    
    @property
    def max_execution_time(self) -> Optional[float]:
        """
        Get the maximum execution time.
        
        Returns:
            Maximum execution time in seconds, or None if unlimited
        """
        return self._max_execution_time
    
    @max_execution_time.setter
    def max_execution_time(self, value: Optional[float]):
        """
        Set the maximum execution time.
        
        This can be set before or during execution. If set during execution,
        the new value takes effect immediately.
        
        Args:
            value: Maximum execution time in seconds, or None for unlimited
                   Example: 1200.0 for 20 minutes
        """
        self._max_execution_time = value
        if self._timing_guard:
            self._timing_guard.max_execution_time = value
        
        max_str = f"{value:.1f}s" if value else "∞ (unlimited)"
        self.get_logger().info(f"[KirkTemporalExecutor] Max execution time set to: {max_str}")
    
    @property
    def duration_epsilon(self) -> float:
        """
        Get the duration epsilon tolerance.
        
        Returns:
            Duration epsilon in seconds
        """
        return self._duration_epsilon
    
    @duration_epsilon.setter
    def duration_epsilon(self, value: float):
        """
        Set the duration epsilon tolerance.
        
        This is the tolerance added to expected durations before generating
        a warning for own events.
        
        Args:
            value: Duration epsilon in seconds
        """
        self._duration_epsilon = value
        if self._timing_guard:
            self._timing_guard.duration_epsilon = value
        
        self.get_logger().info(f"[KirkTemporalExecutor] Duration epsilon set to: {value:.1f}s")
    
    def get_timing_status(self) -> Dict[str, Any]:
        """
        Get comprehensive timing status.
        
        Returns:
            Dictionary with timing configuration and status
        """
        status = {
            'max_execution_time': self._max_execution_time,
            'duration_epsilon': self._duration_epsilon,
            'default_delay': self._default_delay,  # Used for both observation and external timeout
            'elapsed_time': self.get_elapsed_execution_time(),
            'remaining_time': None,
            'is_cancelled': False,
            'cancel_reason': None,
            'violations': [],
            'pending_external_events': list(self._external_events_pending.keys()),
            'task_anticipation': self._task_anticipation,
            'anticipated_tasks': self.get_anticipated_tasks(),
        }
        
        if self._timing_guard:
            status['remaining_time'] = self._timing_guard.get_remaining_time()
            status['is_cancelled'] = self._timing_guard.is_cancelled
            status['cancel_reason'] = self._timing_guard.cancel_reason
            status['violations'] = [str(v) for v in self._timing_guard.get_violations()]
        
        return status
    
    def log_timing_status(self):
        """Log the current timing status in a formatted manner."""
        if self._timing_guard:
            self._timing_guard.log_status()
        else:
            self.get_logger().info("[KirkTemporalExecutor] No timing guard configured")
    
    # =========================================================================
    # Task Anticipation Configuration API
    # =========================================================================
    
    @property
    def task_anticipation(self) -> bool:
        """
        Whether task anticipation is enabled.
        
        When enabled, tasks blocked by unfulfilled external asks can begin
        executing non-critical actions early. Critical actions (defined by
        critical_action_indices) are held back until all asks are fulfilled.
        
        When disabled (default), the executor waits for all external asks 
        and introduces a no-op if exceeded bounds.
        
        Returns:
            True if task anticipation is enabled
        """
        return self._task_anticipation
    
    @task_anticipation.setter
    def task_anticipation(self, value: bool):
        """
        Enable or disable task anticipation.
        
        Can be set before execution starts. If set during execution,
        the scheduler is updated immediately.
        
        Args:
            value: True to enable, False to disable
        """
        self._task_anticipation = value
        if self._scheduler:
            self._scheduler.task_anticipation = value
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] Task anticipation "
            f"{'enabled' if value else 'disabled'}"
        )
    
    @property
    def critical_action_indices(self) -> Dict[str, List[int]]:
        """
        Get the critical action indices per task name.
        
        Maps task_name (lowercase) -> list of 0-based action indices that
        are critical and require all external asks to be fulfilled before
        execution.
        
        Example:
            {"deliver": [1, 2, 3]}
            # For the "deliver" task:
            # - Action 0 (e.g., "fly_to") is non-critical -> can execute early
            # - Actions 1, 2, 3 are critical -> held back until asks fulfilled
        
        Returns:
            Dictionary mapping task names to critical action index lists
        """
        return self._critical_action_indices
    
    @critical_action_indices.setter
    def critical_action_indices(self, value: Dict[str, List[int]]):
        """
        Set the critical action indices per task name.
        
        Args:
            value: Dictionary mapping task names (lowercase) to lists of
                   0-based action indices that are critical.
        """
        self._critical_action_indices = value
        if self._scheduler:
            self._scheduler.critical_action_indices = value
        
        self.get_logger().info(
            f"[KirkTemporalExecutor] Critical action indices set: {value}"
        )
    
    def get_anticipated_tasks(self) -> Dict[int, List[str]]:
        """
        Get tasks currently in anticipation mode with their held-back actions.
        
        Returns:
            Dictionary mapping task_id to list of critical action names
        """
        with self._lock:
            return {
                task_id: [a.name for a in actions]
                for task_id, actions in self._anticipated_tasks.items()
            }
