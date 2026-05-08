#!/usr/bin/env python3
"""
Base Executor Module

This module provides the abstract base class for all executor implementations.
It contains common functionality shared across all executor types including:
- ROS2 node setup and communication
- Platform command handling
- Knowledge base interactions
- Origin/home position management
- Basic action execution infrastructure
"""

import json
import time
import threading
import traceback
from abc import ABC, abstractmethod
from typing import Optional, List, Dict

import rclpy
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from action_msgs.msg import GoalStatus
from geographic_msgs.msg import GeoPoint
from std_msgs.msg import Header

from auspex_executor.platform_executor.sequence_action_client import SequenceActionClient
from auspex_executor.platform_executor.executors.task_event_communication import (
    TaskEventCommunication,
    TaskEventData,
    TaskEventType,
)

from auspex_executor.utils.knowledge_client_ros import KnowledgeClientROS
from auspex_executor.utils.utils import create_plan_msg_from_kb, enum_to_str

from auspex_msgs.msg import (
    ExecutorCommand, 
    PlannerCommand, 
    ExecutorState, 
    ExecutionInfo, 
    Task, 
    Action, 
    ActionStatus, 
    PlanStatus, 
    PlatformCommand,
    TaskEvent,
)

from auspex_msgs.srv import (
    GetOrigin,
    SetOrigin,
    GetAltitude,
    GetHighestPoint,
    CancelGoal,
)


class BaseExecutor(Node, ABC):
    """
    Abstract base class for AUSPEX executors.
    
    Provides common functionality for all executor implementations including
    ROS2 communication, platform control, and knowledge base interactions.
    Subclasses must implement the execution strategy methods.
    """
    
    # Class attribute to identify executor type
    EXECUTOR_TYPE = "base"
    
    def __init__(self, platform_id: str, knowledge_client_ros: Optional[KnowledgeClientROS] = None):
        """
        Initialize the base executor.
        
        Args:
            platform_id: Unique identifier for the platform
            knowledge_client_ros: Knowledge base client instance (optional)
        """
        super().__init__(platform_id + "_executor")
        
        # Executor State
        self._executor_state = ExecutorState.STATE_IDLE
        
        # Lock for position
        self._position_lock = threading.Lock()
        
        # Callback Groups
        self._cb_group_actions = ReentrantCallbackGroup()
        self._cb_group_services = ReentrantCallbackGroup()
        self._cb_group_pubsub = ReentrantCallbackGroup()
        
        # Knowledge Base Interface
        self._knowledge_client_ros = knowledge_client_ros
        
        # Scheduling variables
        self._gps_position = GeoPoint()
        self._home_position = GeoPoint()
        self._is_vhcl_paused = False
        
        # Platform ID
        self._platform_id = platform_id
        
        # Plan state
        self._current_plan = None
        self._current_tasks: List[Task] = []
        self._current_actions: List[Action] = []
        self._plan_id = -1
        self._team_id = ""
        self._current_tpn = None  # Stores parsed TPN JSON
        self._current_upf_plan = None  # Stores the complete upf_msgs/Plan (TimeTriggeredPlan)
        self._execution_start_time = None  # Synchronized start time
        
        # Execution control
        self._pending_execution = False
        
        # Task event communication (initialized in init_executor)
        self._task_event_comm: Optional[TaskEventCommunication] = None
        
        # Communication delay configuration 
        self._comm_delay_enabled = True
        self._comm_delay_min = 2.0  # seconds
        self._comm_delay_max = 8.0  # seconds
        
        # Task completion overrun delay (percentage of task duration).
        # -1 means disabled (no delay). >= 0 introduces a sleep of
        # (overrun / 100) * task_duration seconds after all actions of a
        # task complete, before the completion notification is sent.
        # E.g. 10 means 10% overrun: a task that took 60s will sleep 6s.
        self._overrun: float = 0.0
        
        # Track wall-clock start time per task_id (set at dispatch,
        # used to compute task duration for percentage-based overrun).
        self._task_start_times: Dict[int, float] = {}
        
        # Idle wait tracking (waiting for external observation)
        self._idle_wait_start: Optional[float] = None

    def init_executor(self):
        """
        Initialize ROS2 publishers, subscribers, clients, and timers.
        Should be called after construction.
        """
        # Create publishers
        self._platform_command_publisher = self.create_publisher(PlatformCommand, '/' + self._platform_id + '/executor2platform', 10, callback_group=self._cb_group_pubsub)
        self._controller_command_publisher = self.create_publisher(PlannerCommand, '/' + self._platform_id + '/executor2controller', 10, callback_group=self._cb_group_pubsub)
        
        # Create subscriber
        self._executor_command_subscriber = self.create_subscription(ExecutorCommand, '/' + self._platform_id + '/controller2executor', self.controller_callback, 10, callback_group=self._cb_group_pubsub)
        
        # Create service clients
        self._get_home_client = self.create_client(GetOrigin, self._platform_id + '/get_origin', callback_group=self._cb_group_services)
        self._set_home_client = self.create_client(SetOrigin, self._platform_id + '/set_origin', callback_group=self._cb_group_services)
        self._altitude_getter = self.create_client(GetAltitude, "auspex_get_altitude", callback_group=self._cb_group_services)
        self._hp_getter = self.create_client(GetHighestPoint, "auspex_get_highest_point", callback_group=self._cb_group_services)
        
        # Create action client
        self._sequence_client = SequenceActionClient(platform_id=self._platform_id,node=self,feedback_callback=self.feedback_callback,result_callback=self.result_callback,callback_group=self._cb_group_actions)
        
        # Initialize task event communication for multi-platform coordination
        self._task_event_comm = TaskEventCommunication(
            node=self,
            platform_id=self._platform_id,
            team_id=self._team_id,
            callback_group=self._cb_group_pubsub,
            verbose=False,
            delay_enabled=self._comm_delay_enabled,
            delay_min=self._comm_delay_min,
            delay_max=self._comm_delay_max
        )
        self._task_event_comm.add_callback(self._handle_external_task_event)
        self._task_event_comm.activate()
        
        # Initialize subclass-specific components
        self._init_executor_specifics()
        
        # Setup execution checking timer
        self.create_timer(0.1, callback=self._check_pending_execution, callback_group=self._cb_group_actions)
        
        # Setup origin initialization
        self._setup_timer = self.create_timer(0.5,self._delayed_setup_origin,callback_group=self._cb_group_actions)

    @abstractmethod
    def _init_executor_specifics(self):
        """
        Initialize executor-specific components.
        Subclasses should override this to set up their specific functionality.
        """
        pass

    @abstractmethod
    def _check_pending_execution(self):
        """
        Check for and process pending executions.
        Each executor type implements its own execution strategy.
        """
        pass

    @abstractmethod
    def _on_plan_ready(self):
        """
        Called when a plan is ready for execution.
        Subclasses implement their initialization logic here.
        """
        pass

    @abstractmethod
    def _handle_actions_succeeded(self, transmitted_actions: List[Action], result):
        """Handle successful completion of actions."""
        pass

    @abstractmethod
    def _handle_actions_canceled(self, transmitted_actions: List[Action], result):
        """Handle canceled actions."""
        pass

    @abstractmethod
    def _handle_actions_aborted(self, transmitted_actions: List[Action], result):
        """Handle aborted actions."""
        pass

    @abstractmethod
    def _handle_actions_failed(self, transmitted_actions: List[Action], result):
        """Handle failed actions."""
        pass

    @abstractmethod
    def _handle_external_task_event(self, event: TaskEventData):
        """
        Handle task events from other platforms.
        
        Subclasses must implement this to define their coordination behavior.
        
        Args:
            event: Received task event data
        """
        pass

    @abstractmethod
    def _reset_executor_specifics(self):
        """Reset executor-specific state. Subclasses implement this."""
        pass


    # =========================================================================
    # End abstract methods
    # =========================================================================
    

    def result_callback(self, transmitted_actions: List[Action], result):
        """
        Handle result from action server.
        Can be overridden by subclasses for specialized handling.
        """
        if result.status == GoalStatus.STATUS_SUCCEEDED:
            if result.result.error_code == 5:
                return
            for action in transmitted_actions:
                try:
                    self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.COMPLETED))
                except Exception as e:
                    self.get_logger().error(f'Failed to update action {action.id} status: {e}')
            self._handle_actions_succeeded(transmitted_actions, result)
            
        elif result.status == GoalStatus.STATUS_CANCELED:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for action in transmitted_actions:
                try:
                    self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.CANCELED))
                except Exception as e:
                    self.get_logger().error(f'Failed to update action {action.id} status: {e}')
            self._handle_actions_canceled(transmitted_actions, result)
            
        elif result.status == GoalStatus.STATUS_ABORTED:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for action in transmitted_actions:
                try:
                    self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.ABORTED))
                except Exception as e:
                    self.get_logger().error(f'Failed to update action {action.id} status: {e}')
            self._handle_actions_aborted(transmitted_actions, result)
            
        else:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for action in transmitted_actions:
                try:
                    self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.ABORTED))
                except Exception as e:
                    self.get_logger().error(f'Failed to update action {action.id} status: {e}')
            self._handle_actions_failed(transmitted_actions, result)

    def feedback_callback(self, actions, feedback_msg):
        """
        Handle feedback from action server.
        Updates current GPS position.
        """
        feedback = feedback_msg.feedback
        self._position_lock.acquire()
        self._gps_position.latitude = feedback.current_pose.pose.position.x
        self._gps_position.longitude = feedback.current_pose.pose.position.y
        self._gps_position.altitude = feedback.current_pose.pose.position.z
        self._position_lock.release()

    # =========================================================================
    # Platform Control Methods
    # =========================================================================
    
    def send_pause(self):
        """Pause the current action execution."""
        if self._is_vhcl_paused:
            return

        if self._current_plan is None:
            return

        self.get_logger().info('Pausing current actions...')
        self.send_platform_command(PlatformCommand.PLATFORM_PAUSE)
        self._is_vhcl_paused = True
        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.PAUSED))

        for action in (self._sequence_client._actions or []):
            self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.PAUSED))

        self.change_executor_state(ExecutorState.STATE_PAUSED)

    def send_resume(self):
        """Resume paused action execution."""
        if not self._is_vhcl_paused:
            return
        self.get_logger().info('Resuming current actions...')
        self.send_platform_command(PlatformCommand.PLATFORM_RESUME)
        self._is_vhcl_paused = False

        self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ACTIVE))
        
        for action in (self._sequence_client._actions or []):
            self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=action.id, status=enum_to_str(ActionStatus, ActionStatus.ACTIVE))
        self.change_executor_state(ExecutorState.STATE_EXECUTING)

    def cancelGoal(self):
        """Cancel all current goals."""
        if self._executor_state == ExecutorState.STATE_EXECUTING or \
           self._executor_state == ExecutorState.STATE_PAUSED:
            self.get_logger().info("Sending cancel command...")
            self._sequence_client.cancel_action_goal()

    def terminatePlatform(self):
        """Terminate the platform gracefully."""
        if self._executor_state != ExecutorState.STATE_IDLE:
            self.change_executor_state(ExecutorState.STATE_IDLE)
        self.get_logger().info("Terminating platform...")
        self.send_platform_command(PlatformCommand.PLATFORM_TERMINATE)
        self.reset_execution()
        rclpy.shutdown()

    def killPlatform(self):
        """Kill the platform immediately."""
        if self._executor_state != ExecutorState.STATE_IDLE:
            self.change_executor_state(ExecutorState.STATE_IDLE)
        self.get_logger().info("Killing platform...")
        self.send_platform_command(PlatformCommand.PLATFORM_KILL)
        self.reset_execution()
        rclpy.shutdown()

    def send_platform_command(self, command):
        """Send a command to the platform."""
        msg = PlatformCommand()
        msg.platform_command = command

        if command == PlatformCommand.PLATFORM_PAUSE:
            if not self._is_vhcl_paused:
                self._is_vhcl_paused = True
        elif command == PlatformCommand.PLATFORM_RESUME:
            if self._is_vhcl_paused:
                msg.platform_command = command
                self._is_vhcl_paused = False

        self._platform_command_publisher.publish(msg)

    # =========================================================================
    # State Management
    # =========================================================================
    
    def reset_execution(self):
        """Reset execution state to initial values."""
        self._current_plan = None
        self._plan_id = -1
        self._current_actions = []
        self._pending_execution = False
        self._current_tasks = []
        self._current_tpn = None
        self._current_upf_plan = None
        self._execution_start_time = None
        self._task_start_times.clear()
    
        self._reset_executor_specifics()
    
    # =========================================================================
    # Communication Delay Configuration (for testing)
    # =========================================================================
    
    def set_comm_delay(
        self,
        enabled: bool,
        delay_min: float = 0.0,
        delay_max: float = 0.0
    ):
        """
        Configure communication delay for testing inter-agent communication.
        
        When enabled, received task events will be delayed by a random amount
        between delay_min and delay_max seconds. This simulates network latency
        or processing delays in multi-agent coordination.
        
        Args:
            enabled: Enable/disable communication delay
            delay_min: Minimum delay in seconds (lower bound)
            delay_max: Maximum delay in seconds (upper bound)
        """
        self._comm_delay_enabled = enabled
        self._comm_delay_min = delay_min
        self._comm_delay_max = delay_max
        
        # Update TaskEventCommunication if already initialized
        if self._task_event_comm:
            self._task_event_comm.set_delay_config(
                enabled=enabled,
                delay_min=delay_min,
                delay_max=delay_max
            )
        
        if enabled:
            self.get_logger().info(
                f"[{self._platform_id}] Communication delay enabled: "
                f"{delay_min:.3f}s - {delay_max:.3f}s"
            )
        else:
            self.get_logger().info(f"[{self._platform_id}] Communication delay disabled")
    
    def get_comm_delay_config(self) -> tuple:
        """
        Get current communication delay configuration.
        
        Returns:
            Tuple of (enabled, min_delay, max_delay)
        """
        return (self._comm_delay_enabled, self._comm_delay_min, self._comm_delay_max)

    def change_executor_state(self, state):
        """Change the executor state and notify controller."""
        if state != self._executor_state:
            self._executor_state = state
            self.publish_controllerCommand(
                command=PlannerCommand.STATE_FEEDBACK, 
                info_msg=ExecutionInfo(platform_id=self._platform_id)
            )

    def publish_controllerCommand(self, command, info_msg):
        """Publish a command to the controller."""
        msg = PlannerCommand()
        msg.command = command
        msg.info = info_msg
        msg.executor_state.value = self._executor_state
        self._controller_command_publisher.publish(msg)

    # =========================================================================
    # Origin/Home Position Management
    # =========================================================================
    
    def _delayed_setup_origin(self):
        """Delayed setup of origin position."""
        self._setup_timer.cancel()
        try:
            self.setup_origin()
        except Exception as e:
            self.get_logger().error(f"setup_origin failed: {e}")

    def setup_origin(self):
        """Request current GPS coordinates to set home with altitude."""
        srv = GetOrigin.Request()
        srv.requested = True
        while not self._get_home_client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info(f"get_origin service not available")
        self.get_logger().info(f"get_origin service is available")

        future = self._get_home_client.call_async(srv)
        future.add_done_callback(self.get_origin_callback)

    def get_origin_callback(self, future):
        """Handle origin service response."""
        resp = future.result()
        if resp is None:
            self.get_logger().error("GetOrigin service call returned None! Do not Take OFF!")
            return 0.0, 0.0, 0.0

        # Set home position
        self.set_home(resp.origin.latitude, resp.origin.longitude, resp.origin.altitude)

    def set_home(self, lat, lon, alt):
        """Set the home position."""
        self._position_lock.acquire()
        self._home_position.latitude = float(lat)
        self._home_position.longitude = float(lon)
        self._home_position.altitude = float(alt)
        self._position_lock.release()
        
        home_json = json.dumps({
            "name": "home_" + self._platform_id, 
            "points": [[lat, lon, alt]]
        })
        try:
            self._knowledge_client_ros.insertArea(area_name="home_" + self._platform_id, area_json=home_json)
        except AttributeError:
            self.get_logger().info("KB write_async not available, skipping home write to KB")

    # =========================================================================
    # Controller Command Handling
    # =========================================================================
    
    def controller_callback(self, msg):
        """Handle commands from the controller."""
        if msg.command == ExecutorCommand.EXECUTE:
            self.get_logger().info(f'Received EXECUTE command. Current state: {self._executor_state}, pending: {self._pending_execution}')
            if self._executor_state == ExecutorState.STATE_EXECUTING:
                self.get_logger().info('Executor already executing...')
                return
            if self._executor_state == ExecutorState.STATE_PAUSED:
                self.get_logger().info(f'Executor {self._platform_id} was paused. Resuming with highest priority plan...')
            
            # Store execution start time for synchronized execution
            if msg.execution_start_time.sec != 0 or msg.execution_start_time.nanosec != 0:
                self._execution_start_time = msg.execution_start_time
            else:
                self._execution_start_time = None
            
            self._request_plan_async()

        elif msg.command == ExecutorCommand.PAUSE:
            if self._executor_state == ExecutorState.STATE_PAUSED:
                self.get_logger().info('Executor already paused, cannot pause...')
                return
            self.send_pause()
        elif msg.command == ExecutorCommand.CONTINUE:
            if self._executor_state != ExecutorState.STATE_PAUSED:
                self.get_logger().info('Executor not paused, cannot continue...')
                return
            self.send_resume()
        elif msg.command == ExecutorCommand.CANCEL:
            self.cancelGoal()
        elif msg.command == ExecutorCommand.KILL:
            self.killPlatform()
        elif msg.command == ExecutorCommand.TERMINATE:
            self.terminatePlatform()

    # =========================================================================
    # Plan Management
    # =========================================================================
    
    def _request_plan_async(self):
        """Request plan from KB asynchronously."""
        self.get_logger().info(f'Requesting plans from KB for platform {self._platform_id}...')
        if self._is_vhcl_paused:
            self._is_vhcl_paused = False

        future = self._knowledge_client_ros.getPlansOfPlatformID(platform_id=self._platform_id)

        if future is None:
            self.get_logger().error('Failed to send async query request (KB service not ready)')
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.publish_controllerCommand(
                command=PlannerCommand.ABORTED, 
                info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
            )
            return
        self.get_logger().info("EXEC waiting for done callback...")
        future.add_done_callback(self._handle_plan_query_result)

    def _handle_plan_query_result(self, future):
        """Handle async plan query result."""
        try:
            result = future.result()
            if result is None or not result.instances:
                self.get_logger().info('No plans in Knowledge Base, returning...')
                self.change_executor_state(ExecutorState.STATE_IDLE)
                self.publish_controllerCommand(
                    command=PlannerCommand.ABORTED, 
                    info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
                )
                return

            self.get_logger().info(f'Knowledge Base returned {len(result.instances)} plan entries')

            # Parse the query result into plan objects
            
            plans = []
            for item in result.instances:
                try:
                    item = item.replace("'", '"')
                    item_dict = json.loads(item)
                    plans.append(item_dict)
                except (json.JSONDecodeError, TypeError) as e:
                    self.get_logger().info(f'Failed to parse plan item: {e}')

            if not plans:
                self.get_logger().info('No valid plans found, returning...')
                self.change_executor_state(ExecutorState.STATE_IDLE)
                self.publish_controllerCommand(
                    command=PlannerCommand.ABORTED, 
                    info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
                )
                return

            self._process_plans(plans)

        except Exception as e:
            self.get_logger().error(f'Error processing plan query: {e}')
            traceback.print_exc()
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.publish_controllerCommand(
                command=PlannerCommand.ABORTED, 
                info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
            )

    def _process_plans(self, plans):
        """Process plans after async query."""
        plan_msg = create_plan_msg_from_kb(plans)
        self.get_logger().info(f'Processing {len(plan_msg)} plans: {[(p.plan_id, p.status, len(p.actions)) for p in plan_msg]}')
        first_incomplete_idx = None

        while first_incomplete_idx is None:
            highest_plan_prio = -1
            for idx, plan in enumerate(plan_msg):
                if plan.status == "INACTIVE":
                    if plan.priority > highest_plan_prio:
                        highest_plan_prio = plan.priority

            lowest_index = 10000
            lowest_plan_id = 10000

            for idx, plan in enumerate(plan_msg):
                if plan.status == "INACTIVE":
                    if plan.plan_id < lowest_plan_id and plan.priority == highest_plan_prio:
                        lowest_plan_id = plan.plan_id
                        lowest_index = idx

            if lowest_index == 10000:
                self.get_logger().info('No INACTIVE plans found, returning...')
                self.change_executor_state(ExecutorState.STATE_IDLE)
                self.publish_controllerCommand(
                    command=PlannerCommand.ABORTED, 
                    info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
                )
                return
            actions = plan_msg[lowest_index].actions

            first_incomplete_idx = next(
                (i for i, a in enumerate(actions) 
                 if a.status != enum_to_str(ActionStatus, ActionStatus.COMPLETED) 
                 and a.status != enum_to_str(ActionStatus, ActionStatus.CANCELED)), 
                None
            )
            if first_incomplete_idx is None:
                completed_plan_id = plan_msg[lowest_index].plan_id
                plan_msg[lowest_index].status = enum_to_str(PlanStatus, PlanStatus.COMPLETED)
                self.get_logger().info(f'Plan {completed_plan_id} has all actions completed, marking as COMPLETED')
                try:
                    self._knowledge_client_ros.updatePlanStatus(plan_id=completed_plan_id, status=enum_to_str(PlanStatus, PlanStatus.COMPLETED))
                except Exception as e:
                    self.get_logger().error(f'Failed to update plan {completed_plan_id} status in KB: {e}')

        next_actions = actions[first_incomplete_idx:]

        if len(next_actions) == 0:
            self.get_logger().info(f'No actions to execute in Plan with id: {plan_msg[lowest_index].plan_id}')
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.publish_controllerCommand(
                command=PlannerCommand.ABORTED, 
                info_msg=ExecutionInfo(platform_id=self._platform_id, success=False)
            )
            return

        self.get_logger().info('Received plan. Now Executing...')
        self._current_plan = plan_msg[lowest_index]
        self._plan_id = self._current_plan.plan_id
        self._team_id = self._current_plan.team_id
        self._current_actions = next_actions
        self._current_tasks = plan_msg[lowest_index].tasks

        # Parse TPN data
        tpn_data = self._current_plan.tpn
        if tpn_data:
            if isinstance(tpn_data, dict):
                self._current_tpn = tpn_data
            elif isinstance(tpn_data, str):
                try:
                    self._current_tpn = json.loads(tpn_data)
                except json.JSONDecodeError as e:
                    self.get_logger().info(f'Failed to parse TPN JSON: {e}')
                    self._current_tpn = None
            else:
                self._current_tpn = None
        else:
            self._current_tpn = None

        self._current_upf_plan = self._current_plan.upf_plan

        if not self._on_plan_ready():
            return 

        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ACTIVE))
        except Exception as e:
            self.get_logger().error(f'Failed to update plan status to ACTIVE in KB: {e}')
        
        self._pending_execution = True

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_elapsed_execution_time(self) -> float:
        """
        Get elapsed time since execution started.
        
        Returns:
            Elapsed time in seconds, or 0.0 if no start time set.
        """
        if self._execution_start_time is None:
            return 0.0
        
        current_time = self.get_clock().now().to_msg()
        start_sec = self._execution_start_time.sec + self._execution_start_time.nanosec * 1e-9
        current_sec = current_time.sec + current_time.nanosec * 1e-9
        
        elapsed = current_sec - start_sec
        return max(0.0, elapsed)

    def _publish_task_event(self, event_type: int, event_id: str):
        """
        Publish a task event to notify other executors.
        
        Args:
            event_type: TaskEvent.EVENT_* constant
            event_id: Unique event ID
        """
        if self._task_event_comm is None:
            return
        
        # Update team_id in communication if it changed
        self._task_event_comm.set_team_id(self._team_id)
        
        # Map the event type constant to the appropriate publish method
        if event_type == TaskEvent.EVENT_STARTED:
            self._task_event_comm.publish_task_started(
                event_id=event_id,
                plan_id=self._plan_id
            )
        elif event_type == TaskEvent.EVENT_COMPLETED:
            self._task_event_comm.publish_task_completed(
                event_id=event_id,
                plan_id=self._plan_id
            )
        elif event_type == TaskEvent.EVENT_FAILED:
            self._task_event_comm.publish_task_failed(
                event_id=event_id,
                plan_id=self._plan_id
            )
        elif event_type == TaskEvent.EVENT_CANCELLED:
            self._task_event_comm.publish_task_cancelled(
                event_id=event_id,
                plan_id=self._plan_id
            )

    def execute_sequence(self, sequence_length: int):
        """
        Execute a sequence of actions.
        
        Args:
            sequence_length: Number of actions to execute in sequence
        """
        self.get_logger().info("Begin execution of actions...")
        sequence_length = min(len(self._current_actions), sequence_length)

        actions = []
        for i in range(sequence_length):
            action = self._current_actions.pop(0)
            actions.append(action)
            str_args = ", ".join(action.args)
            self.get_logger().info(f"Next action: {action.name} with args: {str_args}")

        self.get_logger().info("End of action sequence")
        
        if actions:
            self._knowledge_client_ros.updateActionStatus(plan_id=self._plan_id, action_id=actions[-1].id, status=enum_to_str(ActionStatus, ActionStatus.ACTIVE))
        
        self.change_executor_state(ExecutorState.STATE_EXECUTING)
        self._sequence_client.send_action_goal(actions)

    def get_executor_status(self) -> dict:
        """
        Get the current status of the executor.
        
        Returns:
            Dictionary with executor status information
        """
        return {
            'executor_type': self.EXECUTOR_TYPE,
            'platform_id': self._platform_id,
            'state': self._executor_state,
            'plan_id': self._plan_id,
            'team_id': self._team_id,
            'pending_actions': len(self._current_actions),
            'is_paused': self._is_vhcl_paused,
        }
