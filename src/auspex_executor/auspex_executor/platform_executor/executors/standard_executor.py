#!/usr/bin/env python3
"""
Standard Executor Module

This module provides the standard sequential executor implementation.
It executes actions one at a time in sequence without temporal constraints.
"""

from typing import List, Optional

from auspex_executor.platform_executor.executors.base_executor import BaseExecutor
from auspex_executor.platform_executor.executors.task_event_communication import TaskEventData
from auspex_executor.utils.knowledge_client_ros import KnowledgeClientROS
from auspex_executor.utils.utils import enum_to_str
from auspex_msgs.msg import (
    ExecutorState,
    ExecutionInfo,
    PlannerCommand,
    PlanStatus,
    Action,
)

class StandardExecutor(BaseExecutor):
    """
    Standard sequential executor.
    
    Executes plan actions one at a time in the order they appear in the plan.
    This is the simplest execution strategy without temporal constraints.
    """
    
    EXECUTOR_TYPE = "standard"
    
    def __init__(self, platform_id: str, knowledge_client_ros: Optional[KnowledgeClientROS] = None):
        """
        Initialize the standard executor.
        
        Args:
            platform_id: Unique identifier for the platform
            knowledge_client_ros: Knowledge base client instance
        """
        super().__init__(platform_id, knowledge_client_ros)
        
    def _init_executor_specifics(self):
        """Initialize standard executor specifics - nothing additional needed."""
        self.get_logger().info(f"[StandardExecutor] Initialized for platform {self._platform_id}")

    def _reset_executor_specifics(self):
        """Reset standard executor specifics - nothing additional needed."""
        pass

    def _check_pending_execution(self):
        """
        Check for pending executions and process them.
        Standard execution processes actions sequentially.
        """
        if self._sequence_client._result_mutex.acquire(blocking=False):
            try:
                if self._pending_execution and len(self._current_actions) > 0:
                    self._pending_execution = False
                    self.get_logger().info("Processing pending actions")
                    self.execute_sequence(sequence_length=1)
            finally:
                self._sequence_client._result_mutex.release()

    def _on_plan_ready(self):
        """
        Called when a plan is ready for execution.
        Standard executor doesn't need special initialization.
        """
        self.get_logger().info(f"[StandardExecutor] Plan ready with {len(self._current_actions)} actions")
        return True

    def _handle_external_task_event(self, event: TaskEventData):
        """
        Handle task events from other executors.
        Standard executor ignores these as it doesn't coordinate.
        """
        # Standard executor doesn't participate in multi-platform coordination
        pass

    def _handle_actions_succeeded(self, transmitted_actions: List[Action], result):
        """
        Handle successful action completion.
        Continue with next action or finish plan.
        """
        
        if len(self._current_actions) == 0:
            # Plan finished
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.get_logger().info(f"Executor of {self._platform_id}: Plan finished")
            
            try:
                self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.COMPLETED))
            except Exception as e:
                self.get_logger().error(f"Failed to update plan status to COMPLETED in KB: {e}")
            
            self.publish_controllerCommand(
                command=PlannerCommand.FINISHED, 
                info_msg=ExecutionInfo(platform_id=self._platform_id, success=True)
            )
            self.reset_execution()
        else:
            # Continue with next action
            self._pending_execution = True

    def _handle_actions_canceled(self, transmitted_actions: List[Action], result):
        """Handle canceled actions - abort plan."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.CANCELED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to CANCELED in KB: {e}")
        
        self.reset_execution()
        self.get_logger().info(f"Executor {self._platform_id}: Goal canceled.")
        self.publish_controllerCommand(
            command=PlannerCommand.CANCEL_DONE, 
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )

    def _handle_actions_aborted(self, transmitted_actions: List[Action], result):
        """Handle aborted actions - abort plan."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to ABORTED in KB: {e}")
        
        self.reset_execution()
        self.get_logger().info(f"Executor {self._platform_id}: Goal Aborted.")
        self.publish_controllerCommand(
            command=PlannerCommand.ABORTED, 
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )

    def _handle_actions_failed(self, transmitted_actions: List[Action], result):
        """Handle failed actions - abort plan."""
        try:
            self._knowledge_client_ros.updatePlanStatus(plan_id=self._plan_id, status=enum_to_str(PlanStatus, PlanStatus.ABORTED))
        except Exception as e:
            self.get_logger().error(f"Failed to update plan status to ABORTED in KB: {e}")
        
        self.reset_execution()
        self.get_logger().info(f"Executor {self._platform_id}: Goal FAILED.")
        self.publish_controllerCommand(
            command=PlannerCommand.FAILED, 
            info_msg=ExecutionInfo(platform_id=self._platform_id)
        )
