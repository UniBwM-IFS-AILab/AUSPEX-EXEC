from rclpy import logging
from auspex_msgs.msg import ExecutorCommand, PlannerCommand, ExecutorState
from auspex_executor.utils import enum_to_str
from rclpy.qos import qos_profile_sensor_data
import traceback

class ExecutorInterface:
    """
    Communication interface between executor and monitor of a platform.

    monitor-to-executor commands: "execute", "continue", "pause", "cancel".
    executor-to-monitor commands: "replan", "finished", "updateplan", (optionally "feedback").
    """
    def __init__(self, node, platform_id, team_id, feedback_callback, result_callback, cancel_done_callback):
        """
        Constructor method

        :param Node node: the planner node
        :param Callable feedback_callback: function pointer for feedback callback function in plan executor
        :param Callable result_callback: function pointer for result callback function in plan executor
        """
        self._executor_status = ExecutorState.STATE_IDLE
        self._node = node
        self._platform_id = platform_id
        self._team_id = team_id
        self._logger = logging.get_logger(self.__class__.__name__)

        # To indicate a finished execution
        self._finished_execution = False

        # Publisher for commands from planner to executor.
        topic_pub = f"/{platform_id}/monitor_to_executor"
        self._pub = self._node.create_publisher(ExecutorCommand, topic_pub, 10)

        # Subscriber for commands from executor to planner.
        topic_sub = f"/{platform_id}/executor_to_monitor"
        self._sub = self._node.create_subscription(PlannerCommand, topic_sub, self.executor_callback, 10)

        self._feedback_callback = feedback_callback
        self._result_callback = result_callback
        self._cancel_done_callback = cancel_done_callback

    def send_command(self, command):
        """
        Sends a command from the monitor to the executor.

        :param str command: One of "execute", "continue", "pause", or "cancel".
        :param dict or list plan: Optional plan data to include.
        """
        if command == ExecutorCommand.EXECUTE:
            self._finished_execution = False
        msg = ExecutorCommand()
        msg.command = command
        self._pub.publish(msg)
        self._logger.info(f"Sent command '{enum_to_str(ExecutorCommand, command)}' to executor of platform: {self._platform_id}.")

    def executor_callback(self, msg):
        """
        Callback for handling messages from executor to monitor.
        The expected JSON payload should include a "command" field.
        Recognized commands: "replan", "finished", "updateplan", or "feedback".
        """
        self._executor_status = msg.executor_state.value
        try:
            command = msg.command
            if command in [PlannerCommand.REPLAN, PlannerCommand.FINISHED, PlannerCommand.UPDATEPLAN]:
                self._logger.info(f"Received '{enum_to_str(PlannerCommand, command)}' from executor of {self._platform_id}.")
                self._result_wrapper(msg)
            elif command == PlannerCommand.FEEDBACK:
                self._logger.info(f"Received feedback from executor of {self._platform_id}.")
                self._feedback_wrapper(msg)
            elif command == PlannerCommand.RESET:
                self._logger.info(f"Reset Monitor for: TODO Implement {self._platform_id}.")
                self._reset_wrapper(msg)
            elif command == PlannerCommand.CANCEL_DONE:
                self._logger.info(f"Canceled Plan of {self._platform_id}.")
                self._cancel_done_wrapper(msg)
                self._logger.info(f"Executor State of {self._platform_id} changed to {enum_to_str(ExecutorState,msg.executor_state.value)}.")
            elif command == PlannerCommand.STATE_FEEDBACK:
                self._logger.info(f"Executor State of {self._platform_id} changed to {enum_to_str(ExecutorState,msg.executor_state.value)}.")
            elif command in [PlannerCommand.ABORTED, PlannerCommand.FAILED]:
                self._logger.info(f"Plan for {self._platform_id}: {enum_to_str(PlannerCommand, command)}.")
                self._result_wrapper(msg)
            else:
                self._logger.info(f"Unknown command received: {command}")
        except Exception as e:
            self._logger.error(f"Error processing executor message: {e}")

    def _result_wrapper(self, msg):
        """
        Handles the result msg of the executor and forwards it to the planning main
        """
        self._result_callback(self, msg)

    def _cancel_done_wrapper(self, msg):
        """
        Processes the cancel feedback message from the executor and forwards it to the planing_main
        """
        self._cancel_done_callback(self, msg)


    def _feedback_wrapper(self, msg):
        """
        Processes Feedback messages from the executor and forwards it to the planing_main
        """
        self._feedback_callback(self, msg)

    def _reset_wrapper(self, msg):
        """
        Processes reset messages
        """
        pass

    def cancel_plan(self):
        """
        Cancels the current action
        """
        if self._executor_status == ExecutorState.STATE_EXECUTING:
            self.send_command(ExecutorCommand.CANCEL)

