import time
import threading
import traceback
from rclpy import logging
from rclpy.action import ActionClient
from auspex_msgs.action import ExecuteSequence

class SequenceActionClient:
    """
    Action client for sending sequence actions to the action server
    """

    def __init__(self, platform_id, node, feedback_callback, result_callback, callback_group=None):

        self.platform_id = platform_id
        self.node = node
        self.logger = logging.get_logger(self.__class__.__name__)
        self._action_client = ActionClient(node, ExecuteSequence, platform_id + "/action_sequence", callback_group=callback_group)

        self._actions = None

        self._goal_handle = None

        self.feedback_callback = feedback_callback
        self.result_callback = result_callback
        self.rejection_callback = None  # Called when OBC rejects a goal

        self._send_goal_future = None
        self._result_mutex = threading.Lock()


    def send_action_goal(self, actions: list):
        if self._send_goal_future and not self._send_goal_future.done():
            self.logger.error("Previous goal still pending. Rejecting new goal.")
            return

        if len(actions) == 0:
            self.logger.info("No actions to send to action server.")
            return

        while not self._action_client.wait_for_server(timeout_sec=1.0):
            self.logger.info("Action server not available, waiting...")

        self.logger.info(f"Sending action(s) to platform.")
        self._actions = actions

        goal_msg = ExecuteSequence.Goal()
        goal_msg.actions = actions

        try:
            self._send_goal_future = self._action_client.send_goal_async(goal_msg, feedback_callback=self.feedback_wrapper)
            self._send_goal_future.add_done_callback(self.goal_response_callback)
        except Exception as e:
            self.logger.error(f"Error sending goal: {e}")
            traceback.print_exc()

    def goal_response_callback(self, future):
        """
        Handles the goal response from the action server
        :param Future future: the future containing the response from the action server
        """
        try:
            self._goal_handle = future.result()
            if not self._goal_handle.accepted:
                self.logger.info('Goal rejected by action server (OBC busy)')
                # Reset so a new goal can be sent
                self._send_goal_future = None
                # Notify the executor so it can retry
                if self.rejection_callback is not None:
                    rejected_actions = self._actions
                    self.rejection_callback(rejected_actions)
                return
            self.logger.info('Goal accepted by action server.')
            self._get_result_future = self._goal_handle.get_result_async()
            self._get_result_future.add_done_callback(self.get_result_callback)
        except Exception as e:
            self.logger.error(f"Error sending goal: {e}")
            traceback.print_exc()

    def get_result_callback(self, future):
        """
        Handles the result from the action server and forwards it to the plan executor
        :param Future future: the future containing the response from the action server
        """
        self._result_mutex.acquire()
        try:
            self.logger.info(f'Goal returned by action server.')
            self.result_callback(self._actions, future.result())
        except Exception as e:
            self.logger.error(f"Error getting result: {e}")
            traceback.print_exc()
        finally:
            self._result_mutex.release()

    def cancel_action_goal(self):
        """
        Cancels the current action asynchronously
        """
        if self._goal_handle is not None:
            try:
                self.cancel_future = self._goal_handle.cancel_goal_async()
                self.cancel_future.add_done_callback(self._cancel_response_callback)
                return self.cancel_future
            except Exception as e:
                self.logger.error(f"Error canceling goal: {e}")
                traceback.print_exc()
        else:
            self.logger.warn("No active action_goal to cancel")

        return None

    def _cancel_response_callback(self, future):
        """
        Callback for handling cancellation response
        """
        try:
            self.logger.info("Goal cancellation accepted")
        except Exception as e:
            self.logger.error(f"Error in cancel response callback: {e}")
            traceback.print_exc()

    def feedback_wrapper(self, feedback_msg):
        """
        Processes Feedback messages from the action server and forwards it to the plan executor, can be overwritten in inheriting classes
        :param action_type.Feedback() feedback_msg: the message containing the feedback from the action server
        """
        self.feedback_callback(self._actions, feedback_msg)