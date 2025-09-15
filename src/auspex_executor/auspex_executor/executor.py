#!/usr/bin/env python3
import json
import time
import rclpy
import threading

from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from action_msgs.msg import GoalStatus
from geographic_msgs.msg import GeoPoint
from rclpy.action import CancelResponse

from auspex_executor.action_clients.sequence_action_client import SequenceActionClient

from auspex_db_client.kb_client import KB_Client
from .utils import parse_plan_from_dict, enum_to_str
from auspex_msgs.msg import ExecutorCommand, PlannerCommand, ExecutorState, ExecutionInfo, ActionInstance, ActionStatus, PlanStatus

from msg_context.loader import ObjectKnowledge, UserCommand

from auspex_msgs.srv import (
    GetOrigin,
    SetOrigin,
    GetAltitude,
    GetHeighestPoint,
    CancelGoal,
    AddAction,
)



class AuspexExecutor(Node):
    """
    Ros2 node used to execute a plan

    """
    def __init__(self, platform_id):
        """
        Constructor method
        """
        super().__init__(platform_id + "_executor")

        """
        Executor State
        """
        self._executor_state = ExecutorState.STATE_IDLE

        """
        Lock for position
        """
        self._position_lock = threading.Lock()

        """
        Callback Group
        """
        self._cb_group_actions = ReentrantCallbackGroup()
        self._cb_group_services = ReentrantCallbackGroup()
        self._cb_group_pubsub = ReentrantCallbackGroup()

        """
        Knowledge Base Interface
        """
        self._kb_client = KB_Client(node_name_prefix="executor_"+ platform_id + "_")

        """
        Added for LLM-planner
        """
        self._gps_position = GeoPoint()
        self._home_position = GeoPoint()
        self._finished_actions_in_sequence = 0
        self._is_vhcl_paused = False

        """
        Get Vehicle prefix
        """
        self._platform_id = platform_id

        """
        Stores the plan as a sequence of UPF actions
        """
        self._current_tasks = []
        self._current_plan = []
        self._plan_id = -1

        """
        A sub_node for adding service, which does not block the future
        """
        self.sub_node =  rclpy.create_node('executor_' + self._platform_id , use_global_arguments=False)

    def init_executor(self):
        """
        Create publisher
        """
        self._user_command_publisher = self.sub_node.create_publisher(UserCommand, '/' +self._platform_id + '/user_command', 10, callback_group=self._cb_group_pubsub)
        self._monitor_command_publisher = self.sub_node.create_publisher(PlannerCommand, '/' +self._platform_id + '/executor_to_monitor', 10, callback_group=self._cb_group_pubsub)

        """
        Create subscriber
        """
        self._executor_command_subscriber = self.sub_node.create_subscription(ExecutorCommand, '/' +self._platform_id + '/monitor_to_executor', self.monitor_callback, 10, callback_group=self._cb_group_pubsub)

        """
        Create Clients
        """
        self._get_home_client = self.sub_node.create_client(GetOrigin, self._platform_id + '/srv/get_origin', callback_group=self._cb_group_services)
        self._set_home_client = self.sub_node.create_client(SetOrigin, self._platform_id + '/srv/set_origin', callback_group=self._cb_group_services)
        self._add_action_service = self.sub_node.create_client(AddAction, self._platform_id + '/srv/add_action', callback_group=self._cb_group_services)
        self._altitude_getter = self.sub_node.create_client(GetAltitude, "auspex_get_altitude", callback_group=self._cb_group_services)
        self._hp_getter = self.sub_node.create_client(GetHeighestPoint, "auspex_get_heighest_point", callback_group=self._cb_group_services)


        """
        Create action client
        """
        self._sequence_client = SequenceActionClient(   self.sub_node,
                                                        self._kb_client,
                                                        self.sequence_feedback_callback,
                                                        self.finished_sequence_callback,
                                                        self._platform_id,
                                                        callback_group=self._cb_group_actions)

        """
        Requests the current gps coordinates to set home with an altitude
        """
        self.set_home(0,0,0)
        self._pending_execution = False
        self.create_timer(0.1, callback=self._check_pending_execution, callback_group=self._cb_group_actions)

        self._setup_timer = self.sub_node.create_timer(
            0.1,
            self._delayed_setup_origin,
            callback_group=self._cb_group_actions
        )

    def _check_pending_execution(self):
        """
        Check if there are pending executions and process them outside of callbacks
        """
        if self._sequence_client._result_mutex.acquire(blocking=False):
            if self._pending_execution and len(self._current_plan) > 0:
                self._pending_execution = False  # Reset flag
                self.get_logger().info("Processing pending actions")
                self.execute_sequence(sequence_length=1)
            self._sequence_client._result_mutex.release()

    def _delayed_setup_origin(self):
        self._setup_timer.cancel()
        try:
            self.setup_origin()
            self.get_logger().info("Origin setup completed.")
        except Exception as e:
            self.get_logger().error(f"setup_origin failed: {e}")

    def set_home(self, lat, lon, alt):
        """
        Writes home to operation areas
        """
        self.get_logger().info(f"Setting home position to ({lat}, {lon}, {alt})")

        self._position_lock.acquire()
        self._home_position.latitude = float(lat)
        self._home_position.longitude = float(lon)
        self._home_position.altitude = float(alt)
        self._position_lock.release()

        coordinates = json.dumps([[str(lat), str(lon), str(alt)]])
        self._kb_client.write(collection='area', entity=coordinates,field='points', key='name', value='home')

    def setup_origin(self):
        """
        calls to a ROS2 service from the flight controller interface to get the origin position

        :param origin_result_callback: the callback to be executed when the async call of the service request is done
        """
        srv = GetOrigin.Request()
        while not self._get_home_client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info(f"getHome service not available")
        self.get_logger().info(f"getHome service is available")
        future = self._get_home_client.call_async(srv)
        future.add_done_callback(self.get_origin_callback)

    def get_origin_callback(self, future):
        resp = future.result()
        if resp is None:
            self.get_logger().error("GetOrigin service call returned None! Do not Take OFF!")
            return 0.0, 0.0, 0.0
        #compute alt
        srv = GetAltitude.Request()
        srv.gps_position.latitude = resp.origin.latitude
        srv.gps_position.longitude = resp.origin.longitude
        srv.gps_position.altitude = resp.origin.altitude

        self.set_home(float(resp.origin.latitude), float(resp.origin.longitude), float(resp.origin.altitude))

        self.get_logger().info(f"FC HOME set to lat: {srv.gps_position.latitude}")
        self.get_logger().info(f"FC HOME set to long: {srv.gps_position.longitude}")
        self.get_logger().info(f"Initialised.")

        # srv.resolution = 1
        # while not self._altitude_getter.wait_for_service(timeout_sec=1.0):
        #     self.get_logger().warn("Altitude Service not available. Waiting...")

        # self.get_logger().info(f"getAltitude service is available")
        # future = self._altitude_getter.call_async(srv)
        # future.add_done_callback(self.get_altitude_callback)

    # def get_altitude_callback(self, future):
    #     respAlt = future.result()

    #     if respAlt is None:
    #         self.get_logger().error("GetAltitude service call returned None!")
    #         return 0.0, 0.0, 0.0

    #     if respAlt.success == True:
    #         alt_aMSL = respAlt.altitude_amsl

    #     self.set_home(self._home_position.latitude, self._home_position.longitude, alt_aMSL)
    #     self.set_origin_remote([self._home_position.latitude, self._home_position.longitude, alt_aMSL])

    # def set_origin_remote(self, new_origin):
    #     """
    #     calls to a ROS2 service from the flight controller interface to set the origin position to a new_origin (to set the new altitude)

    #     :param new_origin: a List[float] of 3 floats containing the new origin coordinates:
    #                        new_origin[0]:= latitude, new_origin[1]:= longitude, new_origin[2]:= altitude
    #     :param origin_result_callback: the callback to be executed when the async call of the service request is done
    #     """
    #     srv_request = SetOrigin.Request()
    #     srv_request.origin.latitude = float(new_origin[0])
    #     srv_request.origin.longitude = float(new_origin[1])
    #     srv_request.origin.altitude = float(new_origin[2])
    #     while not self._set_home_client.wait_for_service(timeout_sec=1.0):
    #         self.get_logger().warn("_set_home_client Service not available. Waiting...")
    #     future = self._set_home_client.call_async(srv_request)
    #     future.add_done_callback(self.set_origin_callback)

    # def set_origin_callback(self, future):
    #     resp = future.result()
    #     if resp is None:
    #         self.get_logger().error("SetOrigin service call returned None!")
    #         return
    #     if resp.success == True:
    #         self.get_logger().info(f"Altitude set on drone.")
    #     else:
    #         self.get_logger().info(f"Failed to set altitude.")

    def finished_sequence_callback(self, actions = [], params = [[]], result = 0):
        """
        Wrapper for the callback from the action sequence client after the sequence was executed/canceled.

        :param action: The list of actions (in UP format) that was completed
        :param params: The list of params (in UP format) of the completed action
        :param result: Returned result of the completed sequence, contains goal status as well as action specific fields (see https://docs.ros2.org/galactic/api/action_msgs/msg/GoalStatus.html for goal status ENUMS)
        """

        if result.status == GoalStatus.STATUS_SUCCEEDED:
            for idx, action in enumerate(actions):
                self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.COMPLETED))

            if len(self._current_plan) == 0:
                self.change_executor_state(ExecutorState.STATE_IDLE)
                self.get_logger().info(f"Executor of {self._platform_id}: Plan finished")
                # Set Plan to Completed in KB
                self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.COMPLETED), field='status', key='plan_id', value=str(self._plan_id))
                self.publish_monitorCommand(command=PlannerCommand.FINISHED, info_msg=ExecutionInfo(platform_id=self._platform_id, success=True))
                self._plan_id = -1
            else:
                self._pending_execution = True

        elif result.status == GoalStatus.STATUS_CANCELED:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for idx, action in enumerate(actions):
                self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.CANCELED))
            self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.CANCELED), field='status', key='plan_id', value=str(self._plan_id))
            self._current_plan = []
            self._plan_id = -1
            self.get_logger().info(f"Executor {self._platform_id}: Canceled action confirmation.")
            self.publish_monitorCommand(command=PlannerCommand.CANCEL_DONE, info_msg=ExecutionInfo(platform_id=self._platform_id))
        elif result.status == GoalStatus.STATUS_ABORTED:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for idx, action in enumerate(actions):
                self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.ABORTED))
            self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.ABORTED), field='status', key='plan_id', value=str(self._plan_id))
            self._plan_id = -1
            self._current_plan = []
            self.get_logger().info(f"Executor {self._platform_id}: Goal Aborted.")
            self.publish_monitorCommand(command=PlannerCommand.ABORTED, info_msg=ExecutionInfo(platform_id=self._platform_id))
        else:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            for idx, action in enumerate(actions):
                self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.ABORTED))
            self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.ABORTED), field='status', key='plan_id', value=str(self._plan_id))
            self._plan_id = -1
            self._current_plan = []
            self.get_logger().info(f"Executor {self._platform_id}: Goal FAILED.")
            self.publish_monitorCommand(command=PlannerCommand.FAILED, info_msg=ExecutionInfo(platform_id=self._platform_id))


        return params

    def sequence_feedback_callback(self, action, params, feedback):
        """
        Handles the feedback received by an action client
        """
        self._position_lock.acquire()
        self._gps_position.latitude = feedback.current_pose.pose.position.x
        self._gps_position.longitude = feedback.current_pose.pose.position.y
        self._gps_position.altitude = feedback.current_pose.pose.position.z
        self._position_lock.release()

        self._finished_actions_in_sequence = feedback.finished_actions_in_sequence

    def add_actions2action(self, atoms_list):
        """
        A service call which pushes a new action in front of the others
        """
        self.get_logger().info('Adding Actions to action...')
        request = AddAction.Request()
        parsed_actions = self._sequence_client.create_action_list(atoms_list)
        request.execute_atoms = parsed_actions
        while not self._add_action_service.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('service not available, waiting again...')
        self._add_action_service.call_async(request)

    def send_pause(self):
        """
        Pauses the current action
        """
        if self._is_vhcl_paused:
            return

        if self._current_plan == []:
            return

        self.get_logger().info('Pausing current actions...')
        self.send_user_command(UserCommand.USER_PAUSE)
        self._is_vhcl_paused = True
        self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.PAUSED), field='status', key='plan_id', value=str(self._plan_id))


        for action in self._sequence_client._actions:
            self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.PAUSED))

        self.change_executor_state(ExecutorState.STATE_PAUSED)

    def send_resume(self):
        """
        Resumes the current action
        """
        if not self._is_vhcl_paused:
            return
        self.get_logger().info('Resuming current actions...')
        self.send_user_command(UserCommand.USER_RESUME)
        self._is_vhcl_paused = False

        self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.ACTIVE), field='status', key='plan_id', value=str(self._plan_id))
        for action in self._sequence_client._actions:
            self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.ACTIVE))
        self.change_executor_state(ExecutorState.STATE_EXECUTING)

    def cancelGoal(self):
        """"
        Cancels all current goals
        """
        if self._executor_state == ExecutorState.STATE_EXECUTING or self._executor_state == ExecutorState.STATE_PAUSED:
            self.get_logger().info("Sending cancel command...")
            self.cancel_future = self._sequence_client.cancel_action_goal()
            self._current_plan = []

    def send_user_command(self, command):
        """
        Sends a user command to the drone
        """
        if not self._is_vhcl_paused:
            if command == UserCommand.USER_PAUSE:
                msg = UserCommand()
                msg.user_command = command
                self._user_command_publisher.publish(msg)
                self._is_vhcl_paused = True
        elif self._is_vhcl_paused:
            if command == UserCommand.USER_RESUME:
                msg = UserCommand()
                msg.user_command = command
                self._user_command_publisher.publish(msg)
                self._is_vhcl_paused = False

    def execute_sequence(self, sequence_length):
        """
        Executes a sequence of #sequence_length actions of the plan.

        :param sequence_length: number of actions that should be grouped into a single sequence. Use the sequence_action_client for this purpose
        """

        self.get_logger().info("Begin execution of sequence...")
        sequence_length = min(len(self._current_plan), sequence_length)

        actions = []
        params_list = []

        for i in range(sequence_length):
            action = self._current_plan.pop(0)
            actions.append(action)
            self._current_tasks.append(action.task_id)

            params = []
            for param in action.parameters:
                if len(param.symbol_atom) != 0:
                    params.append(param.symbol_atom[0])
                elif len(param.int_atom) != 0:
                    params.append(str(param.int_atom[0]))
                elif len(param.real_atom) != 0:
                    params.append(str(param.real_atom[0]))
                elif len(param.bool_atom) != 0:
                    params.append(str(param.bool_atom[0]))
            params_list.append(params)
            self.get_logger().info("Next action in sequence: " + action.action_name+"("+", ".join(params)+")")

        self.get_logger().info("End of sequence")
        self._kb_client.update_action_status(plan_id=self._plan_id, action_id=action.id, new_status=enum_to_str(ActionStatus, ActionStatus.ACTIVE))
        self._sequence_client.send_action_goal(actions, params_list)

    def execute_plan(self):
        """
        Gets a plan in the up format and executes it
        """
        if self._is_vhcl_paused:
            self._is_vhcl_paused = False

        self.change_executor_state(ExecutorState.STATE_EXECUTING)

        """
        Get PLAN FROM KB
        """
        plans = self._kb_client.query(collection='plan', key='platform_id', value=self._platform_id)

        if not plans:
            self.get_logger().info('No plans in KB, returning...')
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.publish_monitorCommand(command=PlannerCommand.ABORTED, info_msg=ExecutionInfo(platform_id=self._platform_id, success=False))
            return

        plan_msg = parse_plan_from_dict(plans)
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

            if lowest_index == 10000:  # Use a large integer as a substitute for infinity
                self.get_logger().info('No INACTIVE plans found, returning...')
                self.change_executor_state(ExecutorState.STATE_IDLE)
                self.publish_monitorCommand(command=PlannerCommand.ABORTED, info_msg=ExecutionInfo(platform_id=self._platform_id, success=False))
                return

            actions = plan_msg[lowest_index].actions
            first_incomplete_idx = next((i for i, a in enumerate(actions) if (a.status != enum_to_str(ActionStatus, ActionStatus.COMPLETED) and a.status != enum_to_str(ActionStatus, ActionStatus.CANCELED))), None)
            if first_incomplete_idx is None:
                self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.COMPLETED), field='status', key='plan_id', value=str(self._plan_id))

        plan = actions[first_incomplete_idx:]

        if len(plan) == 0:
            self.get_logger().info('Received empty plan, returning...')
            self.change_executor_state(ExecutorState.STATE_IDLE)
            self.publish_monitorCommand(command=PlannerCommand.ABORTED, info_msg=ExecutionInfo(platform_id=self._platform_id, success=False))
            return

        self.get_logger().info('Received plan. Now Executing...')
        self._current_plan = plan
        self._plan_id = plan_msg[lowest_index].plan_id
        self._team_id = plan_msg[lowest_index].team_id

        self._kb_client.update(collection='plan', new_value=enum_to_str(PlanStatus, PlanStatus.ACTIVE), field='status', key='plan_id', value=str(self._plan_id))

        self._pending_execution = True # start execution


    def publish_monitorCommand(self, command, info_msg):
        msg = PlannerCommand()
        msg.command = command
        msg.info = info_msg
        msg.executor_state.value = self._executor_state
        self._monitor_command_publisher.publish(msg)

    def change_executor_state(self, state):
        if state != self._executor_state:
            self._executor_state = state
            self.publish_monitorCommand(command=PlannerCommand.STATE_FEEDBACK, info_msg=ExecutionInfo(platform_id=self._platform_id))

    def monitor_callback(self, msg):
        if msg.command == ExecutorCommand.EXECUTE:
            if self._executor_state == ExecutorState.STATE_EXECUTING:
                self.get_logger().info('Executor already executing...')
                return
            if self._executor_state == ExecutorState.STATE_PAUSED:
                self.get_logger().info(f'Executor {self._platform_id} was paused. Resuming with highest priority plan...')
            self.execute_plan()

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
