#!/usr/bin/env python3
import rclpy
import json

import threading
from rclpy.task import Future
from rclpy.action import ActionServer
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from auspex_executor.action_clients.sequence_action_client import SequenceActionClient
from rclpy.action import ActionServer, CancelResponse

from ament_index_python.packages import get_package_share_directory

from auspex_msgs.action import ExecutePlan

from msg_context.loader import ObjectKnowledge
from msg_context.loader import UserCommand

from auspex_msgs.srv import (
    GetOrigin, 
    SetOrigin,
    GetAltitude,
    GetHeighestPoint,
    CancelGoal,
    AddAction,
)

from action_msgs.msg import GoalStatus
from geographic_msgs.msg import GeoPoint


class AuspexExecutor(Node):
    """
    Ros2 node used to execute a plan

    """
    def __init__(self, platform_id):
        """
        Constructor method
        """
        super().__init__(platform_id+"_executor")

        self._position_lock = threading.Lock()

        self._planner_goal_handle = None

        """
        Added for LLM-planner
        """
        self._gps_position = GeoPoint()
        self._is_vhcl_paused = False

        """
        Get Vehicle prefix
        """
        self._platform_id = platform_id

        """
        Stores the plan as a sequence of UPF actions
        """
        self._current_plan = []

        """
        Home Altitude
        """
        self._home_altitude_amsl = 0.0

        """
        The current future to be processed
        """
        self._current_action_future = None


        """
        A sub_node for adding service, which does not block the future
        """
        self.sub_node = rclpy.create_node('executor_' + self._platform_id , use_global_arguments=False)

        """
        Create the operation_areas table for action clients requiring waypoint coordinates
        """
        self._operation_areas = dict()
        # TODO get from KB
        with open('/home/companion/auspex_params/geographic/areas/location1_areas_shrinked.json') as file:
            self._operation_areas.update(json.load(file))

        """
        Copernicus client
        """
        self._altitude_getter = self.sub_node.create_client(GetAltitude, "auspex_get_altitude")
        self._hp_getter = self.sub_node.create_client(GetHeighestPoint, "auspex_get_heighest_point")

        """
        Create Action Server
        """
        self._plan_executor_server = ActionServer(self, ExecutePlan , '/' + self._platform_id +"/plan_execution", execute_callback=self.execute_plan_server_callback, cancel_callback=self.cancel_plan_callback)

        """
        Create action client
        """
        self._sequence_client = SequenceActionClient(   self.sub_node,
                                                        self.sequence_feedback_callback,
                                                        self.finished_sequence_callback, 
                                                        self._platform_id,
                                                        operation_areas=self._operation_areas, 
                                                        altitude_server=self._altitude_getter,
                                                        hp_server=self._hp_getter)
        """
        Create publisher
        """
        self._user_command_publisher = self.sub_node.create_publisher(UserCommand, '/' +self._platform_id + '/user_command', 10)

        """
        Create Clients
        """
        self._origin_getter = self.sub_node.create_client(GetOrigin, self._platform_id + '/srv/get_origin')
        self._origin_setter = self.sub_node.create_client(SetOrigin, self._platform_id + '/srv/set_origin')
        self._add_action_service = self.sub_node.create_client(AddAction, self._platform_id + '/srv/add_action')


        """
        Create Services
        """
        self._cancelService = self.sub_node.create_service(CancelGoal, "/" +self._platform_id +"/cancelService", self.cancelGoal)

        """
        Requests the current gps coordinates to set home with an altitude
        """
        self.set_home(0,0,0)
        lat, lon, alt = self.get_origin_remote()
        self.set_origin_remote([lat, lon, alt])

    def set_home(self, lat, lon, alt):
        """
        Writes home to operation areas
        """
        self.get_logger().info(f"Setting home position to ({lat}, {lon}, {alt})")
        self._home_altitude_amsl = alt

        self._position_lock.acquire()
        self._gps_position.latitude = float(lat)
        self._gps_position.longitude = float(lon)
        self._gps_position.altitude = float(alt)
        self._position_lock.release()

        self._operation_areas['home']['centre'] = [lat,lon,alt]

    def get_origin_remote(self):
        """
        calls to a ROS2 service from the flight controller interface to get the origin position

        :param origin_result_callback: the callback to be executed when the async call of the service request is done
        """
        srv = GetOrigin.Request()
        self.get_logger().info(f"Waiting for getHome service to be available...")
        self._origin_getter.wait_for_service()
        self.get_logger().info(f"getHome service is available")
        future = self._origin_getter.call_async(srv)
        rclpy.spin_until_future_complete(self.sub_node, future)

        resp = future.result()
        alt_aMSL = 450
        #compute alt
        srv = GetAltitude.Request()
        srv.gps_position.latitude = resp.origin.latitude
        srv.gps_position.longitude = resp.origin.longitude
        srv.gps_position.altitude = resp.origin.altitude
        alt_aMSL = resp.origin.altitude

        if resp.origin.altitude < 100: #should be changed here 
            print(f"Got lat: {srv.gps_position.latitude}")
            print(f"Got long: {srv.gps_position.longitude}")
            srv.resolution = 1
            self.get_logger().info(f"Waiting for getAltitude service to be available...")

            if not self._altitude_getter.wait_for_service(timeout_sec=10.0):
                self.get_logger().warn("Service not available. Setting default value.")
                alt_aMSL = 0.0 
            else:
                self.get_logger().info(f"getAltitude service is available")
                future = self._altitude_getter.call_async(srv)
                rclpy.spin_until_future_complete(self.sub_node, future)

                if future.result().success == True:
                    alt_aMSL = future.result().altitude_amsl

        self.set_home(resp.origin.latitude, resp.origin.longitude, alt_aMSL)

        return resp.origin.latitude, resp.origin.longitude, alt_aMSL

    def set_origin_remote(self, new_origin):
        """
        calls to a ROS2 service from the flight controller interface to set the origin position to a new_origin (to set the new altitude)

        :param new_origin: a List[float] of 3 floats containing the new origin coordinates: 
                           new_origin[0]:= latitude, new_origin[1]:= longitude, new_origin[2]:= altitude
        :param origin_result_callback: the callback to be executed when the async call of the service request is done
        """
        srv_request = SetOrigin.Request()
        srv_request.origin.latitude = float(new_origin[0])
        srv_request.origin.longitude = float(new_origin[1])
        srv_request.origin.altitude = float(new_origin[2])

        self._origin_setter.wait_for_service()
        future = self._origin_setter.call_async(srv_request)
        rclpy.spin_until_future_complete(self.sub_node, future)

        response = future.result()
        if response.success == True:
            self.get_logger().info(f"Altitude set on drone.")
        else:
            self.get_logger().info(f"Failed to set altitude.")

    def finished_sequence_callback(self, actions = [], params = [[]], result = 0):
        """
        Wrapper for the callback from the action sequence client after the sequence was executed/canceled.

        :param action: The list of actions (in UP format) that was completed
        :param params: The list of params (in UP format) of the completed action
        :param result: Returned result of the completed sequence, contains goal status as well as action specific fields (see https://docs.ros2.org/galactic/api/action_msgs/msg/GoalStatus.html for goal status ENUMS)
        """

        if result.status == GoalStatus.STATUS_SUCCEEDED:

            self.get_logger().info(f"Completed sequence of {len(actions)} actions")
            for idx, action in enumerate(actions):
                feedback_msg = ExecutePlan.Feedback()
                feedback_msg.platform_id = self._platform_id
                self._planner_goal_handle.publish_feedback(feedback_msg)

        elif result.status == GoalStatus.STATUS_CANCELED:

            self.get_logger().info("Canceled action confirmation.")
            self._current_plan = []

        else:
            self._current_plan = []

        self._current_action_future.set_result(result.status)

    def sequence_feedback_callback(self, action, params, feedback):
        """
        Handles the feedback received by an action client
        """ 
        self._position_lock.acquire()
        self._gps_position.latitude = feedback.current_pose.pose.position.x
        self._gps_position.longitude = feedback.current_pose.pose.position.y
        self._gps_position.altitude = feedback.current_pose.pose.position.z
        self._position_lock.release()

    def getPlatformID(self):
        return self._platform_id

    def add_actions2action(self, atoms_list):
        """
        A service call which pushes a new action in front of the others
        """
        self.get_logger().info('Adding Actions to action...')
        request = AddAction.Request()
        parsed_actions = self._sequence_client.create_goalmsg_from_up(atoms_list)
        request.execute_atoms = parsed_actions
        while not self._add_action_service.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('service not available, waiting again...')
        actions2action_future = self._add_action_service.call_async(request) 

    def send_pause(self):
        """
        Pauses the current action
        """
        self.get_logger().info('Pausing current actions...')
        self.send_user_command(UserCommand.USER_PAUSE)

    def send_resume(self):
        """
        Resumes the current action
        """
        self.get_logger().info('Resuming current actions...')
        self.send_user_command(UserCommand.USER_RESUME)

    def cancelGoal(self, request, response):
        """
        A Service function to cancel all current goals
        """
        self.get_logger().info("Canceling all goals....")
        self._sequence_client.cancel_action_goal()
        self.get_logger().info("Canceled current goals.")
        response.result = True
        return response

    def cancelGoalLocal(self):
        """"
        Cancels all current goals
        """
        self._sequence_client.cancel_action_goal()
        self.get_logger().info("Canceled current goals.")

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

    def execute_sequence(self, sequence_length, action_finished_future, plan_finished_future):
        """
        Executes a sequence of #sequence_length actions of the plan. If no next action is available -> plan is completed and terminate

        :param sequence_length: number of actions that should be grouped into a single sequence. Use the sequence_action_client for this purpose
        :param action_finished_future: future that should be set when an action is completed so next action in the plan can be started
        :param plan_finished_future: future that should be set when a plan is completed so next plan can be started
        """

        if len(self._current_plan) == 0:
            self.get_logger().info('Plan completed!')
            plan_finished_future.set_result("Finished")
            action_finished_future.set_result("NoAction")
            return

        self.get_logger().info("Begin execution of sequence")

        sequence_length = min(len(self._current_plan), sequence_length)

        self._current_action_future = action_finished_future
        actions = []
        params_list = []

        for i in range(sequence_length):
            action = self._current_plan.pop(0)
            actions.append(action)

            params = []
            for param in action.parameters:
                if len(param.symbol_atom) != 0:
                    params.append(param.symbol_atom[0])
                elif len(param.int_atom) != 0:
                    params.append(str(param.int_atom[0]))
                elif len(param.real_atom) != 0:
                    params.append(str(param.real_atom[0]))
                elif len(param.boolean_atom) != 0:
                    params.append(str(param.bool_atom[0]))
            params_list.append(params)
            self.get_logger().info("Next action in sequence: " + action.action_name+"("+", ".join(params)+")")

        self.get_logger().info("End of sequence")

        self._sequence_client.send_action_goal(actions, params_list)

    def cancel_plan_callback(self, goal_handle):
        """
        Is called by the planner and requests to cancel the current goals...
        """
        self.get_logger().info(self._platform_id +" received cancel request. Now cancelling...")
        self.cancelGoalLocal()
        return CancelResponse.ACCEPT

    def execute_plan(self, plan):
        """
        Gets a plan in the up format and executes it
        """
        if self._is_vhcl_paused:
            self._sequence_client.cancel_action_goal()
            self._is_vhcl_paused = False

        mte = MultiThreadedExecutor()
        self._current_plan = plan

        plan_finished_future = Future(executor = mte)
        while plan_finished_future.done() == False:
            action_finished_future = Future(executor = mte)
            self.execute_sequence(2 , action_finished_future,plan_finished_future)
            rclpy.spin_until_future_complete(self.sub_node,action_finished_future,mte)

    def execute_plan_server_callback(self, goal_handle):
        """
        Called by the planner with a given plan
        """
        self._planner_goal_handle = goal_handle
        plan = goal_handle.request.plan.actions

        if len(plan) == 0:
            self.get_logger().info('Received empty plan, returning...')
            result_msg = ExecutePlan.Result()
            result_msg.success == False
            goal_handle.abort()
            return result_msg

        self.get_logger().info('Received plan. Now Executing...')

        self.execute_plan(plan)

        result_msg = ExecutePlan.Result()
        result_msg.platform_id = self._platform_id
        result_msg.result = str(self._current_action_future.result())

        """
        Check for result of the last action
        """
        if self._current_action_future.result() == GoalStatus.STATUS_SUCCEEDED:
            goal_handle.succeed()
            result_msg.success = True
        else:
            goal_handle.abort()
            result_msg.success = False

        return result_msg
