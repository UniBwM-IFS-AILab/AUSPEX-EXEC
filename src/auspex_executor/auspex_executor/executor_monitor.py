#!/usr/bin/env python3
import json
import time
import rclpy
import threading
from .utils import compute_coverage
from rclpy.node import Node
from .utils import enum_to_str
from rclpy.callback_groups import ReentrantCallbackGroup
from auspex_executor.action_clients.executor_interface import ExecutorInterface
from auspex_db_client.kb_client import KB_Client
from auspex_msgs.msg import ExecutorCommand, PlannerCommand, ExecutorState, ExecutionInfo, ActionInstance, ActionStatus, PlanStatus

from msg_context.loader import ObjectKnowledge

class AuspexExecutorMonitor(Node):
    """
    Ros2 node used to monitor the executors of a team.

    """
    def __init__(self, team_id):
        """
        Constructor method
        """
        super().__init__(team_id + "_executor_monitor")

        """
        Executor State
        """
        self._executor_state = ExecutorState.STATE_IDLE

        """
        Callback Group
        """
        self._cb_group_actions = ReentrantCallbackGroup()
        self._cb_group_services = ReentrantCallbackGroup()
        self._cb_group_pubsub = ReentrantCallbackGroup()

        """
        Knowledge Base Interface
        """
        self._kb_client = KB_Client(node_name_prefix="executor_monitor"+ team_id + "_")

        self._is_team_paused = False

        """
        Assign team_id
        """
        self._team_id = team_id

        """
        Managed Executor Nodes
        """
        self._executor_nodes = {}
        self._executor_interfaces = {}

        """
        Detected Objects
        """
        self._detected_objects_list = []

        """
        Current goals for this platform
        """
        self._current_goals = []
        self._current_constraints = []

        """
        A sub_node for adding service, which does not block the future
        """
        self.sub_node =  rclpy.create_node('executor_monitor' + self._team_id , use_global_arguments=False)

    def init_executor(self):
        self._planner_command_publisher = self.sub_node.create_publisher(PlannerCommand, '/' +self._team_id + '/monitor_to_planner', 10, callback_group=self._cb_group_pubsub)
        self._object_detections = self.sub_node.create_subscription(ObjectKnowledge, '/detections', self.object_detection_callback, 10, callback_group=self._cb_group_pubsub)
        self._monitor_command_subscriber = self.sub_node.create_subscription(ExecutorCommand, '/' +self._team_id + '/planner_to_monitor', self.planner_callback, 10, callback_group=self._cb_group_pubsub)
        self.create_timer(0.1, callback=self._check_current_execution, callback_group=self._cb_group_actions)

    def register_executor(self, executor):
        self._executor_nodes[executor._platform_id] = executor
        self._executor_interfaces[executor._platform_id] = ExecutorInterface(self.sub_node, executor._platform_id, self._team_id, self.feedback_callback, self.result_callback, self.cancel_done_callback)
        self.get_logger().info("Registered executor for platform:" + executor._platform_id)

    def unregister_executor(self, executor):
        if executor in self._executor_nodes:
            self._executor_nodes.remove(executor)
            self._monitor_to_executor_publisher[executor._platform_id].destroy()
            self._monitor_to_executor_subscriber[executor._platform_id].destroy()
            self.get_logger().info("Unregistered executor for platform:" + executor._platform_id)
        else:
            self.get_logger().error("Executor not found in the list of registered executors.")

    def result_callback(self, executor_interface, msg):
        self.get_logger().info(f'Result: {enum_to_str(PlannerCommand, msg.command)}')

        team_id = executor_interface._team_id
        platform_id = executor_interface._platform_id

        if msg.info.success == True:
            self.get_logger().info("Plan for platform: " + platform_id + " succeeded")
        else:
            self.get_logger().info("Plan for platform: " + platform_id + " failed")

        executor_interface._finished_execution = True

    def feedback_callback(self, executor_interface, feedback_msg):
        self.get_logger().info('Received feedback')
        team_id = executor_interface._team_id

    def cancel_done_callback(self, executor_interface, msg):
        self.get_logger().info("Canceled Plan of platform " + executor_interface._platform_id + " in team " + self._team_id)

        if all(interface._executor_status == ExecutorState.STATE_IDLE for interface in self._executor_interfaces.values()):
            self.publish_plannerCommand(command=PlannerCommand.CANCEL_DONE, info_msg=ExecutionInfo(platform_id=self._team_id, ))
            self.change_executor_state(ExecutorState.STATE_IDLE)

    def _check_current_execution(self):
        if self._executor_state == ExecutorState.STATE_EXECUTING:
            flags = self.check_execution()
            if flags and len(flags) > 0:
                print(flags)
                if len(flags) == 1 and flags[0] == "FINISHED":
                    self.publish_plannerCommand(command=PlannerCommand.FINISHED, info_msg=ExecutionInfo(team_id=self._team_id, success=True))
                    self.change_executor_state(ExecutorState.STATE_IDLE)
                else:
                    self.send_pause()
                    self.publish_plannerCommand(command=PlannerCommand.UPDATEPLAN, info_msg=ExecutionInfo(team_id=self._team_id, status_flags=flags))

    def object_detection_callback(self, msg):
        """
        Object Detection Callback form AUSPEX-SENS
        """
        self._detected_objects_list.append(msg)

    def has_incomplete_goals(self):
        if not self._current_goals:
            return False
        for goal in self._current_goals:
            if goal.get('description', '').upper() == 'TOP_LEVEL_GOAL':
                if goal.get('status', '').upper() != 'COMPLETED':
                    return True
                else:
                    return False
        return False

    def check_constraints(self):
        violated_constraints = []
        if not self._current_constraints:
            return None
        if violated_constraints:
            return "Constraints violated: " + ", ".join(violated_constraints)
        return None

    def get_goal_by_id(self, goal_id):
        for goal in self._current_goals:
            if goal.get('goal_id') == goal_id:
                return goal
        return None

    def check_goals(self):
        flags = []
        if not self._current_goals:
            return flags

        """
        Check if any goals have been completed
        """
        for goal in self._current_goals:
            goal_type = goal.get('type', '').upper()
            goal_id = goal.get('goal_id', '')
            goal_status = goal.get('status', '').upper()
            if goal_status == 'COMPLETED':
                continue

            params = goal.get('parameters_json',{})

            if goal_type == 'FIND' and params.get('objects'):

                for detected_obj in self._detected_objects_list:
                    if detected_obj.detection_class in params['objects'] and detected_obj.confidence > 0.5:

                        self._kb_client.update(collection='object', new_value='10', field='priority', key='id', value=detected_obj.id)
                        self._kb_client.update(collection='goal', new_value='COMPLETED', field='status', key='goal_id', value=goal_id)
                        flag_msg = f"OBJECT_CONFIRMED"
                        flags.append(flag_msg)

                    elif detected_obj.detection_class in params['objects'] and detected_obj.confidence > 0.2 and detected_obj.confidence < 0.5:

                        flag_msg = f"POSSIBLE_OBJECT_DETECTED"
                        flags.append(flag_msg)
                        self._kb_client.update(collection='object', new_value='0.6', field='confidence', key='id', value=detected_obj.id)

                self._detected_objects_list = []

            elif goal_type == 'SEARCH':

                polygon_points = []
                polygon_points_list = []
                if params.get('search_area') and params.get('search_area').get('points'):

                    polygon_points = params.get('search_area').get('points')
                    polygon_points_list.append(polygon_points)

                elif params.get('locations'):
                    for location in params['locations']:
                        areas_db = self._kb_client.query(collection='area', key='', value='')
                        for area in areas_db:
                            if location in area['name']:
                                polygon_points = []
                                for point in area['points'][1:]:
                                    polygon_points.append({"latitude": point[0], "longitude": point[1], "altitude": point[2]})
                                polygon_points_list.append(polygon_points)

                if polygon_points_list:
                    platform_histories = self._kb_client.query(collection='history', key='team_id', value=self._team_id)

                    coverage = 0.0
                    for polygon in polygon_points_list:
                        if not polygon:
                            continue
                        coverage = compute_coverage(polygon, platform_histories, downsample=1, grid_size=10, uav_radius_m=5.0)

                    coverage = coverage/len(polygon_points_list)

                    if coverage > 0.6:
                        self._kb_client.update(collection='goal', new_value='COMPLETED', field='status', key='goal_id', value=goal_id)

            elif goal_type == 'LAND':

                platform_states = self._kb_client.query(collection='platform', key='team_id', value=self._team_id)
                if platform_states and all(ps.get('platform_status', '').upper() == 'LANDED' for ps in platform_states):
                    self._kb_client.update(collection='goal', new_value='COMPLETED', field='status', key='goal_id', value=goal_id)

            elif goal_type == 'AND':

                goal_children = goal.get('goal_condition').get('goal_ids')
                completed_children = True
                for child_goal_id in goal_children:
                    child_goal = self.get_goal_by_id(child_goal_id)
                    if child_goal.get('status', '').upper() != 'COMPLETED':
                        completed_children = False
                        break
                if completed_children:
                    self._kb_client.update(collection='goal', new_value='COMPLETED', field='status', key='goal_id', value=goal_id)

            elif goal_type == 'OR':

                goal_children = goal.get('goal_condition').get('goal_ids')
                for child_goal_id in goal_children:
                    child_goal = self.get_goal_by_id(child_goal_id)
                    if child_goal.get('status', '').upper() == 'COMPLETED':
                        self._kb_client.update(collection='goal', new_value='COMPLETED', field='status', key='goal_id', value=goal_id)
                        break
        return flags

    def check_action_completion(self):
        if not all(executor._finished_execution for executor in self._executor_interfaces.values()):
            return []

        flags = []
        plans = self._kb_client.query(collection='plan', key='team_id', value=self._team_id)
        if not plans:
            self.change_executor_state(ExecutorState.STATE_IDLE)
            return flags

        is_execution_finished = True

        for plan in plans:
            if plan.get('status') == 'ACTIVE' or plan.get('status') == 'INACTIVE':
                is_execution_finished = False
                break

        if is_execution_finished:
            if self.has_incomplete_goals():
                flags.append("INCOMPLETE_GOALS")
                return flags
            else:
                flags.append("FINISHED")

        self.change_executor_state(ExecutorState.STATE_IDLE)
        return flags

    def check_execution(self):
        interrupt_flags = []

        self._current_goals = self.update_current_goals()
        self._current_constraints = self.update_current_constraints()

        # global constraints
        resCons = self.check_constraints()
        if resCons:
            interrupt_flags.extend(resCons)

        resGoal = self.check_goals()
        if resGoal:
            interrupt_flags.extend(resGoal)

        resActions = self.check_action_completion()
        if resActions:
            interrupt_flags.extend(resActions)

        if len(interrupt_flags) > 0:
            return interrupt_flags

    def send_pause(self):
        if self._is_team_paused:
            return
        self._is_team_paused = True
        for executor in self._executor_interfaces.values():
            executor.send_command(ExecutorCommand.PAUSE)
        self.change_executor_state(ExecutorState.STATE_PAUSED)

    def send_resume(self):
        if not self._is_team_paused:
            return
        for executor in self._executor_interfaces.values():
            executor.send_command(ExecutorCommand.CONTINUE)
        self._is_team_paused = False
        self.change_executor_state(ExecutorState.STATE_EXECUTING)

    def cancelGoal(self):
        """"
        Cancels all current goals for all platforms in the team
        """
        if self._executor_state == ExecutorState.STATE_EXECUTING or self._executor_state == ExecutorState.STATE_PAUSED:
            for executor in self._executor_interfaces.values():
                executor.send_command(ExecutorCommand.CANCEL)

    def update_current_goals(self):
        goals = self._kb_client.query(collection='goal', key='team_id', value=self._team_id)
        if not goals:
            return []
        return goals

    def update_current_constraints(self):
        constraints = []#self._kb_client.query(collection='constraint', key='team_id', value=self._team_id)
        current_constraints = []
        for constraint_entry in constraints:
            constraint = constraint_entry['value']
            if not constraint:
                print("No constraint found in entry, skipping...")
                continue
            try:
                constraint = constraint.replace("'", '"')
                constraint = constraint.replace('"{', '{').replace('}"', '}')
                constraint = json.loads(constraint)
                current_constraints.append(constraint)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for constraint {constraint_entry}: {e}")
                continue
        if not current_constraints:
            return []
        return current_constraints

    def execute_plan(self):
        """
        Gets a plan in the up format and executes it
        """
        if self._is_team_paused:
            self._is_team_paused = False

        self.change_executor_state(ExecutorState.STATE_EXECUTING)

        for executor in self._executor_interfaces.values():
            executor.send_command(ExecutorCommand.EXECUTE)
            self.get_logger().info("Executing plan for platform: " + executor._platform_id)


    def change_executor_state(self, state):
        if state != self._executor_state:
            self._executor_state = state
            self.publish_plannerCommand(command=PlannerCommand.STATE_FEEDBACK, info_msg=ExecutionInfo(team_id=self._team_id))

    def publish_plannerCommand(self, command, info_msg):
        msg = PlannerCommand()
        msg.command = command
        msg.info = info_msg
        msg.executor_state.value = self._executor_state
        self._planner_command_publisher.publish(msg)

    def planner_callback(self, msg):
        if msg.command == ExecutorCommand.EXECUTE:
            if self._executor_state == ExecutorState.STATE_EXECUTING:
                self.get_logger().info('Executor already executing...')
                return
            if self._executor_state == ExecutorState.STATE_PAUSED:
                self.get_logger().info('Executor was paused. Resuming with highest prio plan...')
            self.execute_plan()

        elif msg.command == ExecutorCommand.PAUSE:
            self.send_pause()
        elif msg.command == ExecutorCommand.CONTINUE:
            if self._executor_state != ExecutorState.STATE_PAUSED:
                self.publish_plannerCommand(command=PlannerCommand.UPDATEPLAN, info_msg=ExecutionInfo(team_id=self._team_id, success=False))
                self.get_logger().info('Executor not paused, cannot continue...')
                return
            self.send_resume()
        elif msg.command == ExecutorCommand.CANCEL:
            self.cancelGoal()
