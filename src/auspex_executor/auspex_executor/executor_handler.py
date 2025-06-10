#!/usr/bin/env python3
import rclpy
import ast
import threading
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

from .executor import AuspexExecutor
from .executor_monitor import AuspexExecutorMonitor
from auspex_msgs.srv import QueryKnowledge

class ExecutorHandler(Node):
    """
    Ros2 node used to execute a plan
    """
    def __init__(self, mte=None):
        """
        Constructor method
        """
        super().__init__("platform_exe_handler")
        self._team_nodes_dict = {}
        self._platform_nodes_dict = {}
        self._query_client = self.create_client(QueryKnowledge, '/query_knowledge')
        self._executor_creation_lock = threading.Lock()
        self._main_executor = mte

        self.create_timer(1, self._get_platform_data)

    def update_executors(self, platforms_list):
        platforms = ast.literal_eval(platforms_list[0])
        if platforms == []:
            return

        if platforms[0].get('team_id') == None:
            return

        if self._executor_creation_lock.acquire(blocking=False):
            current_team_ids = [platform.get('team_id') for platform in platforms]
            current_platform_ids = [platform.get('platform_id') for platform in platforms]

            for platform in platforms:
                if platform.get('team_id') not in self._team_nodes_dict:
                    self.run_team_executor(platform.get('team_id'))

                if platform.get('platform_id') not in self._platform_nodes_dict:
                    self.run_platform_executor(platform.get('platform_id'))
                    if platform.get('team_id') is not None:
                        self._team_nodes_dict[platform.get('team_id')].register_executor(self._platform_nodes_dict[platform.get('platform_id')])

            for key in self._platform_nodes_dict.keys():
                if key not in current_platform_ids:
                    team_id = self._platform_nodes_dict[key]._team_id
                    self._team_nodes_dict[team_id].unregister_executor(self._platform_nodes_dict[key])
                    self.shutdown_platform_executor(key)

            for key in self._team_nodes_dict.keys():
                if key not in current_team_ids:
                    self.shutdown_team_executor(key)

            self._executor_creation_lock.release()

    def shutdown_platform_executor(self, platform_id):
        """
        Shutdown the executor.
        """
        self._platform_nodes_dict[platform_id]._sequence_client.node.destroy_node()
        self._platform_nodes_dict[platform_id]._kb_client.destroy_node()
        self._platform_nodes_dict[platform_id].sub_node.destroy_node()
        self._platform_nodes_dict[platform_id].destroy_node()
        del self._platform_nodes_dict[platform_id]

        self.get_logger().info(f"Stopped executor for platform {platform_id}.")

    def shutdown_team_executor(self, team_id):
        """
        Shutdown the executor.
        """
        self._team_nodes_dict[team_id]._kb_client.destroy_node()
        self._team_nodes_dict[team_id].sub_node.destroy_node()
        self._team_nodes_dict[team_id].destroy_node()
        del self._team_nodes_dict[team_id]

        self.get_logger().info(f"Stopped executor for team {team_id}.")

    def run_platform_executor(self, platform_id):
        """
        Create and run a new executor in a separate thread.
        """
        platform_node = AuspexExecutor(platform_id)
        platform_node.init_executor()

        self._main_executor.add_node(platform_node)
        self._main_executor.add_node(platform_node.sub_node)
        self._platform_nodes_dict[platform_id] = platform_node

        self.get_logger().info(f"Started executor for platform {platform_id} in a separate thread.")

    def run_team_executor(self, team_id):
        """
        Create and run a new executor in a separate thread.
        """
        team_node = AuspexExecutorMonitor(team_id)
        team_node.init_executor()

        self._main_executor.add_node(team_node)
        self._main_executor.add_node(team_node.sub_node)
        self._team_nodes_dict[team_id] = team_node

        self.get_logger().info(f"Started executor monitoring for team {team_id} in a separate thread.")

    def _spin_executor(self, mte):
        """
        Spin the executor in a separate thread.
        """
        try:
            mte.spin()
        except rclpy.shutdown:
            self.get_logger().info("Executor shutdown.")
        except Exception as e:
            self.get_logger().error(f"Error in executor spin: {e}")

    def kill_all(self):
        for platform_id in list(self._platform_nodes_dict.keys()):
            self.shutdown_platform_executor(platform_id)

        for team_id in list(self._team_nodes_dict.keys()):
            self.shutdown_team_executor(team_id)

    def _get_platform_data(self):
        while not self._query_client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('Request platform list service not available, waiting again...')
            return
        query_request = QueryKnowledge.Request()
        query_request.collection = 'platform'
        query_request.path = '$'
        self.future = self._query_client.call_async(query_request)
        self.future.add_done_callback(self._request_platform_done)

    def _request_platform_done(self, future):
        try:
            result = future.result()
            if result is not None and result.answer is not None:
                platforms = result.answer
                self.update_executors(platforms)
            else:
                self.get_logger().warn("Received empty platform list from service.")
        except Exception as e:
            self.get_logger().error(f"Service call failed: {e}")



def main():
    print("Starting AUSPEX Executor Handler...")
    rclpy.init(args=None)
    main_mte = MultiThreadedExecutor()
    executor_handler = ExecutorHandler(mte=main_mte)
    main_mte.add_node(executor_handler)
    try:
        main_mte.spin()
    except KeyboardInterrupt:
        executor_handler.get_logger().info("Keyboard Interrupt (CTRL+C) detected.")
    finally:
        executor_handler.kill_all()
        executor_handler.destroy_node()
        main_mte.shutdown()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()
