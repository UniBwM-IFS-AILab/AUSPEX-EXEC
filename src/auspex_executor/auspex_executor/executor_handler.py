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
        try:
            platforms = ast.literal_eval(platforms_list[0])
        except (ValueError, SyntaxError, IndexError) as e:
            self.get_logger().error(f"Failed to parse platforms list: {e}")
            return
            
        if platforms == []:
            return

        # Check if all required fields are present and not None
        for platform in platforms:
            if platform.get('team_id') is None or platform.get('platform_id') is None:
                self.get_logger().warn(f"Platform missing required fields: {platform}")
                return

        # Use blocking lock to ensure thread-safe operations
        with self._executor_creation_lock:
            try:
                current_team_ids = [platform.get('team_id') for platform in platforms if platform.get('team_id') is not None]
                current_platform_ids = [platform.get('platform_id') for platform in platforms if platform.get('platform_id') is not None]

                for platform in platforms:
                    team_id = platform.get('team_id')
                    platform_id = platform.get('platform_id')
                    
                    # Skip if either ID is None
                    if team_id is None or platform_id is None:
                        continue
                        
                    if team_id not in self._team_nodes_dict:
                        self.run_team_executor(team_id)

                    if platform_id not in self._platform_nodes_dict:
                        self.run_platform_executor(platform_id)
                        # Add a small delay to ensure executor is fully initialized
                        import time
                        time.sleep(0.1)
                        if team_id in self._team_nodes_dict and platform_id in self._platform_nodes_dict:
                            self._team_nodes_dict[team_id].register_executor(self._platform_nodes_dict[platform_id])

                # Create a list copy of keys to avoid "dictionary changed size during iteration" error
                for key in list(self._platform_nodes_dict.keys()):
                    if key not in current_platform_ids:
                        platform_node = self._platform_nodes_dict.get(key)
                        if platform_node:
                            team_id = getattr(platform_node, '_team_id', None)
                            if team_id and team_id in self._team_nodes_dict:
                                self._team_nodes_dict[team_id].unregister_executor(platform_node)
                            self.shutdown_platform_executor(key)

                # Create a list copy of keys to avoid "dictionary changed size during iteration" error
                for key in list(self._team_nodes_dict.keys()):
                    if key not in current_team_ids:
                        self.shutdown_team_executor(key)
            except Exception as e:
                self.get_logger().error(f"Error in update_executors: {e}")

    def shutdown_platform_executor(self, platform_id):
        """
        Shutdown the executor.
        """
        try:
            # Use pop to safely remove and get the node, avoiding race conditions
            platform_node = self._platform_nodes_dict.pop(platform_id, None)
            if platform_node is None:
                self.get_logger().warn(f"Platform {platform_id} not found in dictionary.")
                return
            
            # Clean up all associated nodes
            try:
                if hasattr(platform_node, '_sequence_client') and platform_node._sequence_client:
                    platform_node._sequence_client.node.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying sequence client for platform {platform_id}: {e}")
            
            try:
                if hasattr(platform_node, '_kb_client') and platform_node._kb_client:
                    platform_node._kb_client.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying kb client for platform {platform_id}: {e}")
            
            try:
                if hasattr(platform_node, 'sub_node') and platform_node.sub_node:
                    platform_node.sub_node.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying sub_node for platform {platform_id}: {e}")
            
            try:
                platform_node.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying platform node for platform {platform_id}: {e}")
            
            self.get_logger().info(f"Stopped executor for platform {platform_id}.")
        except Exception as e:
            self.get_logger().error(f"Error shutting down platform executor {platform_id}: {e}")
            # Ensure the entry is removed even if cleanup fails
            self._platform_nodes_dict.pop(platform_id, None)

    def shutdown_team_executor(self, team_id):
        """
        Shutdown the executor.
        """
        try:
            # Use pop to safely remove and get the node, avoiding race conditions
            team_node = self._team_nodes_dict.pop(team_id, None)
            if team_node is None:
                self.get_logger().warn(f"Team {team_id} not found in dictionary.")
                return
            
            # Clean up all associated nodes
            try:
                if hasattr(team_node, '_kb_client') and team_node._kb_client:
                    team_node._kb_client.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying kb client for team {team_id}: {e}")
            
            try:
                if hasattr(team_node, 'sub_node') and team_node.sub_node:
                    team_node.sub_node.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying sub_node for team {team_id}: {e}")
            
            try:
                team_node.destroy_node()
            except Exception as e:
                self.get_logger().error(f"Error destroying team node for team {team_id}: {e}")
            
            self.get_logger().info(f"Stopped executor for team {team_id}.")
        except Exception as e:
            self.get_logger().error(f"Error shutting down team executor {team_id}: {e}")
            # Ensure the entry is removed even if cleanup fails
            self._team_nodes_dict.pop(team_id, None)

    def run_platform_executor(self, platform_id):
        """
        Create and run a new executor in a separate thread.
        """
        try:
            if platform_id is None:
                self.get_logger().error("Cannot create platform executor: platform_id is None")
                return
                
            platform_node = AuspexExecutor(platform_id)
            platform_node.init_executor()

            self._main_executor.add_node(platform_node)
            self._main_executor.add_node(platform_node.sub_node)
            self._platform_nodes_dict[platform_id] = platform_node

            self.get_logger().info(f"Started executor for platform {platform_id} in a separate thread.")
        except Exception as e:
            self.get_logger().error(f"Failed to create platform executor for {platform_id}: {e}")

    def run_team_executor(self, team_id):
        """
        Create and run a new executor in a separate thread.
        """
        try:
            if team_id is None:
                self.get_logger().error("Cannot create team executor: team_id is None")
                return
                
            team_node = AuspexExecutorMonitor(team_id)
            team_node.init_executor()

            self._main_executor.add_node(team_node)
            self._main_executor.add_node(team_node.sub_node)
            self._team_nodes_dict[team_id] = team_node

            self.get_logger().info(f"Started executor monitoring for team {team_id} in a separate thread.")
        except Exception as e:
            self.get_logger().error(f"Failed to create team executor for {team_id}: {e}")

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
