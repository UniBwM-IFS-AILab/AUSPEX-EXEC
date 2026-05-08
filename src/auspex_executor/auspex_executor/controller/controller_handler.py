#!/usr/bin/env python3
import rclpy
import ast
import threading
import time
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from .controller import AuspexTeamController
from auspex_planning.planner.utils.knowledge_client_utils.knowledge_client_interface import KnowledgeClientInterface

class ControllerHandler(Node):
    """
    Ros2 node used to execute a plan
    """
    def __init__(self, mte=None):
        """
        Constructor method
        """
        super().__init__("controller_handler")
        self._team_nodes_dict = {}
        self._platform_dict = {}
        self._knowledge_interface = KnowledgeClientInterface()
        self._controller_creation_lock = threading.Lock()
        self._main_executor = mte

        self.create_timer(1, self._get_platform_data)

    def update_controllers(self, platform_ids_list): 
        if platform_ids_list == []:
            return

        # Check if all required fields are present and not None
        for platform_id in platform_ids_list:
            if self._knowledge_interface.getTeamIDOfPlatform(platform_id) is None:
                self.get_logger().warn(f"Platform missing required field: team_id: {platform_id}")
                return

        # Use blocking lock to ensure thread-safe operations
        with self._controller_creation_lock:
            try:
                current_team_ids = [self._knowledge_interface.getTeamIDOfPlatform(platform_id) for platform_id in platform_ids_list if self._knowledge_interface.getTeamIDOfPlatform(platform_id) is not None]
                current_platform_ids = [platform_id for platform_id in platform_ids_list if platform_id is not None]

                for platform_id in platform_ids_list:
                    team_id = self._knowledge_interface.getTeamIDOfPlatform(platform_id)
                    
                    # Skip if either ID is None
                    if team_id is None or platform_id is None:
                        continue
                        
                    if team_id not in self._team_nodes_dict:
                        if team_id:
                            self.run_team_controller(team_id)

                    if platform_id not in self._platform_dict:

                        self._platform_dict[platform_id] = team_id

                        time.sleep(0.1)
                        if team_id in self._team_nodes_dict and platform_id in self._platform_dict:
                            self._team_nodes_dict[team_id].register_executor_interface(platform_id)

                for key in list(self._platform_dict.keys()):
                    if key not in current_platform_ids:
                        team_id = self._platform_dict.get(key)
                        if team_id and team_id in self._team_nodes_dict:
                            self._team_nodes_dict[team_id].unregister_executor_interface(key)
                        self._platform_dict.pop(key, None)

                for key in list(self._team_nodes_dict.keys()):
                    if key not in current_team_ids:
                        self.shutdown_team_controller(key)
            except Exception as e:
                self.get_logger().error(f"Error in update_controllers: {e}")


    def shutdown_team_controller(self, team_id):
        """
        Shutdown the controller.
        """
        try:
            # Use pop to safely remove and get the node, avoiding race conditions
            team_node = self._team_nodes_dict.pop(team_id, None)
            if team_node is None:
                self.get_logger().warn(f"Team {team_id} not found in dictionary.")
                return
            
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

    def run_team_controller(self, team_id):
        """
        Create and run a new executor in a separate thread.
        """
        try:
            if team_id is None:
                self.get_logger().error("Cannot create team controller: team_id is None")
                return
                
            team_node = AuspexTeamController(team_id)
            team_node.init_controller()

            self._main_executor.add_node(team_node)
            self._main_executor.add_node(team_node.sub_node)
            self._team_nodes_dict[team_id] = team_node

            self.get_logger().info(f"Started controller for team {team_id} in a separate thread.")
        except Exception as e:
            self.get_logger().error(f"Failed to create team controller for {team_id}: {e}")

    def kill_all(self):
        for team_id in list(self._team_nodes_dict.keys()):
            self.shutdown_team_controller(team_id)

    def _get_platform_data(self):
        platform_names_list = self._knowledge_interface.getPlatformIDs() 
        self.update_controllers(platform_names_list)

def main():
    print("Starting AUSPEX Controller Handler...")
    rclpy.init(args=None)
    main_mte = MultiThreadedExecutor()
    controller_handler = ControllerHandler(mte=main_mte)
    main_mte.add_node(controller_handler)
    try:
        main_mte.spin()
    except KeyboardInterrupt:
        controller_handler.get_logger().info("Keyboard Interrupt (CTRL+C) detected.")
    finally:
        controller_handler.kill_all()
        controller_handler.destroy_node()
        main_mte.shutdown()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()
