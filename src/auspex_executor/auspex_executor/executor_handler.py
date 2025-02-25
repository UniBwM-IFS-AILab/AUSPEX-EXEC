#!/usr/bin/env python3
import rclpy
import time

import threading
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor

from .executor import AuspexExecutor
from auspex_msgs.srv import QueryKnowledge

class ExecutorHandler(Node):
    """
    Ros2 node used to execute a plan
    """
    def __init__(self):
        """
        Constructor method
        """
        super().__init__("platform_exe_handler")

        self._executors_dict = {}
        self._threads_dict = {}
        self._query_client = self.create_client(QueryKnowledge, '/query_knowledge')

    def update_executors(self): 
        platforms_list = self._get_platforms()

        for platform_id in platforms_list:      
            if platform_id not in self._executors_dict:
                self.run_executor(platform_id) 

        for key in self._executors_dict.keys():
            if key not in platforms_list:
                self.shutdown_executor(key)

    def shutdown_executor(self, platform_id):
        """
        Shutdown the executor.
        """
        self._executors_dict[platform_id].shutdown()
        self._threads_dict[platform_id].join()

        # Clean up
        del self._executors_dict[platform_id]
        del self._threads_dict[platform_id]

        self.get_logger().info(f"Stopped executor for platform {platform_id}.")

    def run_executor(self, platform_id):
        """
        Create and run a new executor in a separate thread.
        """
        new_executor = MultiThreadedExecutor()
        platform_node = AuspexExecutor(platform_id)
        new_executor.add_node(platform_node)

        # Start the executor in a separate thread
        executor_thread = threading.Thread(target=self._spin_executor, args=(new_executor,))
        executor_thread.start()

        # Store the executor and thread
        self._executors_dict[platform_id] = new_executor
        self._threads_dict[platform_id] = executor_thread

        self.get_logger().info(f"Started executor for platform {platform_id} in a separate thread.")          

    def _spin_executor(self, executor):
        """
        Spin the executor in a separate thread.
        """
        try:
            executor.spin()
        except rclpy.shutdown:
            self.get_logger().info("Executor shutdown.")
        except Exception as e:
            self.get_logger().error(f"Error in executor spin: {e}")

    def kill_all(self):
        for platform_id in self._executors_dict.keys():
            self.shutdown_executor(platform_id)

    def _get_platforms(self):
        while not self._query_client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('Request platform list service not available, waiting again...')

        query_request = QueryKnowledge.Request()
        query_request.collection = 'platform'
        query_request.path = '$[*].platform_id'
        self.future = self._query_client.call_async(query_request)
        rclpy.spin_until_future_complete(self, self.future)
        return self.future.result().answer

def main():
    print("Initializing AUSPEX Executor...")
    rclpy.init(args=None)
    executor_handler = ExecutorHandler()

    while rclpy.ok():
        executor_handler.update_executors()
        time.sleep(1)
        rclpy.spin_once(executor_handler)

    executor_handler.kill_all()
    executor_handler.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
