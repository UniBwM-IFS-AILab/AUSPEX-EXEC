#!/usr/bin/env python3
"""
AUSPEX Executor Main Module

This is the main entry point for the AUSPEX executor.
It creates and runs the appropriate executor based on configuration.

The executor can be launched with different types:
- standard: Sequential execution without temporal constraints
- temporal: TPN-based execution with temporal constraints and coordination
- auto: Automatically selects based on plan content (prefers temporal)

Usage:
    ros2 run auspex_executor executor <platform_id>
"""

import sys
import rclpy
from rclpy.executors import MultiThreadedExecutor
from enum import Enum, auto
from typing import Optional, Type, Dict
from auspex_executor.platform_executor.executors.base_executor import BaseExecutor
from auspex_executor.platform_executor.executors.standard_executor import StandardExecutor
from auspex_executor.platform_executor.executors.kirk_temporal_executor import KirkTemporalExecutor

from auspex_executor.utils.knowledge_client_ros import KnowledgeClientROS

class ExecutorType(Enum):
    """Enumeration of available executor types."""
    STANDARD = auto()
    KIRK = auto()

_EXECUTOR_REGISTRY: Dict[ExecutorType, Type[BaseExecutor]] = {
    ExecutorType.STANDARD: StandardExecutor,
    ExecutorType.KIRK: KirkTemporalExecutor,
}

def get_available_executor_types() -> list:
    return list(_EXECUTOR_REGISTRY.keys())

def create_executor(platform_id: str, executor_type: ExecutorType = ExecutorType.STANDARD, knowledge_client_ros: Optional[KnowledgeClientROS] = None,**kwargs) -> BaseExecutor:
    if executor_type not in _EXECUTOR_REGISTRY:
        raise ValueError(f"Unknown executor type: {executor_type}. Available: {list(_EXECUTOR_REGISTRY.keys())}")
    executor_class = _EXECUTOR_REGISTRY[executor_type]
    return executor_class(platform_id, knowledge_client_ros=knowledge_client_ros, **kwargs)

def main():
    """Main entry point for the executor."""
    print("Starting AUSPEX Executor...")       
    platform_id = sys.argv[1]
    
    executor_type = ExecutorType.KIRK # Default executor type

    # Initialize ROS2
    rclpy.init(args=None)
    main_mte = MultiThreadedExecutor()
    
    # Create KB client
    knowledge_client_ros = KnowledgeClientROS(platform_id=platform_id)
    
    try:
        executor = create_executor(
            platform_id=platform_id,
            executor_type=executor_type,
            knowledge_client_ros=knowledge_client_ros
        )
        if executor_type == ExecutorType.KIRK:
            executor.critical_action_indices = {"deliver_object": [1, 2, 3], "scan_3D_area": [1]}  # Example critical action indices

    except ValueError as e:
        print(f"Error creating executor: {e}")
        rclpy.shutdown()
        sys.exit(1)
    
    # Initialize the executor
    executor.init_executor()    
    
    print(f"Platform ID: {platform_id}")
    print(f"Executor type: {executor_type}")
    print(f"Executor created: {executor.EXECUTOR_TYPE}")
    
    
    # Add nodes to executor
    main_mte.add_node(executor)
    main_mte.add_node(knowledge_client_ros)
    
    try:
        main_mte.spin()
    except KeyboardInterrupt:
        executor.get_logger().info("Keyboard Interrupt (CTRL+C) detected.")
    finally:
        executor.destroy_node()
        main_mte.shutdown()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
