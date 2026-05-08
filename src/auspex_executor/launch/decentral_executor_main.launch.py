#!/usr/bin/env python3
from launch import LaunchDescription
from launch.substitutions import LaunchConfiguration
from launch.actions import DeclareLaunchArgument, OpaqueFunction, TimerAction
from launch_ros.actions import Node
import os

def launch_setup(context, *args, **kwargs):
    count = int(context.perform_substitution(LaunchConfiguration('count')))

    platform_id = os.environ.get("PLATFORM_NAME", "").strip().strip('"')
    if not platform_id:
        print("Error: PLATFORM_NAME environment variable is not set!")
        exit(1)

    pkg_name = "auspex_executor"
    launch_array =  []
    delay_seconds = 0.5 

    for i in range(0,count):
        current_platform_id = platform_id
        if platform_id == "simulation":
            current_platform_id = platform_id + '_' + str(i)
        executors = Node(
            package=pkg_name,
            executable="executor_main_node",
            output='screen',
            shell=True,
            arguments=[current_platform_id]
        )
        if i == 0:
            launch_array.append(executors)
        else:
            launch_array.append(
                TimerAction(
                    period=delay_seconds * i,
                    actions=[executors]
                )
            )
    return launch_array

def generate_launch_description():
    return LaunchDescription(
        [DeclareLaunchArgument('count', default_value='1')] + [OpaqueFunction(function=launch_setup)]
    )