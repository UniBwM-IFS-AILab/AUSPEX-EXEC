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
    mode = "sim"

    for i in range(0,count):
        current_platform_id = platform_id
        if platform_id == "simulation":
            current_platform_id = platform_id + '_' + str(i)
        else:
            if count > 1:
                print("Error: Multiple instances requested but platform_id is not 'simulation'. Using single instance.")
                return []
            mode = "real"
        executors = Node(
            package=pkg_name,
            executable="kirk_server_handler",
            output='screen',
            shell=True,
            arguments=[current_platform_id, mode]
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