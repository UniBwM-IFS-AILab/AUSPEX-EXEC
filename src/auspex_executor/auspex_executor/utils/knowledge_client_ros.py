import sys
import rclpy
import json
from rclpy.node import Node
from rclpy.callback_groups import ReentrantCallbackGroup
from auspex_msgs.msg import (
    ExecutorCommand, 
    PlannerCommand, 
    ExecutorState, 
    ExecutionInfo, 
    Task, 
    Action, 
    ActionStatus, 
    PlanStatus, 
    PlatformCommand,
    TaskEvent,
)

from auspex_msgs.srv import (
    UpsertSubframe,
    SetSlot,
    ReadSlot,
    GetAllInstances,
    GetInstance,
    GetInstanceIds,
    FindInstances,
    FindInstanceIds,
    DeleteInstance,
    DeleteAllInstances,
    DeleteSubframe,
    DeleteSlot,
    DeleteInstances,
)

class KnowledgeClientROS(Node):
    def __init__(self, platform_id: str):
        super().__init__(platform_id + "_knowledge_client_ros")

        self._upsert_subframe_client = self.create_client(UpsertSubframe, 'upsert_subframe')

        self._set_slot_client = self.create_client(SetSlot, 'set_slot')

        self._read_slot_client = self.create_client(ReadSlot, 'read_slot')

        self._get_all_instances_client = self.create_client(GetAllInstances, 'get_all_instances')
        self._get_instance_client = self.create_client(GetInstance, 'get_instance')
        self._get_instance_ids_client = self.create_client(GetInstanceIds, 'get_instance_ids')

        self._find_instances_client = self.create_client(FindInstances, 'find_instances')
        self._find_instance_ids_client = self.create_client(FindInstanceIds, 'find_instance_ids')
        
        self._delete_instance_client = self.create_client(DeleteInstance, 'delete_instance')
        self._delete_all_instances_client = self.create_client(DeleteAllInstances, 'delete_all_instances')
        self._delete_subframe_client = self.create_client(DeleteSubframe, 'delete_subframe')
        self._delete_slot_client = self.create_client(DeleteSlot, 'delete_slot')
        self._delete_instances_client = self.create_client(DeleteInstances, 'delete_instances')

    def insertArea(self, area_name: str, area_json: str):
        msg = UpsertSubframe.Request()
        msg.frame = 'area'
        msg.instance_id = str(area_name)
        msg.subframe = 'data'
        msg.item = area_json
        future = self._upsert_subframe_client.call_async(msg)

    def updateActionStatus(self, plan_id: str, action_id: int, status: str):
        msg = SetSlot.Request()
        msg.frame = 'plan'
        msg.instance_id = str(plan_id)
        msg.subframe = 'data'
        msg.slot = 'actions'
        msg.path = f"$[?(@.id=={action_id})].status"
        msg.value = json.dumps(status)
        future = self._set_slot_client.call_async(msg)

    def updateTaskStatus(self, plan_id: str, task_id:str, status: str):
        msg = SetSlot.Request()
        msg.frame = 'plan'
        msg.instance_id = str(plan_id)
        msg.subframe = 'data'
        msg.slot = 'tasks'
        msg.path = f"$[?(@.id=={task_id})].status"
        msg.value = json.dumps(status)
        future = self._set_slot_client.call_async(msg)

    def updatePlanStatus(self, plan_id:str, status: str):
        msg = SetSlot.Request()
        msg.frame = 'plan'
        msg.instance_id = str(plan_id)
        msg.subframe = 'data'
        msg.slot = 'status'
        msg.value = json.dumps(status)
        future = self._set_slot_client.call_async(msg)

    def getPlansOfPlatformID(self, platform_id: str)-> list[dict]:
        msg = FindInstances.Request()
        msg.frame = 'plan'
        msg.subframe = 'data'
        msg.slot = 'platform_id'
        msg.value = json.dumps(platform_id)
        return self._find_instances_client.call_async(msg)
        