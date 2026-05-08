#!/usr/bin/env python3
"""
Task Event Communication Module

This module provides a unified communication class for task event messaging
between executors. It abstracts the ROS2 publisher/subscriber logic for
task events and provides a consistent interface for both TemporalExecutor
and KirkTemporalExecutor.

The TaskEventCommunication class handles:
- Publishing task events (started, completed, failed, cancelled)
- Subscribing to task events from other platforms
- Message queue management for asynchronous processing
- Integration with Kirk dispatcher's mailbox pattern
"""

import threading
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Any
from queue import Queue, Empty
from enum import Enum

from rclpy.node import Node
from rclpy.callback_groups import CallbackGroup
from std_msgs.msg import Header

from auspex_msgs.msg import TaskEvent


class TaskEventType(Enum):
    """Unified task event types matching ROS message constants."""
    STARTED = TaskEvent.EVENT_STARTED
    COMPLETED = TaskEvent.EVENT_COMPLETED
    FAILED = TaskEvent.EVENT_FAILED
    CANCELLED = TaskEvent.EVENT_CANCELLED


@dataclass
class TaskEventData:
    """
    Unified data structure for task events.
    
    This structure is used internally by the communication class and
    can be converted to/from ROS TaskEvent messages.
    
    Attributes:
        event_type: Type of the event (started, completed, failed, cancelled)
        event_id: The TPN event ID (e.g., end event for completion)
        platform_id: Platform that generated the event
        team_id: Team this platform belongs to
        plan_id: Plan ID this task belongs to
        metadata_json: Additional metadata (can include tells as JSON)
        timestamp: Event timestamp (seconds since epoch)
    """
    event_type: TaskEventType
    event_id: str = ""  # The TPN event_id that completed (the "tell")
    platform_id: str = ""
    team_id: str = ""
    plan_id: int = -1
    metadata_json: str = ""
    timestamp: float = 0.0
    
    @property
    def tells(self) -> List[str]:
        """
        Get the tells (event IDs) from this event.
        
        The event_id itself is a tell - it signals that this event has
        completed, which can satisfy asks on other platforms.
        Additional tells can be stored in metadata_json.
        
        Returns:
            List of event IDs that this event "tells" (has completed)
        """
        tells = []
        if self.event_id:
            tells.append(self.event_id)
        
        return tells
    
    def to_ros_msg(self, header: Optional[Header] = None) -> TaskEvent:
        """
        Convert to ROS TaskEvent message.
        
        Args:
            header: Optional header with timestamp
            
        Returns:
            ROS TaskEvent message
        """
        msg = TaskEvent()
        msg.header = header or Header()
        msg.event_type = self.event_type.value
        msg.event_id = self.event_id
        msg.platform_id = self.platform_id
        msg.team_id = self.team_id
        msg.plan_id = self.plan_id
        msg.metadata_json = self.metadata_json
        return msg
    
    @classmethod
    def from_ros_msg(cls, msg: TaskEvent) -> 'TaskEventData':
        """
        Create TaskEventData from ROS TaskEvent message.
        
        Args:
            msg: ROS TaskEvent message
            
        Returns:
            TaskEventData instance
        """
        # Map int to enum
        try:
            event_type = TaskEventType(msg.event_type)
        except ValueError:
            event_type = TaskEventType.STARTED
        
        timestamp = 0.0
        if hasattr(msg, 'header') and msg.header:
            timestamp = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
        
        return cls(
            event_type=event_type,
            event_id=msg.event_id,
            platform_id=msg.platform_id,
            team_id=msg.team_id,
            plan_id=msg.plan_id,
            metadata_json=msg.metadata_json,
            timestamp=timestamp
        )


class TaskEventHandler(ABC):
    """
    Abstract interface for handling received task events.
    
    Implement this interface to process task events in executors.
    """
    
    @abstractmethod
    def on_task_event_received(self, event: TaskEventData):
        """
        Called when a task event is received from another platform.
        
        Args:
            event: The received task event data
        """
        pass


class TaskEventCommunication:
    """
    Unified communication class for task events.
    
    This class provides a consistent interface for publishing and
    subscribing to task events, abstracting the ROS2 communication
    details. It can be used by both TemporalExecutor and KirkTemporalExecutor.
    
    Features:
    - Automatic filtering of own events
    - Team-based filtering
    - Optional mailbox queue for Kirk dispatcher integration
    - Callback-based event handling
    
    Attributes:
        node: ROS2 node for communication
        platform_id: This platform's ID
        team_id: Current team ID for filtering
        mailbox: Optional queue for Kirk dispatcher integration
    """
    
    TASK_EVENT_TOPIC = '/auspex/task_events'
    
    def __init__(
        self,
        node: Node,
        platform_id: str,
        team_id: str = "",
        callback_group: Optional[CallbackGroup] = None,
        verbose: bool = False,
        delay_enabled: bool = False,
        delay_min: float = 0.0,
        delay_max: float = 0.0,
    ):
        """
        Initialize the task event communication.
        
        Args:
            node: ROS2 node for creating publishers/subscribers
            platform_id: This platform's identifier
            team_id: Team ID for filtering events (empty = accept all)
            callback_group: ROS2 callback group for subscriptions
            verbose: Enable verbose logging
            delay_enabled: Enable random delay for testing inter-agent communication
            delay_min: Minimum delay in seconds (lower bound)
            delay_max: Maximum delay in seconds (upper bound)
        """
        self._node = node
        self._platform_id = platform_id
        self._team_id = team_id
        self._callback_group = callback_group
        self._mailbox = Queue()
        self._verbose = verbose
        
        # Communication delay configuration (for testing)
        self._enable_delay = delay_enabled
        self._delay_min = delay_min
        self._delay_max = delay_max
        
        # Event handlers
        self._handlers: List[TaskEventHandler] = []
        self._callbacks: List[Callable[[TaskEventData], None]] = []
        
        # State
        self._active = False
        self._lock = threading.Lock()
        
        # ROS2 interfaces (created when activated)
        self._publisher = None
        self._subscriber = None
    
    def activate(self):
        """
        Activate the communication by creating ROS2 interfaces.
        
        Call this method after the ROS2 node is fully initialized.
        """
        if self._active:
            return
        
        # Create publisher
        self._publisher = self._node.create_publisher(
            TaskEvent,
            self.TASK_EVENT_TOPIC,
            10,
            callback_group=self._callback_group
        )
        
        # Create subscriber
        self._subscriber = self._node.create_subscription(
            TaskEvent,
            self.TASK_EVENT_TOPIC,
            self._on_task_event_received,
            10,
            callback_group=self._callback_group
        )
        
        self._active = True
        self._log(f"TaskEventCommunication activated for {self._platform_id}")
    
    def deactivate(self):
        """
        Deactivate the communication.
        
        Note: In ROS2, we typically don't destroy publishers/subscribers
        during runtime, but this can be used to mark the communication
        as inactive.
        """
        self._active = False
        self._log(f"TaskEventCommunication deactivated for {self._platform_id}")
    
    def set_team_id(self, team_id: str):
        """
        Update the team ID for filtering events.
        
        Args:
            team_id: New team ID
        """
        self._team_id = team_id
    
    def set_delay_config(
        self,
        enabled: bool,
        delay_min: float = 0.0,
        delay_max: float = 0.0
    ):
        """
        Configure communication delay for testing.
        
        Args:
            enabled: Enable/disable delay
            delay_min: Minimum delay in seconds
            delay_max: Maximum delay in seconds
        """
        self._enable_delay = enabled
        self._delay_min = delay_min
        self._delay_max = delay_max
        
        if enabled:
            self._log(f"Communication delay enabled: {delay_min:.3f}s - {delay_max:.3f}s")
        else:
            self._log("Communication delay disabled")
    
    def get_delay_config(self) -> tuple:
        """
        Get current delay configuration.
        
        Returns:
            Tuple of (enabled, min_delay, max_delay)
        """
        return (self._enable_delay, self._delay_min, self._delay_max)
    
    def add_handler(self, handler: TaskEventHandler):
        """
        Add an event handler for received events.
        
        Args:
            handler: TaskEventHandler implementation
        """
        with self._lock:
            if handler not in self._handlers:
                self._handlers.append(handler)
    
    def remove_handler(self, handler: TaskEventHandler):
        """
        Remove an event handler.
        
        Args:
            handler: Handler to remove
        """
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
    
    def add_callback(self, callback: Callable[[TaskEventData], None]):
        """
        Add a callback function for received events.
        
        Args:
            callback: Callback function taking TaskEventData
        """
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[TaskEventData], None]):
        """
        Remove a callback function.
        
        Args:
            callback: Callback to remove
        """
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)
    
    def publish_event(self, event: TaskEventData):
        """
        Publish a task event.
        
        Args:
            event: Task event data to publish
        """
        if not self._active or self._publisher is None:
            self._log("Warning: Cannot publish event - communication not active")
            return
        
        # Create header with current timestamp
        header = Header()
        header.stamp = self._node.get_clock().now().to_msg()
        
        # Ensure platform_id is set
        if not event.platform_id:
            event.platform_id = self._platform_id
        
        msg = event.to_ros_msg(header)
        self._publisher.publish(msg)
        
        self._log(f"Published event: {event.event_type.name} (event_id: {event.event_id})")
    
    def publish_task_started(
        self,
        event_id: str,
        plan_id: int = -1,
        metadata_json: str = ""
    ):
        """
        Publish a task started event.
        
        Args:
            event_id: The TPN event ID that started
            plan_id: Plan ID
            metadata_json: Additional metadata
        """
        event = TaskEventData(
            event_type=TaskEventType.STARTED,
            event_id=event_id,
            platform_id=self._platform_id,
            team_id=self._team_id,
            plan_id=plan_id,
            metadata_json=metadata_json
        )
        self.publish_event(event)
    
    def publish_task_completed(
        self,
        event_id: str,
        plan_id: int = -1,
        metadata_json: str = ""
    ):
        """
        Publish a task completed event.
        
        The event_id serves as the "tell" - other platforms waiting on this
        event (with an "ask") will be notified.
        
        Args:
            event_id: The TPN event ID that completed (the "tell")
            plan_id: Plan ID
            metadata_json: Additional metadata
        """
        event = TaskEventData(
            event_type=TaskEventType.COMPLETED,
            event_id=event_id,
            platform_id=self._platform_id,
            team_id=self._team_id,
            plan_id=plan_id,
            metadata_json=metadata_json
        )
        self.publish_event(event)
    
    def publish_task_failed(
        self,
        event_id: str,
        plan_id: int = -1,
        metadata_json: str = ""
    ):
        """
        Publish a task failed event.
        
        Args:
            event_id: The TPN event ID that failed
            plan_id: Plan ID
            metadata_json: Additional metadata (can include error info)
        """
        event = TaskEventData(
            event_type=TaskEventType.FAILED,
            event_id=event_id,
            platform_id=self._platform_id,
            team_id=self._team_id,
            plan_id=plan_id,
            metadata_json=metadata_json
        )
        self.publish_event(event)
    
    def publish_task_cancelled(
        self,
        event_id: str,
        plan_id: int = -1,
        metadata_json: str = ""
    ):
        """
        Publish a task cancelled event.
        
        Args:
            event_id: The TPN event ID that was cancelled
            plan_id: Plan ID
            metadata_json: Additional metadata
        """
        event = TaskEventData(
            event_type=TaskEventType.CANCELLED,
            event_id=event_id,
            platform_id=self._platform_id,
            team_id=self._team_id,
            plan_id=plan_id,
            metadata_json=metadata_json
        )
        self.publish_event(event)
    
    def _on_task_event_received(self, msg: TaskEvent):
        """
        Internal callback for received ROS messages.
        
        Args:
            msg: Received TaskEvent message
        """
        # Ignore our own messages
        if msg.platform_id == self._platform_id:
            return
        
        # Filter by team if team_id is set
        if self._team_id and msg.team_id != self._team_id:
            return
        
        # Convert to internal format
        event = TaskEventData.from_ros_msg(msg)

        # Introduce random delay for testing purposes (if enabled)
        if self._enable_delay and self._delay_max > 0:
            # Generate random delay between min and max bounds
            delay = random.uniform(self._delay_min, self._delay_max)
            if delay > 0:
                self._log(f"Applying communication delay of {delay:.3f}s for event from {event.platform_id}")
                time.sleep(delay)
        
        self._log(f"Received event from {event.platform_id}: {event.event_type.name} "
                  f"(event_id: {event.event_id})")
        
        # Put in mailbox if configured (for Kirk dispatcher integration)
        if self._mailbox is not None:
            self._mailbox.put(event)
        
        # Notify handlers
        with self._lock:
            handlers = list(self._handlers)
            callbacks = list(self._callbacks)
        
        for handler in handlers:
            try:
                handler.on_task_event_received(event)
            except Exception as e:
                self._log(f"Error in event handler: {e}")
        
        for callback in callbacks:
            try:
                callback(event)
            except Exception as e:
                self._log(f"Error in event callback: {e}")
    
    def _log(self, message: str):
        """Log a message if verbose mode is enabled."""
        if self._verbose:
            self._node.get_logger().info(f"[TaskEventComm] {message}")
    
    def get_pending_events(self, timeout: float = 0.0) -> List[TaskEventData]:
        """
        Get pending events from the mailbox queue.
        
        This method is useful for Kirk dispatcher integration where
        events need to be processed in the dispatch loop.
        
        Args:
            timeout: Timeout in seconds (0 = non-blocking)
            
        Returns:
            List of pending task event data
        """
        if self._mailbox is None:
            return []
        
        events = []
        try:
            while True:
                if timeout > 0:
                    event = self._mailbox.get(timeout=timeout)
                    timeout = 0  # Only wait on first item
                else:
                    event = self._mailbox.get_nowait()
                events.append(event)
        except Empty:
            pass
        
        return events
    
    def has_pending_events(self) -> bool:
        """
        Check if there are pending events in the mailbox.
        
        Returns:
            True if there are pending events
        """
        if self._mailbox is None:
            return False
        return not self._mailbox.empty()