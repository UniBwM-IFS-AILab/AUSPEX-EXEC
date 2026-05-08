#!/usr/bin/env python3
"""
Kirk Module - Temporal Plan Execution Components

This module provides Python translations of the MIT enterprise kirk-v2 
temporal execution framework. It includes:

- KirkScheduler: Dynamic scheduler implementing FAST-EX algorithm
- KirkDispatcher: Event dispatcher with VDC semantics
- RTED: Real-Time Execution Decision data structure
- ExecutionTimingGuard: Comprehensive timing management with max execution time,
                        duration monitoring, and external event timeout handling

The Kirk system executes Temporal Plan Networks (TPNs) in real-time,
handling both controllable (free) and uncontrollable (contingent) events.

Reference: enterprise/kirk-v2
"""

from auspex_executor.platform_executor.executors.kirk.kirk_scheduler import (
    KirkScheduler,
    RTED,
    EventState,
    ZERO_POINT,
    INFINITY,
)

from auspex_executor.platform_executor.executors.kirk.kirk_dispatcher import (
    KirkDispatcher,
    MessageType,
    DispatchMessage,
    DispatchCommand,
    DispatchStatus,
    DispatcherCallback,
)

from auspex_executor.platform_executor.executors.kirk.execution_timing_guard import (
    ExecutionTimingGuard,
    TimingViolationType,
    TimingViolation,
    OwnEventTracking,
    ExternalEventTracking,
)

__all__ = [
    # Scheduler
    'KirkScheduler',
    'RTED',
    'EventState',
    'ZERO_POINT',
    'INFINITY',
    
    # Dispatcher
    'KirkDispatcher',
    'MessageType',
    'DispatchMessage',
    'DispatchCommand',
    'DispatchStatus',
    'DispatcherCallback',
    'create_dispatcher',
    
    # Timing Guard
    'ExecutionTimingGuard',
    'TimingViolationType',
    'TimingViolation',
    'OwnEventTracking',
    'ExternalEventTracking',
]
