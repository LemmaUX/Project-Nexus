from __future__ import annotations

from enum import StrEnum


class WorkflowExecutionState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_FOR_INPUT = "WAITING_FOR_INPUT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


VALID_TRANSITIONS: dict[WorkflowExecutionState, set[WorkflowExecutionState]] = {
    WorkflowExecutionState.PENDING: {
        WorkflowExecutionState.RUNNING,
        WorkflowExecutionState.FAILED,
        WorkflowExecutionState.TIMED_OUT,
    },
    WorkflowExecutionState.RUNNING: {
        WorkflowExecutionState.WAITING_FOR_INPUT,
        WorkflowExecutionState.COMPLETED,
        WorkflowExecutionState.FAILED,
        WorkflowExecutionState.TIMED_OUT,
    },
    WorkflowExecutionState.WAITING_FOR_INPUT: {
        WorkflowExecutionState.RUNNING,
        WorkflowExecutionState.FAILED,
        WorkflowExecutionState.TIMED_OUT,
    },
    WorkflowExecutionState.COMPLETED: set(),
    WorkflowExecutionState.FAILED: set(),
    WorkflowExecutionState.TIMED_OUT: set(),
}


def is_valid_transition(
    source: WorkflowExecutionState,
    target: WorkflowExecutionState,
) -> bool:
    return target in VALID_TRANSITIONS[source]

