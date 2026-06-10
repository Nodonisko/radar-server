from __future__ import annotations

from dataclasses import dataclass
from queue import Empty

import pytest

from radar_server.queueing import PriorityWorkQueue


@dataclass(frozen=True)
class _Task:
    name: str

    @property
    def key(self) -> tuple:
        return ("task", self.name)


def test_get_orders_by_priority_then_fifo() -> None:
    work_queue = PriorityWorkQueue()
    work_queue.put_if_absent(10, _Task("b1"))
    work_queue.put_if_absent(0, _Task("a"))
    work_queue.put_if_absent(10, _Task("b2"))
    work_queue.put_if_absent(5, _Task("mid"))

    order = [work_queue.get().name for _ in range(4)]

    assert order == ["a", "mid", "b1", "b2"]


def test_put_if_absent_dedupes_until_task_done() -> None:
    work_queue = PriorityWorkQueue()

    assert work_queue.put_if_absent(0, _Task("x")) is True
    assert work_queue.put_if_absent(0, _Task("x")) is False

    task = work_queue.get()
    assert work_queue.put_if_absent(0, _Task("x")) is False  # still in flight

    work_queue.task_done(task)
    assert work_queue.put_if_absent(0, _Task("x")) is True


def test_is_idle_tracks_queued_and_in_flight_work() -> None:
    work_queue = PriorityWorkQueue()
    assert work_queue.is_idle()

    work_queue.put_if_absent(0, _Task("x"))
    assert not work_queue.is_idle()

    task = work_queue.get()
    assert not work_queue.is_idle()  # nothing queued, but one task in flight

    work_queue.task_done(task)
    assert work_queue.is_idle()


def test_get_raises_empty_on_timeout() -> None:
    work_queue = PriorityWorkQueue()
    with pytest.raises(Empty):
        work_queue.get(timeout=0)


def test_pending_count_and_keys_snapshot() -> None:
    work_queue = PriorityWorkQueue()
    work_queue.put_if_absent(0, _Task("a"))
    work_queue.put_if_absent(1, _Task("b"))

    assert work_queue.pending_count() == 2
    assert work_queue.keys() == frozenset({("task", "a"), ("task", "b")})
