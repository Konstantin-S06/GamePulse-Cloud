"""ConsumerLoop behaviour with injected fakes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from gamepulse.ingestion.consumer import ConsumerLoop


class _FakeMsg:
    def __init__(self, value: bytes, *, error=None, ts: int | None = None) -> None:
        self._value = value
        self._error = error
        self._ts = ts or int(datetime.now(timezone.utc).timestamp() * 1000)
        self._topic = "gamepulse.events.v1"
        self._partition = 0
        self._offset = 1

    def error(self):
        return self._error

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def timestamp(self):
        return (1, self._ts)


def test_consumer_parses_and_flushes():
    written: list[tuple[list[dict[str, Any]], int, str]] = []
    consumer = MagicMock()
    messages = [
        _FakeMsg(b'{"event_id":"e1","match_id":"m_20260105_00001"}'),
        None,
        None,
    ]
    consumer.poll.side_effect = messages + [None] * 20
    consumer.commit = MagicMock()
    consumer.close = MagicMock()
    dlq = MagicMock()
    dlq.flush = MagicMock()

    def writer(rows, batch_seq, batch_id):
        written.append((rows, batch_seq, batch_id))

    loop = ConsumerLoop(
        consumer,
        dlq,
        writer,
        consumer_id="c0",
        dlq_topic="gamepulse.events.v1.dlq",
        events_topic="gamepulse.events.v1",
        batch_size=10,
        flush_seconds=5,
        idle_timeout_seconds=0,
    )
    stats = loop.run()
    assert stats.messages_consumed == 1
    assert stats.rows_written == 1
    assert len(written) == 1
    consumer.commit.assert_called()
    consumer.close.assert_called()
