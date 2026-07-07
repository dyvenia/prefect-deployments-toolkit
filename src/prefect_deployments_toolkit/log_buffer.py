"""Per-thread log buffering to prevent interleaved output across concurrent deployments."""

import logging
import threading
from collections.abc import Generator
from contextlib import contextmanager

_flush_lock = threading.Lock()

# Thread IDs that are currently being buffered — root handlers should skip these
_buffered_thread_ids: set[int] = set()
_buffered_ids_lock = threading.Lock()


class _SkipBufferedThreadsFilter(logging.Filter):
    """Filter attached to root handlers to suppress records from buffered threads.

    Without this, records from buffered threads would be printed immediately by
    the root handler AND captured by the buffer, causing duplication.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        with _buffered_ids_lock:
            return record.thread not in _buffered_thread_ids


class _ThreadLocalBufferHandler(logging.Handler):
    """Captures log records emitted only by the thread that owns this handler."""

    def __init__(self, owner_thread_id: int) -> None:
        super().__init__()
        self._owner_thread_id = owner_thread_id
        self._lock = threading.Lock()
        self._records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self._owner_thread_id:
            return
        with self._lock:
            self._records.append(record)

    def drain(self) -> list[logging.LogRecord]:
        with self._lock:
            records, self._records = self._records, []
            return records


# Singleton filter instance — added once to root handlers on first use
_skip_filter = _SkipBufferedThreadsFilter()
_filter_installed = False
_filter_lock = threading.Lock()


def _ensure_skip_filter_installed() -> None:
    global _filter_installed
    with _filter_lock:
        if _filter_installed:
            return
        for handler in logging.getLogger().handlers:
            handler.addFilter(_skip_filter)
        _filter_installed = True


def _get_root_formatter() -> logging.Formatter:
    for handler in logging.getLogger().handlers:
        if handler.formatter:
            return handler.formatter
    return logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")


@contextmanager
def buffered_deployment_log(deployment_name: str) -> Generator[None, None, None]:
    """Buffer all log output for the current thread and flush it atomically on exit.

    Installs a filter on root handlers so this thread's records are not printed
    immediately — they are held in the buffer and flushed as a contiguous block
    once the deployment finishes, under a shared lock that prevents interleaving
    with other deployments' flushes.
    """
    _ensure_skip_filter_installed()

    thread_id = threading.get_ident()
    handler = _ThreadLocalBufferHandler(owner_thread_id=thread_id)
    handler.setFormatter(_get_root_formatter())

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    with _buffered_ids_lock:
        _buffered_thread_ids.add(thread_id)

    try:
        yield
    finally:
        with _buffered_ids_lock:
            _buffered_thread_ids.discard(thread_id)

        root_logger.removeHandler(handler)
        records = handler.drain()

        # Only flush when we actually captured something; no early return that can
        # interfere with exception propagation.
        if records:
            with _flush_lock:
                for root_handler in root_logger.handlers:
                    for record in records:
                        try:
                            root_handler.emit(record)
                        except Exception:  # noqa: BLE001
                            pass
