"""Unit tests for prefect_deployments_toolkit.log_buffer."""

import logging
import threading

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(msg: str = "hello", level: int = logging.INFO) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )
    # Fix thread id to the current thread so ownership checks work
    record.thread = threading.get_ident()
    return record


# ---------------------------------------------------------------------------
# _SkipBufferedThreadsFilter
# ---------------------------------------------------------------------------


class TestSkipBufferedThreadsFilter:
    """Tests for the filter that suppresses records from buffered threads."""

    def setup_method(self):
        # Import fresh so module-level state is shared (singleton sets)
        from prefect_deployments_toolkit import log_buffer as lb

        self.lb = lb
        # Start with a clean buffered set for isolation
        lb._buffered_thread_ids.clear()

    def test_passes_records_from_non_buffered_thread(self):
        f = self.lb._SkipBufferedThreadsFilter()
        record = _make_record()
        # current thread is NOT in _buffered_thread_ids → filter should pass it
        assert f.filter(record) is True

    def test_suppresses_records_from_buffered_thread(self):
        f = self.lb._SkipBufferedThreadsFilter()
        record = _make_record()
        self.lb._buffered_thread_ids.add(record.thread)
        try:
            assert f.filter(record) is False
        finally:
            self.lb._buffered_thread_ids.discard(record.thread)


# ---------------------------------------------------------------------------
# _ThreadLocalBufferHandler
# ---------------------------------------------------------------------------


class TestThreadLocalBufferHandler:
    """Tests for the per-thread log capture handler."""

    def setup_method(self):
        from prefect_deployments_toolkit import log_buffer as lb

        self.lb = lb

    def test_emit_captures_own_thread_record(self):
        handler = self.lb._ThreadLocalBufferHandler(threading.get_ident())
        record = _make_record("captured")
        handler.emit(record)
        assert handler.drain() == [record]

    def test_emit_ignores_other_thread_record(self):
        # Use an impossible thread id so no record matches
        handler = self.lb._ThreadLocalBufferHandler(owner_thread_id=999999999)
        record = _make_record("ignored")
        handler.emit(record)
        assert handler.drain() == []

    def test_drain_is_destructive(self):
        handler = self.lb._ThreadLocalBufferHandler(threading.get_ident())
        record = _make_record()
        handler.emit(record)
        first = handler.drain()
        second = handler.drain()
        assert len(first) == 1
        assert second == []

    def test_drain_returns_records_in_order(self):
        handler = self.lb._ThreadLocalBufferHandler(threading.get_ident())
        for i in range(5):
            r = _make_record(f"msg {i}")
            handler.emit(r)
        drained = handler.drain()
        assert [r.msg for r in drained] == [f"msg {i}" for i in range(5)]


# ---------------------------------------------------------------------------
# _get_root_formatter
# ---------------------------------------------------------------------------


class TestGetRootFormatter:
    """Tests for the formatter extraction helper."""

    def setup_method(self):
        from prefect_deployments_toolkit import log_buffer as lb

        self.lb = lb

    def test_returns_default_formatter_when_no_handlers(self):
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        root.handlers.clear()
        try:
            fmt = self.lb._get_root_formatter()
            assert fmt is not None
            assert isinstance(fmt, logging.Formatter)
        finally:
            root.handlers = original_handlers

    def test_returns_existing_formatter_when_present(self):
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        root.handlers.clear()
        handler = logging.StreamHandler()
        custom_fmt = logging.Formatter("%(message)s")
        handler.setFormatter(custom_fmt)
        root.addHandler(handler)
        try:
            fmt = self.lb._get_root_formatter()
            assert fmt is custom_fmt
        finally:
            root.handlers = original_handlers


# ---------------------------------------------------------------------------
# buffered_deployment_log (context manager)
# ---------------------------------------------------------------------------


class TestBufferedDeploymentLog:
    """Integration-style tests for the public context manager."""

    def setup_method(self):
        from prefect_deployments_toolkit import log_buffer as lb

        self.lb = lb
        # Reset module-level state so tests don't interfere with each other
        lb._buffered_thread_ids.clear()
        lb._filter_installed = False

    def test_thread_id_added_during_context_and_removed_after(self):
        lb = self.lb
        tid = threading.get_ident()

        with lb.buffered_deployment_log("test-deployment"):
            assert tid in lb._buffered_thread_ids

        assert tid not in lb._buffered_thread_ids

    def test_thread_id_removed_even_on_exception(self):
        lb = self.lb
        tid = threading.get_ident()
        caught = False

        try:
            with lb.buffered_deployment_log("test-deployment"):
                raise RuntimeError("boom")
        except RuntimeError:
            caught = True

        assert caught, "RuntimeError should have propagated"
        assert tid not in lb._buffered_thread_ids

    def test_records_are_flushed_to_root_handler_on_exit(self):
        lb = self.lb
        emitted: list[logging.LogRecord] = []

        # Add a capturing handler to the root logger
        capture = logging.StreamHandler()
        capture.emit = lambda r: emitted.append(r)  # type: ignore[method-assign]
        root = logging.getLogger()
        root.addHandler(capture)
        original_level = root.level
        root.setLevel(logging.DEBUG)

        try:
            with lb.buffered_deployment_log("my-dep"):
                logging.getLogger("test_flush").info("flush me")

            assert any(r.getMessage() == "flush me" for r in emitted)
        finally:
            root.removeHandler(capture)
            root.setLevel(original_level)

    def test_no_duplicate_records_emitted(self):
        """Records must appear exactly once — not from root handler AND buffer."""
        lb = self.lb
        emitted: list[str] = []

        capture = logging.StreamHandler()
        capture.emit = lambda r: emitted.append(r.getMessage())  # type: ignore[method-assign]
        root = logging.getLogger()
        root.addHandler(capture)
        original_level = root.level
        root.setLevel(logging.DEBUG)

        try:
            with lb.buffered_deployment_log("my-dep"):
                logging.getLogger("dup_test").info("unique message")

            count = emitted.count("unique message")
            assert count == 1, f"Expected 1 emit, got {count}"
        finally:
            root.removeHandler(capture)
            root.setLevel(original_level)
