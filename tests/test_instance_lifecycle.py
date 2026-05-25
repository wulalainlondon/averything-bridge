"""
Tests for instance_lifecycle.py — process spawn/stop/status management.

Run: python -m pytest bridge/tests/test_instance_lifecycle.py -v
"""
from __future__ import annotations

import os
import signal
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_BRIDGE_ROOT.parent))
sys.path.insert(0, str(_BRIDGE_ROOT))

import instance_lifecycle as lc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_item(tmp_path: Path, *, name: str = "test", port: int = 8800,
               root_dir: str = "", backend: str = "", model: str = "",
               ollama_host: str = "") -> dict:
    data_dir = str(tmp_path)
    return {
        "name": name,
        "port": port,
        "data_dir": data_dir,
        "root_dir": root_dir,
        "backend": backend,
        "model": model,
        "ollama_host": ollama_host,
    }


# ---------------------------------------------------------------------------
# start_instance
# ---------------------------------------------------------------------------

class TestStartInstance(unittest.TestCase):

    def test_data_dir_missing_returns_error(self):
        item = _make_item(Path("/nonexistent/path/that/does/not/exist"))
        ok, err = lc.start_instance(item)
        self.assertFalse(ok)
        self.assertEqual(err, "data_dir_missing")

    def test_supervisor_not_found_returns_error(self, tmp_path=None):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d))
            with patch.object(lc, "SUPERVISOR_SCRIPT", "/no/such/supervisor.sh"):
                ok, err = lc.start_instance(item)
            self.assertFalse(ok)
            self.assertEqual(err, "supervisor_not_found")

    def test_spawn_called_with_required_args(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d), name="alpha", port=9001)
            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen") as mock_popen, \
                 patch("builtins.open", unittest.mock.mock_open()):
                mock_popen.return_value = MagicMock()
                ok, err = lc.start_instance(item)

            self.assertTrue(ok)
            self.assertIsNone(err)
            mock_popen.assert_called_once()
            argv = mock_popen.call_args.args[0]
            self.assertIn("--name", argv)
            self.assertIn("alpha", argv)
            self.assertIn("--port", argv)
            self.assertIn("9001", argv)
            self.assertIn("--data-dir", argv)
            self.assertIn(d, argv)
            # Optional args must NOT appear when empty.
            self.assertNotIn("--root-dir", argv)
            self.assertNotIn("--backend", argv)
            self.assertNotIn("--model", argv)
            self.assertNotIn("--ollama-host", argv)

    def test_spawn_includes_optional_args_when_set(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(
                Path(d),
                name="beta",
                port=9002,
                root_dir="/projects/beta",
                backend="ollama",
                model="llama3",
                ollama_host="http://localhost:11434",
            )
            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen") as mock_popen, \
                 patch("builtins.open", unittest.mock.mock_open()):
                mock_popen.return_value = MagicMock()
                ok, err = lc.start_instance(item)

            self.assertTrue(ok)
            argv = mock_popen.call_args.args[0]
            self.assertIn("--root-dir", argv)
            self.assertIn("/projects/beta", argv)
            self.assertIn("--backend", argv)
            self.assertIn("ollama", argv)
            self.assertIn("--model", argv)
            self.assertIn("llama3", argv)
            self.assertIn("--ollama-host", argv)
            self.assertIn("http://localhost:11434", argv)

    def test_popen_uses_start_new_session(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d))
            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen") as mock_popen, \
                 patch("builtins.open", unittest.mock.mock_open()):
                mock_popen.return_value = MagicMock()
                lc.start_instance(item)

            kwargs = mock_popen.call_args.kwargs
            self.assertTrue(kwargs.get("start_new_session"), "start_new_session must be True")

    def test_no_spawn_when_pid_alive(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d), name="gamma", port=9003)
            pid_path = Path(d) / "bridge.pid"
            pid_path.write_text("9999")

            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen") as mock_popen, \
                 patch("instance_lifecycle.os.kill") as mock_kill:
                # os.kill(9999, 0) succeeds → process is alive.
                mock_kill.return_value = None
                ok, err = lc.start_instance(item)

            self.assertTrue(ok)
            self.assertIsNone(err)
            mock_popen.assert_not_called()

    def test_state_file_written_enabled(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d))
            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen") as mock_popen, \
                 patch("builtins.open", unittest.mock.mock_open()) as mock_open_fn:
                mock_popen.return_value = MagicMock()
                lc.start_instance(item)

            # The first open() call targets .bridge_state with "w" mode and writes "enabled".
            state_path = str(Path(d) / ".bridge_state")
            write_calls = [
                c for c in mock_open_fn.call_args_list
                if len(c.args) >= 1 and ".bridge_state" in str(c.args[0])
            ]
            self.assertTrue(len(write_calls) >= 1, ".bridge_state was not opened")

    def test_spawn_failed_returns_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d))
            fake_supervisor = str(Path(d) / "supervisor_instance.sh")
            Path(fake_supervisor).write_text("#!/bin/bash\n")

            with patch.object(lc, "SUPERVISOR_SCRIPT", fake_supervisor), \
                 patch("instance_lifecycle.subprocess.Popen", side_effect=OSError("boom")), \
                 patch("builtins.open", unittest.mock.mock_open()):
                ok, err = lc.start_instance(item)

            self.assertFalse(ok)
            self.assertEqual(err, "spawn_failed")


# ---------------------------------------------------------------------------
# stop_instance
# ---------------------------------------------------------------------------

class TestStopInstance(unittest.TestCase):

    def test_sigterm_sent_first(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            pid_path = Path(d) / "bridge.pid"
            pid_path.write_text("5555")
            item = _make_item(Path(d), port=9100)

            kill_calls: list[tuple] = []

            def fake_kill(pid, sig):
                kill_calls.append((pid, sig))
                if sig == signal.SIGTERM:
                    return  # process "dies" after SIGTERM on next poll
                raise ProcessLookupError

            with patch("instance_lifecycle.os.kill", side_effect=fake_kill), \
                 patch("instance_lifecycle.subprocess.run") as mock_run, \
                 patch("instance_lifecycle.time.sleep"):
                # Make _pid_alive return False after SIGTERM (process died).
                alive_sequence = [True, False]
                alive_iter = iter(alive_sequence)

                def fake_pid_alive(pid):
                    try:
                        return next(alive_iter)
                    except StopIteration:
                        return False

                mock_run.return_value = MagicMock(stdout="", returncode=0)
                with patch("instance_lifecycle._pid_alive", side_effect=fake_pid_alive):
                    ok, err = lc.stop_instance("test", item)

            self.assertTrue(ok)
            self.assertIsNone(err)
            first_sig = kill_calls[0][1]
            self.assertEqual(first_sig, signal.SIGTERM)

    def test_sigkill_sent_if_still_alive_after_timeout(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            pid_path = Path(d) / "bridge.pid"
            pid_path.write_text("7777")
            item = _make_item(Path(d), port=9101)

            kill_calls: list[tuple] = []

            def fake_kill(pid, sig):
                kill_calls.append((pid, sig))

            with patch("instance_lifecycle.os.kill", side_effect=fake_kill), \
                 patch("instance_lifecycle.subprocess.run") as mock_run, \
                 patch("instance_lifecycle.time.sleep"), \
                 patch("instance_lifecycle.time.monotonic", side_effect=[0.0, 10.0, 10.0]):
                # Process stays alive throughout.
                mock_run.return_value = MagicMock(stdout="", returncode=0)
                with patch("instance_lifecycle._pid_alive", return_value=True):
                    ok, err = lc.stop_instance("test", item)

            sigs = [s for _, s in kill_calls]
            self.assertIn(signal.SIGTERM, sigs)
            self.assertIn(signal.SIGKILL, sigs)
            # SIGTERM must come before SIGKILL.
            term_idx = next(i for i, s in enumerate(sigs) if s == signal.SIGTERM)
            kill_idx = next(i for i, s in enumerate(sigs) if s == signal.SIGKILL)
            self.assertLess(term_idx, kill_idx)

    def test_no_pid_file_and_no_port_listener_returns_not_found(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d), port=9102)

            with patch("instance_lifecycle.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(stdout="", returncode=1)
                ok, err = lc.stop_instance("test", item)

            self.assertFalse(ok)
            self.assertEqual(err, "not_found")

    def test_port_listener_killed_after_supervisor_stop(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            pid_path = Path(d) / "bridge.pid"
            pid_path.write_text("6666")
            item = _make_item(Path(d), port=9103)

            kill_calls: list[tuple] = []

            def fake_kill(pid, sig):
                kill_calls.append((pid, sig))

            with patch("instance_lifecycle.os.kill", side_effect=fake_kill), \
                 patch("instance_lifecycle.subprocess.run") as mock_run, \
                 patch("instance_lifecycle.time.sleep"), \
                 patch("instance_lifecycle.time.monotonic", return_value=0.0), \
                 patch("instance_lifecycle._pid_alive", side_effect=[True, False, False]):
                # lsof returns a port listener PID.
                mock_run.return_value = MagicMock(stdout="4321\n", returncode=0)
                ok, err = lc.stop_instance("test", item)

            self.assertTrue(ok)
            killed_pids = {p for p, _ in kill_calls}
            self.assertIn(4321, killed_pids)


# ---------------------------------------------------------------------------
# instance_status
# ---------------------------------------------------------------------------

class TestInstanceStatus(unittest.TestCase):

    def test_state_disabled_returns_stopped(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            Path(d, ".bridge_state").write_text("disabled")
            Path(d, "bridge.pid").write_text("1234")
            item = _make_item(Path(d), name="x", port=8800)

            with patch("instance_lifecycle._pid_alive", return_value=True):
                status = lc.instance_status(item)

            self.assertEqual(status["state"], "stopped")

    def test_enabled_with_alive_pid_returns_running(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            Path(d, ".bridge_state").write_text("enabled")
            Path(d, "bridge.pid").write_text("2345")
            item = _make_item(Path(d), name="y", port=8801)

            with patch("instance_lifecycle._pid_alive", return_value=True):
                status = lc.instance_status(item)

            self.assertEqual(status["state"], "running")
            self.assertEqual(status["supervisor_pid"], 2345)

    def test_enabled_with_dead_pid_returns_crashed(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            Path(d, ".bridge_state").write_text("enabled")
            Path(d, "bridge.pid").write_text("3456")
            item = _make_item(Path(d), name="z", port=8802)

            with patch("instance_lifecycle._pid_alive", return_value=False):
                status = lc.instance_status(item)

            self.assertEqual(status["state"], "crashed")

    def test_no_pid_file_returns_stopped(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d), name="w", port=8803)
            # No bridge.pid written, no .bridge_state.
            status = lc.instance_status(item)

            self.assertEqual(status["state"], "stopped")
            self.assertIsNone(status["supervisor_pid"])

    def test_bridge_pid_read_from_bridge_v2_pid(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            Path(d, ".bridge_state").write_text("enabled")
            Path(d, "bridge.pid").write_text("5678")
            Path(d, "bridge_v2.pid").write_text("5679")
            item = _make_item(Path(d), name="v2", port=8804)

            with patch("instance_lifecycle._pid_alive", return_value=True):
                status = lc.instance_status(item)

            self.assertEqual(status["bridge_pid"], 5679)
            self.assertEqual(status["supervisor_pid"], 5678)

    def test_bridge_pid_none_when_file_absent(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            Path(d, ".bridge_state").write_text("enabled")
            Path(d, "bridge.pid").write_text("9876")
            item = _make_item(Path(d), name="nopid", port=8805)

            with patch("instance_lifecycle._pid_alive", return_value=True):
                status = lc.instance_status(item)

            self.assertIsNone(status["bridge_pid"])

    def test_status_fields_present(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            item = _make_item(Path(d), name="fields", port=8806, root_dir="/root")
            status = lc.instance_status(item)

            for key in ("name", "port", "root_dir", "data_dir", "state",
                        "supervisor_pid", "bridge_pid"):
                self.assertIn(key, status)


# ---------------------------------------------------------------------------
# list_status
# ---------------------------------------------------------------------------

class TestListStatus(unittest.TestCase):

    def test_returns_status_for_all_items(self):
        import tempfile
        items = []
        dirs = []
        for i in range(3):
            d = tempfile.mkdtemp()
            dirs.append(d)
            items.append(_make_item(Path(d), name=f"inst{i}", port=9200 + i))

        try:
            result = lc.list_status(items)
            self.assertEqual(len(result), 3)
            names = {s["name"] for s in result}
            self.assertEqual(names, {"inst0", "inst1", "inst2"})
        finally:
            import shutil
            for d in dirs:
                shutil.rmtree(d, ignore_errors=True)

    def test_empty_list_returns_empty(self):
        self.assertEqual(lc.list_status([]), [])

    def test_calls_instance_status_per_item(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            items = [_make_item(Path(d), name="single", port=9300)]

            with patch("instance_lifecycle.instance_status", return_value={"state": "stopped"}) as mock_status:
                result = lc.list_status(items)

            mock_status.assert_called_once_with(items[0])
            self.assertEqual(result, [{"state": "stopped"}])


if __name__ == "__main__":
    unittest.main()
