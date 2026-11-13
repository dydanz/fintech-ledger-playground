"""Tests for check.py's fault expectations: python3 -m unittest scripts/chaos/check_test.py"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

CHECK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "check.py")


def run_dir(failed_attempts):
    """A minimal postgres run: one credit, answered 201 after the given failed attempts."""
    d = tempfile.mkdtemp()
    log = [{"source": "console", "time": "2026-11-12T06:00:01Z", "msg": json.dumps(a)} for a in failed_attempts]
    log.append({"source": "console", "time": "2026-11-12T06:00:05Z", "msg": json.dumps(
        {"tx": "t1", "tries": len(failed_attempts) + 1, "ms": 900, "status": 201, "key": "chaos-1-a", "chaos": 1,
         "acct": "a1", "delta": 100})})
    files = {
        "k6-log.json": "\n".join(json.dumps(l) for l in log),
        "tx.txt": "t1|a1|SETTLED|",
        "balances.txt": "a1|100",
        "outbox.txt": "e1",
        "kafka.txt": "e1",
        "timeline.txt": "2026-11-12T06:00:00Z FAULT postgres\n2026-11-12T06:00:04Z RECOVER postgres",
    }
    for name, body in files.items():
        with open(os.path.join(d, name), "w") as f:
            f.write(body + "\n")
    return d


def check(failed_attempts):
    out = subprocess.run([sys.executable, CHECK, run_dir(failed_attempts), "postgres"], capture_output=True, text=True)
    line = next(l for l in out.stdout.splitlines() if "postgres: failures" in l)
    return out.returncode, line


class PostgresDrill(unittest.TestCase):
    def test_retries_answered_request_in_progress_are_expected(self):
        # #57: a write caught between Tx1 and Tx2 is answered 409 until the sweeper completes it.
        rc, line = check([{"status": 503, "code": "ledger_unavailable", "at": "06:00:01"},
                          {"status": 409, "code": "request_in_progress", "at": "06:00:03"}])
        self.assertEqual(rc, 0, line)
        self.assertTrue(line.startswith("PASS"), line)

    def test_any_other_failure_still_fails(self):
        rc, line = check([{"status": 409, "code": "idempotency_key_reused", "at": "06:00:02"}])
        self.assertEqual(rc, 1, line)
        self.assertIn("1 of another kind", line)

    def test_no_failures_at_all_fails(self):
        rc, line = check([])
        self.assertEqual(rc, 1, line)
        self.assertIn("did the fault happen?", line)


if __name__ == "__main__":
    unittest.main()
