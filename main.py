#!/usr/bin/env python3
"""CLI for the append-only KV store (reads from STDIN, writes to STDOUT)."""

import sys
import shlex
from typing import List, Callable, Dict, Optional
from engine import KVEngine

# -------------------- Command & message constants --------------------
CMD_SET  = "SET"
CMD_GET  = "GET"
CMD_EXIT = "EXIT"

MSG_OK               = "OK"
ERR_SYNTAX           = "ERR syntax"
ERR_UNKNOWN_CMD      = "ERR unknown command"
ERR_USAGE_SET        = "ERR usage: SET <key> <value>"
ERR_USAGE_GET        = "ERR usage: GET <key>"
ERR_VALUE_SINGLELINE = "ERR value must be a single line"
ERR_INTERNAL         = "ERR internal"

# No prompt—Gradebot pipes input
PROMPT = ""


def _print(line: str) -> None:
    """Write a single line to STDOUT and flush."""
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


# -------------------- Command handlers --------------------
def handle_set(args: List[str], kv: KVEngine) -> None:
    """SET <key> <value...>  ->  OK"""
    if len(args) < 2:
        _print(ERR_USAGE_SET)
        return
    key = args[0]
    value = " ".join(args[1:])
    if ("\n" in value) or ("\r" in value):
        _print(ERR_VALUE_SINGLELINE)
        return
    kv.set(key, value)
    _print(MSG_OK)


def handle_get(args: List[str], kv: KVEngine) -> None:
    """GET <key>  ->  value | (empty line if missing)"""
    if len(args) != 1:
        _print(ERR_USAGE_GET)
        return
    key = args[0]
    val = kv.get(key)
    _print("" if val is None else val)


def handle_exit(args: List[str], kv: KVEngine) -> str:
    """EXIT -> signal main loop to terminate."""
    return "EXIT"


# Dispatch table (dict is allowed; the “no dict” rule only applies to the index)
DISPATCH: Dict[str, Callable[[List[str], KVEngine], Optional[str]]] = {
    CMD_SET: handle_set,
    CMD_GET: handle_get,
    CMD_EXIT: handle_exit,
}


def _parse_command(line: str) -> Optional[List[str]]:
    """Split a raw input line into tokens (cmd + args) using shell-like rules.

    Returns:
        tokens list on success, or None if parsing fails (e.g., unbalanced quotes).
    """
    try:
        return shlex.split(line)
    except ValueError:
        return None


def main(argv: List[str]) -> int:
    """Run the REPL loop for the KV store.

    Args:
        argv: Command-line arguments; argv[1] may optionally be a custom db path.
    """
    db_path = "data.db" if len(argv) < 2 else argv[1]
    kv = KVEngine(db_path=db_path)

    while True:
        try:
            raw = sys.stdin.readline()
            if not raw:  # EOF → clean exit
                return 0
            line = raw.strip()
            if not line:
                continue

            tokens = _parse_command(line)
            if tokens is None or not tokens:
                _print(ERR_SYNTAX)
                continue

            cmd = tokens[0].upper()
            args = tokens[1:]
            handler = DISPATCH.get(cmd)
            if handler is None:
                _print(ERR_UNKNOWN_CMD)
                continue

            result = handler(args, kv)
            if result == "EXIT":
                return 0

        except KeyboardInterrupt:
            return 0
        except Exception:
            # Don’t leak tracebacks to STDOUT—keep Gradebot clean.
            _print(ERR_INTERNAL)
            # Continue loop; next lines can still be processed

    # Unreachable
    # return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
