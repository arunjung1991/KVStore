# main.py
#!/usr/bin/env python3
"""CLI for the append-only KV store (reads from STDIN, writes to STDOUT)."""

import sys
import shlex
from typing import List
from engine import KVEngine

PROMPT = ""  # no prompt; Gradebot pipes input

def _print(s: str) -> None:
    """Write a single line to STDOUT and flush."""
    sys.stdout.write(s + "\n")
    sys.stdout.flush()

def main(argv: List[str]) -> int:
    """Run the REPL loop for the KV store."""
    db_path = "data.db"
    if len(argv) >= 2:
        db_path = argv[1]

    kv = KVEngine(db_path=db_path)

    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                return 0
            line = line.strip()
            if not line:
                continue

            try:
                parts = shlex.split(line)
            except ValueError:
                _print("ERR syntax")
                continue

            cmd = parts[0].upper()

            if cmd == "EXIT":
                return 0

            elif cmd == "SET":
                if len(parts) < 3:
                    _print("ERR usage: SET <key> <value>")
                    continue
                key = parts[1]
                value = " ".join(parts[2:])
                if "\n" in value or "\r" in value:
                    _print("ERR value must be a single line")
                    continue
                kv.set(key, value)
                _print("OK")

            elif cmd == "GET":
                if len(parts) != 2:
                    _print("ERR usage: GET <key>")
                    continue
                key = parts[1]
                v = kv.get(key)
                _print("" if v is None else v)   # empty line for missing keys

            else:
                _print("ERR unknown command")

        except KeyboardInterrupt:
            return 0
        except Exception:
            _print("ERR internal")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
