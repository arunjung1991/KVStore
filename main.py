
#!/usr/bin/env python3
import sys
import shlex
from typing import List
from engine import KVEngine

PROMPT = ""  # Empty to play nice with black-box testers piping input

def _print(s: str) -> None:
    sys.stdout.write(s + "\n")
    sys.stdout.flush()

def main(argv: List[str]) -> int:
    db_path = "data.db"
    # Optional: allow passing a custom db path as first arg
    if len(argv) >= 2:
        db_path = argv[1]

    kv = KVEngine(db_path=db_path)

    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                # EOF -> exit cleanly
                return 0
            line = line.strip()
            if not line:
                continue

            # Use shlex so values with spaces can be quoted: SET k "hello world"
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
                # Join remaining parts to allow spaces without quotes too
                value = " ".join(parts[2:])
                # Disallow newlines in value to keep the on-disk format simple
                if "\n" in value or "\r" in value:
                    _print("ERR value must be a single line")
                    continue
                kv.set(key, value)
                _print("OK")  # Conventional ack for writes

            elif cmd == "GET":
                if len(parts) != 2:
                    _print("ERR usage: GET <key>")
                    continue
                key = parts[1]
                v = kv.get(key)
                _print(v if v is not None else "NULL")  # NULL for missing

            else:
                _print("ERR unknown command")

        except KeyboardInterrupt:
            return 0
        except Exception as e:
            # Fail closed with a simple error message; keep black-box stable
            _print("ERR internal")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
