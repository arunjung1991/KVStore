
# Simple Append-Only KV Store (Project 1)

A minimal persistent key–value store with a CLI:

- Commands on STDIN: `SET <key> <value>`, `GET <key>`, `EXIT`
- Append-only persistence to `data.db` in the working directory
- Crash recovery by log replay on startup (last-write-wins)
- **No built-in dict/map** used for the in-memory index (linear scan list instead)
- Designed for black-box testing (Gradebot)

## Run

```bash
cd kvstore_project
python3 main.py
```

The process reads commands from STDIN and writes responses to STDOUT.
By default it uses `./data.db` in the current working directory.

### Example

```bash
$ python3 main.py
SET a 1
OK
GET a
1
GET b
NULL
SET a 2
OK
GET a
2
EXIT
```

## With Gradebot

- **Command to run your database**: `python3 main.py`
- **Working directory**: this folder (where `data.db` should be created)
- The program reads from STDIN and prints to STDOUT.

## Project Structure

- `main.py` — CLI entry point
- `engine.py` — KV engine coordinating storage and index
- `storage.py` — append-only log (`data.db`) with fsync on every write
- `index.py` — simple linear in-memory index (custom list; no dict usage)

## Notes

- We parse CLI input with a simple split. Values with spaces are supported if quoted (e.g., `SET key "hello world"`), but Gradebot typically uses single-token values.
- "Last write wins" is enforced by scanning the in-memory list from the end.
- For simplicity, the in-memory index stores full key-value pairs; persistence is ensured by the append-only log.
