# KVStore: Append-Only Key–Value Store with B+ Tree Index

This project implements a simple persistent key–value database in Python.  
It uses an append-only log for durability and an in-memory B+ tree for fast lookups.  
A minimal command-line interface (CLI) is provided for interacting with the database.

---

## Project Structure

```
KVStore/
├── engine.py       # Coordinates the log and index, rebuilds state on startup
├── index.py        # B+ Tree implementation
├── storage.py      # Append-only log (fsync on each write)
├── main.py         # Command-line interface (REPL)
├── README.md       # Project documentation
└── data.db         # Log file created at runtime (default)
```

---

## Features

- Append-only durability  
  All SET operations are appended to a log file and fsync'ed to ensure durability even after crashes.

- B+ Tree Index  
  Provides efficient lookups and ordered iteration.

- Log Replay on Startup  
  The engine replays the log when starting to restore the latest state.

- Simple CLI Protocol  
  Reads from STDIN and writes to STDOUT. No prompts are printed, so it works well with automated grading.

---

## Requirements

- Python 3.8 or later
- No external dependencies are required

(Optional) Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate   # Linux / macOS
venv\Scripts\activate      # Windows
```

---

## How to Run

To run the KVStore interactively:

```bash
python3 main.py
```

This starts a simple loop that reads commands from standard input and writes responses to standard output.

You can also specify a custom database log path:

```bash
python3 main.py /path/to/custom.db
```

### Running with Gradebot

The project can be tested using Gradebot with a command similar to:

```bash
/tmp/gradebot/gradebot project-1 --dir /home/UNT/ak2102/Database/kvstore_project --run "python3 main.py"
```

Important notes for Gradebot:
- The program must not print any prompts or extra output.
- All output must exactly match the expected format.
- Error messages and logs should go to STDERR, not STDOUT.

---

## Supported Commands

| Command               | Description |
|-----------------------|-------------|
| `SET <key> <value>`   | Insert or update a key-value pair. Overwrites existing values. |
| `GET <key>`          | Retrieve the value for the given key, or print an empty line if missing. |
| `EXIT`                | Exit the program gracefully. |

### Example Session

```bash
$ python3 main.py
SET user Alice
OK
GET user
Alice
SET user Bob
OK
GET user
Bob
EXIT
```

Log file contents after the session:

```
SET user Alice
SET user Bob
```

---

## Persistence Details

1. Each SET operation is appended to the log file and flushed to disk.  
2. The in-memory B+ tree is updated immediately.  
3. On startup, the log is replayed to rebuild the in-memory state, ensuring that later writes overwrite earlier ones.

---

## Internals

- storage.py  
  Handles writing and replaying the append-only log.

- index.py  
  Implements a basic B+ tree for indexing keys and values.  
  All values are stored in the leaf nodes, and internal nodes only contain routing keys.

- engine.py  
  Connects the index and the log. Replays the log on startup to restore state.

- main.py  
  Implements the CLI for interacting with the key–value store.

---

## Testing

You can test the CLI manually by piping commands into it:

```bash
echo -e "SET a 1\nGET a\nEXIT" | python3 main.py
```

Expected output:

```
OK
1
```


