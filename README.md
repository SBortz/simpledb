# World's Simplest Database

A simple key-value database implementation demonstrating different storage strategies and optimization techniques. This project showcases four progressively optimized versions of a database, each with different trade-offs between simplicity, performance, and features.

## Overview

This project implements a simple key-value database with four different versions:

- **V1**: Basic text-based append-only database
- **V2**: Binary format with length-prefixed entries
- **V3**: Binary format with in-memory index for fast lookups
- **V4**: SSTable-based implementation with Write-Ahead Log (WAL) and crash recovery

## Features

### Version 1: Text-Based Database
- **Storage Format**: Text file with entries in `key;value\n` format
- **Write Complexity**: O(1) - Simple append to file
- **Read Complexity**: O(n) - Full file scan for each read
- **Pros**:
  - Very simple to understand and debug
  - Human-readable format
  - Minimal code
- **Cons**:
  - Very slow for many entries (full scan on every get)
  - Inefficient storage format (text encoding overhead)
  - No optimization possible
- **Use Case**: Prototyping, very small datasets (<1,000 entries)

### Version 2: Binary Format Database
- **Storage Format**: Binary format with length-prefixed entries
  ```
  ┌─────────┬─────────┬─────────┬─────────┐
  │ keyLen  │ keyData │valueLen │valueData│
  │ (4B)    │ (N B)   │ (4B)    │ (M B)   │
  └─────────┴─────────┴─────────┴─────────┘
  ```
- **Write Complexity**: O(1) - Binary append to file
- **Read Complexity**: O(n) - Sequential scan through file
- **Pros**:
  - More efficient than text format (no parsing, no encoding overhead)
  - Can store arbitrary strings (including semicolons and newlines)
  - Slightly faster reads due to structured format
- **Cons**:
  - Still O(n) read operations (full scan)
  - No index structure for fast lookups
  - Not human-readable
- **Use Case**: Small to medium datasets (1,000-10,000 entries) when binary format is preferred

### Version 3: Indexed Database
- **Storage Format**: Same as V2 (binary format with length-prefixed entries)
- **Additional**: In-memory index (`Dictionary<string, long>`) mapping keys to file offsets
- **Write Complexity**: O(1) - Append + index update in memory
- **Read Complexity**: O(1) - Direct seek to position via index (no scan!)
- **Pros**:
  - Extremely fast reads even with millions of entries
  - Writes remain fast (append + RAM update)
  - Scales very well
- **Cons**:
  - Index requires RAM (~50-100 bytes per key)
  - Startup time to build index (one-time scan of file)
  - More complex architecture with dependency injection
- **Use Case**: Production applications with many entries (>10,000) requiring fast read access

### Version 4: SSTable-Based Database
- **Architecture**: Log-Structured Merge Tree (LSM-tree) principles with double-buffering
- **Storage Format**: Sorted String Tables (SSTables) - immutable, sorted files
- **Components**:
  - **Memtable**: In-memory sorted dictionary (Red-Black Tree) for buffering writes
  - **SSTables**: Immutable sorted files on disk
  - **Write-Ahead Log (WAL)**: Crash recovery mechanism
- **Write Complexity**: O(log n) - Insert into sorted dictionary (memtable)
- **Read Complexity**: O(log n * m) - Binary search in memtable + m SSTables
- **Features**:
  - Non-blocking flush: New writes can continue to a new memtable while old one flushes
  - Atomic SSTable writes: Temporary files ensure no corruption
  - Crash recovery: WAL replays on startup
  - Explicit cleanup: Flushes and WAL deletion on application exit
- **Pros**:
  - Very fast writes (in-memory until flush)
  - Non-blocking flush: Writes continue during background flush
  - Efficient reads through binary search in sorted SSTables
  - Immutable SSTables (no corruption risk)
  - Sorting enables range queries (not implemented, but possible)
  - Scales well
  - Compaction possible (not implemented, but prepared)
- **Cons**:
  - More complex architecture
  - Read amplification (multiple files must be searched)
  - WAL overhead (every write is written twice: WAL + memtable)
  - SSTables can become fragmented (compaction needed)
- **Use Case**: Production applications requiring both fast writes and efficient reads, with crash recovery

## Projects

### worldssimplestdb.console
Interactive and command-line database application supporting all four versions.

### worldssimplestdb.filldata
Utility application for filling databases with test data for performance testing.

## Building

This project requires .NET 8.0 or later.

```bash
# Build all projects
dotnet build

# Build specific project
dotnet build worldssimplestdb.console
dotnet build worldssimplestdb.filldata
```

## Usage

### Interactive Mode

Run the console application without arguments to enter interactive mode:

```bash
cd worldssimplestdb.console
dotnet run
```

You'll be prompted to select a database version (V1-V4), then you can use the following commands:

- `set <key> <value>` - Store a key-value pair
- `get <key>` - Retrieve a value by key
- `help` - Show available commands
- `exit` or `quit` - Exit the program (flushes and cleans up for V4)

Example:
```
db> set name "John Doe"
OK
db> get name
John Doe
db> exit
Goodbye!
```

### Command-Line Mode

You can also use the console application from the command line:

```bash
# Set version explicitly
dotnet run -- --version v4 set name "John Doe"
dotnet run -- --version v4 get name

# Or use default version (V3)
dotnet run -- set name "John Doe"
dotnet run -- get name
```

### Filling Database with Test Data

Use the `filldata` application to populate a database with test data:

```bash
cd worldssimplestdb.filldata
dotnet run <version> <size> <minValueLen> <maxValueLen>
```

**Parameters:**
- `version`: Database version (`v1`, `v2`, `v3`, or `v4`)
- `size`: Target database size (e.g., `10mb`, `100mb`, `1gb`)
- `minValueLen`: Minimum value length in characters
- `maxValueLen`: Maximum value length in characters

**Examples:**
```bash
# Fill V4 database with 10MB of data, values between 50-200 characters
dotnet run v4 10mb 50 200

# Fill V3 database with 100MB of data, values between 10-100 characters
dotnet run v3 100mb 10 100
```

## File Structure

### V1/V2/V3 Database Files
- **V1**: `database.txt` (text format)
- **V2**: `database.bin` (binary format)
- **V3**: `database.bin` (binary format) + in-memory index

### V4 Database Files
- **SSTables**: `sstables/sstable_*.sst` (immutable sorted files)
- **WAL**: `wal.log` (Write-Ahead Log for crash recovery)
- Database files are stored in the solution root directory

## Technical Details

### V4 Implementation Details

#### Memtable
- Implemented as `SortedDictionary<string, string>` (Red-Black Tree internally)
- O(log n) insert and lookup operations
- Automatically flushes to disk when reaching a threshold

#### SSTable Format
- Magic number: `0x53535442` ("SSTB") for file validation
- Binary format with sorted key-value pairs
- Index at the end for binary search
- Atomic writes using temporary files and rename

#### Write-Ahead Log (WAL)
- Records all write operations before applying to memtable
- Replayed on startup for crash recovery
- Automatically reopened if closed during writes
- Explicitly deleted on application shutdown

#### Double-Buffering
- Two memtables: one active for writes, one immutable for flushing
- Non-blocking writes: new writes continue to new memtable during flush
- Background flush: old memtable flushes asynchronously without blocking

## Performance Characteristics

| Version | Write | Read | Startup | Memory | Best For |
|---------|-------|------|---------|--------|----------|
| V1 | O(1) | O(n) | Instant | Minimal | Prototyping, <1K entries |
| V2 | O(1) | O(n) | Instant | Minimal | 1K-10K entries |
| V3 | O(1) | O(1) | O(n) | O(n) | >10K entries, read-heavy |
| V4 | O(log n) | O(log n*m) | O(n) | O(m) | Write-heavy, crash recovery |

Where:
- `n` = number of entries
- `m` = number of SSTables (V4)

## License

This project is for educational purposes, demonstrating different database storage strategies and optimization techniques.

