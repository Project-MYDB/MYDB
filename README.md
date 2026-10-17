# MYDB

MYDB is a partition-isolated relational database that aims to provide the
isolation and performance of running a separate database instance per user,
without the memory cost of extra instances or servers, all behind one interface.

Each user gets a partition: their own data directory, disk quota, buffer pool
and transactions, so one user's workload never touches another's. All partitions
live inside a single `mydbd` daemon and are reached through the same SQL
interface and connection, so you manage one server instead of many.

Underneath, MYDB has its own SQL lexer and parser, a cost-based query planner, an
execution engine, and a storage layer modeled on InnoDB that keeps every table as
a B+ tree clustered on its primary key. Clients reach the daemon over a Unix
socket or TCP.

The storage side is C11. The parser, execution engine and command-line client are
C++17. It is built and tested on Linux (Fedora).

## Contents

- [Features](#features)
- [Current work](#current-work)
- [Getting started](#getting-started)
- [Current SQL support](#current-sql-support)
- [Architecture](#architecture)
- [Client/server and protocol](#clientserver-and-protocol)
- [Storage internals](#storage-internals)
- [Data types and constraints](#data-types-and-constraints)
- [Testing](#testing)
- [Project structure](#project-structure)
- [Known limitations](#known-limitations)
- [Future directions](#future-directions)
- [License](#license)

---

## Features

**Partitions as the unit of isolation.** Each user's data lives in its own
partition, with its own data directory, disk quota, catalog, schema cache and
transaction manager. Partitions share no data, so one user's tables and
transactions are never mixed with another's.

**Per-partition buffer pools.** Every partition has its own buffer pool, so a
heavy workload in one partition cannot evict another partition's pages. A
partition is loaded when its user first logs in and freed when their last
connection closes, so idle users cost no memory.

**One server, one interface.** All partitions run inside a single `mydbd`
process and are reached through the same SQL interface, over a Unix socket or
TCP. You run and manage one server instead of one per user.

**Cost-based query planner.** `ANALYZE TABLE` collects per-column statistics
(distinct counts, most common values, histograms). The planner uses them to pick
an access path for each table, a join order (System R style search), and a join
algorithm for each step: sort-merge, index nested loop, or hash join.

**InnoDB-style clustered storage.** Every table is a B+ tree clustered on its
primary key, stored in 16 KB slotted pages, with secondary B+ tree indexes for
`UNIQUE` columns and `CREATE INDEX`.

**Secure login.** Passwords are stored as salted SHA-256, and login is a
challenge-response exchange, so the password never crosses the wire.

**Clients.** A thin `mydb` REPL with no engine code inside it, and a Python
client, [`pymydb-client`](https://github.com/Hasnat056/pymydb-client), that
speaks the same protocol using only the standard library.

---

## Current work

`main` holds the released engine described in this README. Work in progress
lives on other branches and is merged into `develop` before it reaches `main`.

**Write-ahead log (branch `wal`, PR #6 into `develop`).** This is the main
effort right now. MYDB does not have crash recovery yet: a crash in the middle
of a write can leave a table file inconsistent. The `wal` branch adds a WAL
module (`wal/`) that every change will go through before it touches a data
page. Each partition gets its own WAL, matching the per-partition buffer pools.
What is built so far:

- A log made of 2 MB segment files, split into 4 KB pages, each page with
  a small header and a CRC32 checksum.
- An LSN that is a real position in the log (segment number times 2 MB, plus
  the byte offset in the segment file), so any LSN tells you exactly where a
  record lives on disk.
- An in-memory ring buffer that appenders write into under one lock, and a
  dedicated Flusher thread that copies full pages out of the ring, writes them
  to the current segment and calls `fdatasync`. A caller can wait until its
  record's LSN is durable.
- Records that span pages and segments, marked with continuation flags in the
  page headers, so there is no fixed upper limit on record size.

Still to come: logging the actual row changes from the partition manager,
ARIES-style recovery at startup, checkpoints, and protection against torn data
pages.

**Schema metadata cache (on `develop`, not yet on `main`).** The per-partition
schema cache was reworked in four phases: table and schema IDs are now stored
on disk, `RelationDef` entries can be evicted and pinned, schema metadata uses
a write-back flush model, and the partition catalog is folded into the same
cache.

---

## Getting started

### Prerequisites

- Linux (tested on Fedora)
- CMake 3.20 or newer
- GCC or Clang with C11 and C++17 support
- `readline` headers (`readline-devel` on Fedora, `libreadline-dev` on
  Debian/Ubuntu)

### Build

```bash
./build.sh
```

This configures and builds with CMake, symlinks `mydbd` and `mydb` into
`~/.local/bin`, and on the first run bootstraps the engine with `mydbd init`,
which asks you for the root password.

```bash
./build.sh --clean   # wipe build/ and $MYDB_HOME, then rebuild from scratch
```

### Run as a systemd service

```bash
sudo packaging/install-service.sh
sudo systemctl start mydbd
journalctl -u mydbd -f
```

On first install, the script also bootstraps the engine and asks for the root
password. The service runs as a dedicated `mydb` system user. Data lives in
`/var/lib/mydb` (mode `0750`) and the socket at `/run/mydb/mydb.sock`
(mode `0666`, since database login still controls access).

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MYDB_HOME` | `~/.mydb/` | Engine data directory |
| `MYDB_SOCKET` | `$MYDB_HOME/mydb.sock`, else `/run/mydb/mydb.sock` | Unix socket path |
| `MYDB_TCP_BIND_ADDR` | `0.0.0.0` | TCP listen address |
| `MYDB_TCP_PORT` | `4442` | TCP listen port |
| `MYDB_INIT_USER` | `root` | User bootstrapped on first install |

### Command line

```
mydb connect -u <user> [-S|--socket <path>]   connect over the Unix socket
mydb connect -u <user> -h|--host <host> [-p|--port <port>]
                                              connect over TCP (default port 4442)
```

`--socket` and `--host` cannot be used together.

---

## Current SQL support

**Statements.** `CREATE`/`DROP DATABASE`, `CREATE`/`DROP TABLE`, `CREATE INDEX`,
`CREATE`/`DROP`/`ALTER USER`, `SHOW DATABASES`, `SHOW TABLES`, `ANALYZE TABLE`,
`INSERT` (single and multi-row), `UPDATE`, `DELETE`, and `SELECT` with `WHERE`,
`GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, the five standard
aggregates, and `INNER`, `LEFT`, `RIGHT` and `FULL OUTER` joins (explicit or
comma-style, any number of tables, with aliases). `BEGIN`, `COMMIT` and
`ROLLBACK` for transactions.

**Constraints.** `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY` with
`ON DELETE SET NULL | RESTRICT | CASCADE`, `AUTO_INCREMENT`, `DEFAULT`.

Data types and how each constraint is checked are listed under
[Data types and constraints](#data-types-and-constraints).

### SELECT

```
SELECT [DISTINCT] col [AS alias] {, col [AS alias]}*  |  *
  FROM table [alias] {, table [alias]}*
  [INNER|LEFT|RIGHT|FULL OUTER JOIN table [alias] ON col = col]*
  [WHERE expr]
  [GROUP BY col {, col}*]
  [HAVING expr]
  [ORDER BY col [ASC|DESC] {, col [ASC|DESC]}*]
  [LIMIT n [OFFSET m]]
```

Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`, each with
optional `DISTINCT`. Operator precedence, high to low: `NOT`, comparisons
(`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`), `IS [NOT] NULL`, `[NOT] BETWEEN`,
`[NOT] IN (…)`, `[NOT] LIKE`, `AND`, `OR`. Columns can be written as
`table.column` anywhere a column name is accepted.

### CREATE TABLE

```sql
CREATE TABLE name (
    col type_spec [col_constraint]*
    [, col type_spec [col_constraint]*]*
    [, table_constraint]*
);
```

`type_spec`: `INT | INTEGER | DECIMAL[(p,s)] | VARCHAR[(n)] | ENUM(v,…) | BOOL |
BOOLEAN | DATE[(format)] | DATETIME`

Column constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`,
`AUTO_INCREMENT`/`AUTOINCR`, `DEFAULT value`.

Table constraints: `[CONSTRAINT name] PRIMARY KEY (col)`,
`FOREIGN KEY (col) REFERENCES table(col) [ON DELETE SET NULL|RESTRICT|CASCADE]`.

### Users

```sql
CREATE USER name IDENTIFIED BY 'password' [PARTITION p] [QUOTA nM|nG]
ALTER USER name IDENTIFIED BY 'new_password'
ALTER USER name SET QUOTA nM|nG
DROP USER name     -- root only; removes the user's partition directory
```

Quotas range from 100 MB to 5 GB.

---

## Architecture

A SQL string goes through the layers below before any bytes reach the disk.

```
  mydb / pymydb-client
    │  Unix socket or TCP
    ▼
  mydbd   listener · session · auth · dispatch · poll() loop
    │
    │  engine_execute_sql(conn_id, sql)
    │    1. parser_parse()         → AST
    │    2. build ExecContext      (engine, partition, connection, stats)
    │    3. exec_engine_execute()  → result text
    │    4. parser_free_ast()
    ▼
┌──────────────────────────────────────────────────┐
│  Engine (global)                                  │
│  DatabaseFile · SystemSchema · StatsBuffer        │
│  ConnectionPool (up to 32 connections)            │
└───────────────────────┬──────────────────────────┘
                        │  each connection is routed to its user's partition
            ┌───────────┴─────────────┐
            ▼                         ▼
  ┌───────────────────┐     ┌───────────────────┐
  │ PartitionCtx      │     │ PartitionCtx      │   one per active user,
  │  Catalog          │     │  Catalog          │   shared by all of that
  │  PartitionBuffer  │     │  PartitionBuffer  │   user's connections
  │  TxnManager       │     │  TxnManager       │
  │  StorageEngine    │     │  StorageEngine    │
  │   └ BufferPool    │     │   └ BufferPool    │
  └───────────────────┘     └───────────────────┘
```

Layer stack, top to bottom:

| Layer | Directory | Language | Job |
|-------|-----------|----------|-----|
| Query parser | `query_parser/` | C++17 | Lexer, parser, AST |
| Execution engine | `execution_engine/` | C++17 | Walks the AST, runs DDL/DML/DQL/TCL through `pm_*` |
| Query planner | `planner/` | C11 | Access paths, join order, join algorithms |
| Partition manager | `partition_manager/` | C11 | Per-user state, constraints, quota, transactions, stats |
| Storage engine | `storage_engine/` | C11 | Stateless B+ tree API, buffer pool, pages, disk I/O |

### Query parser

A `Lexer` turns the SQL text into tokens (keywords are uppercased, identifiers
keep their case) and a recursive-descent `Parser` builds an `ASTNode` tree. 64
keywords are reserved. 22 others, such as `TABLE`, `INDEX` and `KEY`, can still
be used as table or column names.

The parser has two faces. `parser_api.h` is a C header the engine uses: to the
engine the AST is an opaque handle it creates and frees but never reads.
`AST.hpp` is the C++ header the execution engine includes, because it has to walk
the tree.

### Execution engine

The executor dispatches each statement to `ddl.cpp`, `dml.cpp`, `dql.cpp` or
`tcl.cpp`. It never calls the storage engine directly; it calls the partition
manager's `pm_*` functions, which are the one place that checks constraints and
quota and keeps statistics up to date.

For a single-table `SELECT`, `extract_sargs` turns the `WHERE` clause into a list
of simple predicates, `planner_choose_path` picks the cheapest access path, and
`plan_to_ap` maps that choice to a `pm_*` lookup or cursor. Joins go through
`exec_join_select`, which asks the planner for a join order and then runs a
left-deep join, one step at a time.

### Query planner

For one table, the planner first tries a few shortcut rules (no `WHERE` means a
full scan, equality on the primary key means a primary key lookup, equality on a
unique index means an index lookup). If none applies it costs every candidate,
counting one sequential page read as 1 and a random read as 4:

| Access path | Cost |
|-------------|------|
| Full scan | `pages` |
| PK lookup | `height × 4` |
| PK range | `height × 4 + selectivity × pages` |
| Index lookup | `(height + 1) × 4` |
| Index range | `height × 4 + selectivity × rows × 4` |

Selectivity comes from the statistics written by `ANALYZE TABLE`: row count,
null count, distinct values, min and max, plus either a most-common-values list
(low-cardinality columns, `ENUM`, `BOOL`) or a 16-bucket equi-height histogram.
Before `ANALYZE TABLE` has run, the planner uses fixed guesses (5% for equality,
33% for a range).

For joins, a bitmask dynamic program over left-deep orders picks the cheapest
order for each group of inner joins. Outer joins act as barriers that keep their
written position. For each step the planner then chooses sort-merge when both
join columns are indexed, index nested loop when only the inner one is, and hash
join otherwise.

### Partition manager

Every logged-in user gets a `PartitionCtx`, loaded on first use and freed when
that user's last connection logs out. It holds:

- **Catalog** (`__catalog.mydb`): the partition's schemas and its quota usage.
- **PartitionBuffer**: an LRU cache of up to 8 open schema files, so switching
  between schemas does not reopen them each time.
- **TransactionManager**: `BEGIN`, `COMMIT`, `ROLLBACK`.
- **StorageEngine**: an embedded struct with its own buffer pool.

Before a write reaches storage, the `pm_*` functions check `NOT NULL`, `UNIQUE`
(through the secondary index) and foreign keys (the referenced row must exist,
and `ON DELETE` actions run when a referenced row is deleted). After the write
they update row counts and quota usage.

### Storage engine

The storage engine is a library with no global state: every call takes the
`StorageEngine*` that belongs to a partition. Each table is a B+ tree clustered
on its primary key, with full rows in the leaves and no separate heap file.
`UNIQUE` columns and `CREATE INDEX` get secondary B+ trees that map a column value
to the primary key.

All page access goes through a 64-frame LRU buffer pool with dirty-page tracking.
Below that, the disk manager does all I/O with `pread`/`pwrite` at offset
`page_number × 16384`, one `.mydb` file per table. The engine never loads a whole
file into memory.

### Engine

The engine (`engine/`) owns the global pieces: the database registry
(`__database.mydb`), the user and privilege tables in `system_schema/`, the
connection pool and the statistics file handles. `engine_execute_sql` is the only
way into the SQL pipeline. It checks that the connection is logged in, parses,
builds the `ExecContext`, runs the executor and frees the AST. The engine also
handles challenge-response login, logout (which rolls back an open transaction),
and the `mydbd init` bootstrap.

`users.mydb` and `privileges.mydb` are small fixed-layout files. They skip the
buffer pool and use direct `pread`/`pwrite` with an in-memory hash map.

### On-disk layout

```
$MYDB_HOME/                            (default ~/.mydb/)
├── __database.mydb                    engine registry: partitions and users
├── system_schema/                     engine-level, outside any partition
│   ├── users.mydb
│   ├── privileges.mydb
│   └── stats/
│       └── stats_<id>_<schema>.mydb   planner statistics, one file per schema
├── root/                              the root user's partition
│   ├── __catalog.mydb                 schemas and quota
│   └── <schema>/
│       ├── __schema.mydb              table definitions
│       └── <table>.mydb               B+ tree pages
└── <other partitions>/...
```

Every `.mydb` file starts with the same header and the magic number `0x4D594442`.

---

## Client/server and protocol

`mydbd` runs a single-threaded `poll()` loop over both listeners and all open
sessions. Many clients can be connected at once, from the same or different
users, but one statement runs at a time, so no locking is needed yet. While a
statement runs, that session is marked busy. When a client disconnects, its open
transaction is rolled back.

Both transports share the same session, auth and protocol code. The protocol
(`server/src/protocol.c`, built as the small `mydb_proto` library used by both
binaries) frames every message with a 9-byte header:

```
┌───────────────┬──────────┬──────────────┐
│ length (4 B)  │ type (1) │ seq_no (4 B) │   network byte order
└───────────────┴──────────┴──────────────┘
│ payload (length bytes)                  │
```

Each direction has its own sequence number, and a mismatch closes the connection.

| Packet | Direction | Purpose |
|--------|-----------|---------|
| `PKT_HANDSHAKE` | server → client | protocol version |
| `PKT_AUTH_CHALLENGE` | server → client | 16-byte salt and 32-byte nonce |
| `PKT_AUTH_RESPONSE` | client → server | `SHA-256(nonce ‖ SHA-256(salt ‖ password))` |
| `PKT_AUTH_OK` / `PKT_AUTH_FAIL` | server → client | login result |
| `PKT_QUERY` | client → server | SQL text |
| `PKT_RESPONSE` | server → client | formatted result |
| `PKT_QUIT` | client → server | clean disconnect |

The server sends a fresh nonce on every login and zeroes it right after the
check. An unknown username gets a fake salt, so the reply does not reveal whether
the account exists. All result formatting happens in the daemon; clients print
the response text as they receive it.

The Unix socket path is `$MYDB_SOCKET` if set, otherwise `$MYDB_HOME/mydb.sock`,
otherwise `/run/mydb/mydb.sock`. The socket is created with mode `0666`.

---

## Storage internals

### Page layout (16 KB, InnoDB style)

```
offset 0
┌──────────────────────────────────┐
│ Page header (38 B)               │  checksum, page_no, prev/next, LSN, type, space ID
├──────────────────────────────────┤ 38
│ Infimum + supremum (26 B)        │  two fixed boundary records
├──────────────────────────────────┤ 64
│ User records                     │  grow downward
│                                  │
│ (free space)                     │
│                                  │
│ Page directory                   │  2-byte slots, grow upward
├──────────────────────────────────┤ 16376
│ Page trailer (8 B)               │  old checksum + low 32 bits of LSN
└──────────────────────────────────┘ 16384
```

### Record header (5 bytes)

| Field | Size | Meaning |
|-------|------|---------|
| Info flags | 1 B | bit 0 deleted, bit 1 min-record |
| Owned count | 1 B | records owned by this directory slot |
| Heap no | 13 bits | insertion order (infimum 0, supremum 1, rows from 2) |
| Record type | 3 bits | 0 row, 1 internal node, 2 infimum, 3 supremum |
| Next offset | 2 B | relative offset of the next record |

### Hidden columns

| Column | Size | Present |
|--------|------|---------|
| `DB_TRX_ID` | 6 B | always |
| `DB_ROLL_PTR` | 7 B | always |
| `DB_ROW_ID` | 6 B | only when the table has no primary key |

A record ID (RID) is `(page_number, slot_number)`.

---

## Data types and constraints

| Type | Storage | Notes |
|------|---------|-------|
| `INT` | 4 B | 32-bit signed |
| `DECIMAL(p,s)` | 8 B | fixed point, default `DECIMAL(10,2)` |
| `VARCHAR(n)` | 2 B length + n B | default and maximum `VARCHAR(150)` |
| `ENUM(a,b,…)` | 1 B | index into the value list |
| `BOOL` | 1 B | `TRUE`/`FALSE` or `1`/`0` |
| `DATE` | 4 B | stored as `YYYYMMDD`, shown as `DD-MM-YYYY` unless another format is given |
| `DATETIME` | 8 B | stored as `YYYYMMDDHHmmSS`, shown as `YYYY-MM-DD HH:MM:SS` |

Column modifiers: `DEFAULT <value>` (any type, and `DEFAULT NOW` for
`DATETIME`), `AUTO_INCREMENT` (only on an `INT` primary key), inline and named
`CONSTRAINT`s.

| Constraint | Checked on | How |
|------------|------------|-----|
| `PRIMARY KEY` | INSERT, UPDATE | not null, unique, clustered index |
| `NOT NULL` | INSERT, UPDATE | |
| `UNIQUE` | INSERT, UPDATE | secondary index lookup |
| `FOREIGN KEY` | INSERT, UPDATE | referenced row must exist |
| `ON DELETE` | DELETE of a referenced row | `SET NULL`, `RESTRICT` or `CASCADE` |

---

## Testing

```bash
cd build
ctest --output-on-failure
```

All 15 test binaries pass on `main`.

| Library | Tests |
|---------|-------|
| `storage_engine` | `test_disk_manager`, `test_page`, `test_buffer_pool`, `test_btree`, `test_file_header`, `test_relation_def` |
| `partition_manager` | `test_transaction`, `test_partition`, `test_schema_file` |
| `engine` | `test_database_file`, `test_crypto` |
| others | `test_system_schema`, `test_parser`, `test_join_order`, `test_joins` |

`test_joins` runs JOIN queries end to end through the parser, planner, executor
and storage. The older end-to-end tests for the full engine lifecycle and login
were written against the pre-partition-manager API and were removed. A rewrite
against the current API is pending, so that path is covered only by manual
testing for now.

---

## Project structure

```
MyDB-Database-Engine/
├── query_parser/        lexer, parser, AST (C++17)
├── execution_engine/    DDL / DML / DQL / TCL handlers (C++17)
├── planner/             access paths, join order, join algorithms
├── partition_manager/   per-user state: catalog, schema cache, transactions,
│                        statistics, constraint checks, pm_* API
├── storage_engine/      stateless B+ tree storage: disk manager, pages,
│                        buffer pool, btree, relation definitions
├── system_schema/       users.mydb and privileges.mydb
├── engine/              orchestrator: global state, connections, SQL pipeline
├── crypto/              SHA-256 and salting, shared by engine and client
├── server/              mydbd: listeners, protocol, sessions, auth, dispatch
├── client/              mydb: connection and REPL (no engine code)
├── bin/                 mydbd.c and mydb.c entry points
├── packaging/           systemd unit and install script
└── tests/               ctest binaries, grouped by library
```

---

## Known limitations

| Area | Status |
|------|--------|
| Crash recovery / WAL | In progress on the `wal` branch (see [Current work](#current-work)). On `main`, a crash during a write can leave a table inconsistent. |
| Parallel execution | Not yet. Many clients can connect, but one statement runs at a time across the daemon. |
| `ALTER TABLE` | Not implemented. Use `DROP` and `CREATE`. |
| `GROUP BY` / aggregates in a JOIN query | Not supported yet. Each works on its own. |
| Aggregates inside `HAVING` | Not supported yet. `HAVING` takes plain column and literal predicates. |
| Index range scan with only an upper bound | Falls back to a full scan with a filter. |
| Cross-schema and cross-partition queries | Not supported. A connection has one active schema. |
| Views, triggers, stored procedures | Not implemented. |

---

## Future directions

These are the longer-term goals for MYDB.

**ARIES recovery, per partition and in parallel.** Build crash recovery on the
per-partition WAL following ARIES: an analysis pass, a redo pass that repeats
history, and an undo pass that rolls back unfinished transactions. Since every
partition has its own log and its own data, a partition is also the unit of
recovery: partitions can be recovered in parallel, and one damaged partition
does not hold the others back.

**Process-thread hybrid model.** Replace today's single-threaded loop, where one
statement runs at a time, with a hybrid of processes and threads, so statements
in different partitions run in parallel while partitions stay isolated from each
other.

**Resizable buffer pools.** Today every partition gets a fixed 64-frame buffer
pool. The goal is to grow and shrink each pool while the engine is running,
giving memory to the partitions that are busy and taking it back from the ones
that are idle.

**Learned predictability for resizing.** Instead of only reacting after a pool
is already under pressure, learn each partition's workload pattern over time and
use it to predict how much memory it will need next, so pools are resized ahead
of demand.

**PostgreSQL compatibility.** Speak enough of the PostgreSQL wire protocol and
SQL dialect that existing PostgreSQL clients, drivers and tools can connect to
MYDB and run their queries without changes.

**Backups and replication.** Online backups that do not stop the daemon, and
replication that streams the write-ahead log to standby servers so they can
take over if the primary fails.

---

## License

MIT. See [LICENSE](LICENSE).
