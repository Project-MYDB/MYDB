# MYDB Server: Design Specification

| | |
|---|---|
| **Document ID** | MYDB-DD-03 |
| **Version** | 1.0 |
| **Author** | Hasnat Akram, NUM-BSCS-2023-01 |
| **Date** | 2026-11-08 |
| **Supersedes** | *MyDB: Server Design* (`MYDB_Server_Design.md`, proposal) |
| **Related documents** | MYDB-DD-01 Database Hierarchy Design · MYDB-DD-02 Storage Engine Design · MYDB-DD-04 WAL Design |

---

## 1. Introduction

### 1.1 Purpose

This document specifies how clients reach the MYDB engine. It covers the two binaries, the transports, the wire protocol, authentication, the per-connection session model, the server's event loop, the interactive client, and how the daemon is installed and run under systemd.

### 1.2 Scope

In scope: the `server`, `client` and `bin` modules, the `packaging` directory, and the engine entry points the server calls (`engine_get_user_salt`, `engine_login_response`, `engine_execute_sql`, `engine_logout`).

Out of scope:

| Topic | Document |
|---|---|
| Users, partitions, credentials on disk, access control | MYDB-DD-01 |
| What happens inside a statement | Execution engine and storage documents |
| Durability and recovery | MYDB-DD-04 |

### 1.3 Terms

| Term | Meaning |
|---|---|
| **Daemon** | `mydbd`, the process that owns the engine and serves clients |
| **Client** | `mydb`, the interactive terminal program |
| **Session** | The server's record of one client socket: network state only |
| **Connection** | The engine's record of one logged-in user: identity, partition, current schema |
| **`conn_id`** | Index of a connection in the engine's connection pool; the only link between a session and its connection |

---

## 2. Overview

### 2.1 Deployment model

MYDB runs as a client–server system. The engine lives in one long-running daemon process, and users reach it only through the wire protocol. The client binary contains no engine or storage code. That keeps the client small, keeps engine internals out of what is handed to users, and lets each side be rebuilt on its own as long as both agree on the protocol.

```
   mydb (client)                                   mydbd (daemon)
 ┌──────────────────┐   Unix socket or TCP    ┌──────────────────────────────┐
 │ REPL (readline)  │ ──────────────────────> │ listeners (Unix + TCP)       │
 │ client_conn      │ <────────────────────── │ sessions · auth · dispatch   │
 │ protocol, crypto │     MYDB wire protocol  │ poll() event loop            │
 └──────────────────┘                         │ engine (EngineState)         │
                                              └──────────────────────────────┘
```

### 2.2 Layers

| Layer | Name | Responsibility | Source |
|---|---|---|---|
| 8 | Service integration | systemd unit, system user, directories | `packaging/` |
| 7 | Client | connect, authenticate, REPL, print | `client/`, `bin/mydb.c` |
| 6 | Event loop | `poll()` over listeners and sessions; routes packets | `server/server.c`, `bin/mydbd.c` |
| 5 | Dispatch | hands SQL to the engine, returns the result | `server/dispatch.c` |
| 4 | Authentication | challenge–response exchange | `server/auth.c` |
| 3 | Sessions | per-socket network state | `server/session.c` |
| 2 | Protocol | packet framing, sequence numbers, full reads and writes | `server/protocol.c` |
| 1 | Listeners | Unix and TCP sockets: bind, listen, accept | `server/listener.c`, `server/tcp_listener.c` |

Each layer uses only the layer below it. Nothing above layer 2 calls `read()` or `write()`. Nothing above layer 1 knows which transport a socket came from.

### 2.3 Process model

The daemon is a single process with a single thread. One `poll()` loop serves every socket, and exactly one statement executes at a time. The engine therefore needs no locking for concurrent statements. Clients are concurrent in the sense that many can be connected at once, but their statements are serialised by the loop.

---

## 3. Binaries and Build Structure

### 3.1 Binaries

| Binary | Command | Purpose |
|---|---|---|
| `mydbd` | `mydbd init -u <username>` | Create a new engine (bootstrap), prompting twice for the root password |
| `mydbd` | `mydbd` | Open the engine and serve clients until stopped |
| `mydb` | `mydb connect -u <user> [-S <socket>]` | Connect over the Unix socket |
| `mydb` | `mydb connect -u <user> -h <host> [-p <port>]` | Connect over TCP |

### 3.2 Libraries and link rules

| Library | Contents | Linked into |
|---|---|---|
| `mydb_proto` | `protocol.c`: framing, `proto_socket_path` | both binaries |
| `crypto` | SHA-256, password hash, random bytes | both binaries |
| `mydb_server` | listeners, sessions, auth, dispatch, event loop | `mydbd` |
| `engine` and everything below it | the database engine | `mydbd` |
| `mydb_client` | connection, handshake, REPL | `mydb` |
| `readline` | line editing and history | `mydb` |

`mydb_proto` and `crypto` are the only code shared by both binaries, and `mydb_proto` has no dependency on the engine. The client binary links no engine, storage or parser code.

---

## 4. Configuration

All configuration comes from environment variables and command-line flags. There is no configuration file.

| Variable | Used by | Meaning | Default |
|---|---|---|---|
| `MYDB_HOME` | daemon, client | Engine root directory | `$HOME/.mydb` (daemon) |
| `MYDB_SOCKET` | daemon, client | Full path of the Unix socket | see §5.1 |
| `MYDB_TCP_BIND_ADDR` | daemon | IPv4 address the TCP listener binds | `0.0.0.0` |
| `MYDB_TCP_PORT` | daemon | TCP port | `4442` |
| `MYDB_INIT_USER` | install scripts | Name of the user created at bootstrap | `root` |

An invalid `MYDB_TCP_PORT` (non-numeric, or outside 1–65535) is ignored and the default is used.

The client's `-S/--socket` flag sets `MYDB_SOCKET` for that run, so it overrides the environment. `-h/--host` and `-S` cannot be combined. The client's TCP port defaults to 4442 to match the daemon.

---

## 5. Transports

The daemon always listens on both transports at once. Both produce ordinary stream socket descriptors, and every layer above the listeners treats them identically.

### 5.1 Unix domain socket

**Path resolution**, shared by daemon and client through `proto_socket_path`, highest priority first:

1. `$MYDB_SOCKET`, if set;
2. `$MYDB_HOME/mydb.sock`, if `MYDB_HOME` is set;
3. `/run/mydb/mydb.sock`.

`MYDB_SOCKET` lets a production install keep the data directory private while putting the socket in a directory every user can reach (§12.2).

**Listener behaviour:**

| Step | Detail |
|---|---|
| Path check | The path must fit in `sun_path` (at most 107 bytes) |
| Stale file | Any existing file at the path is removed before `bind` (left by a crash) |
| Bind and listen | `AF_UNIX`, `SOCK_STREAM`, backlog 8 |
| Permissions | The socket file is set to mode `0666` so clients running as other users can connect; database authentication is what controls access |
| Close | The listening descriptor is closed and the socket file removed |

### 5.2 TCP

| Step | Detail |
|---|---|
| Address | IPv4, from `MYDB_TCP_BIND_ADDR` (default `0.0.0.0`, all interfaces) |
| Port | `MYDB_TCP_PORT` (default 4442) |
| Options | `SO_REUSEADDR`, so a restarted daemon can rebind at once |
| Listen | backlog 8 |

The client resolves the host with `getaddrinfo` and tries each returned address in turn, so it accepts host names as well as IPv4 or IPv6 literals. The daemon itself listens on IPv4 only.

---

## 6. Wire Protocol

### 6.1 Framing

Every message is one packet: a 9-byte header followed by `length` bytes of payload.

| Offset | Size | Field | Encoding |
|---|---|---|---|
| 0 | 4 | `length` | payload size in bytes, big-endian (network order) |
| 4 | 1 | `type` | packet type, §6.2 |
| 5 | 4 | `seq_no` | sender's sequence number, big-endian |
| 9 | `length` | payload | type-specific |

The largest payload is 65 536 bytes (`PROTO_MAX_PAYLOAD`). A sender refuses to send anything larger. A receiver that reads a header announcing a larger payload treats it as a protocol error.

**Full transfers.** A single `read()` or `write()` on a socket may move fewer bytes than asked. The protocol layer loops until the whole header or payload has moved, retrying after `EINTR`. A return of zero bytes, meaning the peer closed, is an error.

### 6.2 Packet types

| Value | Type | Direction | Payload |
|---|---|---|---|
| 1 | `PKT_HANDSHAKE` | server → client | version string, `"MyDB 1.0"` (no terminator) |
| 2 | `PKT_AUTH_INIT` | client → server | username bytes (1–31, no terminator) |
| 3 | `PKT_AUTH_CHALLENGE` | server → client | `salt[16]` then `nonce[32]`, 48 bytes |
| 4 | `PKT_AUTH_RESPONSE` | client → server | username, one `\0`, then `response[32]` |
| 5 | `PKT_AUTH_OK` | server → client | empty |
| 6 | `PKT_AUTH_ERR` | server → client | empty |
| 7 | `PKT_QUERY` | client → server | SQL text (no terminator) |
| 8 | `PKT_RESPONSE` | server → client | result text exactly as the engine formatted it |
| 9 | `PKT_QUIT` | client → server | empty |

There is one response type for every statement result, whether rows, a DML count, a DDL acknowledgement or an error message. The engine formats the text and the client prints it as received.

### 6.3 Sequence numbers

Each side keeps two counters per connection, one for packets it sends and one for packets it expects to receive. Both start at 0 and increase by 1 per packet. A receiver that gets a packet whose `seq_no` differs from its expected value treats it as a protocol error. A missing, duplicated or reordered packet therefore ends the connection. A packet replayed from an earlier connection also fails, because its sequence number belongs to that connection's counter.

### 6.4 Error handling

The protocol has no error-recovery messages. When the server detects any violation, it drops the session:

- an I/O error or EOF;
- a sequence mismatch;
- an oversized payload;
- an unknown packet type;
- a packet that is not valid in the session's current state.

Dropping a session releases its engine connection (§10). The client treats any error while reading a response as a lost connection.

---

## 7. Sessions

### 7.1 Session record

```c
typedef struct {
    int          client_fd;              /* socket; -1 when the slot is free  */
    int          conn_id;                /* engine connection; -1 until login */
    uint32_t     send_seq, recv_seq;     /* §6.3                              */
    SessionState state;
    uint8_t      nonce[32];              /* current auth challenge            */
    bool         active;
} Session;
```

Sessions live in a fixed table of 32 slots (`MAX_SESSIONS`), matching the engine's 32 connection slots. A session holds only network state. The user, partition, current schema and transaction belong to the engine and are reached through `conn_id`.

### 7.2 States

| State | Entered when | Packets accepted |
|---|---|---|
| `CONNECTING` | socket accepted | none (the server sends the handshake at once) |
| `AUTHENTICATING` | handshake sent | `AUTH_INIT`, `AUTH_RESPONSE`, `QUIT` |
| `READY` | login succeeded | `QUERY`, `QUIT` |
| `BUSY` | a query is executing | none: the socket is not polled |

```
accept ──> CONNECTING ──handshake──> AUTHENTICATING ──AUTH_OK──> READY <-> BUSY
                                          │                         │
                                       AUTH_ERR                QUIT / error
                                          v                         v
                                       dropped                   dropped
```

`AUTH_INIT` may be repeated while authenticating; each one issues a new nonce and challenge. A `QUERY` before login, or an auth packet after login, drops the session. A fifth state, `CLOSING`, is defined but not used: `QUIT` drops the session directly.

### 7.3 Destroying a session

Destroying a session closes its socket and clears the slot, including the nonce. If the session had logged in, the server first calls `engine_logout(conn_id)` (§10.2).

---

## 8. Authentication

### 8.1 Credential model

For each user, the engine stores a random 16-byte salt and `h = SHA-256(salt || password)` (MYDB-DD-01 §4.2). The password itself is never stored and never sent. On the network the client proves that it can compute `h` by hashing it together with a single-use nonce chosen by the server.

### 8.2 Exchange

```
Server                                         Client
──────                                         ──────
PKT_HANDSHAKE "MyDB 1.0"           ───────>
                                   <───────    PKT_AUTH_INIT  username
salt  = stored salt
        (random if no such user)
nonce = 32 random bytes, kept in session
PKT_AUTH_CHALLENGE salt || nonce   ───────>
                                               password = getpass()
                                               h        = SHA-256(salt || password)
                                               response = SHA-256(nonce || h)
                                   <───────    PKT_AUTH_RESPONSE  user \0 response
engine_login_response(username, response, nonce)
  expected = SHA-256(nonce || stored h)
  compare, check is_active, open connection
wipe nonce
PKT_AUTH_OK  (conn_id kept, READY) ───────>
   or PKT_AUTH_ERR, then drop
```

### 8.3 Checks the server makes

| Packet | Rejected (session dropped) when |
|---|---|
| `AUTH_INIT` | not in `AUTHENTICATING`; username empty or 32 bytes or longer |
| `AUTH_RESPONSE` | not in `AUTHENTICATING`; no terminator, empty name or name 32 bytes or longer; total length not exactly name + 1 + 32 |

### 8.4 Properties

| Property | How it is provided |
|---|---|
| Password never on the wire | Only `SHA-256(nonce || h)` is sent |
| Captured responses cannot be replayed | The nonce is fresh per challenge, and the sequence counters restart per connection |
| Usernames cannot be probed | An unknown user receives a random salt and the same `PKT_AUTH_ERR` as a wrong password |
| Nonce does not linger | It is wiped as soon as verification returns, whatever the outcome, and again when the session is destroyed |
| No cryptography in the server layer | `auth.c` only moves packets and generates the nonce; hashing and comparison happen in the engine and the client |
| Plaintext password short-lived in the client | The client wipes the buffer returned by `getpass` as soon as it has computed `h` |

### 8.5 Engine side of login

On a successful verification, the engine opens a connection:

1. claim a connection slot;
2. if the user owns a partition, acquire its runtime context (loading it if it is not yet loaded);
3. record `last_login`;
4. return the slot index as `conn_id`.

A user with no partition of their own gets a connection with no partition attached. One is attached on their first `USE` of a schema they hold a grant for (MYDB-DD-01 §10.3–10.4). Login fails, with `PKT_AUTH_ERR`, if the connection pool is full or the partition cannot be loaded.

---

## 9. Query Dispatch

When a `READY` session sends `PKT_QUERY`, the server:

1. copies the payload into a buffer and terminates it;
2. marks the session `BUSY`;
3. calls `engine_execute_sql(conn_id, sql, result, 65 536)`;
4. marks the session `READY` again;
5. sends the result text as one `PKT_RESPONSE`.

The server sends `PKT_RESPONSE` whatever the return code, because the engine writes error messages into the same result buffer. Results longer than 65 535 bytes are cut off at that length by the engine.

Dispatch never parses SQL. Transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`), `USE`, DDL, DML and queries all go to the engine unchanged. On success the engine appends the execution time to the result, for example `(0.02s)`.

---

## 10. Event Loop and Connection Lifecycle

### 10.1 Loop

Each iteration does the following:

1. **Build the watch list.** Entry 0 is the Unix listener and entry 1 the TCP listener; both are always present. Then add one entry per active session that is not `BUSY`.
2. **`poll()`** with a 5-second timeout. A timeout or `EINTR` simply restarts the loop, which is how a stop request is noticed.
3. **New connections.** Accept on each listener that is readable, create a session and send the handshake. If the session table is full, close the new socket immediately.
4. **Ready sessions.** For each session that reports input, hangup or error, receive one packet and act on it (§10.3).

### 10.2 Ending a connection

| Cause | Action |
|---|---|
| `PKT_QUIT` | drop the session |
| EOF, I/O error, protocol violation | drop the session |
| Authentication failure | send `PKT_AUTH_ERR`, then drop |
| Server shutdown | drop every session |

Dropping a logged-in session calls `engine_logout(conn_id)`, which does three things:

1. roll back any open transaction on the connection's partition;
2. free the connection slot;
3. release the connection's reference on the partition. When the last reference goes, the partition is flushed, closed and unloaded (MYDB-DD-01 §9.4).

The server does no transaction or partition handling of its own.

### 10.3 Packet routing

| Packet | Handler |
|---|---|
| `PKT_AUTH_INIT` | `auth_handle_init` |
| `PKT_AUTH_RESPONSE` | `auth_handle_response` |
| `PKT_QUERY` | `dispatch_query` |
| `PKT_QUIT`, anything else | drop the session |

### 10.4 Signals and shutdown

`SIGINT` and `SIGTERM` set a flag that ends the loop at the next iteration, so the daemon stops within one poll timeout. `SIGPIPE` is ignored, so writing to a client that has gone away fails with an error instead of killing the daemon.

On stop, the daemon shuts down in this order:

1. drop every session, logging each out of the engine;
2. close both listeners and remove the socket file;
3. close the engine, which flushes and closes every loaded partition and then the engine-level files.

---

## 11. Client

### 11.1 Connecting

`mydb connect` opens either the Unix socket (path from §5.1) or a TCP connection. It then runs the handshake of §8.2, prompting for the password with `getpass`, which turns off terminal echo. If it cannot connect it says so (`ERROR: cannot connect to MyDB server`), and on a failed login it prints `ERROR: authentication failed`.

### 11.2 REPL

| Input | Behaviour |
|---|---|
| SQL text | Collected across lines (prompt `mydb>`, continuation `...`) until the buffer ends with `;`, then sent as one `PKT_QUERY` |
| `exit`, `quit`, `.exit`, `.quit` (optional `;`) | Leave the REPL |
| Ctrl-D | Leave the REPL |
| `clear` | Clear the terminal |
| `SOURCE <file>` or `\. <file>` | Read the file locally and send each `;`-terminated statement in turn, printing each result. The file is not sent to the server. |

Completed statements are added to readline history. A statement longer than 8 KB is discarded with a message. Each response is printed exactly as received. When the REPL ends, the client sends `PKT_QUIT` and closes the socket.

---

## 12. Deployment

### 12.1 Development install

`build.sh` builds without root:

1. symlink `mydbd` and `mydb` into `~/.local/bin`;
2. on first run, bootstrap an engine in `$MYDB_HOME` (default `~/.mydb`) interactively.

The daemon is then run in the foreground with `mydbd`. Its socket is `~/.mydb/mydb.sock` when `MYDB_HOME` is set. `build.sh --clean` removes the build and the development data directory.

### 12.2 Production install

`packaging/install-service.sh` installs MYDB as a system service. It needs root and is safe to re-run.

1. Build the binaries.
2. Create the system user and group `mydb` (no login shell, home `/var/lib/mydb`).
3. Create `/var/lib/mydb` owned by `mydb`, mode `0750`.
4. Copy both binaries to `/usr/local/bin`.
5. Install `mydbd.service` to `/etc/systemd/system` and reload systemd.
6. If the engine does not exist yet, bootstrap it **as the `mydb` user**, prompting for the root password.
7. Enable and restart the service.

`--uninstall` stops and disables the service, then removes the unit, the binaries, the data directory and the system user.

**Unit settings:**

| Setting | Value | Purpose |
|---|---|---|
| `Type` | `simple` | the daemon runs in the foreground; systemd tracks its PID |
| `ExecStart` | `/usr/local/bin/mydbd` | |
| `Restart` / `RestartSec` | `on-failure` / 5 s | restart after a crash, not after a clean stop |
| `User` / `Group` | `mydb` | never runs as root |
| `MYDB_HOME` | `/var/lib/mydb` | data directory |
| `MYDB_SOCKET` | `/run/mydb/mydb.sock` | socket outside the private data directory |
| `RuntimeDirectory` | `mydb`, mode `0755` | systemd creates `/run/mydb` at start and removes it at stop |
| Output | journal, identifier `mydbd` | startup lines and diagnostics go to journald |

**File-system layout:**

| Path | Owner | Mode | Notes |
|---|---|---|---|
| `/usr/local/bin/mydbd`, `/usr/local/bin/mydb` | root | `0755` | copies, not symlinks |
| `/var/lib/mydb/` | `mydb` | `0750` | all engine data; clients never enter it |
| `/run/mydb/` | `mydb` | `0755` | traversable by everyone |
| `/run/mydb/mydb.sock` | `mydb` | `0666` | any local user can connect; login decides access |

With `MYDB_HOME` unset, the client's default socket path is `/run/mydb/mydb.sock`, so `mydb connect -u <user>` reaches the system service with no options.

### 12.3 Operations

| Task | Command |
|---|---|
| Status | `systemctl status mydbd` |
| Stop (clean, `SIGTERM`) | `systemctl stop mydbd` |
| Restart | `systemctl restart mydbd` |
| Logs | `journalctl -u mydbd -f` |

---

## 13. Responsibility Boundaries

| Concern | Owner |
|---|---|
| Socket creation, binding, socket-file cleanup | Listeners |
| Framing, sequence numbers, full reads and writes | Protocol |
| Per-socket state, nonce storage | Sessions |
| Challenge exchange, nonce generation, anti-enumeration | Authentication |
| Password hashing and verification | Engine and client (`crypto`) |
| Connection slots, partition loading and release | Engine |
| Rolling back on disconnect | Engine (`engine_logout`) |
| SQL parsing, execution, result formatting | Engine |
| Ordering of statements | Event loop (one at a time) |
| Process lifetime, restart, directories | systemd and the install script |

---

## 14. Capacity Limits

| Limit | Value |
|---|---|
| Concurrent sessions | 32 (`MAX_SESSIONS`) |
| Engine connections | 32 (`MAX_CONNECTIONS`) |
| Loaded partitions | 16 |
| Packet payload | 64 KB |
| Result text | 65 535 bytes |
| Statement typed in the client | 8 KB |
| Username | 31 bytes |
| Unix socket path | 107 bytes |
| Listen backlog | 8 per listener |

---

## 15. Known Limitations

| # | Limitation | Consequence |
|---|---|---|
| L1 | **Socket reads and writes block the only thread.** The loop waits for a whole packet once a session becomes readable, and for a whole response to be written. | One peer that sends part of a packet and stops, or that never reads its responses, freezes the server for every client until that peer disconnects. Authentication is not needed to do this, and the TCP listener is open on all interfaces by default. Non-blocking sockets with per-session buffers remove the problem. |
| L2 | **Transaction state belongs to the partition, not the connection.** All sessions attached to one partition share one transaction manager. | A `BEGIN` in one session pulls other sessions' autocommit statements into that transaction. A `ROLLBACK` or a logout in any session undoes them all, including writes another session was told had succeeded. Each connection needs its own transaction context. |
| L3 | **No transport encryption.** | Over TCP, SQL text and results cross the network in clear, and an established session can be hijacked by anyone on the path. Authentication itself never exposes the password. |
| L4 | **The stored hash is enough to log in.** | Anyone who can read `users.mydb` can answer a challenge without knowing the password. This is inherent in the challenge–response scheme over a stored hash. |
| L5 | **No timeouts.** | An unauthenticated or idle session holds a slot indefinitely. Combined with L1, a slow peer is especially costly. |
| L6 | **Full session table closes new sockets silently.** | The client sees the connection close with no explanation. |
| L7 | **Results are cut off at 64 KB without notice.** | A large `SELECT` returns partial output that looks complete. |
| L8 | **No version negotiation.** The client ignores the handshake string. | A protocol change between client and server versions shows up as a broken connection rather than a clear error. |
| L9 | **The TCP listener cannot be turned off and is IPv4 only.** | A deployment that wants local access only must bind it to `127.0.0.1` with `MYDB_TCP_BIND_ADDR`. |

---

## Appendix A: Changes from the Proposal

| Area | Proposal | Current design |
|---|---|---|
| Daemon binary | `mydb-server`, `src/main_server.c` | `mydbd`, `bin/mydbd.c`; bootstrap is `mydbd init -u <user>` |
| Client binary | `src/main_client.c` | `bin/mydb.c`; command `mydb connect` with `-S`, `-h`, `-p` |
| Transports | Unix socket only; TCP deferred | Unix socket and TCP, both always listening |
| Socket permissions | not specified | socket file `0666`; `/run/mydb` `0755` |
| systemd unit | `mydb.service`, runtime directory `0750` | `mydbd.service`, runtime directory `0755`, `MYDB_SOCKET` set |
| Install | manual command list | `packaging/install-service.sh` (and `--uninstall`); `build.sh` for development |
| Login entry point | `engine_login(eng, user, response, nonce)` | `engine_login_response(...)`; `engine_login` is the local plaintext path |
| Partition attachment | per-partition `SubConnPool` | reference count on the partition context, maintained by the engine |
| Client extras | none | readline history, multi-line input, `SOURCE` files, `clear` |
| Distributed routing note | coordinator sketch | not part of the design |
