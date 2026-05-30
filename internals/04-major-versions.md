# 04 — Major Versions: Redis 7.x → Valkey 8.x → Valkey 9.x

> Based on the Valkey 9.1 codebase, release notes (00-RELEASENOTES), and git history.

---

## Table of Contents

1. [Project History](#1-project-history)
2. [Version Constants](#2-version-constants)
3. [Redis OSS 7.2.x — Baseline](#3-redis-oss-72x--baseline)
4. [Valkey 8.0 — First Release](#4-valkey-80--first-release)
5. [Valkey 8.1 — Hashtable Redesign](#5-valkey-81--hashtable-redesign)
6. [Valkey 9.0 — Major Feature Release](#6-valkey-90--major-feature-release)
7. [Valkey 9.1 — Lua Module, ACL, IO Threads](#7-valkey-91--lua-module-acl-io-threads)
8. [Feature Comparison Matrix](#8-feature-comparison-matrix)
9. [Backward Compatibility Breaks](#9-backward-compatibility-breaks)
10. [Deprecations](#10-deprecations)
11. [Upgrade Path Recommendations](#11-upgrade-path-recommendations)

---

## 1. Project History

Valkey originated as a fork of **Redis OSS 7.2.4** in March 2024, following Redis Ltd's license change from BSD to a dual-license (SSPL/RSAL) model. The early commits systematically:

- Renamed all `redis` references to `valkey`
- Replaced `RedisModule` with `ValkeyModule`
- Updated the COPYING file to BSD 3-clause
- Changed terminology from master/slave to primary/replica
- Introduced the `extended-redis-compatibility` config

The first Valkey-branded release was **8.0.0** (September 2024).

---

## 2. Version Constants

From `src/version.h`:

| Constant | Value | Description |
|---|---|---|
| `VALKEY_VERSION` | "9.1.0" | Current Valkey version |
| `VALKEY_VERSION_NUM` | 0x00090100 | Numeric version |
| `REDIS_VERSION` | "7.2.4" | Compatibility cap — will never exceed 7.2.x |
| `REDIS_VERSION_NUM` | 0x00070204 | Compatibility cap numeric |
| `RDB_VERSION` | 80 | RDB file format version |
| `VALKEYMODULE_APIVER_1` | 1 | Module API version |

The `REDIS_VERSION` constants are kept for compatibility with client libraries that check `redis_version` in the INFO response. The `extended-redis-compatibility` config (default off) makes Valkey report itself as "redis" in INFO, LOLWUT, and error messages.

---

## 3. Redis OSS 7.2.x — Baseline

The codebase Valkey forked from. Key features present at fork:

| Feature | Status in 7.2 |
|---|---|
| RESP3 protocol | Supported |
| Functions API | Supported (replacing EVAL-based scripting) |
| ACL with categories | Supported |
| Active defragmentation | Supported (jemalloc only) |
| Multi-part AOF (MP-AOF) | Supported |
| kvstore for cluster per-slot dictionaries | Supported |
| Dual-channel replication | Experimental |
| IO threads | Original design (with locks) |
| Terminology | Mixed (master/slave + primary/replica transition) |
| Lazyfree defaults | `no` for all lazyfree configs |

---

## 4. Valkey 8.0 — First Release (September 2024)

**Theme**: Stability, branding, compatibility with Redis OSS 7.2.4.

### Behavioral Changes

| Change | Impact |
|---|---|
| **Lazyfree defaults changed to YES** | All `lazyfree-lazy-*` configs now default to `1` (eviction, expire, server-del, user-del, user-flush). This was a significant behavioral shift from Redis defaults. |
| **Replica flush after RDB validation** | Replicas now flush old data *after* checking RDB file is valid, preventing partial data loss on corrupt RDB. |
| **Primary/replica terminology** | All references changed from master/slave to primary/replica throughout logs, CLI, config, tests, and error messages. |

### Performance

- SUNION/SDIFF optimization: 41% improvement for SUNION, 27% for SDIFF (hashtable as default for temp set objects)

### New Config

- `extended-redis-compatibility` (default off) — makes Valkey report itself as "redis" in INFO, LOLWUT, error messages

### Bug Fixes

- Fixed async IO threads crash with TLS
- Fixed AOF incorrectly disabled after loading RDB data
- Fixed cluster config file not saved before shutdown
- Fixed replica unable to trigger migration when receiving CLUSTER SETSLOT in advance

### Compatibility

Fully compatible with Redis OSS 7.2.4 data files and client protocols.

---

## 5. Valkey 8.1 — Hashtable Redesign (March 2025)

**Theme**: New hashtable implementation, performance improvements, cluster reliability.

### Major Architectural Change — New Hashtable

Valkey 8.1 introduced a **completely new hashtable** data structure replacing the legacy `dict` across all data types:

- Keys and expires kvstore
- Pubsub channels
- Command tables
- Hash datatype
- Sets datatype
- Sorted set datatype

The new hashtable is more memory-efficient and cache-friendly, with **prefetching-accelerated iterators**. The old `dict` was reduced to a thin wrapper.

**Why it matters**: This is the most significant internal data structure change in the codebase. It affects memory footprint, iteration speed, and rehashing behavior.

### New Commands

| Command | Description |
|---|---|
| `SET IFEQ` | Conditional update — sets key only if current value matches specified comparison value |
| `BGSAVE CANCEL` | Cancel an in-progress BGSAVE |

### Performance & Efficiency

- x86 SIMD optimization for BITCOUNT
- Hash value embedded in hash data type entries to reduce memory footprint
- Free strings during BGSAVE/BGAOF rewrite to reduce copy-on-write
- Active defrag refactored to reduce latencies
- `fast_float` optionally replaces `strtod`
- TLS improvements with IO threads
- IO threads max increased to 256
- Hash table iterator with prefetching

### Cluster Improvements

- `cluster-manual-failover-timeout` config for manual failover timeout control
- TCP_NODELAY enabled by default for engine-initiated cluster and replication connections
- Epoch broadcast ASAP when configEpoch changes

### Module API

- New flag to bypass command validation to reduce processing overhead for modules
- `availability_zone` added to HELLO response

### Observability

- LATENCY LATEST extended with sum/cnt stats
- `paused_actions` and `paused_timeout_milliseconds` for INFO CLIENTS
- `paused_reason` added to INFO CLIENTS
- Client capabilities shown in CLIENT LIST / CLIENT INFO
- Latency stats around cluster config file operations

### Bug Fixes

- ACL LOAD crash on replica fixed
- RANDOMKEY infinite loop during CLIENT PAUSE fixed
- rax crash with keys larger than 512MB fixed
- Replica disconnecting with TLS replication fixed
- Lua cjson unicode optimization removed to avoid OOM on large strings

---

## 6. Valkey 9.0 — Major Feature Release (October 2025)

**Theme**: Atomic slot migration, hash field expiration, multi-database cluster mode.

### Major New Features

#### 1. Atomic Slot Migration (ASM)

Complete redesign of cluster slot migration. Uses a child snapshot process to stream data atomically. Includes:
- Separate RDB snapshotting from migration
- COW size metrics
- State machine with formal export/import states
- Modules must explicitly opt-in via `VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION`

**Why it matters**: Traditional migration is slow and manual (key-by-key MIGRATE). ASM provides atomic, fast slot transfer with consistent state.

#### 2. Hash Field Expiration (HFE)

Individual hash fields can now have their own TTLs:

| Command | Description |
|---|---|
| `HPEXPIRE` | Set TTL on hash field (milliseconds) |
| `HPEXPIREAT` | Set absolute expiration on hash field |
| `HPTTL` | Get remaining TTL of hash field |
| `HPERSIST` | Remove expiration from hash field |
| `HSETEX` | Set hash field with expiration |

- Active expiry and active defrag for hash objects with volatile items
- New `keys_with_volatile_items` per-db tracking structure
- Per-slot volatile set storage within hash objects

#### 3. Multi-Database Cluster Mode

Cluster mode now supports multiple databases (numbered databases), removing the long-standing limitation of only DB 0 in cluster mode.

#### 4. Dual-Channel Replication (Stabilized)

Replica can use a separate RDB channel connection for full sync, allowing the main replication link to continue handling the command stream. Configurable via `dual-channel-replication-enabled`.

### New Commands

| Command | Description |
|---|---|
| `CLUSTERSCAN` | Cluster-wide key scanning across nodes. Supports MATCH patterns, slot-bounded scanning |
| `MSETEX` | Set multiple keys with shared expiration |
| `HGETDEL` | Get and delete hash fields atomically |
| `DELIFEQ` | Delete key if its value matches a specified string |
| `GEOSEARCH BYPOLYGON` | Polygon-based geographic search |
| `CLUSTER FLUSHSLOT` | Flush specific slots |
| `CLUSTER REPLICATE NO ONE` | Allow replicas to become primaries without data |

### New Configs

- Auto-failover on shutdown — unified config for triggering manual failover on SIGTERM/shutdown of a cluster primary
- `SHUTDOWN SAFE` — reject shutdown when unsafe situations exist
- `SHUTDOWN ABORT` — abort an ongoing shutdown
- `cluster-announce-client-port` / `cluster-announce-client-tls-port` — separate announced client ports

### Performance & Efficiency

- ARM NEON SIMD for hashtable findBucket()
- ARM NEON SIMD for BITCOUNT
- AVX512 optimization for string-to-integer conversion (string2ll with IFUNC resolver)
- Optimized pipelining: parsing and prefetching multiple commands
- Hashtable shrinking in low memory situations
- Optimized WATCH with early length check
- Optimized GEORADIUS with pre-allocated buffer
- Optimized ZCOUNT combining rank calculation with search
- SCAN/SSCAN/HSCAN/ZSCAN optimized by replacing list with vector
- Skiplist random level generation optimized
- Direct response writing to clients
- System responsiveness improved by limiting new cluster link connections per cycle

### Security Fixes (RC3)

| CVE | Description |
|---|---|
| CVE-2025-49844 | Lua script RCE |
| CVE-2025-46817 | Lua integer overflow / potential RCE |
| CVE-2025-46818 | Lua script execution in another user's context |
| CVE-2025-46819 | Lua out-of-bound read |

### Build/Tooling

- LTTNG-based tracing support
- RDB analysis reports
- RPS control for valkey-benchmark
- RDMA support for valkey-cli and benchmark
- MPTCP support
- `--sequential` option for valkey-benchmark
- Valkey-cli word-jump navigation

---

## 7. Valkey 9.1 — Lua Module, ACL, IO Threads (May 2026)

**Theme**: Lua as module, database-level ACL, IO threads lock-free redesign.

### Major Architectural Changes

#### 1. Lua Scripting Engine as a Module

The Lua engine has been **extracted from the server core** and implemented as a Valkey module (`src/modules/lua/`). It is statically linked by default.

- New `lua-enable-insecure-api` config (protected) to control deprecated Lua APIs
- Lua scripting now loaded as a module — cleaner separation of concerns

#### 2. Database-Level Access Control (ACL)

ACL selectors can now be restricted to specific databases:

- New `alldbs` flag for users who need access to all databases
- New `VALKEYMODULE_ACL_LOG_DB` module API code for database permission failures
- Users without `alldbs` flag are restricted to specific databases

#### 3. IO Threads Redesign

Complete redesign of the IO threading communication model using **lock-free queues (MPSC)**:

- **8–17% throughput gain** over previous lock-based design
- New active time metrics for main thread and I/O threads

### New Commands (9.1)

| Command | Description |
|---|---|
| `MSETEX` | Multiple set with shared expiration |
| `HGETDEL` | Get and delete hash fields |
| `HSETEX` (enhanced) | NX/XX flags, always issues keyspace notifications after validation |

**Note**: `CLUSTER KEYSLOT` now available in standalone mode.

### Performance & Efficiency

| Improvement | Impact |
|---|---|
| IO threads lock-free queues | 8–17% throughput gain |
| Embedded string threshold increased 64→128 bytes | 30% GET throughput gain |
| ARM NEON SIMD for pvFind() in vset.c | 2–3x speedup |
| WATCH duplicate key check: O(N)→O(1) | Per-db hashtable for dedup |
| CLUSTERSCAN MATCH optimization | Uses specific slot when given |
| COB memory tracking | Improved with copy avoidance |
| ZSET memory | Embed element in skiplist |
| Small strings | Remove internal server object pointer overhead |
| Skiplist | Embed header for better query efficiency |
| Rehashing | Abort and swap tables if ht1 is very full |
| SREM/ZREM/HDEL | Pause auto shrink when deleting multiple items |
| COMMAND | Cached responses for better performance |
| Writable replicas | Asynchronous freeing of keys |
| XRANGE/XREVRANGE | Hot-path optimization |
| **Replica RDB reuse** | After disk-based full sync, replica can reuse RDB file as AOF preamble |

### New Features

- Automatic TLS reload
- TLS authentication using SAN URI (automatic client auth from certificate fields)
- Cross-node consistency for SCAN commands via configurable DB hash seed
- `cluster-config-save-behavior` option to control nodes.conf save behavior
- Failing to save cluster config file no longer exits the process
- Immediate failover if replica is best ranked
- Module command result callback

### Observability

- Cluster bus network traffic usage metric in bytes
- Cumulative metrics for active I/O threads and main thread usage
- Whole cluster info for INFO command
- `remaining_repl_size` in CLUSTER GETSLOTMIGRATIONS
- Dual-channel replication buffer memory in INFO MEMORY
- `rdb_transmitted` state in INFO replica state
- New INFO section for scripting engines
- JSON support for log-format config
- Server-side TLS certificate expiry tracking
- libbacktrace option for crash report backtraces

### Security Fixes

| CVE | Description |
|---|---|
| CVE-2026-23479 | Use-After-Free in unblock client flow |
| CVE-2026-25243 | Invalid Memory Access in RESTORE command |
| CVE-2026-23631 | Use-after-free during full sync with yielding Lua/function |

### Build/Tooling

- RPS histogram in valkey-benchmark
- `--warmup` and `--duration` parameters for valkey-benchmark
- Lazy loading of RDMA libs
- Atomic slot migration support in valkey-cli
- `fast_float` dependency replaced with pure C implementation (ffc)

### ZSET B+ Tree (In Progress)

PR 1 extracted skiplist into a dedicated module as groundwork for a B+ tree replacement. This is a significant architectural change that may appear in a future version.

---

## 8. Feature Comparison Matrix

| Feature | Redis 7.2 | Valkey 8.0 | Valkey 8.1 | Valkey 9.0 | Valkey 9.1 |
|---------|-----------|------------|------------|------------|------------|
| Lazyfree default | no | **yes** | yes | yes | yes |
| Primary/replica terminology | mixed | **yes** | yes | yes | yes |
| Extended Redis compat | N/A | **yes** | yes | yes | yes |
| New hashtable | no (dict) | no (dict) | **yes** | yes | yes |
| COMMANDLOG | no | no | **yes** | yes | yes |
| SET IFEQ | no | no | **yes** | yes | yes |
| BGSAVE CANCEL | no | no | **yes** | yes | yes |
| Atomic Slot Migration | no | no | no | **yes** | yes |
| Hash Field Expiration | no | no | no | **yes** | yes |
| Multi-DB Cluster | no | no | no | **yes** | yes |
| Dual-Channel Replication | experimental | yes | yes | **stabilized** | yes |
| RDB bio thread save | no | no | no | no | **yes** |
| Lua as module | no | no | no | no | **yes** |
| Database-level ACL | no | no | no | no | **yes** |
| IO threads lock-free | no | no | no | no | **yes** |
| CLUSTERSCAN | no | no | no | **yes** | yes |
| DELIFEQ | no | no | no | **yes** | yes |
| HPEXPIRE/HPTTL/HPERSIST | no | no | no | **yes** | yes |
| MSETEX | no | no | no | no | **yes** |
| HGETDEL | no | no | no | no | **yes** |
| SHUTDOWN SAFE/ABORT | no | no | no | **yes** | yes |
| Auto-failover on shutdown | no | no | no | **yes** | yes |
| TLS cert auto-auth | no | no | no | no | **yes** |
| Automatic TLS reload | no | no | no | no | **yes** |
| MPTCP support | no | no | no | yes | yes |
| RDMA support | no | no | no | no | **yes** |
| SIMD (ARM/x86) | limited | BITCOUNT | hashtable+BITCOUNT | extensive | vset+hashtable |
| Module ASM opt-in | N/A | N/A | N/A | **yes** | yes |
| Static module support | no | no | no | no | **yes** |

---

## 9. Backward Compatibility Breaks

| Change | Version | Impact |
|--------|---------|--------|
| **Lazyfree defaults changed from `no` to `yes`** | 8.0 | DEL, EXPIRE, eviction now async by default. May affect latency profiles and monitoring expectations. |
| **`redis` branding replaced with `valkey`** | 8.0 | Client libraries checking server name need updates. Mitigated by `extended-redis-compatibility`. |
| **`sds` type removed from libvalkey public API** | 8.0 | Client code using `sds` directly must migrate. |
| **`hiredis`/`hiredis-cluster` APIs renamed to `valkey*`** | 8.0 | Client migration required (prefix change, SSL→TLS, option struct changes). |
| **Multi-key command slot splitting removed** (DEL, EXISTS, MGET, MSET) | 8.0 | Clients must partition keys by slot themselves in cluster mode. |
| **`redisClusterConnect2`, `redisClusterAsyncConnect` removed** | 8.0 | Use `valkeyClusterConnectWithOptions` / `valkeyClusterAsyncConnectWithOptions`. |
| **Most deprecated commands un-deprecated then re-evaluated** | 9.0 RC2 | Commands like HMSET, ZADD variants restored but marked with history. |
| **Lua engine moved to module** | 9.1 | Lua scripting now loaded as a module. `lua-enable-insecure-api` config controls legacy APIs. |
| **ACL database restrictions** | 9.1 | Users without `alldbs` flag are restricted to specific databases. Existing ACL configs may need updates. |
| **IO threads lock-free redesign** | 9.1 | Internal threading model changed; modules using thread-sensitive behavior may need review. |
| **Module ASM opt-in required** | 9.0 | Modules must call `ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION)` or ASM is disabled in the cluster. |
| **RDB version bumped** | 9.0 | RDB files from 9.0 may not load on older versions. |

---

## 10. Deprecations

| Deprecated | Replaced By | Version |
|---|---|---|
| `VALKEYMODULE_POSTPONED_ARRAY_LEN` | `VALKEYMODULE_POSTPONED_LEN` | 8.0+ |
| `redis*` prefix in client libraries | `valkey*` prefix | 8.0+ |
| Legacy `tests/cluster` framework | `unit/cluster` | 8.0+ |
| `lua-enable-insecure-api` | (controls deprecated Lua functions) | 9.1 |
| `used_memory_lua` in INFO | `used_memory_vm_eval` | 8.0+ |
| Replication Backup events | (never fired in Valkey) | 8.0+ |
| `no-slowlog` module flag | `no-commandlog` | 8.1+ |
| `random` module flag | (silently ignored) | 8.0+ |

---

## 11. Upgrade Path Recommendations

### From Redis OSS 7.2.x to Valkey 8.0

**Low risk**: 8.0 is essentially Redis 7.2.4 with branding and lazyfree defaults changed.

1. Test with `extended-redis-compatibility yes` if client libraries check server name
2. Review `lazyfree-lazy-*` configs — they now default to `yes`
3. RDB files from 7.2.x load without issues
4. Client libraries: update from `redis*` to `valkey*` prefix

### From Valkey 8.0 to 8.1

**Medium risk**: New hashtable changes internal behavior.

1. Test memory footprint — new hashtable is more efficient but behavior may differ
2. Review `COMMANDLOG` configuration if slow command tracking is needed
3. Cluster: test failover with new `cluster-manual-failover-timeout`

### From Valkey 8.1 to 9.0

**Higher risk**: Major new features change cluster behavior.

1. **ASM opt-in**: Review all modules — they must opt-in to atomic slot migration
2. **Multi-DB cluster**: If using cluster mode with multiple databases, test thoroughly
3. **Hash field expiration**: New commands and data structures — test HPEXPIRE behavior
4. **Dual-channel replication**: Enable for faster full sync on large datasets

### From Valkey 9.0 to 9.1

**Medium risk**: IO threads redesign and Lua module extraction.

1. **IO threads**: Test with `io-threads` settings — the lock-free redesign may change behavior under load
2. **Lua as module**: Review `lua-enable-insecure-api` if using deprecated Lua APIs
3. **Database-level ACL**: Review ACL configurations — users without `alldbs` are now restricted
4. **Replica RDB reuse**: AOF preamble from replication RDB improves startup time — verify AOF after full sync
