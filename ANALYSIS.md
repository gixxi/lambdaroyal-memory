# lambdaroyal-memory: Code Analysis

## Overview

Analysis of the STM-backed in-memory database (~2,000 LOC across 8 source files) covering correctness bugs, performance issues, and architectural concerns. Findings are grouped by severity.

---

## Critical Bugs

### 1. `alter-meta!` is non-transactional inside `dosync` (tx.clj:597)

```clojure
(alter-meta! x assoc :deleted true)
;; ...
(ref-set x @x)  ;; triggers evictor watch
```

`alter-meta!` mutates ref metadata **immediately and irrecoverably** — it does not participate in the STM transaction. If the surrounding `dosync` retries after this line executes, the ref is permanently marked `:deleted true` even though the delete never committed. The subsequent `ref-set` fires the evictor watch, which calls `evict/delete` — a side-effecting I/O operation that will execute on every retry. This means a single logical delete can produce multiple evictor delete calls.

**Impact:** Spurious delete notifications to CouchDB; ref metadata corruption on retry.

### 2. `ReferencedIntegrityConstraint.precommit` — single-arg `=` always returns true (tx.clj:395)

```clojure
(if (and match (= (-> match meta :unique-key)))
```

`(= x)` with one argument always returns `true`. The `:unique-key` metadata check is completely ineffective — the condition reduces to just `(if match ...)`. This means the RIC delete-guard fires for *any* match, even matches that aren't true unique-key tuples.

**Impact:** Referential integrity constraints may be over-enforced (preventing valid deletes) or behave inconsistently.

### 3. Bulk mode skips unique constraint with no deferred validation (tx.clj:284–285, 476–493)

When `(bulk-mode?)` is true, `AttributeIndex.precommit` skips the uniqueness check entirely. The comment says "will be validated at flush time," but `flush-bulk-indexes` never performs any uniqueness validation — it just does `(alter idx-data into index-entries)`.

**Impact:** Data loaded via `with-bulk-mode` can silently violate unique index constraints.

### 4. `delete-document` retry loop re-deletes instead of re-fetching (couchdb.clj:126) — *Downgraded to Low*

```clojure
(loop [existing (clutch/get-document ...)]
  (if-not (delete existing)
    (recur (clutch/delete-document ...))))
```

On conflict retry, the `recur` calls `clutch/delete-document` again rather than `clutch/get-document`. This means the retry path performs a delete and feeds its result as `existing` for the next iteration, rather than fetching a fresh revision. Since the in-memory STM is the single source of truth at runtime, the intent ("just keep trying to delete") is sound and works if the second attempt succeeds. However, under sustained contention from an external CouchDB writer, the loop could spiral because `existing` would hold a failed-delete result rather than a fresh document with the latest `_rev`. In practice, CouchDB delete conflicts are rare when this system is the sole writer.

**Impact:** Low — latent issue under edge-case contention with external CouchDB writers. Functionally correct in the normal single-writer scenario.

### 5. `get-database-url-by-channel` / `get-database` — bare `name` instead of `(name coll-name)` (couchdb.clj:53, 58)

```clojure
db-name (if (keyword? coll-name) (name coll-name) name)
```

When `coll-name` is a string (not a keyword), this resolves to `clojure.core/name` (the function object itself), producing a database URL like `http://localhost:5984/clojure.core$name@1a2b3c`. This would be called on every CouchDB operation.

**Impact:** Any collection whose name is passed as a string (rather than keyword) will produce CouchDB errors. This may be masked if collection names are always keywords, but is a latent bug.

### 6. `filter-key` 5-arity is broken (search.clj:419–423)

```clojure
(with-meta (fn [& opts]
              (with-meta (tx/select tx coll-name start-test start-key stop-test stop-key))
              meta) meta)
```

Two issues: (a) `with-meta` is called with 1 argument (needs 2) — will throw `ArityException` at runtime; (b) even if fixed, the function body returns `meta` (the metadata map) rather than the select results. The 3-arity version works correctly.

**Impact:** Any range-based `filter-key` call in projections will crash.

### 7. `tree-referencees` uses outer `coll-name` in loop (tx.clj:720–722)

```clojure
(loop [todo #{[coll-name user-scope-tuple]} ...]
  (let [coll (get ctx coll-name)  ;; always the OUTER coll-name
        next (first todo)         ;; next could be from a different collection
        ...]
```

After the first iteration, the loop always processes constraints from the original collection rather than the collection of the current `todo` item. Multi-level referential integrity traversals will produce incorrect results.

---

## High-Severity Issues

### 8. Failed CouchDB writes are silently dropped (couchdb.clj:103–104, eviction/core.clj:73)

Both `put-document` and the consumer loop catch `Exception`, log at FATAL level, and continue. The queued item has already been dequeued and is never re-enqueued. This is a **data loss** path — the in-memory state diverges from CouchDB with no recovery mechanism.

### 9. Unbounded ConcurrentLinkedQueue (eviction/core.clj:48)

The eviction proxy uses an unbounded `ConcurrentLinkedQueue`. If CouchDB is slow or unreachable, the queue grows without limit. There is no backpressure, no queue-depth monitoring, and no alerting. Under sustained load with CouchDB down, this leads to OOM.

### 10. Consumer thread can die silently (eviction/core.clj:54–74)

The catch block on line 73 catches `Exception` but not `Error`. An `OutOfMemoryError`, `StackOverflowError`, or similar will kill the consumer future silently. No health check or watchdog exists — all subsequent queue items are never processed, and the queue grows until OOM.

### 11. Unbounded conflict retry loop (couchdb.clj:96–102)

The `put-document` conflict retry has no maximum retry count. If two writers keep conflicting (e.g., external CouchDB writer), this loops forever, blocking the single consumer thread and starving all other eviction operations.

### 12. `decorate-coll-with-gtid` — atom reset inside dosync (tx.clj:54–58)

```clojure
(reset! (:gtid coll) gtid)
```

`:gtid` is an atom, and `reset!` is a non-transactional side effect inside `dosync`. On transaction retry, the atom is reset multiple times. The check-then-set pattern (`@old` then `reset!`) is also not atomic across concurrent transactions.

### 13. `sorted-set-aggregator` is broken (search.clj:118–123)

The `doseq [x data]` loop never references `x` — it always operates on `data` as a whole, inserting the same entry `(count data)` times. It also uses `assoc` on a sorted-set, which is not a valid operation.

---

## Performance Issues

### 14. No CouchDB bulk API usage — *Acknowledged trade-off*

Every insert/update/delete is sent to CouchDB as an individual HTTP request. CouchDB's `_bulk_docs` API is never used. Under high write load, this means one round-trip per document, plus an extra `get-database` call per operation.

However, since transactions can be large (thousands of updates), a single `_bulk_docs` call for an entire transaction risks payload size issues and CouchDB memory pressure — especially on older CouchDB versions. The per-document approach is the safest strategy when CouchDB serves as a persistence backstop behind the STM.

**Recommendation (if throughput becomes a bottleneck):** Micro-batch in the consumer thread — drain the queue in chunks of 50–200 documents and send small `_bulk_docs` requests. This reduces HTTP overhead without risking payload explosions. Only worth pursuing if CouchDB write latency is measurably problematic.

### 15. `satisfies?` calls in hot paths (tx.clj:544–582)

`alter-document` filters constraints using `(satisfies? Index %)` which uses reflection in Clojure. This is called on every update, for every constraint.

**Recommendation:** Pre-partition constraints by type at collection creation time, or use `instance?` checks on the concrete types.

### 16. Multiple dereferences of the same ref (tx.clj:228–269)

`AttributeIndex.find` dereferences `:data` twice (once for `primary-key-is-string` check, once for `subseq`). `rfind` dereferences it three times. Outside a `dosync`, these can see different values.

**Recommendation:** Bind the deref result once per call.

### 17. `concat-aggregator` builds deeply nested lazy chains (search.clj:94–98)

```clojure
(commute ref concat data)
```

Each `concat` wraps the previous result. After N aggregations, traversing the result costs O(N) per element access — the classic "concat of concats" antipattern.

**Recommendation:** Use `into` with a vector, or accumulate in a transient collection.

### 18. Sequential collection loading on startup (couchdb.clj:152–164)

Collections are loaded sequentially from CouchDB with individual `insert-raw` calls inside `dosync` for each document. No parallelism across collections, no bulk insertion.

**Recommendation:** Load collections in parallel; use `with-bulk-mode` for the initial load.

### 19. Busy-wait polling in eviction consumer (eviction/core.clj:74)

```clojure
(Thread/sleep (or delay 100))
```

A `LinkedBlockingQueue.take()` would eliminate CPU waste during idle periods and provide instant wakeup.

### 20. Full table scan realizes entire collection (tx.clj:627–640)

For collections over 100 elements, `select` eagerly materializes everything into a vector. This is O(n) in memory and time.

### 21. `by-ric` ratio-full-scan uses wrong collection size (search.clj:289–293)

The ratio divides `(count keys)` by the **target** (parent) collection size. The decision to full-scan should compare against the **source** (child) collection being scanned.

---

## Memory Concerns

### 22. No `remove-watch` on deleted refs

After deletion, the ref is removed from the collection's sorted-map, but its `:evictor` watch is never removed. If external code holds a reference to a previously-returned tuple, the ref and its watch closure (which closes over the entire `coll`) cannot be garbage collected.

### 23. `*bulk-index-entries*` holds all index entries in memory

During bulk inserts, the volatile accumulates ALL index entries across all indexes until `flush-bulk-indexes` is called. For large bulk loads with multiple indexes, this can cause significant memory pressure.

### 24. Parked go blocks after timeout in `combined-search`

When `combined-search` times out, per-function go blocks that haven't delivered their result park forever on `>!` to an unclosed, unread channel. They retain references to potentially large realized result sets.

---

## Correctness Concerns (Medium)

### 25. `start` future failures are unobserved (couchdb.clj:143)

The startup loading runs in a `future` with no try/catch. If it throws (CouchDB unreachable, malformed doc), the future completes exceptionally but nobody calls `.get()`. The `started` atom stays `false`, and all subsequent persistence operations silently no-op.

### 26. Keyword vs. string collection names in revs atom (couchdb.clj:162 vs 74)

During startup, collection names come from the dependency ordering (potentially strings). During writes, they come from the protocol call (potentially keywords). `[:foo "bar"]` and `["foo" "bar"]` are different keys in the revs atom, causing revision mismatches and unnecessary conflicts.

### 27. `remove-attr-index` / `remove-ric` read context outside dosync (context.clj:62–72, 93–118)

The context ref is deref'd before the `dosync`. Between the deref and the transaction, the context could be modified by another thread (TOCTOU).

### 28. `delete-document` and `delete-coll` omit exception from log (couchdb.clj:127–128, 137–138)

The `log/fatal` calls do not pass the exception object `e`, so stack traces and root causes are lost from logs. Compare with `put-document` line 104 which correctly includes `e`.

---

## Low-Severity / Informational

- **`^:const` on mutable atom** (tx.clj:8): `(def ^:const debug (atom true))` — `^:const` inlines the value at compile time, so subsequent `reset!` calls won't be visible to already-compiled code.
- **`gtid` counter can overflow `Long/MAX_VALUE`** (tx.clj:35): Uses `inc` (not `inc'`), so after many transactions it will throw `ArithmeticException`.
- **Evictor watch fires on STM retry** (tx.clj:87–111): Ref watches fire on every `ref-set`/`alter` including retried transactions. Evictors must be idempotent, which is mostly true for CouchDB puts (last-writer-wins) but not for deletes.
- **`compaction` catches `Throwable`** (couchdb.clj:216): Swallows JVM errors like `OutOfMemoryError`.
- **No channel cleanup**: Channels created by `abstract-search` and `combined-search` are never explicitly closed.
- **Break condition race**: Shared atom in `break-condition-state` across parallel searches can yield off-by-one results under contention.

---

## Summary Table

| # | Severity | Category | Location | Issue |
|---|----------|----------|----------|-------|
| 1 | Critical | Bug | tx.clj:597 | `alter-meta!` non-transactional in dosync |
| 2 | Critical | Bug | tx.clj:395 | Single-arg `=` always true in RIC check |
| 3 | Critical | Bug | tx.clj:284 | Bulk mode skips unique constraint, never validates |
| 4 | Low | Bug | couchdb.clj:126 | Delete retry re-deletes instead of re-fetching (works in single-writer scenario) |
| 5 | Critical | Bug | couchdb.clj:53,58 | Bare `name` fn instead of `(name coll-name)` |
| 6 | Critical | Bug | search.clj:419 | `filter-key` 5-arity broken (wrong arity + wrong return) |
| 7 | Critical | Bug | tx.clj:720 | `tree-referencees` uses wrong coll-name in loop |
| 8 | High | Data Loss | couchdb.clj:103 | Failed CouchDB writes silently dropped |
| 9 | High | Reliability | core.clj:48 | Unbounded eviction queue (OOM risk) |
| 10 | High | Reliability | core.clj:54 | Consumer thread dies silently on Errors |
| 11 | High | Reliability | couchdb.clj:96 | Unbounded conflict retry blocks consumer |
| 12 | High | Bug | tx.clj:54 | Atom reset inside dosync (non-transactional) |
| 13 | High | Bug | search.clj:118 | `sorted-set-aggregator` broken |
| 14 | Low | Perf | couchdb.clj | No `_bulk_docs` usage (acknowledged trade-off; micro-batching possible if needed) |
| 15 | Medium | Perf | tx.clj:544 | `satisfies?` reflection in hot path |
| 16 | Medium | Perf | tx.clj:228 | Multiple dereferences of same ref |
| 17 | Medium | Perf | search.clj:94 | Nested lazy concat chains |
| 18 | Medium | Perf | couchdb.clj:152 | Sequential startup loading |
| 19 | Low | Perf | core.clj:74 | Busy-wait polling |
| 20 | Low | Perf | tx.clj:627 | Eager full-scan materialization |
| 21 | Low | Perf | search.clj:289 | Wrong collection for ratio-full-scan |
| 22 | Low | Memory | tx.clj:597 | No remove-watch on deleted refs |
| 23 | Low | Memory | tx.clj:501 | Bulk index entries held in memory |
| 24 | Low | Memory | search.clj:66 | Parked go blocks after timeout |
