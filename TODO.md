# TODO

## Ideas

These are some ideas for improvements that may or may not make sense
or come to fruition. Here just to percolate.

### Provide a way to do schema migration for subscriptions `updated_at`
`ensure-schema` creates the `updated_at` column on fresh installs but
existing installs won't get it. Add `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS updated_at ...` so upgrades work without manual intervention.

### Return `:index-key?` in return of `list-topics`
The `topics` table doesn't store whether a btree key index was
created, so there's no way to retrieve that info later short of
querying `pg_indexes`. Either store it in the table or document the
limitation.

### Consumer start and `ensure-subscription :from` interaction
`start-consumer` always calls `ensure-subscription` at cursor 0, so
calling `reset-consumer-offset!` before start has no effect. Docs
should make clear that `ensure-subscription :from :latest` before
`start-consumer` is the correct approach for skipping the existing
backlog. Alternatively, accept another option for `start-consumer`
that specifies the "creation mode" for when the subscription is new.

### Pause/resume a consumer
Closing and restarting is the only option. A pause/resume mechanism
would let operators temporarily halt consumption without losing the
connection or restarting threads.

## Deserializer errors crash the whole batch
Currently, if a deserializer throws for a single record, the entire
batch fails and the consumer's exception handler is invoked. Consider
per-record error handling so that one corrupted record doesn't prevent
the rest of the batch from being processed (e.g. skip-and-log, or a
dead-letter callback option on `start-consumer`).


## Correctness

### Concurrent producers can cause consumers to skip records
`eid` comes from an identity sequence, so values are handed out when a
row is inserted, while rows become visible in commit order. If txn A
gets eid 10, txn B gets eid 11, and B commits first, a consumer can
fetch 11 and commit cursor 11 before A commits. When A commits, eid 10
is below the cursor and is never delivered (the classic "outbox gap"
problem). Options:
- Add an `xid8` column (`pg_current_xact_id()`) and only fetch rows
  whose xid is below `pg_snapshot_xmin(pg_current_snapshot())`, i.e.
  rows that can no longer have uncommitted predecessors.
- Serialize appends per topic with a transaction-level advisory lock
  (simple, but caps producer throughput).
- Maintain a "safe high-water mark" that producers advance under lock.

Add a test with concurrent producers (overlapping transactions,
out-of-order commits) to cover this.

### `:auto` commit mode is at-most-once, contradicting the README
In `:auto`, `fetch-records*` commits the offset in its own transaction
before the handler runs (`consumer/fetch-and-handle`). If the handler
throws, the batch is lost. The README's "Delivery guarantees" section
says records will not be skipped. Either make `:tx-wrap` the default,
or document `:auto` as at-most-once with respect to handler failures.

### Multiple consumers in the same group behave inconsistently
There are no partitions, so a group is really one logical cursor.
With two consumers in one group:
- `:tx-wrap` — the `FOR UPDATE` on the subs row is held through the
  handler, so they take turns (safe, no parallelism).
- `:auto` — the lock is released after the fetch, so they process
  different batches concurrently and out of order.
- `:manual` — the offset doesn't move until the handler commits, so
  both fetch the same batch (duplicates).

Either document "one consumer per group", or have the consumer take a
session-level advisory lock on (topic, group) so additional consumers
wait as hot standbys (roughly Kafka's failover behavior).

### `:manual` mode can hot-loop
If the handler doesn't commit and the batch is full
(`= max-records num-handled`), `do-work-fn` immediately re-queues and
re-fetches the same batch with no backoff. Only re-queue when the
cursor actually advanced, or add a backoff.

### Small doc/behavior mismatches
- `start-consumer` docstring says `:reconnect-ms` defaults to 5000;
  the code uses 2000.
- `:lag` is `max_eid - cursor`, which over-counts when there are
  sequence gaps (rolled-back inserts). Document it as an upper bound,
  or compute it with a count when it's small.

## Performance

### NOTIFY serializes notifying commits database-wide
Postgres takes a global lock at commit for any transaction that issued
a NOTIFY, so commits from all producers on all topics are serialized
through it. High-rate single-record `append-one!` will hit this
ceiling well before Kafka would. Document that batching via `append!`
is the way to scale producers. Possibly consider an opt-in mode that
skips the trigger and relies on polling for very hot topics.

### Per-call overhead in `append!`
- With a string topic, `insert-records` runs a `fetch-topic` lookup on
  every call. Cache topic metadata (it's immutable after creation) or
  let callers pass the topic map.
- Without a `:conn` in the config, `insert-records` opens a new
  connection per call via `conn-map`. Steer users towards a pool in
  the docs, or warn when this path is taken.

### Retention via DELETE causes bloat
`trim-topic!` deletes rows, which leaves dead tuples and depends on
vacuum. Kafka just drops whole log segments. Consider an opt-in
time-range partitioned layout for `log__*` tables so retention becomes
`DROP`/`DETACH PARTITION`.

### Hot subs row per group
Each consumer fetch is its own transaction that locks and updates the
group's `subs` row. That's fine at default batch sizes, but under
heavy load it's a hot row churning dead tuples. Consider tuning
`fillfactor` on `subs` (to enable HOT updates), and document the
trade-off with `:max-records`.

### Two dedicated connections per consumer
Many topics × groups will exhaust `max_connections`, and LISTEN doesn't
work through PgBouncer in transaction mode. A shared per-process
listener connection that fans notifications out to consumers (and a
shared fetch pool) would cut this substantially.

## Kafka parity (document as non-goals, or consider)
- Partitions / scale-out within a single group.
- Log compaction by key.
- Seeking by timestamp (`reset-consumer-offset!` only takes an eid;
  the BRIN index on `timestamp` would make this cheap).
- Group rebalancing.

## Docs
- Highlight that `:tx-wrap` gives effectively-once processing when the
  handler writes to the same database, and that `append!` with a
  caller-supplied `:conn` acts as a transactional outbox. Both are real
  advantages over Kafka and are currently undersold.
