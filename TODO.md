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

### Expose more status from consumer
`(status consumer)` only returns `:running`/`:shutdown`/`:terminated`. Could surface
topic/group, record count, last-processed timestamp, etc.

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

