# TODO

## Deserializer errors crash the whole batch

Currently, if a deserializer throws for a single record, the entire batch fails and
the consumer's exception handler is invoked. Consider per-record error handling so
that one corrupted record doesn't prevent the rest of the batch from being processed
(e.g. skip-and-log, or a dead-letter callback option on `start-consumer`).

