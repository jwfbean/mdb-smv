# DRAFT: Streaming Materialized Views with Atlas Stream Processing

---

## What is a Materialized View?

A materialized view is a pre-computed query result stored as a real collection on disk. Unlike a standard view — which re-executes its pipeline on every read — a materialized view is queried directly, making reads fast regardless of source data volume or pipeline complexity.

MongoDB supports on-demand materialized views using the `$merge` or `$out` aggregation stages. The [MongoDB documentation](https://www.mongodb.com/docs/manual/core/materialized-views/) describes a canonical incremental refresh pattern: a pipeline filters source data to only records changed since a known cutoff date, aggregates them, and merges the results into the view collection. Only the changed portion of the dataset is reprocessed on each refresh, keeping the cost proportional to new data rather than total data.

This approach works well when a view can tolerate being minutes, hours, or days stale — monthly sales rollups, nightly reporting aggregates, and periodic ETL summaries are natural fits.

---

## When Incremental Refresh Becomes Impractical

The incremental pattern has a fundamental constraint: **the view is only as fresh as the last refresh**. Refreshes must be triggered externally — by a scheduled job, a trigger, or application code — and each refresh has a latency floor determined by pipeline execution time.

For use cases with near-real-time requirements, this becomes impractical:

- A **live support queue dashboard** that reflects the current count of open tickets by priority cannot tolerate a five-minute lag. Agents making routing decisions need current data.
- A **gaming leaderboard** updated every ten minutes is effectively useless during an active session.
- **Fraud detection** that aggregates transaction velocity across a sliding window needs sub-second freshness to be actionable.

In these cases, the operational cost of managing refresh frequency, handling late-arriving data, and tuning `startDate` windows grows until it exceeds the cost of a different architecture entirely.

---

## Streaming Materialized Views

A **streaming materialized view** (SMV) is continuously maintained by an active stream processor rather than refreshed on demand. As documents are inserted, updated, or deleted in the source collection, the view is updated within seconds — or less. There is no scheduler, no `startDate` to manage, and no staleness window.

| | On-Demand (Incremental) | Streaming |
|---|---|---|
| **Update trigger** | Manual or scheduled | Change-driven, continuous |
| **Latency** | Minutes to days | Sub-second to low-second |
| **Data freshness** | Point-in-time snapshot | Perpetually synchronized |
| **Operational overhead** | Refresh scheduling, startDate management | Stream processor lifecycle |
| **Best for** | Batch reporting, periodic aggregation | Live dashboards, operational analytics |

MongoDB Atlas Stream Processing (ASP) provides the engine for building streaming materialized views natively within the Atlas ecosystem. Because ASP reads from MongoDB change streams and writes back to Atlas collections, the streaming engine and the operational data store are unified — there is no external Kafka cluster, Flink deployment, or separate CDC connector required.

---

## The Delta Logic Pattern

Because ASP does not yet handle stream retractions natively, maintaining an accurate aggregate across inserts, updates, and deletes requires explicit **delta logic** in the pipeline.

The core idea: rather than recomputing the aggregate from scratch on each event, compute the *change* the event contributes to the aggregate and apply that change incrementally to the stored view.

For each change stream event, a `$switch` on `operationType` determines the delta:

| `operationType` | Delta |
|---|---|
| `insert` | The full value of the new document's contribution |
| `update` | Post-image value minus pre-image value |
| `delete` | Pre-image value negated (multiplied by -1) |

This requires the source collection to have `changeStreamPreAndPostImages` enabled, and the `$source` stage to be configured with `fullDocument: "updateLookup"` and `fullDocumentBeforeChange: "whenAvailable"`. Without the pre-image, delta calculations for updates and deletes are impossible.

### Delta logic for COUNT aggregations

The pattern above is most naturally expressed when summing a numeric field (e.g., total watts consumed, total order value). When the aggregate is a **count of documents matching a condition** — such as open ticket count — the delta logic adapts:

- `insert` of a matching document → `+1`
- `update` that causes a document to leave the matched set (e.g., status changes from `open` to `resolved`) → `-1`
- `update` that causes a document to enter the matched set → `+1`
- `delete` of a matching document → `-1`

### Aggregations that affect multiple group keys

A standard delta produces one adjustment per event. Some updates affect two group buckets simultaneously — for example, a priority escalation on a support ticket moves a document from one priority bucket to another, requiring a `-1` on the old priority and a `+1` on the new one.

The pattern for this is to compute an `_adjustments` array rather than a scalar delta, then use `$unwind` to fan a single event into multiple documents — one per affected bucket. Normal events produce a one-element array; multi-bucket events produce two elements; noise events produce an empty array that `$unwind` drops silently.

---

## Implementation in Atlas Stream Processing

A streaming materialized view pipeline in ASP has the following structure:

```
$source → $addFields (delta) → [$unwind] → $tumblingWindow ($group) → $merge
```

**`$source`** reads from the MongoDB change stream with pre- and post-image support enabled.

**`$addFields`** computes the delta or adjustments array using a `$switch` on `operationType`.

**`$unwind`** (optional) is required when an event can affect multiple group keys.

**`$tumblingWindow`** wraps the `$group` stage. ASP currently requires all `$group` operations to reside within a windowing stage — windowless global aggregation is on the roadmap. The window interval controls how frequently deltas are flushed to the sink. A one-second window provides near-real-time behavior; larger windows reduce write frequency at the cost of latency.

**`$merge`** writes accumulated deltas to the view collection using an additive `whenMatched` pipeline rather than a replacement — this is what allows counts to accumulate correctly across windows:

```js
whenMatched: [{ $set: { open_count: { $add: ["$open_count", "$$new.open_count"] } } }]
```

### Known constraints

**`initialSync` is incompatible with windowing stages.** ASP's `initialSync` option baselines a stream processor from the existing state of a collection before switching to live events. As of the current release, this cannot be combined with `$tumblingWindow` or `$hoppingWindow`. A streaming materialized view therefore starts from zero at processor start time; pre-existing documents are not counted. If a baseline is required, the view collection must be seeded separately before the processor is started.

**`$group` requires a window.** Windowless global aggregation — grouping across all time without a fixed interval — is not yet supported in ASP. The tumbling window is a required wrapper, not an optional optimization.

**`$merge` is required; `$out` is not available in ASP.** `$merge` with an additive `whenMatched` pipeline is the only way to accumulate running totals across windows. `$out` (which replaces the entire output collection on each run) would destroy prior window state and cannot be used for this pattern.

---

## Production Considerations

**Dead Letter Queue (DLQ).** Always configure a DLQ collection when creating a stream processor. Documents that fail processing land in the DLQ rather than being silently dropped. Monitor `dlqMessageCount` via `sp.<processor>.stats()` — any value above zero requires investigation.

**Two connection planes.** ASP management commands (`sp.*`) require a connection string that targets the stream processing instance (`?streamProcessingInstance=<name>`). Application queries against the resulting view collection use a standard Atlas connection string. Mixing these in the same session will not work.

**Idempotency.** The additive `$merge` pattern is idempotent when the processor restarts cleanly and resumes from its last checkpoint. Ensure the processor is fully stopped before dropping and recreating the view collection to avoid partial state.

**Pipelines as code.** Treat ASP pipeline definitions as versioned artifacts. Store them in source control and manage processor creation and lifecycle via Terraform to ensure your streaming infrastructure is reproducible.

---

*For a worked example implementing this pattern against a support ticket queue, see [README.md](README.md).*
