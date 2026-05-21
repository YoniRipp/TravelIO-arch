# Webhook Ingestion — Design

`POST /webhooks/cal/card-status` is a public endpoint that receives card-lifecycle
events from Cal and turns each one into **at most one** push notification, **at most
one** audit entry, and a card-state update in MongoDB — across multiple Fargate
pods, Cal retries, network blips, and process crashes.

Diagram: see `architecture.md` (Mermaid renders on GitHub).

## Shape of the system

Cal → CloudFront/WAF → ALB → Fargate handler → (HMAC verify) → atomic insert
into Mongo `events` (unique index on `event_id`) → SQS → 202 → worker pool →
state-machine update + push + audit. The handler does the minimum durable work
before acking; everything else is deferred to a worker behind a queue with a DLQ.

## 1. Edge / perimeter

CloudFront + AWS WAF in front of the ALB. WAF rate-limits per source IP and drops
obvious garbage (bad methods, oversized bodies, known-bad ASNs) before any compute
runs. TLS terminates at the ALB.

Authenticity is established by an HMAC-SHA256 signature: Cal signs
`timestamp + "." + body` with a shared secret and sends it as `X-Cal-Signature`
plus `X-Cal-Timestamp`. The handler recomputes the HMAC in constant time, rejects
anything where the timestamp is more than five minutes off wall clock, and the
secret lives in AWS Secrets Manager with rotation. If Cal publishes egress IP
ranges, we add them as a WAF allowlist as defence in depth — but the HMAC is the
real gate, not the IP filter.

An attacker who curls the endpoint without a valid signature gets a flat
`401 Unauthorized` with an empty body. No event shape leaks, no Mongo write, no
SQS message, and the failure is logged at debug level only so attacker traffic
doesn't drown real signal.

## 2. Sync vs. async + durability

**Synchronous, before 2xx:** parse, verify signature and timestamp, then
`db.events.insertOne({ _id: event_id, status: "RECEIVED", raw, received_at })`.
The `events` collection has a **unique index on `_id`**, so a duplicate delivery
fails fast with `E11000` — we catch it and treat it as success (the event is
already durable, nothing more to do here). Then `SendMessage` to SQS with
`MessageDeduplicationId = event_id`. Return **202 Accepted**.

**Asynchronous, after 2xx:** workers consume SQS and drive each event through a
state machine — `RECEIVED → PERSISTED → NOTIFIED → AUDITED → DONE` — using
conditional `findOneAndUpdate` operations (see §3). Card state and audit entry
land in their respective collections; the SQS message is deleted only after the
terminal transition.

**Failure walk-through:**

- **(a) Process crashes after receiving the request but before doing anything.**
  Cal's TCP connection drops or it sees a 5xx. Nothing was written to Mongo or
  SQS. Cal retries; the retry succeeds against another pod. No duplicate work,
  no lost event.

- **(b) MongoDB unreachable for 30 seconds.** The synchronous `insertOne` fails;
  the handler returns 5xx; Cal retries. Once Mongo is back, the retry succeeds.
  Awkward subcase: handler inserts into Mongo, then crashes before the SQS
  enqueue. The next Cal retry hits the duplicate-key path and finds
  `status = RECEIVED` with no downstream progress — it re-enqueues (the SQS
  dedup id makes this safe). A scheduled janitor sweeps any `RECEIVED` event
  older than ~5 minutes back onto the queue as a belt-and-braces safety net
  for the case where Cal does not retry.

- **(c) Push provider down for 2 hours.** Worker call to the provider fails;
  the SQS message returns to visibility with exponential backoff. The event
  document stays at `NOTIFIED`'s precondition (`PERSISTED`) until the call
  finally succeeds. After `maxReceiveCount` it lands in the DLQ; an operator
  drains the DLQ when the provider recovers. Card state and audit writes are
  not blocked by the push outage if we sequence them after `NOTIFIED` — but
  the brief says one notification per event, so we keep push on the critical
  path before audit closes the event.

At each stage the event lives in *durable* storage: the raw body on the Mongo
`events` doc from the first millisecond after signature verification, and an
in-flight reference in SQS until the terminal transition. Memory is never the
only copy.

## 3. Exactly-once side effects — the critical constraint

**Dedup key.** `event_id` itself, used as `_id` of the Mongo `events` document.
Unique by construction of the collection's primary index — Mongo will reject a
second insert with `E11000`, no application-level check-then-write.

**Atomic race-resolving operation.** Not the insert (that only dedups *receipt*).
The push and the audit are guarded by a **conditional update on the event's
current status**:

```
events.findOneAndUpdate(
  { _id: event_id, status: "PERSISTED" },
  { $set: { status: "NOTIFIED",
            notified_by: pod_id,
            notified_at: now() } }
)
```

Mongo serialises this update on a single document. Exactly one pod sees a
non-null return; that pod owns sending the push. Every other pod (whether a
retry that slipped through SQS dedup or a worker that picked up the same
message after visibility timeout) gets `null` back, logs `dedup_lost`, deletes
its SQS message, and does nothing. The same pattern guards the audit write
(`NOTIFIED → AUDITED`). The pod that wins the transition is the *only* pod
that calls the push provider.

**TTL / lifetime.** The `events` document is the durable record of "we received
this event and what we did with it" — it stays. If retention is a concern, a
`dedup_expires_at` field with a TTL index of 90 days is enough: Cal's retry
window is hours, not months. Audit entries live in a separate collection with
their own retention policy (likely much longer for compliance).

**What wins on a race.** The first writer to flip the status field wins. The
ordering is decided by Mongo, not by wall-clock or pod identity, so it is
deterministic per document and oblivious to clock skew across pods. Losers ack
silently — they have done the right thing by *not* duplicating the side effect.

**Why not Redis `SETNX` or DynamoDB conditional writes.** Both work. Both add a
second source of truth and a window where Mongo and the dedup store disagree on
crash. Keeping the dedup key co-located with the data it guards — one store, one
truth — is simpler and removes a class of "the lock said yes but the doc says
no" bugs.

## 4. Observability

**Structured JSON logs**, one line per stage, with `event_id`, `card_id`,
`event_type`, `pod_id`, `trace_id`, stage name, latency, outcome, dedup
win/loss, push-provider response code. **OpenTelemetry traces** with one span
per stage; `trace_id` is propagated into SQS message attributes so the async
half of the flow joins the same trace as the synchronous receive.

**Alerts.** 5xx rate on the endpoint; signature-failure rate (possible attack,
or rotated secret not yet propagated); SQS age-of-oldest-message (backlog);
DLQ depth > 0 (something is failing repeatedly); push-provider error rate;
janitor sweep size > 0 (events are getting stuck in `RECEIVED`, which means a
real bug, not a transient).

**Do not log.** Card PAN, CVV, full cardholder PII, the HMAC secret, the raw
signature header. `card_id` and `user_id` are tokenised references and safe.

**"Customer says they didn't get a notification 8 hours ago."**

1. Get `user_id` / `card_id` from support.
2. Query Mongo `events` for that `card_id` in the time window. Pull the
   `event_id` and its terminal `status`.
3. If `status = NOTIFIED` or `DONE`: the push was sent. Read the stored
   provider response code — either the provider accepted it (device or
   delivery problem downstream of us) or returned an error we didn't notice
   (provider problem; check provider dashboard).
4. If `status = PERSISTED`: the worker never advanced it. Search the DLQ for
   `event_id`; pull worker logs by `trace_id`; check whether the push provider
   was throwing in that window.
5. If no event doc at all: search edge logs for the `card_id` and time window.
   Either Cal never sent it, or the signature check rejected it (check the
   signature-failure log line — that's why we keep it even though it's noisy).

## What I'd do differently with more time/budget

I'd replace "insert into Mongo, then enqueue SQS" with an **outbox pattern
driven by Mongo change streams** — Debezium or a homegrown CDC consumer reads
the oplog and produces to Kafka. This removes the dual-write hazard between
Mongo and SQS (handler crashes between the two, janitor cleans up — it works,
but it's a moving part). I'd add a small **schema registry** for Cal events so
a new `event_type` doesn't silently land in the DLQ. I'd add **mTLS to Cal** on
top of HMAC as defence in depth. And I'd switch to **SQS FIFO with
`MessageGroupId = card_id`** for per-card ordering — right now I tolerate
reordering because the brief says Cal does not guarantee it, but ordered
delivery would simplify the state machine for `activated → blocked → replaced`
transitions. I cut these because each of them adds operational surface area
(another consumer, another schema store, another protocol) that isn't required
to meet the brief, and the unique-index-plus-conditional-update primitive is
already enough to make exactly-once correct.
