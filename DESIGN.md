# Webhook Ingestion — Design

`POST /webhooks/cal/card-status` receives card-lifecycle events from Cal and
produces three side effects per event: a card-state update in MongoDB,
**exactly one** push notification, and **exactly one** audit entry — across
multiple Fargate pods, Cal retries, network blips, and process crashes.

> The context also mentions an in-app message as a fourth side effect. It
> would follow the same gated pattern as the push notification. I focus on
> the three explicitly listed requirements below.

Diagram: see `architecture.md` (Mermaid renders on GitHub).

## Shape of the system

Cal → CloudFront/WAF → ALB → Fargate handler → (verify HMAC) → atomic insert
into Mongo `events` collection (unique index on `event_id`) → enqueue to SQS
→ 202 Accepted → worker pool consumes SQS → atomic claim via conditional
update → push + card state + audit → done. The handler does the minimum
durable work before acking; everything else is deferred to workers behind a
queue with a DLQ.

## 1. Edge / perimeter

CloudFront + AWS WAF in front of the ALB. WAF rate-limits per source IP and
drops obvious garbage (bad methods, oversized bodies, known-bad ASNs) before
any compute runs. TLS terminates at the ALB.

Authenticity is established by an HMAC-SHA256 signature: Cal signs
`timestamp + "." + body` with a shared secret and sends it as
`X-Cal-Signature` plus `X-Cal-Timestamp`. The handler recomputes the HMAC in
constant time, rejects anything where the timestamp is more than five minutes
off wall clock, and the secret lives in AWS Secrets Manager with rotation
support. If Cal publishes egress IP ranges, we add them as a WAF allowlist
as defence in depth — but the HMAC is the real gate, not the IP filter.

An attacker who curls the endpoint without a valid signature gets a flat
`401 Unauthorized` with an empty body. No event shape leaks, no Mongo write,
no SQS message, and the failure is logged at debug level only so attacker
traffic doesn't drown real signal.

## 2. Sync vs. async + durability

**Synchronous, before 2xx:** parse → verify signature and timestamp →
`db.events.insertOne({ _id: event_id, status: "RECEIVED", raw, received_at })`.
The `events` collection has a **unique index on `_id`**, so a duplicate
delivery fails fast with `E11000`. On duplicate key the handler still enqueues
to SQS and returns **202 Accepted**. Re-enqueuing is harmless because the
worker's conditional update (§3) is the real dedup gate. This also covers the
case where a previous handler crashed after insert but before enqueue — the
retry re-enqueues and the event is not lost.

SQS is **Standard** (not FIFO). Duplicate messages are possible and expected;
the worker handles them via the conditional update. We chose Standard over
FIFO because FIFO's 5-minute dedup window doesn't cover Cal's full retry
window, and its throughput cap (3,000 msg/sec per group) would become a
bottleneck at scale.

**Asynchronous, after 2xx:** workers consume SQS and process each event
through three states — **RECEIVED → PROCESSING → DONE**:

1. **Claim the event** — `findOneAndUpdate({_id: event_id, status: "RECEIVED"},
   {$set: {status: "PROCESSING", worker_id, started_at}})`. This is the single
   atomic gate. Exactly one worker wins across all pods and all retries. The
   losing worker gets `null`, deletes its SQS message, and does nothing.
2. **Card-state update** — idempotent upsert on the `cards` collection keyed
   by `card_id`. The update is a SET determined by the event payload, safe to
   run more than once. Uses `occurred_at` as a guard so out-of-order events
   don't overwrite newer state.
3. **Send push notification** — the winning worker calls the push provider.
4. **Write audit entry** — the winning worker inserts an audit record.
5. **Complete** — `updateOne({_id}, {$set: {status: "DONE", completed_at}})`.
   Delete SQS message.

All three side effects are performed by the **single winning worker**, in
sequence. No other worker can interfere because the gate already resolved the
race. This avoids a subtle bug where splitting side effects across independent
gates could allow a losing worker to write the audit before the winning worker
finishes sending the push — if the push then fails, the event would be marked
complete with an audit entry but no notification sent.

**Crash recovery:** if a worker crashes mid-processing (after claiming but
before completing), the event stays at `PROCESSING`. A scheduled janitor
sweeps events stuck in `PROCESSING` longer than a threshold (e.g. 5 minutes)
back to `RECEIVED` and re-enqueues them to SQS. A new worker claims the event
and re-runs all side effects. The card-state update and audit write are
idempotent. For the push notification, we include `event_id` as an
idempotency key with the push provider to prevent double-delivery on retry.
If the provider does not support idempotency keys, this crash-between-push-
and-DONE window is the one acknowledged narrow failure mode — and it only
triggers on actual process crashes, not during normal concurrent processing.

**Failure walk-through:**

- **(a) Process crashes after receiving the request but before doing anything.**
  Cal's TCP connection drops or it sees a 5xx. Nothing was written to Mongo or
  SQS. Cal retries; the retry succeeds against another pod. No duplicate work,
  no lost event.

- **(b) MongoDB unreachable for 30 seconds.** The synchronous `insertOne`
  fails; the handler returns 5xx; Cal retries. Once Mongo is back, the retry
  succeeds. Awkward subcase: handler inserts into Mongo, then crashes before
  the SQS enqueue. Cal retries; the retry hits the duplicate-key path,
  re-enqueues to SQS, and returns 202. A scheduled janitor also sweeps any
  `RECEIVED` event older than ~5 minutes back onto the queue as a safety net
  for the edge case where Cal itself does not retry.

- **(c) Push provider down for 2 hours.** Worker claims the event (RECEIVED →
  PROCESSING), calls the provider, call fails. Worker does not mark the event
  DONE — it leaves it at PROCESSING and lets the SQS message return to
  visibility with exponential backoff. After `maxReceiveCount` failed attempts,
  the message lands in the DLQ; an operator drains the DLQ when the provider
  recovers. Meanwhile, the janitor will also sweep the stale PROCESSING event
  back to RECEIVED for re-claiming. Card state may already be up-to-date from
  step 2 (idempotent, so re-running it is harmless).

At each stage the event lives in durable storage: the raw body on the Mongo
`events` doc from the moment after signature verification, and an in-flight
reference in SQS until the worker deletes it. Memory is never the only copy.

## 3. Exactly-once side effects — the critical constraint

**Dedup key.** `event_id` itself, used as `_id` of the Mongo `events`
document. Unique by construction of the collection's primary index — Mongo
rejects a second insert with `E11000`, no application-level check-then-write.

**Atomic race-resolving operation.** A single conditional update claims the
event for processing:

```
events.findOneAndUpdate(
  { _id: event_id, status: "RECEIVED" },
  { $set: { status: "PROCESSING",
            worker_id: pod_id,
            started_at: now() } }
)
```

Mongo serialises this update on a single document. Exactly one pod sees a
non-null return; that pod owns all three side effects — card-state update,
push notification, and audit entry. Every other pod — whether a Cal retry
that re-enqueued, a duplicate SQS delivery, or a worker that picked up the
same message after visibility timeout — gets `null` back, logs `dedup_lost`,
deletes its SQS message, and does nothing.

This is intentionally a **single gate, single winner** design. Splitting side
effects across multiple independent gates would create a window where one
worker writes the audit while another is still sending the push — if the push
then fails, the event ends up marked complete with no notification sent. By
having one winner do everything, the only failure mode is a crash of the
winning worker itself, which is handled by the janitor (see §2).

**TTL / lifetime.** The `events` document is the durable record of what we
received and what we did with it — it stays. If retention is a concern, a
`dedup_expires_at` field with a TTL index of 90 days is enough: Cal's retry
window is hours, not months. Audit entries live in a separate collection with
their own retention policy (likely longer for compliance).

**What wins on a race.** The first writer to flip `RECEIVED` to `PROCESSING`
wins. The ordering is decided by Mongo's document-level serialisation, not by
wall-clock or pod identity, so it is deterministic per document and oblivious
to clock skew across pods. Losers ack silently — they have done the right
thing by *not* duplicating the side effect.

**Why not Redis `SETNX` or DynamoDB conditional writes.** Both work. Both add
a second source of truth and a window where Mongo and the dedup store disagree
on crash. Keeping the dedup key co-located with the data it guards — one
store, one truth — is simpler and removes a class of "the lock said yes but
the doc says no" bugs.

## 4. Observability

**Structured JSON logs**, one line per stage, with `event_id`, `card_id`,
`event_type`, `pod_id`, `trace_id`, stage name, latency, outcome, dedup
win/loss, push-provider response code. **OpenTelemetry traces** with one span
per stage; `trace_id` is propagated into SQS message attributes so the async
half of the flow joins the same trace as the synchronous receive.

**Alerts.** 5xx rate on the endpoint; signature-failure rate (possible attack,
or rotated secret not yet propagated); SQS age-of-oldest-message (processing
backlog); DLQ depth > 0 (something is failing repeatedly); push-provider error
rate; janitor sweep size > 0 (events stuck in `RECEIVED` or stale
`PROCESSING`, indicating a real bug or repeated crashes).

**Do not log.** Card PAN, CVV, full cardholder PII, the HMAC secret, the raw
signature header. `card_id` and `user_id` are tokenised references and safe.

**"Customer says they didn't get a notification 8 hours ago."**

1. Get `user_id` / `card_id` from support.
2. Query Mongo `events` for that `card_id` in the time window. Pull the
   `event_id` and its terminal `status`.
3. If `status = DONE`: the push was sent and the audit was written. Read the
   stored provider response code — either the provider accepted it
   (device-side delivery problem) or returned an error (provider problem;
   check their dashboard).
4. If `status = PROCESSING`: a worker claimed it but never finished. Check
   `worker_id` and `started_at` — if stale, the worker crashed. The janitor
   should have re-enqueued it; check whether re-processing succeeded. Pull
   worker logs by `trace_id` to find the failure.
5. If `status = RECEIVED`: no worker ever claimed it. Search the DLQ for the
   `event_id`; check SQS metrics for that period; check whether workers were
   running at all.
6. If no event doc at all: search edge logs for the `card_id` and time window.
   Either Cal never sent it, or the signature check rejected it.

## What I'd do differently with more time/budget

I'd replace "insert into Mongo, then enqueue SQS" with an **outbox pattern
driven by Mongo change streams** — a CDC consumer tails the oplog and produces
to the queue. This removes the dual-write hazard between Mongo and SQS (the
janitor handles it today, but it's a moving part I'd rather eliminate). I'd add
a small **schema registry** for Cal events so a new `event_type` doesn't
silently land in the DLQ. I'd add **mTLS to Cal** on top of HMAC as defence in
depth. And I'd switch to **SQS FIFO with `MessageGroupId = card_id`** for
per-card ordering — right now I tolerate reordering (with the `occurred_at`
guard on card-state updates), but ordered delivery would simplify card-state
transitions like `activated → blocked → replaced`. I cut these because each
adds operational surface area that isn't required to meet the brief, and the
conditional-update primitive is already sufficient for exactly-once correctness.
