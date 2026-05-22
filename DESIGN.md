# Webhook Ingestion - Design

`POST /webhooks/cal/card-status` receives card lifecycle events from Cal and
turns every accepted event into durable state in MongoDB, one audit entry, one
in-app message, and one push notification workflow. The design assumes multiple
ECS Fargate tasks behind an ALB, duplicate Cal retries, process crashes, MongoDB
or provider outages, and no ordering guarantee from Cal.

Diagram: see `architecture.md` (Mermaid renders on GitHub).

## Shape of the system

Cal -> CloudFront/WAF -> ALB -> Fargate handler -> HMAC verification -> MongoDB
`events` insert with `_id = event_id` -> SQS Standard -> worker pool -> MongoDB
card/audit/in-app/notification records -> push provider. SQS is only an
at-least-once transport; it is not the deduplication or ordering mechanism.
MongoDB unique indexes and atomic conditional updates are the correctness layer.

## 1. Edge / perimeter

CloudFront and AWS WAF sit in front of the ALB. WAF rate-limits obvious abuse,
blocks oversized or invalid methods, and can allowlist Cal egress IPs if Cal
publishes stable ranges. TLS terminates at the ALB.

Authenticity is enforced in the handler with an HMAC-SHA256 signature. Cal signs
`timestamp + "." + raw_body` with a shared secret and sends
`X-Cal-Signature` plus `X-Cal-Timestamp`. The handler verifies the signature
against the raw request body using constant-time comparison, rejects timestamps
outside a short replay window such as five minutes, and reads the secret from
AWS Secrets Manager so it can be rotated.

An attacker who curls the public endpoint without a valid signature receives a
flat `401 Unauthorized` response with an empty body. The request is not parsed
as a Cal event, nothing is written to MongoDB, no SQS message is produced, and
the log line is low-detail so the endpoint does not leak schema or secret
information.

## 2. Sync vs. async + durability

**Synchronous before 2xx:** verify the HMAC and timestamp, parse and validate the
event, then insert the raw event into MongoDB:

```js
db.events.insertOne({
  _id: event_id,
  status: "RECEIVED",
  raw,
  received_at,
  occurred_at,
  card_id,
  user_id,
  event_type
})
```

The `_id` unique index makes duplicate receipt safe across all pods. If MongoDB
returns duplicate key `E11000`, the event is already durable. On the duplicate
path the handler still sends an SQS message before returning `202 Accepted` —
workers are idempotent, so an extra message is cheap insurance against a missed
enqueue on the original attempt. For a new event, the handler sends a small SQS
Standard message containing `event_id` and returns `202 Accepted` only after the
event is durable and the enqueue attempt has succeeded.

**Asynchronous after 2xx:** workers consume SQS messages at least once. They load
the event from MongoDB and perform idempotent side effects:

- update the card document only if this event is strictly newer by
  `(occurred_at, event_id)` than the card's last applied event — the lexical
  tiebreaker handles same-second collisions, and the strict comparison makes
  re-applying the same event a no-op so worker retries are safe;
- insert one audit entry with a unique key on `event_id`;
- insert one in-app message with a unique key on `event_id`;
- claim one notification outbox record with an atomic conditional update, send
  the push with `event_id` as the provider idempotency key when supported, and
  mark it sent after the provider accepts it.

Cal does not guarantee ordering, so the queue is not used to order events.
Current card state is protected by `occurred_at` or, if Cal provides one later,
an issuer sequence/version. A late older event is still stored and audited, but
it must not roll the card document backward.

**Failure walk-through:**

- **(a) Process crashes after receiving the request but before doing anything.**
  Nothing is written to MongoDB or SQS. Cal sees a dropped connection or 5xx and
  retries. A later pod handles the same `event_id`; no event was silently lost.

- **(b) MongoDB is unreachable for 30 seconds.** The handler cannot make the
  event durable, so it returns 5xx and Cal retries. If MongoDB accepts the insert
  but the handler crashes before SQS enqueue, the event is still in `events`.
  Cal retries will hit the duplicate-key path, and a scheduled janitor (runs every
  30 seconds, re-enqueues events stuck in `RECEIVED` for more than 60 seconds)
  also re-enqueues old `RECEIVED` events so a missed enqueue cannot strand the
  event.

- **(c) The push provider is down for 2 hours.** The event and the notification
  outbox record remain in MongoDB. The worker fails the provider call, releases
  or lets the notification claim expire, and SQS redelivers with backoff. If
  retries are exhausted, the message lands in the DLQ and an alert fires; once
  the provider recovers, the operator can replay from the durable event/outbox
  record.

At every stage the event exists in durable storage: first MongoDB, then MongoDB
plus an at-least-once SQS reference until workers finish the side effects. Memory
is never the only copy.

## 3. Exactly-once side effects

**Dedup key.** `event_id` lives in MongoDB as `events._id`. MongoDB rejects a
second insert atomically, so there is no application-level check-then-write race.

**Atomic race resolution.** Receipt deduplication is not enough, because SQS can
redeliver and multiple workers can race. Each side effect has its own idempotent
record keyed by `event_id`. For push, workers claim the notification with one
conditional update:

```js
notification_outbox.findOneAndUpdate(
  {
    _id: event_id,
    status: "PENDING",
    $or: [{ lease_until: { $exists: false } }, { lease_until: { $lt: now } }]
  },
  {
    $set: {
      status: "SENDING",
      owner: pod_id,
      lease_until: now_plus_2_minutes
    }
  }
)
```

MongoDB serializes that update on the single document. Exactly one worker gets a
non-null return and owns the push attempt; losing workers get `null`, delete or
ack their duplicate SQS delivery, and do nothing. Audit and in-app messages use
unique inserts/upserts on `event_id`, so duplicate workers either create the row
once or observe that it already exists.

The Mongo lease is the authoritative claim. SQS visibility timeout is set to
roughly match `lease_until` so the queue and the claim agree on who owns the
in-flight attempt; long provider calls heartbeat-extend both. If they diverge,
either SQS redelivers while the claim is still held (the new worker sees the
claim and no-ops, but the original work is left without an SQS backstop) or the
lease expires while the first worker is still mid-call (a second worker can
claim and issue a duplicate push).

The hard boundary is the external push provider. The order is claim → push →
mark `SENT`. If the provider accepts but the "mark SENT" write fails, the lease
expires and a later worker re-pushes. With an idempotency key (`event_id`), the
provider de-dupes and this is safe. Without one, no backend can perfectly
guarantee external exactly-once delivery after "sent but response lost" — we
deliberately accept the rare duplicate over the rare lost push, because for
`card.blocked` the user-visible cost of a missed notification is higher than a
repeated one. The honest guarantee is exactly one internal owner plus
retry/alert behavior.

**TTL / lifetime.** The event dedup record should live at least longer than Cal's
retry horizon, for example 90 days, and can be retained longer for support.
Audit entries likely have a longer compliance retention policy. Notification
and in-app outbox records can be retained until support no longer needs delivery
history.

**What wins on a race.** For duplicate receipt, the first MongoDB insert wins.
For side effects, the first successful conditional claim or unique insert wins.
For card state, the newest issuer event wins by `(occurred_at, event_id)` (or
issuer version if Cal exposes one), not by the order workers happen to process
messages.

## 4. Observability

Logs are structured JSON with `event_id`, `card_id`, `user_id`, `event_type`,
`pod_id`, `trace_id`, stage, latency, outcome, duplicate/claim result, and push
provider response code. OpenTelemetry traces have spans for receive, Mongo
insert, SQS enqueue, worker claim, card update, audit insert, in-app insert, and
push send; the `trace_id` is copied to SQS message attributes.

Alerts cover endpoint 5xx rate, signature failures, SQS age of oldest message,
DLQ depth, stuck `RECEIVED` events, notification outbox records stuck in
`SENDING`, push provider error/rate-limit responses, and MongoDB write latency.

Do not log PAN, CVV, HMAC secrets, raw signature headers, full raw payloads, or
unnecessary cardholder PII. `event_id`, `card_id`, and `user_id` are useful as
tokenized references.

For "the customer says they did not get a notification 8 hours ago":

1. Query MongoDB `events` by `card_id` / `user_id` and time window.
2. Check whether the event exists and whether its side-effect records exist.
3. If the notification is `SENT`, inspect provider response and device delivery
   diagnostics.
4. If it is `PENDING`, `SENDING`, or in the DLQ, use `trace_id` and worker logs
   to find the failing stage.
5. If no event exists, inspect edge logs and signature-failure metrics to decide
   whether Cal never sent it or we rejected it before persistence.

## What I'd do differently with more time/budget

I would move from "insert into MongoDB, then enqueue SQS" to a formal outbox
driven from MongoDB change streams, which removes the handler's dual-write gap.
I would also add stricter schema validation for Cal event versions and mTLS on
top of HMAC if Cal supports client certificates. I would keep those out of the
first version because the unique MongoDB records plus SQS retry path already
meet the core correctness requirements with less operational surface area.
