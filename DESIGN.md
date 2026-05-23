# Webhook Ingestion - Design

`POST /webhooks/cal/card-status` turns each accepted Cal event into durable
state in MongoDB, one audit entry, one in-app message, and one push
notification — exactly once per event across multiple ECS Fargate pods. See
`architecture.md` for diagrams.

## 1. Edge / perimeter

CloudFront and AWS WAF sit in front of the ALB; WAF rate-limits abuse, blocks
oversized or invalid methods, and can allowlist Cal egress IPs if published.
TLS terminates at the ALB.

Authenticity is enforced in the handler with an HMAC-SHA256 signature. Cal
signs `timestamp + "." + raw_body` and sends `X-Cal-Signature` plus
`X-Cal-Timestamp`. The handler verifies against the raw body with
constant-time comparison, rejects timestamps outside a 5-minute window
(tolerates NTP skew), and reads the rotatable secret from AWS Secrets Manager
with short-TTL caching.

An attacker hitting the endpoint receives a flat `401 Unauthorized` with empty
body — no parsing, no Mongo write, no SQS message, low-detail log so we leak
no schema or secret state. Schema-invalid but signed payloads return `400`
without persisting, and we alert on the 400 rate because Cal would otherwise
retry indefinitely. v1 validates presence of `event_id`, `event_type`,
`card_id`, `user_id`, and `occurred_at`; unknown event types are stored and
audited but skip type-specific side effects until handlers ship.

## 2. Sync vs. async + durability

**Synchronous before 2xx.** Verify HMAC and timestamp, validate schema, then
insert into `events` with `_id = event_id`, `status = RECEIVED`, and the raw
payload — using majority write concern so a primary failover does not lose
it. In the same handler we also insert a `notification_outbox` row keyed on
`event_id` with `status = PENDING`; this is the single source of truth for
which events still need a push, and creating it synchronously keeps the
worker's claim a pure conditional update with no upsert race. The handler
then enqueues an SQS Standard message containing only `event_id` and returns
`202 Accepted`. On duplicate (`E11000`) it still enqueues as cheap insurance
against a missed enqueue on the original attempt; workers are idempotent. The
handler is stateless, so autoscaling is safe — correctness lives in MongoDB's
unique indexes, not in process memory.

**Asynchronous after 2xx.** Workers consume SQS at least once, load the event,
then idempotently: update `cards` only if the new event is strictly newer by
`(occurred_at, event_id)` — the lexical tiebreaker handles same-second
collisions, strict comparison makes retries a no-op, and a late-older event
is audited but never rolls the card backward; unique-insert `audit` and
`in_app` keyed on `event_id`; claim the outbox row (§3); push; mark `SENT`
and event `DONE`.

`events.status` is `RECEIVED → DONE` (or `FAILED` after DLQ).
`notification_outbox.status` is `PENDING → SENDING → SENT`. A janitor every
30s re-enqueues `events` stuck in `RECEIVED` over 60s and resets outbox rows
whose `SENDING` lease has lapsed.

**Failures.** **(a)** Crash before anything: nothing written; Cal retries on
dropped connection or 5xx; a later pod handles the same `event_id`.
**(b1)** Mongo unreachable for 30s: handler returns 5xx, Cal retries until
recovery. **(b2)** Mongo accepted the insert but the handler died before SQS:
the event sits as `RECEIVED`; Cal's retry hits the dup-key path and
re-enqueues, and the janitor backstops it. **(c)** Push provider down for 2h:
event and outbox row remain in MongoDB; the worker fails the call, the lease
expires, SQS redelivers with backoff; exhausted retries land in the DLQ with
an alert, and the operator replays from durable records when the provider
recovers. An ECS task drain during deploy returns 5xx for in-flight requests
and Cal retries hit a healthy task.

## 3. Exactly-once side effects

**Dedup key.** `event_id` is `events._id`. MongoDB rejects a duplicate insert
atomically with `E11000`, so there is no check-then-write race on receipt.
The handler also creates the PENDING `notification_outbox` row; `audit` and
`in_app` use unique inserts on `event_id`.

**Worker claim.** Workers race on one conditional update of the outbox row:
match `_id = event_id` AND `status = PENDING` AND (no `lease_until` OR
`lease_until < now`); set `status = SENDING`, `owner = pod_id`, `lease_until =
now + 2 min`. MongoDB serializes this on the document, so exactly one worker
returns a non-null document and owns the push; losing workers get `null`, ack
the duplicate SQS delivery, and no-op. Audit and in-app duplicates similarly
collapse to first-write-wins.

**Lease ↔ visibility.** SQS visibility timeout is set near the lease, and a
background ticker on the worker extends both during long provider calls. If
they diverge we accept either a no-op duplicate delivery or — rarely — a
duplicate push.

**Hard boundary at the provider.** Order is claim → push → mark `SENT`. If
"mark SENT" fails after the provider accepted, the lease expires and another
worker re-pushes; with `event_id` as the provider idempotency key this is
safe, and without one we deliberately accept the rare duplicate over the
rare lost push because for `card.blocked` a missed notification is more
user-visible than a repeated one.

**TTL.** `events` lives ≥ Cal's retry horizon (e.g. 90 days); audit follows
compliance retention; outbox rows are kept until support no longer needs
delivery history. First Mongo insert wins on receipt, first claim or unique
insert wins on side effects, newest `(occurred_at, event_id)` wins on card
state.

## 4. Observability

Structured JSON logs carry `event_id`, `card_id`, `user_id`, `event_type`,
`pod_id`, `trace_id`, stage, outcome, claim/duplicate result, and provider
response. OpenTelemetry spans cover receive → Mongo insert → SQS enqueue →
worker claim → side-effect writes → push send; `trace_id` propagates via SQS
message attributes. Alerts cover 5xx and 4xx rate, signature failures, SQS
oldest-message age, DLQ depth, stuck `RECEIVED` and `SENDING` counts,
provider error/rate-limit rate, and Mongo write latency.

We never log PAN, CVV, HMAC secrets, raw signature headers, full raw
payloads, or unnecessary cardholder PII; `event_id`, `card_id`, and `user_id`
are tokenized references safe to log.

**"Didn't get a notification 8 hours ago":** query `events` by `card_id` and
window → check the event and its side-effect rows → if `SENT`, inspect
provider response and device diagnostics; if `PENDING/SENDING/DLQ`, follow
`trace_id` to the failing stage; if no event exists, edge logs and
signature-failure metrics decide whether Cal never sent it or we rejected it
before persistence.

## What I'd do differently with more time/budget

I would replace the handler's dual write (Mongo + SQS) with a formal outbox
driven by MongoDB change streams, closing the dual-write gap instead of
leaning on the janitor. I would add richer schema validation including Cal
event-version negotiation, and mTLS on top of HMAC if Cal supports client
certificates. These stayed out of v1 because the unique-index plus claim plus
retry path already meets the correctness requirements with less operational
surface area.

