# Architecture - Cal Card-Status Webhook Ingestion

## Component view

```mermaid
flowchart LR
    Cal((Cal<br/>card issuer))

    subgraph Edge["Public edge — AWS"]
        direction TB
        WAF[CloudFront + WAF<br/>rate limit, size/method filters]
        ALB[ALB<br/>TLS termination]
        WAF --> ALB
    end

    subgraph App["App tier — VPC private"]
        direction TB
        H[Webhook handler<br/>ECS Fargate, 3+ tasks<br/>POST /webhooks/cal/card-status]
        Q[(SQS Standard<br/>at-least-once)]
        W[Worker pool<br/>ECS Fargate]
        DLQ[(SQS DLQ)]
        JAN[Janitor<br/>every 30s; re-enqueues<br/>RECEIVED &gt; 60s]
    end

    subgraph Data["MongoDB"]
        direction TB
        E[(events<br/>_id = event_id)]
        C[(cards<br/>update if strictly newer<br/>by occurred_at, event_id)]
        A[(audit<br/>unique event_id)]
        IN[(in-app messages<br/>unique event_id)]
        NO[(notification outbox<br/>unique event_id + lease)]
    end

    SM[/Secrets Manager<br/>Cal HMAC secret/]
    PUSH[Push provider<br/>idempotency key = event_id]
    OBS[OpenTelemetry + CloudWatch<br/>logs, traces, metrics, alerts]

    Cal -->|HTTPS + HMAC signature| WAF
    ALB --> H
    H -.->|202 Accepted| ALB
    ALB -.->|202 Accepted| Cal

    H -->|read at boot, cache + rotate| SM
    H -->|insertOne unique _id| E
    H -->|SendMessage event_id| Q

    Q --> W
    Q -. maxReceiveCount .-> DLQ

    W -->|load raw event| E
    W -->|update if strictly newer| C
    W -->|insert unique| A
    W -->|insert unique| IN
    W -->|claim PENDING -&gt; SENDING| NO
    W -->|send with idempotency key| PUSH
    W -->|mark SENT, event DONE| NO

    JAN -->|scan stuck RECEIVED| E
    JAN -->|re-enqueue event_id| Q

    H -. logs/traces .-> OBS
    W -. logs/traces .-> OBS
    DLQ -. depth alert .-> OBS
```

**Notes on the diagram.** Solid arrows are request-side data flow; dotted arrows
are responses, observability fan-out, or queue-internal transitions. Secrets
Manager is intentionally drawn outside the MongoDB group — it is an AWS service,
not part of the application's persistent state. In-app messages and the
notification outbox are separate collections because they have different
lifecycles: in-app messages are insert-once, the outbox carries lease state for
the push attempt.

## Request sequence — happy path

```mermaid
sequenceDiagram
    autonumber
    participant Cal
    participant ALB
    participant Handler as Fargate handler
    participant Mongo
    participant SQS
    participant Worker
    participant Push as Push provider

    Cal->>ALB: POST /webhooks/cal/card-status<br/>X-Cal-Signature, X-Cal-Timestamp
    ALB->>Handler: forward to one pod
    Handler->>Handler: verify HMAC over raw body + timestamp window
    Handler->>Mongo: insertOne event {_id: event_id, status: RECEIVED, raw}
    Note over Mongo: unique _id makes duplicate receipt safe
    Handler->>SQS: SendMessage({event_id})
    Handler-->>ALB: 202 Accepted
    ALB-->>Cal: 202 Accepted

    SQS->>Worker: deliver event_id (at least once)
    Worker->>Mongo: load event by event_id
    Worker->>Mongo: update card if strictly newer by (occurred_at, event_id)
    Worker->>Mongo: insert audit entry, unique on event_id
    Worker->>Mongo: insert in-app message, unique on event_id
    Worker->>Mongo: claim notification outbox PENDING -> SENDING
    Note over Worker,Mongo: one worker wins the claim; duplicates get null
    Worker->>Push: sendNotification(..., idempotency_key = event_id)
    Push-->>Worker: accepted
    Worker->>Mongo: mark notification SENT, event DONE
    Worker->>SQS: deleteMessage
```

## Race resolution — duplicate deliveries and workers

```mermaid
sequenceDiagram
    participant CalA as Cal delivery 1
    participant CalB as Cal retry
    participant PodA as Handler Pod A
    participant PodB as Handler Pod B
    participant Mongo
    participant SQS
    participant WorkerA as Worker A
    participant WorkerB as Worker B

    par concurrent deliveries
        CalA->>PodA: POST event_id=evt_123
    and
        CalB->>PodB: POST event_id=evt_123
    end

    PodA->>Mongo: insertOne({_id: evt_123})
    Mongo-->>PodA: OK
    PodB->>Mongo: insertOne({_id: evt_123})
    Mongo-->>PodB: E11000 duplicate key

    Note over PodA,PodB: Both pods enqueue — workers de-dupe via the Mongo claim
    PodA->>SQS: SendMessage(evt_123)
    PodB->>SQS: SendMessage(evt_123)

    PodA-->>CalA: 202
    PodB-->>CalB: 202

    par worker race
        SQS->>WorkerA: deliver evt_123
        WorkerA->>Mongo: claim {_id: evt_123, status: PENDING}
    and
        SQS->>WorkerB: deliver evt_123
        WorkerB->>Mongo: claim {_id: evt_123, status: PENDING}
    end

    Mongo-->>WorkerA: claimed document
    Mongo-->>WorkerB: null
    Note over WorkerA,WorkerB: Worker A owns the push. Worker B acks SQS and no-ops.
```
