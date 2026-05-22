# Architecture - Cal Card-Status Webhook Ingestion

## Component view

```mermaid
flowchart LR
    Cal[Cal<br/>card issuer]

    subgraph Edge["Public edge (AWS)"]
        WAF[CloudFront + WAF<br/>rate limit, size/method filters]
        ALB[ALB<br/>TLS termination]
    end

    subgraph App["VPC - private"]
        direction TB
        H[Webhook handler<br/>ECS Fargate 3+ tasks<br/>POST /webhooks/cal/card-status]
        Q[(SQS Standard<br/>at-least-once queue)]
        W[Worker pool<br/>ECS Fargate]
        DLQ[(SQS DLQ)]
        JAN[Janitor job<br/>sweeps stuck RECEIVED events]
    end

    subgraph Data["MongoDB state"]
        E[(events<br/>_id = event_id)]
        C[(cards<br/>conditional state update)]
        A[(audit<br/>unique event_id)]
        O[(notification + in-app outbox<br/>unique event_id)]
        SM[Secrets Manager<br/>Cal HMAC secret]
    end

    PUSH[Push provider<br/>best-effort, rate-limited]
    OBS[OpenTelemetry + CloudWatch<br/>logs, traces, metrics, alerts]

    Cal -->|HTTPS + HMAC signature| WAF --> ALB --> H
    H -->|read secret| SM
    H -->|verify raw body signature| H
    H -->|insertOne event<br/>unique event_id| E
    H -->|SendMessage event_id| Q
    H -->|202 Accepted| ALB --> Cal

    Q --> W
    W -->|load raw event| E
    W -->|update if newer occurred_at/version| C
    W -->|insert/upsert unique event_id| A
    W -->|insert/upsert and claim unique event_id| O
    W -->|send with idempotency key = event_id| PUSH
    W -->|mark side effects done| E
    Q -. maxReceiveCount .-> DLQ

    JAN -->|find old RECEIVED| E
    JAN -->|re-enqueue event_id| Q

    H -. logs/traces .-> OBS
    W -. logs/traces .-> OBS
    DLQ -. alert .-> OBS
```

## Request sequence - happy path

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
    Handler-->>Cal: 202 Accepted

    SQS->>Worker: deliver event_id at least once
    Worker->>Mongo: load event by event_id
    Worker->>Mongo: update card only if event is newer
    Worker->>Mongo: insert audit entry unique on event_id
    Worker->>Mongo: insert in-app message unique on event_id
    Worker->>Mongo: claim notification outbox PENDING to SENDING
    Note over Worker,Mongo: one worker gets the claim; duplicates get null
    Worker->>Push: sendNotification(..., idempotency_key = event_id)
    Push-->>Worker: accepted
    Worker->>Mongo: mark notification SENT and event DONE
    Worker->>SQS: deleteMessage
```

## Race resolution - duplicate deliveries and workers

```mermaid
sequenceDiagram
    participant CalA as Cal delivery 1
    participant CalB as Cal retry
    participant PodA as Handler Pod A
    participant PodB as Handler Pod B
    participant WorkerA as Worker A
    participant WorkerB as Worker B
    participant Mongo

    par
        CalA->>PodA: POST event_id=evt_123
    and
        CalB->>PodB: POST event_id=evt_123
    end

    PodA->>Mongo: insertOne({_id: evt_123})
    Mongo-->>PodA: OK
    PodB->>Mongo: insertOne({_id: evt_123})
    Mongo-->>PodB: E11000 duplicate key
    PodA-->>CalA: 202
    PodB-->>CalB: 202

    par
        WorkerA->>Mongo: claim notification {_id: evt_123, status: PENDING}
    and
        WorkerB->>Mongo: claim notification {_id: evt_123, status: PENDING}
    end

    Mongo-->>WorkerA: claimed document
    Mongo-->>WorkerB: null
    Note over WorkerA,WorkerB: Worker A owns the push. Worker B acks/no-ops.
```
