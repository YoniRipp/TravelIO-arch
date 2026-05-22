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
        JAN[Janitor job<br/>sweeps stuck RECEIVED<br/>and lapsed SENDING]
    end

    subgraph Data["MongoDB state"]
        E[(events<br/>_id = event_id<br/>RECEIVED → DONE)]
        C[(cards<br/>newer-wins update)]
        A[(audit<br/>unique event_id)]
        N[(notification_outbox<br/>PENDING → SENDING → SENT)]
        I[(in_app<br/>unique event_id)]
        SM[Secrets Manager<br/>Cal HMAC secret]
    end

    PUSH[Push provider<br/>best-effort, rate-limited]
    OBS[OpenTelemetry + CloudWatch<br/>logs, traces, metrics, alerts]

    Cal -->|HTTPS + HMAC signature| WAF --> ALB --> H
    H -->|read cached secret| SM
    H -->|insertOne event<br/>majority write concern| E
    H -->|insertOne outbox<br/>status PENDING| N
    H -->|SendMessage event_id| Q
    H -->|202 Accepted| Cal

    Q --> W
    W -->|load raw event| E
    W -->|update if newer| C
    W -->|unique insert| A
    W -->|unique insert| I
    W -->|conditional claim<br/>PENDING → SENDING| N
    W -->|send idempotency_key = event_id| PUSH
    W -->|mark SENT + event DONE| E
    Q -. maxReceiveCount .-> DLQ

    JAN -->|find old RECEIVED<br/>and lapsed SENDING| E
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
    Handler->>Mongo: insertOne notification_outbox {_id: event_id, status: PENDING}
    Handler->>SQS: SendMessage({event_id})
    Handler-->>Cal: 202 Accepted

    SQS->>Worker: deliver event_id at least once
    Worker->>Mongo: load event by event_id
    Worker->>Mongo: update card only if event is newer
    Worker->>Mongo: insert audit entry unique on event_id
    Worker->>Mongo: insert in-app message unique on event_id
    Worker->>Mongo: claim outbox PENDING → SENDING (conditional update)
    Note over Worker,Mongo: one worker gets the claim; duplicates get null
    Worker->>Push: sendNotification(..., idempotency_key = event_id)
    Push-->>Worker: accepted
    Worker->>Mongo: mark outbox SENT and event DONE
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

    PodA->>Mongo: insertOne event {_id: evt_123}
    Mongo-->>PodA: OK
    PodB->>Mongo: insertOne event {_id: evt_123}
    Mongo-->>PodB: E11000 duplicate key
    PodA-->>CalA: 202
    PodB-->>CalB: 202

    par
        WorkerA->>Mongo: claim outbox {_id: evt_123, status: PENDING}
    and
        WorkerB->>Mongo: claim outbox {_id: evt_123, status: PENDING}
    end

    Mongo-->>WorkerA: claimed document
    Mongo-->>WorkerB: null
    Note over WorkerA,WorkerB: Worker A owns the push. Worker B acks/no-ops.
```
