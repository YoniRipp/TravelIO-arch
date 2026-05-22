# Architecture — Cal Card-Status Webhook Ingestion

## Component view

```mermaid
flowchart LR
    Cal[Cal<br/>card issuer]

    subgraph Edge["Public edge (AWS)"]
        WAF[CloudFront + WAF<br/>rate limit, IP/ASN filter]
        ALB[ALB<br/>TLS termination]
    end

    subgraph App["VPC — private"]
        direction TB
        H[Webhook handler<br/>ECS Fargate ≥3 tasks<br/>POST /webhooks/cal/card-status]
        Q[(SQS queue<br/>standard, with DLQ)]
        W[Worker pool<br/>ECS Fargate]
        DLQ[(SQS DLQ)]
        JAN[Janitor job<br/>scheduled]
    end

    subgraph Data["State"]
        M[(MongoDB<br/>events / cards / audit)]
        SM[Secrets Manager<br/>Cal HMAC secret]
    end

    PUSH[Push provider<br/>3rd party, rate-limited]
    OBS[OTEL + CloudWatch<br/>logs, traces, metrics]

    Cal -->|HTTPS + HMAC sig| WAF --> ALB --> H
    H -->|verify sig| SM
    H -->|"insertOne _id=event_id<br/>(unique index)"| M
    H -->|enqueue<br/>dedup id = event_id| Q
    H -->|202 Accepted| ALB --> Cal

    Q --> W
    W -->|"findOneAndUpdate<br/>status PERSISTED→NOTIFIED"| M
    W -->|send push| PUSH
    W -->|"findOneAndUpdate<br/>status NOTIFIED→AUDITED"| M
    Q -. maxReceiveCount .-> DLQ

    JAN -->|sweep stuck RECEIVED| M
    JAN -->|re-enqueue| Q

    H -. logs/traces .-> OBS
    W -. logs/traces .-> OBS
```

## Request sequence — happy path

```mermaid
sequenceDiagram
    autonumber
    participant Cal
    participant ALB
    participant Handler as Fargate handler
    participant Mongo
    participant SQS
    participant Worker as Worker
    participant Push as Push provider

    Cal->>ALB: POST /webhooks/cal/card-status<br/>X-Cal-Signature, X-Cal-Timestamp
    ALB->>Handler: forward (one pod)
    Handler->>Handler: verify HMAC + timestamp window
    Handler->>Mongo: insertOne({_id: event_id, status: RECEIVED, raw})
    Note over Mongo: unique index on _id —<br/>duplicate ⇒ E11000 ⇒ treat as success
    Handler->>SQS: SendMessage(dedup_id = event_id)
    Handler-->>Cal: 202 Accepted

    SQS->>Worker: deliver message
    Worker->>Mongo: findOneAndUpdate(<br/>{_id, status: PERSISTED},<br/>{$set: status: NOTIFIED})
    Note over Worker,Mongo: only the pod that flips<br/>PERSISTED→NOTIFIED owns the push
    Worker->>Push: sendNotification(card_id, user_id)
    Push-->>Worker: 200
    Worker->>Mongo: findOneAndUpdate(<br/>{_id, status: NOTIFIED},<br/>{$set: status: AUDITED})
    Worker->>Mongo: insert audit entry, update card state
    Worker->>SQS: deleteMessage
```

## Race resolution — two pods, same `event_id`

```mermaid
sequenceDiagram
    participant CalA as Cal (delivery 1)
    participant CalB as Cal (delivery 2 — retry)
    participant PodA as Pod A
    participant PodB as Pod B
    participant Mongo

    par
        CalA->>PodA: POST event_id=evt_123
    and
        CalB->>PodB: POST event_id=evt_123
    end

    PodA->>Mongo: insertOne({_id: evt_123, status: RECEIVED})
    Mongo-->>PodA: OK (winner)
    PodB->>Mongo: insertOne({_id: evt_123, status: RECEIVED})
    Mongo-->>PodB: E11000 duplicate key

    PodA-->>CalA: 202
    PodB-->>CalB: 202 (idempotent — same outcome)

    Note over PodA,PodB: Both ack. Only PodA enqueued.<br/>Worker stage uses the same<br/>conditional findOneAndUpdate to<br/>ensure exactly one push.
```
