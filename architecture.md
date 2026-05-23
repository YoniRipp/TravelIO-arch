# Architecture — Cal Card-Status Webhook Ingestion

## Component view

```mermaid
flowchart LR
    Cal[Cal - card issuer]

    subgraph Edge["Public edge - AWS"]
        WAF[CloudFront + WAF]
        ALB[ALB - TLS termination]
    end

    subgraph App["VPC - private"]
        direction TB
        H[Webhook handler - ECS Fargate x3+]
        Q[(SQS queue - standard)]
        W[Worker pool - ECS Fargate]
        DLQ[(SQS DLQ)]
        JAN[Janitor job - scheduled]
    end

    subgraph Data["State"]
        M[(MongoDB)]
        SM[Secrets Manager]
    end

    PUSH[Push provider - 3rd party]
    OBS[OTEL + CloudWatch]

    Cal -->|"HTTPS + HMAC sig"| WAF
    WAF --> ALB
    ALB --> H

    H -->|"verify sig"| SM
    H -->|"insertOne, unique index on event_id"| M
    H -->|"enqueue event_id"| Q
    ALB -.->|"202 Accepted"| Cal

    Q --> W
    W -->|"1. claim: RECEIVED to PROCESSING"| M
    W -->|"2. upsert card state"| M
    W -->|"3. send push"| PUSH
    W -->|"4. write audit + set DONE"| M
    Q -.->|"maxReceiveCount exceeded"| DLQ

    JAN -->|"sweep stuck RECEIVED or stale PROCESSING"| M
    JAN -->|"re-enqueue"| Q

    H -.->|"logs/traces"| OBS
    W -.->|"logs/traces"| OBS
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
    participant Worker
    participant Push as Push provider

    Cal->>ALB: POST /webhooks/cal/card-status
    ALB->>Handler: forward to one pod
    Handler->>Handler: verify HMAC + timestamp window
    Handler->>Mongo: insertOne with _id = event_id, status = RECEIVED
    Note over Mongo: unique index on _id: duplicate returns E11000
    Note over Handler: on duplicate key: still enqueue and return 202
    Handler->>SQS: SendMessage with event_id
    Handler-->>Cal: 202 Accepted

    SQS->>Worker: deliver message
    Worker->>Mongo: findOneAndUpdate where status = RECEIVED, set PROCESSING
    Note over Worker,Mongo: single gate: only one pod wins, winner does everything
    Mongo-->>Worker: matched - winner
    Worker->>Mongo: upsert card state, guard by occurred_at
    Worker->>Push: sendNotification for card_id, user_id
    Push-->>Worker: 200 OK
    Worker->>Mongo: write audit entry
    Worker->>Mongo: updateOne set status = DONE
    Worker->>SQS: deleteMessage
```

## Race resolution — two workers, same event_id

This is the critical constraint. Two workers receive SQS messages for the
same `event_id` (via Cal retry re-enqueue or SQS redelivery after visibility
timeout). The single atomic gate ensures exactly one worker performs all side
effects.

```mermaid
sequenceDiagram
    participant W1 as Worker A
    participant W2 as Worker B
    participant Mongo
    participant Push as Push provider

    Note over W1,W2: Both pull SQS messages for event_id = evt_123

    W1->>Mongo: findOneAndUpdate status = RECEIVED, set PROCESSING
    W2->>Mongo: findOneAndUpdate status = RECEIVED, set PROCESSING
    Mongo-->>W1: matched - winner
    Mongo-->>W2: null - loss

    Note over W2: lost the gate, deletes SQS message, does nothing

    W1->>Mongo: upsert card state
    W1->>Push: sendNotification
    Push-->>W1: 200 OK
    W1->>Mongo: write audit entry
    W1->>Mongo: set status = DONE
    W1->>W1: delete SQS message

    Note over W1: exactly one push sent, one audit written
    Note over W2: zero pushes, zero audits - correct
```

## Crash recovery — worker dies mid-processing

A worker wins the gate, sends the push, then crashes before writing the
audit. The janitor detects the stale `PROCESSING` event, resets it to
`RECEIVED`, and re-enqueues. A new worker claims it and completes the
remaining work. Push double-send is prevented by including `event_id` as an
idempotency key with the provider.

```mermaid
sequenceDiagram
    participant W1 as Worker A - crashes
    participant JAN as Janitor
    participant SQS
    participant W2 as Worker B - replacement
    participant Mongo
    participant Push as Push provider

    W1->>Mongo: findOneAndUpdate RECEIVED to PROCESSING
    Mongo-->>W1: matched
    W1->>Mongo: upsert card state
    W1->>Push: sendNotification
    Push-->>W1: 200 OK
    Note over W1: CRASH before audit and DONE

    JAN->>Mongo: find PROCESSING events older than 5 min
    JAN->>Mongo: reset status to RECEIVED
    JAN->>SQS: re-enqueue event_id

    SQS->>W2: deliver message
    W2->>Mongo: findOneAndUpdate RECEIVED to PROCESSING
    Mongo-->>W2: matched - new winner
    W2->>Mongo: upsert card state - idempotent, harmless
    W2->>Push: sendNotification with event_id as idempotency key
    Note over Push: provider deduplicates - no double push
    Push-->>W2: 200 OK
    W2->>Mongo: write audit entry
    W2->>Mongo: set status = DONE
    W2->>SQS: deleteMessage
    Note over W2: event complete
```
