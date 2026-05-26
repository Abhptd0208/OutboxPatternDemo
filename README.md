I tackled one challenge in distributed systems ensuring data consistency across microservices without risking data loss. 

When building an ecosystem with multiple microservices (like Order, Inventory, and Payment), a classic problem arises: 

The Dual-Write Trap
Traditionally, when a user creates an order, you save it to your database and immediately publish an event to a message broker like RabbitMQ. But what happens if the database save succeeds, and the network blinks right before the event hits the broker? 

Your database says "Success," but your Inventory and Payment services never get the memo. You are stuck with a corrupted, inconsistent state.

The Outbox Pattern
To solve this, I implemented the Outbox Pattern in .NET Web API using SQL Server and RabbitMQ. 

Instead of writing to two different systems simultaneously, everything happens in one atomic local database transaction:
1. The API saves the business entity (Order) and serializes the event payload into a dedicated `OutboxMessages` table in the same DB transaction. 
2. A separate background worker (`BackgroundService`) continuously polls the Outbox table for unprocessed messages.
3. The worker publishes the pending event to RabbitMQ and marks it as processed only after receiving a successful broker acknowledgment.

By decoupling the database commit from the message broadcast, the system now guarantees At-Least-Once Delivery. Even if RabbitMQ goes completely offline or a network partition occurs:
1. The user experience is unaffected (the API responds instantly).
2. No data or events are ever lost.
3. The system automatically catches up the moment infrastructure recovers.

Huge shoutout to the asynchronous capabilities of the modern RabbitMQ.Client (v7+) which made building the background consumer channels incredibly efficient! 

Onwards to exploring Sagas next to handle distributed rollbacks.⚡

<img width="1761" height="633" alt="image" src="https://github.com/user-attachments/assets/24865c4f-f016-4e94-b3f7-66cdbdf54040" />
<img width="1919" height="1049" alt="Screenshot 2026-05-25 175937" src="https://github.com/user-attachments/assets/13ff1f22-0aeb-4489-9094-034cd1fa5b5c" />
