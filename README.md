# Midas Core

**JP Morgan Chase & Co. — Software Engineering Job Simulation**
Completed by Omkar Dubal | Issued by Forage | March 8th, 2026
Verification Code: `nxdPKccQvd5m3zjdK`

---

## What is Midas Core?

Midas Core is a financial transaction processing backend built as part of the JP Morgan Chase Software Engineering Job Simulation. It simulates the kind of system that a real bank might use to handle incoming money transfers between users.

The system:
- Listens to a **Kafka message queue** for incoming transactions
- **Validates** each transaction against user balances in the database
- **Records** valid transactions to a database and updates user balances
- Calls an **external Incentive API** to apply bonus rewards to recipients
- Exposes a **REST API** so users can query their current balance

---

## What Does It Do?

When a transaction message arrives on the Kafka topic `trader-updates`, Midas Core:

1. Looks up the **sender** and **recipient** in the database by their IDs
2. Checks that the sender has **enough balance** to cover the transaction amount
3. If valid — calls the **Incentive API** to get a bonus reward amount
4. **Deducts** the amount from the sender's balance
5. **Credits** the recipient with the transaction amount **plus** the incentive bonus
6. **Saves** the transaction record (including the incentive) to the database
7. Invalid transactions are **silently discarded** — no changes are made

Users can then query their balance at any time via:
```
GET http://localhost:33400/balance?userId={id}
```

---

## File Structure

```
forage-midas/
│
├── services/
│   └── transaction-incentive-api.jar     # External Incentive API (black box, runs on port 8080)
│
├── src/
│   ├── main/
│   │   ├── java/com/jpmc/midascore/
│   │   │   │
│   │   │   ├── component/                # Spring components (core logic)
│   │   │   │   ├── BalanceController.java     # REST endpoint: GET /balance
│   │   │   │   ├── DatabaseConduit.java       # Helper for saving users to DB
│   │   │   │   ├── KafkaConsumerConfig.java   # Kafka consumer setup
│   │   │   │   ├── KafkaProducerConfig.java   # Kafka producer setup (used by tests)
│   │   │   │   └── TransactionListener.java   # Main logic: validate, incentivize, record
│   │   │   │
│   │   │   ├── entity/                   # JPA database entities
│   │   │   │   ├── TransactionRecord.java     # DB table for processed transactions
│   │   │   │   └── UserRecord.java            # DB table for users and their balances
│   │   │   │
│   │   │   ├── foundation/               # Plain data models (POJOs)
│   │   │   │   ├── Balance.java               # Response model for /balance endpoint
│   │   │   │   ├── Incentive.java             # Response model from Incentive API
│   │   │   │   └── Transaction.java           # Kafka message model (senderId, recipientId, amount)
│   │   │   │
│   │   │   ├── repository/               # Database access interfaces
│   │   │   │   ├── TransactionRepository.java # CRUD for TransactionRecord
│   │   │   │   └── UserRepository.java        # CRUD for UserRecord (find by id, find by name)
│   │   │   │
│   │   │   └── MidasCoreApplication.java      # Spring Boot entry point + RestTemplate bean
│   │   │
│   │   └── resources/
│   │       └── application.yml               # App config: kafka topic, server port
│   │
│   └── test/
│       ├── java/com/jpmc/midascore/
│       │   ├── BalanceQuerier.java            # Test helper: queries /balance endpoint
│       │   ├── FileLoader.java                # Test helper: loads test data files
│       │   ├── KafkaProducer.java             # Test helper: sends messages to Kafka
│       │   ├── UserPopulator.java             # Test helper: seeds users into the database
│       │   ├── TaskOneTests.java              # Test: verifies project setup
│       │   ├── TaskTwoTests.java              # Test: verifies Kafka integration
│       │   ├── TaskThreeTests.java            # Test: verifies H2 database integration
│       │   ├── TaskFourTests.java             # Test: verifies Incentive API integration
│       │   └── TaskFiveTests.java             # Test: verifies REST balance endpoint
│       │
│       └── resources/
│           ├── application.yml               # Test config: embedded Kafka, port overrides
│           └── test_data/                    # Transaction data files used by tests
│               ├── alskdjfh.fhdjsk
│               ├── lkjhgfdsa.hjkl
│               ├── mnbvcxz.vbnm
│               ├── poiuytrewq.uiop
│               └── rueiwoqp.tyruei
│
├── .gitignore
├── application.yml                           # Root-level config
├── mvnw / mvnw.cmd                           # Maven wrapper scripts
├── pom.xml                                   # Project dependencies
└── README.md
```

---

## How It Works — Logic Breakdown

### Transaction Flow

```
Kafka Message (Transaction)
        │
        ▼
TransactionListener.listen()
        │
        ├── userRepository.findById(senderId)     → null? DISCARD
        ├── userRepository.findById(recipientId)  → null? DISCARD
        ├── sender.balance >= amount?             → false? DISCARD
        │
        ▼
POST http://localhost:8080/incentive (Transaction)
        │
        ▼
Incentive.amount (bonus >= 0)
        │
        ▼
sender.balance    -= amount
recipient.balance += amount + incentiveAmount
        │
        ▼
userRepository.save(sender)
userRepository.save(recipient)
transactionRepository.save(new TransactionRecord(...))
```

### Balance Endpoint Flow

```
GET /balance?userId=5
        │
        ▼
userRepository.findById(5)
        │
        ├── found    → return Balance(user.getBalance())
        └── not found → return Balance(0)
```

### Key Design Decisions

- **SQL over NoSQL** — H2 is used because financial data requires strong consistency and failure resilience. NoSQL databases are faster but less robust for money-related operations.
- **JPA abstraction** — All database access goes through Spring Data JPA repositories, meaning the H2 database can be swapped for a production database (PostgreSQL, MySQL, etc.) with minimal code changes.
- **Incentive API as a black box** — The incentive logic is decoupled into a separate service. Midas Core only needs to know the API contract (POST a Transaction, receive an Incentive). Either team can change their implementation without affecting the other.
- **Balance endpoint in Midas Core** — Rather than creating a separate microservice for balance queries, the endpoint was added directly to Midas Core. Since the feature is small, the reduced deployment burden outweighs the architectural benefit of separation.

---

## Dependencies

Defined in `pom.xml`:

| Dependency | Purpose |
|------------|---------|
| `spring-boot-starter-web` | REST controller and embedded Tomcat server |
| `spring-boot-starter-data-jpa` | JPA / Hibernate ORM for database access |
| `spring-kafka` | Apache Kafka producer and consumer support |
| `spring-boot-starter-test` | JUnit 5 testing framework |
| `spring-kafka-test` | Embedded Kafka broker for integration tests |
| `h2` | In-memory SQL database |
| `jackson-databind` | JSON serialization / deserialization |

---

## How to Run

### Prerequisites

- Java 17 or higher
- Maven (use the included `./mvnw` wrapper — no installation needed)

### Start the application

```bash
./mvnw spring-boot:run
```

The app will start on port **33400**. You can query a user balance with:

```bash
curl http://localhost:33400/balance?userId=1
```

---

## How to Run Tests

### Tasks 1, 2, 3 — No setup needed

These tests use an **embedded Kafka broker** so no external services are required.

```bash
# Task 1 — verifies project and Kafka setup
./mvnw test -Dtest=TaskOneTests

# Task 2 — verifies Kafka consumer receives transactions
./mvnw test -Dtest=TaskTwoTests

# Task 3 — verifies transaction validation and database recording
./mvnw test -Dtest=TaskThreeTests
```

> **Note:** Tasks 2 and 3 run in an **infinite loop by design**. Watch the console logs for the answer, then stop the test manually with `Ctrl+C`.

---

### Tasks 4 and 5 — Requires Incentive API running first

The Incentive API JAR must be running before executing these tests.

**Option 1 — Two terminals:**

Terminal 1 (keep running):
```bash
java -jar services/transaction-incentive-api.jar 
```

Terminal 2:
```bash
./mvnw test -Dtest=TaskFourTests
./mvnw test -Dtest=TaskFiveTests
```

**Option 2 — Single PowerShell command:**
```powershell
Start-Process java -ArgumentList "-jar services/transaction-incentive-api.jar"; Start-Sleep 5; ./mvnw test -Dtest=TaskFourTests
```

---

### Run all tests at once

```bash
./mvnw test
```

---

## Test Summary

| Test | What it verifies | Notes |
|------|-----------------|-------|
| `TaskOneTests` | Spring Boot context loads, Kafka broker starts | Auto-passes if setup is correct |
| `TaskTwoTests` | Kafka consumer receives and deserializes transactions | Runs forever — kill after seeing transactions in logs |
| `TaskThreeTests` | Transactions are validated and saved to H2 database | Runs forever — check waldorf's final balance in logs |
| `TaskFourTests` | Incentive API is called and rewards are applied | Requires incentive jar running — check wilbur's balance |
| `TaskFiveTests` | GET /balance returns correct user balances | Requires incentive jar running — note the begin/end output |