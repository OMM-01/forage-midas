# Midas Core

A Spring Boot application that processes financial transactions via Apache Kafka, validates them against user balances, records them to an H2 database, integrates with an external Incentive API, and exposes a REST API for querying user balances.

---

## Project Structure

```
forage-midas/
├── services/
│   └── transaction-incentive-api.jar   # External incentive API (must be running for tests)
├── src/
│   ├── main/
│   │   ├── java/com/jpmc/midascore/
│   │   │   ├── component/
│   │   │   │   ├── BalanceController.java        # REST API for balance queries
│   │   │   │   ├── DatabaseConduit.java          # Database helper
│   │   │   │   ├── KafkaConsumerConfig.java      # Kafka consumer configuration
│   │   │   │   ├── KafkaProducerConfig.java      # Kafka producer configuration
│   │   │   │   └── TransactionListener.java      # Kafka listener + transaction logic
│   │   │   ├── entity/
│   │   │   │   ├── TransactionRecord.java        # JPA entity for transactions
│   │   │   │   └── UserRecord.java              # JPA entity for users
│   │   │   ├── foundation/
│   │   │   │   ├── Balance.java                 # Balance response model
│   │   │   │   ├── Incentive.java               # Incentive response model
│   │   │   │   └── Transaction.java             # Transaction Kafka message model
│   │   │   ├── repository/
│   │   │   │   ├── TransactionRepository.java   # JPA repository for transactions
│   │   │   │   └── UserRepository.java          # JPA repository for users
│   │   │   └── MidasCoreApplication.java        # Spring Boot entry point
│   │   └── resources/
│   │       └── application.yml                  # Main app config
│   └── test/
│       ├── java/com/jpmc/midascore/
│       │   ├── TaskOneTests.java
│       │   ├── TaskTwoTests.java
│       │   ├── TaskThreeTests.java
│       │   ├── TaskFourTests.java
│       │   └── TaskFiveTests.java
│       └── resources/
│           └── application.yml                  # Test config (embedded Kafka)
```

---

## Prerequisites

- Java 17+
- Maven (via `./mvnw` wrapper)
- The `transaction-incentive-api.jar` (included in `services/` folder)

---

## Configuration

### `src/main/resources/application.yml`
```yaml
general:
  kafka-topic: trader-updates

server:
  port: 33400
```

### `src/test/resources/application.yml`
```yaml
general:
  kafka-topic: trader-updates

spring:
  kafka:
    consumer:
      group-id: midas-group

kafka:
  topic:
    transactions: transactions-topic

server:
  port: 33400
```

---

## Running the Application

```bash
./mvnw spring-boot:run
```

The app will start on port **33400**. The balance endpoint will be available at:
```
GET http://localhost:33400/balance?userId={id}
```

---

## Running Tests

### Important: Tasks 4 and 5 require the Incentive API to be running

Start the incentive API in a separate terminal before running Task 4 or Task 5:
```bash
java -jar services/transaction-incentive-api.jar
```

Or use this single command to start the jar and run the test together (PowerShell):
```powershell
Start-Process java -ArgumentList "-jar services/transaction-incentive-api.jar"; Start-Sleep 5; ./mvnw test -Dtest=TaskFourTests
```

---

### Run Individual Tests

```bash
# Task 1
./mvnw test -Dtest=TaskOneTests

# Task 2 - watch logs for first 4 transaction amounts, then kill with Ctrl+C
./mvnw test -Dtest=TaskTwoTests

# Task 3 - watch logs for waldorf's final balance, then kill with Ctrl+C
./mvnw test -Dtest=TaskThreeTests

# Task 4 - requires incentive API running, watch logs for wilbur's final balance
./mvnw test -Dtest=TaskFourTests

# Task 5 - requires incentive API running, captures output between begin/end tags
./mvnw test -Dtest=TaskFiveTests
```

### Run All Tests
```bash
./mvnw test
```

---

## What Each Task Tests

| Task | Description | Answer Format |
|------|-------------|---------------|
| Task 1 | Verifies Kafka setup and embedded broker | Pass/fail |
| Task 2 | Kafka consumer receives transactions — record first 4 amounts | 4 decimal numbers |
| Task 3 | Transaction validation + DB recording — find waldorf's final balance | Integer (rounded down) |
| Task 4 | Incentive API integration — find wilbur's final balance | Integer (rounded down) |
| Task 5 | REST balance endpoint — submit full begin/end tag output | Tagged output string |

---

## Transaction Validation Rules

A transaction is only processed if all of the following are true:
1. The `senderId` exists in the database
2. The `recipientId` exists in the database
3. The sender's balance is greater than or equal to the transaction amount

When a valid transaction is processed:
- Sender balance is **decreased** by the transaction amount
- Recipient balance is **increased** by the transaction amount **plus** any incentive
- The transaction is recorded in the database with the incentive amount
- The incentive is **not** deducted from the sender

---

## REST API

### GET /balance

Returns the balance of a user by ID.

**Request:**
```
GET http://localhost:33400/balance?userId=1
```

**Response:**
```json
{
  "amount": 444.55
}
```

If the user does not exist, returns:
```json
{
  "amount": 0.0
}
```

---

## Tech Stack

- **Java 17**
- **Spring Boot 3.2.5**
- **Apache Kafka** (embedded for tests)
- **Spring Data JPA**
- **H2** (in-memory database)
- **Spring Web** (REST controller)
- **Spring Kafka**
