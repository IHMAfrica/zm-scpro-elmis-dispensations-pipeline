# ELMIS Dispensations Pipeline

A Flink streaming application that processes dispensation messages from Kafka and stores aggregated data in PostgreSQL.

## Overview

This pipeline processes dispensation events from the ELMIS system, extracting key metrics and storing them in the `crt.dispensation` table for analytics and reporting.

## Architecture

- **Source**: Kafka topic `dispensations` 
- **Processing**: Apache Flink streaming job with batch processing
- **Sink**: PostgreSQL `crt.dispensation` table
- **Deployment**: Kubernetes via Flink Kubernetes Operator

## Data Flow

1. **Kafka Message**: Dispensation events from ELMIS system
2. **Processing**: Extract HMIS code, drug count, and prescription reference
3. **Database**: Insert into `crt.dispensation` table with auto-generated timestamps

## Database Schema

```sql
CREATE TABLE crt.dispensation (
    id bigint PRIMARY KEY DEFAULT nextval('crt.dispensation_id_seq'),
    hmis_code varchar(50) NOT NULL,
    drug_count smallint,
    date date NOT NULL DEFAULT CURRENT_DATE,
    time time NOT NULL DEFAULT CURRENT_TIME,
    ref_prescription varchar(50)
);
```

## Configuration

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Topic name (default: `dispensations`)
- `KAFKA_GROUP_ID`: Consumer group ID
- `KAFKA_SASL_USERNAME`: Kafka authentication username
- `KAFKA_SASL_PASSWORD`: Kafka authentication password
- `POSTGRES_URL`: PostgreSQL connection URL
- `POSTGRES_USER`: Database username
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_TABLE`: Target table (default: `crt.dispensation`)

### Command Line Arguments
```bash
java -jar dispensations-pipeline.jar \
  --kafka.bootstrap.servers=broker1:9093,broker2:9093 \
  --kafka.topic=dispensations \
  --postgres.url=jdbc:postgresql://db:5432/hie_manager
```

## Development

### Prerequisites
- Java 17+
- Gradle 8.x
- Apache Flink 1.20+
- PostgreSQL 12+

### Build
```bash
./gradlew clean build
```

### Run Tests
```bash
./gradlew test
```

### Create Fat JAR
```bash
./gradlew shadowJar
```

## Deployment

### Local Testing
```bash
java -jar build/libs/zm-scpro-elmis-dispensations-pipeline-1.0-SNAPSHOT-all.jar
```

### Kubernetes
```bash
kubectl apply -f k8s/fleet/
```

## Monitoring

The pipeline provides structured logging with:
- Message processing counts
- Error handling for malformed messages
- Database insert performance metrics
- Kafka consumer lag monitoring

## Example Message

```json
{
    "msh": {
        "timestamp": "2025-06-09 08:37:43",
        "messageId": "c72defc8-e811-4ea1-a07f-c3cf3414260f",
        "hmisCode": "5006XXHZ"
    },
    "dispensedDrugs": [
        {
            "mslDrugId": "ARV0082",
            "quantityDispensed": 90.0
        }
    ],
    "prescriptionUuid": "e3d28a27-e444-47ec-ae1a-dea4e7e2b13f"
}
```

## CI/CD

Automated build and deployment via GitHub Actions:
- Builds fat JAR on push to `main`
- Publishes as GitHub release
- Creates Kubernetes manifests
