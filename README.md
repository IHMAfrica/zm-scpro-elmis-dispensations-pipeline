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
3. **Database**: UPSERT into `crt.dispensation` table with auto-generated timestamps
4. **Duplicate Handling**: Updates existing records when duplicate prescription references are encountered

## Database Schema

```sql
CREATE TABLE crt.dispensation (
    id bigint PRIMARY KEY DEFAULT nextval('crt.dispensation_id_seq'),
    hmis_code varchar(50) NOT NULL,
    drug_count smallint,
    date date NOT NULL DEFAULT CURRENT_DATE,
    time time(0) NOT NULL DEFAULT CURRENT_TIME,
    ref_prescription varchar(50),
    CONSTRAINT dispensation_ref_prescription_uk UNIQUE (ref_prescription)
);

-- Index for performance
CREATE INDEX idx_dispensation_ref_prescription
    ON crt.dispensation USING btree (ref_prescription);
```

### UPSERT Behavior
- **New prescriptions**: Inserted as new records
- **Duplicate prescriptions**: Existing records updated with latest `hmis_code`, `drug_count`, `date`, and `time`
- **Unique constraint**: Prevents duplicate `ref_prescription` values
- **Performance**: Batch processing with 1000 records per batch, 200ms intervals

## Configuration

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Topic name (default: `dispensations`)
- `KAFKA_GROUP_ID`: Consumer group ID (default: `flink-scpro-elmis-dispensations-consumer`)
- `KAFKA_SASL_USERNAME`: Kafka authentication username
- `KAFKA_SASL_PASSWORD`: Kafka authentication password
- `JDBC_URL`: PostgreSQL connection URL (default: `jdbc:postgresql://db-04.smartcare.com:35616/hie_manager`)
- `JDBC_USER`: Database username
- `JDBC_PASSWORD`: Database password
- `POSTGRES_TABLE`: Target table (default: `crt.dispensation`)

### Command Line Arguments
```bash
# Production deployment via Kubernetes FlinkSessionJob
kubectl apply -f k8s/fleet/flink-sessionjob.yaml

# Or direct JAR execution with custom config
java -jar zm-scpro-elmis-dispensations-pipeline-all.jar \
  --kafka.bootstrap.servers=broker1:9093,broker2:9093 \
  --kafka.topic=dispensations \
  --jdbc.url=jdbc:postgresql://db:5432/hie_manager
```

## Development

### Prerequisites
- Java 17+
- Gradle 8.x
- Apache Flink 1.20.2
- PostgreSQL 12+
- Flink cluster with required connectors (for production)

### Build
```bash
./gradlew clean build
```

### Run Tests
```bash
./gradlew test
```

### Create Production JAR
```bash
./gradlew shadowJar
```
Output: `build/libs/zm-scpro-elmis-dispensations-pipeline-all.jar`

### Cluster Requirements
For production deployment, ensure your Flink cluster has these JARs in `lib/`:
- `flink-connector-kafka-3.3.0-1.20.jar`
- `flink-connector-jdbc-3.3.0-1.20.jar`
- `postgresql-42.7.4.jar`

## Deployment

### Production (Kubernetes)
```bash
# Deploy to Flink cluster
kubectl apply -f k8s/fleet/flink-sessionjob.yaml
```

### Local Development
**Note**: Local execution requires Flink runtime with connectors. For development:
1. Set up local Flink cluster with required connectors
2. Use Flink CLI to submit the job:
```bash
flink run build/libs/zm-scpro-elmis-dispensations-pipeline-all.jar
```

### Configuration Override
```bash
# Via environment variables
export JDBC_URL="jdbc:postgresql://localhost:5432/hie_manager"
export KAFKA_GROUP_ID="dev-dispensations-consumer"
kubectl apply -f k8s/fleet/flink-sessionjob.yaml

# Via command line arguments in FlinkSessionJob
# See k8s/fleet/flink-sessionjob.yaml for examples
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
- Triggers on pull requests (`opened`, `reopened`, `synchronize`)
- Builds production fat JAR using `shadowJar`
- Publishes JAR as GitHub release asset
- Creates resolved Kubernetes manifests with JAR URL
- Uploads k8s manifests as build artifacts

### Pipeline Features
- **UPSERT Support**: Handles duplicate prescription references gracefully
- **Batch Processing**: 1000 records per batch, 200ms intervals, 5 retries
- **Production Ready**: Optimized JAR size, cluster-provided dependencies
- **Monitoring**: Structured logging with performance metrics
