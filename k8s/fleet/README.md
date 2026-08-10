# ELMIS Dispensations Pipeline - Kubernetes Deployment

## FlinkSessionJob Deployment

This directory contains Kubernetes manifests for deploying the ELMIS Dispensations Pipeline using the Flink Kubernetes Operator.

### Prerequisites

1. **Flink Session Cluster**: Ensure a FlinkDeployment session cluster named `session-cluster` exists in your namespace
2. **Required Connectors**: Ensure these JARs are in the cluster lib/ directory:
   - `flink-connector-kafka-3.3.0-1.20.jar`
   - `flink-connector-jdbc-3.3.0-1.20.jar` 
   - `postgresql-42.7.4.jar`
3. **Database Schema**: Ensure PostgreSQL database has the required schema with UPSERT support:
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
   ```

### Deployment

```bash
# Deploy the FlinkSessionJob
kubectl apply -f flink-sessionjob.yaml

# Verify deployment
kubectl get flinksessionjob -n flink-jobs
kubectl describe flinksessionjob scpro-elmis-dispensations-pipeline -n flink-jobs
```

### Configuration

The FlinkSessionJob pulls the JAR from GitHub releases automatically. Current configuration:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: scpro-elmis-dispensations-pipeline
  namespace: flink-jobs
spec:
  deploymentName: session-cluster
  job:
    jarURI: https://github.com/IHMAfrica/zm-scpro-elmis-dispensations-pipeline/releases/download/latest/zm-scpro-elmis-dispensations-pipeline-1.0-SNAPSHOT-all.jar
    entryClass: zm.gov.moh.hie.scp.StreamingJob
    parallelism: 3
    upgradeMode: stateless
    state: running
    args:
      # SANITIZED FOR HANDOVER: fill in the real Postgres host, port, and database name below
      # (or better, remove this and inject via a Secret/ConfigMap instead of a plain CLI arg)
      - "--jdbc.url=jdbc:postgresql://<DB_HOST>:<DB_PORT>/<DB_NAME>"
      - "--kafka.group.id=scpro-elmis-dispensations-pipeline"
```

#### Configuration Parameters
- `--jdbc.url`: PostgreSQL database connection URL
- `--kafka.bootstrap.servers`: Kafka broker addresses (uses defaults if not specified)
- `--kafka.topic`: Kafka topic name (default: `dispensations`)
- `--kafka.group.id`: Consumer group ID
- `--postgres.table`: Target table (default: `crt.dispensation`)

#### Default Values (built into application)
- **Kafka Brokers**: `<KAFKA_BROKER_1>:9093,<KAFKA_BROKER_2>:9093,<KAFKA_BROKER_3>:9093` - get the real broker list from the ops/infra team
- **Kafka Topic**: `dispensations`
- **Database URL**: `jdbc:postgresql://<DB_HOST>:<DB_PORT>/<DB_NAME>` - get the real host/port/db name from the ops/infra team
- **Consumer Group**: `flink-scpro-elmis-dispensations-consumer`

### Security

Provide sensitive configuration (passwords, SASL credentials) via:
- **Environment variables** at the cluster level
- **Kubernetes secrets** mounted to the Flink job manager/task manager pods
- **External secret management** systems (e.g., HashiCorp Vault, AWS Secrets Manager)

**Important**: Do not include secrets directly in the FlinkSessionJob manifest.

#### Example using Kubernetes secrets:
```bash
# Create secret for database credentials
kubectl create secret generic dispensations-db-secret \
  --from-literal=password='your-db-password' \
  -n flink-jobs

# Create secret for Kafka credentials  
kubectl create secret generic dispensations-kafka-secret \
  --from-literal=sasl-password='your-kafka-password' \
  -n flink-jobs
```

### Pipeline Features

- **UPSERT Support**: Handles duplicate `ref_prescription` values gracefully
- **Batch Processing**: 1000 records per batch with 200ms intervals
- **Error Resilience**: 5 retry attempts for failed database operations
- **Performance**: Optimized for high-throughput dispensation message processing
- **Monitoring**: Structured logging with processing metrics and error tracking

### Troubleshooting

#### Common Issues

1. **Missing Connectors**: Ensure all required JAR files are in cluster lib/
2. **Database Connection**: Verify JDBC URL and network connectivity
3. **Kafka Authentication**: Check SASL credentials and topic permissions
4. **Resource Allocation**: Monitor CPU/memory usage, adjust parallelism if needed

#### Monitoring Commands
```bash
# Check job status
kubectl logs -n flink-jobs -l app=flink,component=jobmanager

# View job metrics
kubectl port-forward -n flink-jobs svc/session-cluster-rest 8081:8081
# Access Flink UI at http://localhost:8081

# Check database connectivity
kubectl exec -it <pod-name> -n flink-jobs -- nc -zv db-host 5432
```