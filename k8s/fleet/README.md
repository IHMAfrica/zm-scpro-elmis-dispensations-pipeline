# ELMIS Dispensations Pipeline - Kubernetes Deployment

## FlinkSessionJob Deployment

This directory contains Kubernetes manifests for deploying the ELMIS Dispensations Pipeline using the Flink Kubernetes Operator.

### Prerequisites

1. **Flink Session Cluster**: Ensure a FlinkDeployment session cluster named `session-cluster` exists in your namespace
2. **Connectors**: Provide Kafka and JDBC connector JARs compatible with Flink 1.20 in the cluster lib/ directory
3. **Configuration**: Set up environment variables or job arguments for Kafka and PostgreSQL connection details

### Deployment

```bash
kubectl apply -f flink-sessionjob.yaml
```

### Configuration

The FlinkSessionJob uses the JAR from GitHub releases. Configure the pipeline using job arguments:

```yaml
spec:
  job:
    arguments:
      - "--kafka.bootstrap.servers=broker1:9092,broker2:9092"
      - "--kafka.topic=dispensations"
      - "--kafka.group.id=dispensations-consumer"
      - "--postgres.url=jdbc:postgresql://db:5432/database"
      - "--postgres.user=username"
      - "--postgres.table=crt.dispensation"
```

### Security

Provide sensitive configuration (passwords, SASL credentials) via:
- Environment variables at the cluster level
- Kubernetes secrets
- External secret management systems

Do not include secrets directly in the FlinkSessionJob manifest.