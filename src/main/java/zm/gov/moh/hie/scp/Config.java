package zm.gov.moh.hie.scp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Centralized configuration with support for environment variables and command-line arguments.
 * Precedence: CLI args (--key=value) > Environment variables > Built-in defaults.
 */
public class Config {
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    public final String kafkaBootstrapServers;
    public final String kafkaTopic;
    public final String kafkaGroupId;
    public final String kafkaSecurityProtocol;
    public final String kafkaSaslMechanism;
    public final String kafkaSaslUsername;
    public final String kafkaSaslPassword;

    public final String postgresUrl;
    public final String postgresUser;
    public final String postgresPassword;
    public final String postgresTable;

    private Config(
            String kafkaBootstrapServers,
            String kafkaTopic,
            String kafkaGroupId,
            String kafkaSecurityProtocol,
            String kafkaSaslMechanism,
            String kafkaSaslUsername,
            String kafkaSaslPassword,
            String postgresUrl,
            String postgresUser,
            String postgresPassword,
            String postgresTable
    ) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.kafkaGroupId = kafkaGroupId;
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
        this.kafkaSaslMechanism = kafkaSaslMechanism;
        this.kafkaSaslUsername = kafkaSaslUsername;
        this.kafkaSaslPassword = safeValue(kafkaSaslPassword);
        this.postgresUrl = postgresUrl;
        this.postgresUser = postgresUser;
        this.postgresPassword = postgresPassword;
        this.postgresTable = postgresTable;
    }

    private String safeValue(String value) {
        return value == null ? "" : value;
    }

    public static Config fromEnvAndArgs(String[] args) {
        Map<String, String> params = new HashMap<>();

        // 1) defaults (dispensation-specific defaults)
        params.put("kafka.bootstrap.servers", "154.120.216.119:9093,102.23.120.153:9093,102.23.123.251:9093");
        params.put("kafka.topic", "dispensations");
        params.put("kafka.group.id", "dispensations-consumer");
        params.put("kafka.security.protocol", "SASL_PLAINTEXT");
        params.put("kafka.sasl.mechanism", "SCRAM-SHA-256");
        params.put("kafka.sasl.username", "admin");
        params.put("kafka.sasl.password", "075F80FED7C6");
        params.put("postgres.url", "jdbc:postgresql://localhost:5432/hie_manager");
        params.put("postgres.user", "postgres");
        params.put("postgres.password", "postgres");
        params.put("postgres.table", "crt.dispensation");

        // 2) environment variables (upper snake)
        overlayIfPresent(params, "KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers");
        overlayIfPresent(params, "KAFKA_TOPIC", "kafka.topic");
        overlayIfPresent(params, "KAFKA_GROUP_ID", "kafka.group.id");
        overlayIfPresent(params, "KAFKA_SECURITY_PROTOCOL", "kafka.security.protocol");
        overlayIfPresent(params, "KAFKA_SASL_MECHANISM", "kafka.sasl.mechanism");
        overlayIfPresent(params, "KAFKA_SASL_USERNAME", "kafka.sasl.username");
        overlayIfPresent(params, "KAFKA_SASL_PASSWORD", "kafka.sasl.password");
        overlayIfPresent(params, "POSTGRES_URL", "postgres.url");
        overlayIfPresent(params, "POSTGRES_USER", "postgres.user");
        overlayIfPresent(params, "POSTGRES_PASSWORD", "postgres.password");
        overlayIfPresent(params, "POSTGRES_TABLE", "postgres.table");

        // 3) command-line args in form --key=value
        if (args != null) {
            for (String a : args) {
                if (a != null && a.startsWith("--") && a.contains("=")) {
                    String[] kv = a.substring(2).split("=", 2);
                    if (kv.length == 2 && !kv[0].isBlank()) {
                        params.put(kv[0], kv[1]);
                    }
                }
            }
        }

        Config cfg = new Config(
                params.get("kafka.bootstrap.servers"),
                params.get("kafka.topic"),
                params.get("kafka.group.id"),
                params.get("kafka.security.protocol"),
                params.get("kafka.sasl.mechanism"),
                params.get("kafka.sasl.username"),
                params.get("kafka.sasl.password"),
                params.get("postgres.url"),
                params.get("postgres.user"),
                params.get("postgres.password"),
                params.get("postgres.table")
        );

        logEffective(cfg);
        return cfg;
    }

    private static void overlayIfPresent(Map<String, String> params, String envKey, String paramKey) {
        String v = System.getenv(envKey);
        if (v != null && !v.isBlank()) {
            params.put(paramKey, v);
        }
    }

    private static void logEffective(Config c) {
        try {
            LOG.info("Effective configuration: kafka.bootstrap.servers={}, kafka.topic={}, kafka.group.id={}",
                    c.kafkaBootstrapServers, c.kafkaTopic, c.kafkaGroupId);
            LOG.info("Kafka security: protocol={}, mechanism={}, username={}",
                    c.kafkaSecurityProtocol, c.kafkaSaslMechanism, redact(c.kafkaSaslUsername));
            LOG.info("Postgres: url={}, user={}, table={}", c.postgresUrl, redact(c.postgresUser), c.postgresTable);
        } catch (Throwable ignored) {
        }
    }

    private static String redact(String v) { return v == null || v.isBlank() ? "<empty>" : "***"; }
}