package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.DispensationMessage;
import zm.gov.moh.hie.scp.model.DispensationRecord;

import java.sql.PreparedStatement;
import java.sql.Types;

import org.apache.flink.api.common.functions.MapFunction;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Load effective configuration (CLI > env > defaults)
        final Config cfg = Config.fromEnvAndArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrapServers)
                .setTopics(cfg.kafkaTopic)
                .setGroupId(cfg.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-dispensations");

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SingleOutputStreamOperator<DispensationRecord> records = kafkaStream
                .filter(s -> !StringUtils.isNullOrWhitespaceOnly(s))
                .map(new DispensationMapFunction())
                .name("parse-and-flatten")
                .filter(record -> {
                    if (record == null || record.hmisCode == null) {
                        LOG.warn("Filtered out invalid record with null hmisCode. MessageId: {}",
                                record != null ? record.messageId : "null");
                        return false;
                    }

                    // Filter out placeholder values
                    if ("configure-me".equals(record.hmisCode)) {
                        LOG.warn("Filtered out record with placeholder HMIS code: '{}'. MessageId: {}",
                                record.hmisCode, record.messageId);
                        return false;
                    }

                    LOG.info("Processing dispensation record from HMIS: {} with {} drugs (MessageId: {})",
                            record.hmisCode, record.drugCount, record.messageId);
                    return true;
                })
                .name("filter-invalid-records");

        // Add JdbcSink with batch configuration
        @SuppressWarnings("deprecation")
        var sink = JdbcSink.sink(
                "INSERT INTO " + cfg.postgresTable + " (hmis_code, drug_count, ref_prescription) " +
                        "VALUES (?, ?, ?)",
                (PreparedStatement statement, DispensationRecord record) -> {
                    statement.setString(1, record.hmisCode);
                    if (record.drugCount != null) {
                        statement.setInt(2, record.drugCount);
                    } else {
                        statement.setNull(2, Types.SMALLINT);
                    }
                    statement.setString(3, record.refPrescription);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(cfg.postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(cfg.postgresUser)
                        .withPassword(cfg.postgresPassword)
                        .build()
        );
        records.addSink(sink).name("postgres-sink");

        env.execute("SC ELMIS Dispensations Pipeline");
    }

    private static DispensationRecord toRecord(String json, ObjectMapper mapper) {
        try {
            DispensationMessage msg = mapper.readValue(json, DispensationMessage.class);

            // Extract fields matching the database schema
            String messageId = msg.msh != null ? msg.msh.messageId : null;
            String hmisCode = msg.msh != null ? msg.msh.hmisCode : msg.hmisCode; // Use msh.hmisCode first, fall back to root level

            // Calculate drug count from dispensedDrugs array
            Integer drugCount = null;
            if (msg.dispensedDrugs != null) {
                drugCount = msg.dispensedDrugs.size();
            }

            // Use prescriptionUuid as reference to prescription
            String refPrescription = msg.prescriptionUuid;

            return new DispensationRecord(hmisCode, drugCount, refPrescription, messageId);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse JSON: {}", json, e);
            // Return a record with minimal info to avoid pipeline failure
            return new DispensationRecord(null, null, null, null);
        } catch (Exception e) {
            LOG.error("Unexpected error processing record: {}", json, e);
            return new DispensationRecord(null, null, null, null);
        }
    }

    /**
     * Serializable MapFunction to transform JSON strings to DispensationRecord objects
     */
    public static class DispensationMapFunction implements MapFunction<String, DispensationRecord> {
        private static final long serialVersionUID = 1L;

        // Transient means this won't be serialized, but will be recreated on each task manager
        private transient ObjectMapper objectMapper;

        @Override
        public DispensationRecord map(String json) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .registerModule(new JavaTimeModule())
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }

            return toRecord(json, objectMapper);
        }
    }
}