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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.DispensationMessage;
import zm.gov.moh.hie.scp.model.DispensationRecord;

import java.sql.PreparedStatement;
import java.sql.Types;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

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
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "2000")
                .setProperty("max.poll.interval.ms", "10000")
                .setProperty("max.poll.records", "50")
                .setProperty("request.timeout.ms", "2540000")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("default.api.timeout.ms", "2540000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-dispensations"
        ).startNewChain();

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SingleOutputStreamOperator<DispensationRecord> records = kafkaStream
                .filter(s -> !StringUtils.isNullOrWhitespaceOnly(s))
                .flatMap(new DispensationFlatMapFunction())
                .name("parse-and-flatten")
                .filter(record -> {
                    if (record == null || record.hmisCode == null) {
                        LOG.warn("Filtered out invalid record with null hmisCode. MessageId: {}",
                                record != null ? record.messageId : "null");
                        return false;
                    }

                    if (record.mslDrugId == null) {
                        return false;
                    }

                    // Filter out placeholder values
                    if ("configure-me".equals(record.hmisCode)) {
                        LOG.warn("Filtered out record with placeholder HMIS code: '{}'. MessageId: {}",
                                record.hmisCode, record.messageId);
                        return false;
                    }

                    if(StringUtils.isNullOrWhitespaceOnly(record.messageId))
                        return false;

                    if(StringUtils.isNullOrWhitespaceOnly(record.refPrescription))
                        return false;

                    LOG.info("Processing dispensed drug: {} from prescription {} (MessageId: {})",
                            record.mslDrugId, record.refPrescription, record.messageId);
                    return true;
                })
                .name("filter-invalid-records")
                .disableChaining();

        // Add JdbcSink with batch configuration - one row per dispensed drug
        @SuppressWarnings("deprecation")
        var sink = JdbcSink.sink(
                "INSERT INTO " + cfg.postgresTable + " (hmis_code, ref_prescription, message_id, " +
                        "patient_guid, art_number, next_visit_date, transaction_time, dispensation_date, " +
                        "clinician_id, msl_drug_id, medication_id, quantity_dispensed, unit_qty_per_dose, " +
                        "frequency, unit_of_measurement, msh_timestamp, date, \"time\") " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE, CURRENT_TIME::time(0)) " +
                        "ON CONFLICT (ref_prescription, medication_id, msl_drug_id) " +
                        "DO UPDATE SET " +
                        "hmis_code = EXCLUDED.hmis_code, " +
                        "message_id = EXCLUDED.message_id, " +
                        "quantity_dispensed = EXCLUDED.quantity_dispensed, " +
                        "unit_qty_per_dose = EXCLUDED.unit_qty_per_dose, " +
                        "frequency = EXCLUDED.frequency, " +
                        "date = CURRENT_DATE, " +
                        "time = CURRENT_TIME::time(0)",
                (PreparedStatement statement, DispensationRecord record) -> {
                    statement.setString(1, record.getHmisCode());
                    statement.setString(2, record.getRefPrescription());
                    statement.setString(3, record.getMessageId());
                    statement.setString(4, record.getPatientGuid());
                    statement.setString(5, record.getArtNumber());
                    setDateField(statement, 6, record.getNextVisitDate());
                    setTimeField(statement, 7, record.getTransactionTime());
                    setDateField(statement, 8, record.getDispensationDate());
                    statement.setString(9, record.getClinicianId());
                    statement.setString(10, record.getMslDrugId());
                    statement.setString(11, record.getMedicationId());
                    // quantity_dispensed
                    if (record.getQuantityDispensed() != null) {
                        statement.setBigDecimal(12, new java.math.BigDecimal(record.getQuantityDispensed()));
                    } else {
                        statement.setNull(12, Types.NUMERIC);
                    }
                    // unit_qty_per_dose
                    if (record.getUnitQtyPerDose() != null) {
                        statement.setBigDecimal(13, new java.math.BigDecimal(record.getUnitQtyPerDose()));
                    } else {
                        statement.setNull(13, Types.NUMERIC);
                    }
                    statement.setString(14, record.getFrequency());
                    statement.setString(15, record.getUnitOfMeasurement());
                    // msh_timestamp
                    if (record.getMshTimestamp() != null && !record.getMshTimestamp().isBlank()) {
                        try {
                            java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(record.getMshTimestamp(),
                                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                            statement.setTimestamp(16, java.sql.Timestamp.valueOf(ldt));
                        } catch (Exception e) {
                            statement.setNull(16, Types.TIMESTAMP);
                        }
                    } else {
                        statement.setNull(16, Types.TIMESTAMP);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(cfg.jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(cfg.jdbcUser)
                        .withPassword(cfg.jdbcPassword)
                        .build()
        );
        records.addSink(sink).name("postgres-sink");

        env.execute("SC ELMIS Dispensations Pipeline");
    }

    private static DispensationMessage parseMessage(String json, ObjectMapper mapper) throws JsonProcessingException {
        return mapper.readValue(json, DispensationMessage.class);
    }

    /**
     * Helper method to set a date field from string (yyyy-MM-dd format)
     */
    private static void setDateField(PreparedStatement statement, int parameterIndex, String dateString) throws java.sql.SQLException {
        if (dateString == null || dateString.isBlank()) {
            statement.setNull(parameterIndex, Types.DATE);
        } else {
            try {
                statement.setDate(parameterIndex, java.sql.Date.valueOf(dateString));
            } catch (IllegalArgumentException e) {
                LOG.warn("Failed to parse date '{}': {}", dateString, e.getMessage());
                statement.setNull(parameterIndex, Types.DATE);
            }
        }
    }

    /**
     * Helper method to set a time field from string (HH:mm:ss format)
     */
    private static void setTimeField(PreparedStatement statement, int parameterIndex, String timeString) throws java.sql.SQLException {
        if (timeString == null || timeString.isBlank()) {
            statement.setNull(parameterIndex, Types.TIME);
        } else {
            try {
                statement.setTime(parameterIndex, java.sql.Time.valueOf(timeString));
            } catch (IllegalArgumentException e) {
                LOG.warn("Failed to parse time '{}': {}", timeString, e.getMessage());
                statement.setNull(parameterIndex, Types.TIME);
            }
        }
    }

    /**
     * Helper method to set a numeric field that can be null
     */
    private static void setNumericField(PreparedStatement statement, int parameterIndex, Double value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(parameterIndex, Types.NUMERIC);
        } else {
            statement.setDouble(parameterIndex, value);
        }
    }

    /**
     * Serializable MapFunction to transform JSON strings to DispensationRecord objects
     */
    public static class DispensationFlatMapFunction implements FlatMapFunction<String, DispensationRecord> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper objectMapper;

        @Override
        public void flatMap(String json, Collector<DispensationRecord> out) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .registerModule(new JavaTimeModule())
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }

            try {
                DispensationMessage msg = parseMessage(json, objectMapper);

                String messageId = msg.msh != null ? msg.msh.messageId : null;
                String mshTimestamp = msg.msh != null ? msg.msh.timestamp : null;
                String hmisCode = msg.msh != null ? msg.msh.hmisCode : msg.hmisCode;
                String refPrescription = msg.prescriptionUuid;
                String patientGuid = msg.patientGuid;
                String artNumber = msg.artNumber;
                String nextVisitDate = msg.nextVisitDate;
                String transactionTime = msg.transactionTime;
                String dispensationDate = msg.dispensationDate;
                String clinicianId = msg.clinician != null ? msg.clinician : msg.clinicianId;

                if (msg.dispensedDrugs != null && !msg.dispensedDrugs.isEmpty()) {
                    for (DispensationMessage.DispensedDrug drug : msg.dispensedDrugs) {
                        if (drug != null && drug.mslDrugId != null) {
                            DispensationRecord record = new DispensationRecord(
                                    hmisCode, refPrescription, messageId,
                                    patientGuid, artNumber, nextVisitDate,
                                    transactionTime, dispensationDate, clinicianId,
                                    drug.mslDrugId, drug.medicationId, drug.quantityDispensed,
                                    drug.unitQuantityPerDose, drug.frequency, drug.unitOfMeasurement,
                                    mshTimestamp
                            );
                            out.collect(record);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error flattening dispensation record: {}", json, e);
            }
        }
    }
}