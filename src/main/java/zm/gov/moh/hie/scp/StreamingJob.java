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
import zm.gov.moh.hie.scp.model.DispensationDrugRecord;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
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
                .map(new DispensationMapFunction())
                .name("parse-and-flatten")
                .filter(record -> {
                    if (record == null || record.hmisCode == null) {
                        LOG.warn("Filtered out invalid record with null hmisCode. MessageId: {}",
                                record != null ? record.messageId : "null");
                        return false;
                    }

                    if (record.drugCount == 0)
                        return false;

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

                    LOG.info("Processing dispensation record from HMIS: {} with {} drugs (MessageId: {})",
                            record.hmisCode, record.drugCount, record.messageId);
                    return true;
                })
                .name("filter-invalid-records")
                .disableChaining();

        // Add JdbcSink with batch configuration and UPSERT for unique ref_prescription constraint
        @SuppressWarnings("deprecation")
        var sink = JdbcSink.sink(
                "INSERT INTO " + cfg.postgresTable + " (hmis_code, drug_count, arv_drug_count, ref_prescription, " +
                        "patient_guid, art_number, next_visit_date, transaction_time, dispensation_date) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (ref_prescription) " +
                        "DO UPDATE SET " +
                        "hmis_code = EXCLUDED.hmis_code, " +
                        "drug_count = EXCLUDED.drug_count, " +
                        "arv_drug_count = EXCLUDED.arv_drug_count, " +
                        "patient_guid = EXCLUDED.patient_guid, " +
                        "art_number = EXCLUDED.art_number, " +
                        "next_visit_date = EXCLUDED.next_visit_date, " +
                        "transaction_time = EXCLUDED.transaction_time, " +
                        "dispensation_date = EXCLUDED.dispensation_date, " +
                        "date = CURRENT_DATE, " +
                        "time = CURRENT_TIME::time(0)",
                (PreparedStatement statement, DispensationRecord record) -> {
                    statement.setString(1, record.getHmisCode());
                    // Set drug_count (non-ARV drugs), default to 0 if null
                    statement.setInt(2, record.getDrugCount() != null ? record.getDrugCount() : 0);
                    // Set arv_drug_count (ARV/HIV drugs), default to 0 if null
                    statement.setInt(3, record.getArvDrugCount() != null ? record.getArvDrugCount() : 0);
                    statement.setString(4, record.getRefPrescription());
                    statement.setString(5, record.getPatientGuid());
                    statement.setString(6, record.getArtNumber());
                    // Parse and set date fields
                    setDateField(statement, 7, record.getNextVisitDate());
                    setTimeField(statement, 8, record.getTransactionTime());
                    setDateField(statement, 9, record.getDispensationDate());
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

        // Add sink for drug records
        @SuppressWarnings("deprecation")
        var drugSink = JdbcSink.sink(
                "INSERT INTO crt.dispensation_drug (ref_prescription, msl_drug_id, quantity_dispensed, " +
                        "unit_quantity_per_dose, frequency, unit_of_measurement, medication_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (ref_prescription, msl_drug_id) DO UPDATE SET " +
                        "quantity_dispensed = EXCLUDED.quantity_dispensed, " +
                        "unit_quantity_per_dose = EXCLUDED.unit_quantity_per_dose, " +
                        "frequency = EXCLUDED.frequency, " +
                        "unit_of_measurement = EXCLUDED.unit_of_measurement, " +
                        "medication_id = EXCLUDED.medication_id",
                (PreparedStatement statement, DispensationDrugRecord drug) -> {
                    statement.setString(1, drug.getRefPrescription());
                    statement.setString(2, drug.getMslDrugId());
                    setNumericField(statement, 3, drug.getQuantityDispensed());
                    setNumericField(statement, 4, drug.getUnitQuantityPerDose());
                    statement.setString(5, drug.getFrequency());
                    statement.setString(6, drug.getUnitOfMeasurement());
                    statement.setString(7, drug.getMedicationId());
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

        // Fan out drug records from the filtered records stream
        DataStream<DispensationDrugRecord> drugRecords = records
                .flatMap(new DispensationDrugFlatMapFunction())
                .name("flatten-drugs")
                .filter(d -> d.getRefPrescription() != null && d.getMslDrugId() != null)
                .name("filter-invalid-drugs");
        drugRecords.addSink(drugSink).name("postgres-drug-sink");

        env.execute("SC ELMIS Dispensations Pipeline");
    }

    private static DispensationRecord toRecord(String json, ObjectMapper mapper) {
        try {
            DispensationMessage msg = mapper.readValue(json, DispensationMessage.class);

            // Extract fields matching the database schema
            String messageId = msg.msh != null ? msg.msh.messageId : null;
            String hmisCode = msg.msh != null ? msg.msh.hmisCode : msg.hmisCode; // Use msh.hmisCode first, fall back to root level

            // Calculate drug counts from dispensedDrugs array
            Integer drugCount = null;      // Count of non-ARV drugs (Essential medicines)
            Integer arvDrugCount = null;   // Count of ARV drugs only (HIV)
            List<DispensationDrugRecord> drugs = new ArrayList<>();

            if (msg.dispensedDrugs != null) {
                // Count non-ARV drugs (Essential medicines)
                drugCount = (int) msg.dispensedDrugs.stream()
                        .filter(drug -> drug != null && drug.mslDrugId != null && !drug.mslDrugId.contains("ARV"))
                        .count();

                // Count ARV drugs specifically (HIV drugs with mslDrugId starting with "ARV")
                arvDrugCount = (int) msg.dispensedDrugs.stream()
                        .filter(drug -> drug != null && drug.mslDrugId != null && drug.mslDrugId.contains("ARV"))
                        .count();

                // Build drug records for each dispensed drug
                for (DispensationMessage.DispensedDrug drug : msg.dispensedDrugs) {
                    if (drug != null && drug.mslDrugId != null) {
                        DispensationDrugRecord drugRecord = new DispensationDrugRecord(
                                msg.prescriptionUuid,
                                drug.mslDrugId,
                                drug.quantityDispensed,
                                drug.unitQuantityPerDose,
                                drug.frequency,
                                drug.unitOfMeasurement,
                                drug.medicationId
                        );
                        drugs.add(drugRecord);
                    }
                }
            }

            // Use prescriptionUuid as reference to prescription
            String refPrescription = msg.prescriptionUuid;

            // Extract new fields from payload
            String patientGuid = msg.patientGuid;
            String artNumber = msg.artNumber;
            String nextVisitDate = msg.nextVisitDate;
            String transactionTime = msg.transactionTime;
            String dispensationDate = msg.dispensationDate;

            return new DispensationRecord(
                    hmisCode, drugCount, arvDrugCount, refPrescription, messageId,
                    patientGuid, artNumber, nextVisitDate, transactionTime, dispensationDate, drugs
            );
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse JSON: {}", json, e);
            // Return a record with minimal info to avoid pipeline failure
            return new DispensationRecord(null, null, null, null, null);
        } catch (Exception e) {
            LOG.error("Unexpected error processing record: {}", json, e);
            return new DispensationRecord(null, null, null, null, null);
        }
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

    /**
     * Serializable FlatMapFunction to emit individual drug records from a DispensationRecord
     */
    public static class DispensationDrugFlatMapFunction implements FlatMapFunction<DispensationRecord, DispensationDrugRecord> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(DispensationRecord record, Collector<DispensationDrugRecord> out) throws Exception {
            if (record.getDrugs() != null && !record.getDrugs().isEmpty()) {
                for (DispensationDrugRecord drug : record.getDrugs()) {
                    out.collect(drug);
                }
            }
        }
    }
}