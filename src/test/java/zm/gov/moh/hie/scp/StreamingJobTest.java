package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import zm.gov.moh.hie.scp.dto.DispensationMessage;
import zm.gov.moh.hie.scp.model.DispensationRecord;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class StreamingJobTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void testValidDispensationMessageProcessing() throws Exception {
        String testJson = """
            {
                "msh": {
                    "timestamp": "2025-06-09 08:37:43",
                    "sendingApplication": "elmis",
                    "receivingApplication": "sc+",
                    "messageId": "c72defc8-e811-4ea1-a07f-c3cf3414260f",
                    "hmisCode": "5006XXHZ",
                    "messageType": "dispensation"
                },
                "patientGuid": "24a74b77-09ce-468e-9ce5-1439cd2027ab",
                "artNumber": "5040-HZ1-00706-3",
                "hmisCode": "5006XXHZ",
                "nextVisitDate": "2025-09-04",
                "transactionTime": "08:37:43",
                "dispensationDate": "2025-06-09",
                "clinician": "f41fc173-72ae-49ef-8db8-7482448ebae7",
                "dispensedDrugs": [
                    {
                        "mslDrugId": "ARV0082",
                        "quantityDispensed": 90.0,
                        "doseStrength": "50/300/300",
                        "unitQuantityPerDose": 1.0,
                        "frequency": "od",
                        "unitOfMeasurement": "tab",
                        "medicationId": ""
                    }
                ],
                "submitted": 0,
                "clinicianId": "f41fc173-72ae-49ef-8db8-7482448ebae7",
                "prescriptionUuid": "e3d28a27-e444-47ec-ae1a-dea4e7e2b13f"
            }
            """;

        // Use reflection to access private method for testing
        Method toRecordMethod = StreamingJob.class.getDeclaredMethod("toRecord", String.class, ObjectMapper.class);
        toRecordMethod.setAccessible(true);
        
        DispensationRecord record = (DispensationRecord) toRecordMethod.invoke(null, testJson, objectMapper);

        assertNotNull(record);
        assertEquals("5006XXHZ", record.hmisCode);
        assertEquals(1, record.drugCount); // 1 dispensed drug in array
        assertEquals("e3d28a27-e444-47ec-ae1a-dea4e7e2b13f", record.refPrescription);
        assertEquals("c72defc8-e811-4ea1-a07f-c3cf3414260f", record.messageId);
    }

    @Test
    void testInvalidJsonHandling() throws Exception {
        String invalidJson = "{ invalid json }";

        Method toRecordMethod = StreamingJob.class.getDeclaredMethod("toRecord", String.class, ObjectMapper.class);
        toRecordMethod.setAccessible(true);
        
        DispensationRecord record = (DispensationRecord) toRecordMethod.invoke(null, invalidJson, objectMapper);

        assertNotNull(record);
        assertNull(record.hmisCode);
        assertNull(record.drugCount);
        assertNull(record.refPrescription);
        assertNull(record.messageId);
    }

    @Test
    void testMissingRequiredFields() throws Exception {
        String testJsonMissingFields = """
            {
                "msh": {
                    "messageId": "test-message-002"
                },
                "dispensedDrugs": []
            }
            """;

        Method toRecordMethod = StreamingJob.class.getDeclaredMethod("toRecord", String.class, ObjectMapper.class);
        toRecordMethod.setAccessible(true);
        
        DispensationRecord record = (DispensationRecord) toRecordMethod.invoke(null, testJsonMissingFields, objectMapper);

        assertNotNull(record);
        assertNull(record.hmisCode); // Missing msh.hmisCode and root hmisCode
        assertEquals(0, record.drugCount); // Empty dispensedDrugs array
        assertNull(record.refPrescription); // Missing prescriptionUuid
        assertEquals("test-message-002", record.messageId);
    }

    @Test
    void testMultipleDrugsDispensation() throws Exception {
        String testJsonMultipleDrugs = """
            {
                "msh": {
                    "messageId": "test-message-003",
                    "hmisCode": "50060011"
                },
                "dispensedDrugs": [
                    {
                        "mslDrugId": "ARV0082",
                        "quantityDispensed": 30.0
                    },
                    {
                        "mslDrugId": "ARV0083",
                        "quantityDispensed": 60.0
                    },
                    {
                        "mslDrugId": "ARV0084",
                        "quantityDispensed": 90.0
                    }
                ],
                "prescriptionUuid": "test-prescription-uuid"
            }
            """;

        Method toRecordMethod = StreamingJob.class.getDeclaredMethod("toRecord", String.class, ObjectMapper.class);
        toRecordMethod.setAccessible(true);
        
        DispensationRecord record = (DispensationRecord) toRecordMethod.invoke(null, testJsonMultipleDrugs, objectMapper);

        assertNotNull(record);
        assertEquals("50060011", record.hmisCode);
        assertEquals(3, record.drugCount); // 3 dispensed drugs in array
        assertEquals("test-prescription-uuid", record.refPrescription);
        assertEquals("test-message-003", record.messageId);
    }

    @Test
    void testHmisCodePrecedence() throws Exception {
        String testJsonBothHmisCodes = """
            {
                "msh": {
                    "messageId": "test-message-004",
                    "hmisCode": "MSH_HMIS_CODE"
                },
                "hmisCode": "ROOT_HMIS_CODE",
                "dispensedDrugs": [],
                "prescriptionUuid": "test-uuid"
            }
            """;

        Method toRecordMethod = StreamingJob.class.getDeclaredMethod("toRecord", String.class, ObjectMapper.class);
        toRecordMethod.setAccessible(true);
        
        DispensationRecord record = (DispensationRecord) toRecordMethod.invoke(null, testJsonBothHmisCodes, objectMapper);

        assertNotNull(record);
        // Should use msh.hmisCode first, then fall back to root hmisCode
        assertEquals("MSH_HMIS_CODE", record.hmisCode);
    }

    @Test
    void testMapFunctionSerialization() throws Exception {
        // Test that the MapFunction can be instantiated and used
        StreamingJob.DispensationMapFunction mapFunction = new StreamingJob.DispensationMapFunction();
        
        String testJson = """
            {
                "msh": {
                    "hmisCode": "TEST_FACILITY",
                    "messageId": "test-001"
                },
                "dispensedDrugs": [{"mslDrugId": "TEST_DRUG"}],
                "prescriptionUuid": "test-prescription"
            }
            """;

        DispensationRecord result = mapFunction.map(testJson);
        
        assertNotNull(result);
        assertEquals("TEST_FACILITY", result.hmisCode);
        assertEquals(1, result.drugCount);
        assertEquals("test-prescription", result.refPrescription);
        assertEquals("test-001", result.messageId);
    }
}