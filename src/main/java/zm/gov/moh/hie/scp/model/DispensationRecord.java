package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

/**
 * Represents a normalized dispensation record (1 row per dispensed drug)
 * matching the crt.dispensation table schema
 *
 * Dispensation-level fields (from prescription):
 * - hmis_code, ref_prescription, message_id, patient_guid, art_number
 * - next_visit_date, transaction_time, dispensation_date, clinician_id
 *
 * Drug-level fields (1 row per drug):
 * - msl_drug_id, medication_id, quantity_dispensed, unit_qty_per_dose
 * - frequency, unit_of_measurement, msh_timestamp
 *
 * Database columns:
 * - id (bigint, auto-generated primary key)
 * - date (date, NOT NULL, defaults to CURRENT_DATE)
 * - time (time, NOT NULL, defaults to CURRENT_TIME)
 * - UNIQUE(ref_prescription, medication_id, msl_drug_id)
 */
public class DispensationRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Dispensation-level fields
    public String hmisCode;           // HMIS facility code
    public String refPrescription;    // Reference to prescription UUID
    public String messageId;          // Message ID from msh
    public String patientGuid;        // Patient identifier
    public String artNumber;          // ART number
    public String nextVisitDate;      // Next visit date as string "yyyy-MM-dd"
    public String transactionTime;    // Transaction time as string "HH:mm:ss"
    public String dispensationDate;   // Dispensation date as string "yyyy-MM-dd"
    public String clinicianId;        // Clinician identifier

    // Drug-level fields (normalized - one row per drug)
    public String mslDrugId;          // Drug identifier from ELMIS (e.g., ARV0077)
    public String medicationId;       // Medication identifier UUID
    public Double quantityDispensed;  // Quantity dispensed
    public Double unitQtyPerDose;     // Unit quantity per dose
    public String frequency;          // Dosage frequency (e.g., "od", "bd")
    public String unitOfMeasurement;  // Unit of measurement (e.g., "tab")
    public String mshTimestamp;       // Message timestamp

    // Default constructor
    public DispensationRecord() {}

    // Constructor for normalized record (one per drug)
    public DispensationRecord(String hmisCode, String refPrescription, String messageId,
                              String patientGuid, String artNumber, String nextVisitDate,
                              String transactionTime, String dispensationDate, String clinicianId,
                              String mslDrugId, String medicationId, Double quantityDispensed,
                              Double unitQtyPerDose, String frequency, String unitOfMeasurement,
                              String mshTimestamp) {
        this.hmisCode = hmisCode;
        this.refPrescription = refPrescription;
        this.messageId = messageId;
        this.patientGuid = patientGuid;
        this.artNumber = artNumber;
        this.nextVisitDate = nextVisitDate;
        this.transactionTime = transactionTime;
        this.dispensationDate = dispensationDate;
        this.clinicianId = clinicianId;
        this.mslDrugId = mslDrugId;
        this.medicationId = medicationId;
        this.quantityDispensed = quantityDispensed;
        this.unitQtyPerDose = unitQtyPerDose;
        this.frequency = frequency;
        this.unitOfMeasurement = unitOfMeasurement;
        this.mshTimestamp = mshTimestamp;
    }

    // Getters and Setters
    public String getHmisCode() { return hmisCode; }
    public void setHmisCode(String hmisCode) { this.hmisCode = hmisCode; }

    public String getRefPrescription() { return refPrescription; }
    public void setRefPrescription(String refPrescription) { this.refPrescription = refPrescription; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getPatientGuid() { return patientGuid; }
    public void setPatientGuid(String patientGuid) { this.patientGuid = patientGuid; }

    public String getArtNumber() { return artNumber; }
    public void setArtNumber(String artNumber) { this.artNumber = artNumber; }

    public String getNextVisitDate() { return nextVisitDate; }
    public void setNextVisitDate(String nextVisitDate) { this.nextVisitDate = nextVisitDate; }

    public String getTransactionTime() { return transactionTime; }
    public void setTransactionTime(String transactionTime) { this.transactionTime = transactionTime; }

    public String getDispensationDate() { return dispensationDate; }
    public void setDispensationDate(String dispensationDate) { this.dispensationDate = dispensationDate; }

    public String getClinicianId() { return clinicianId; }
    public void setClinicianId(String clinicianId) { this.clinicianId = clinicianId; }

    public String getMslDrugId() { return mslDrugId; }
    public void setMslDrugId(String mslDrugId) { this.mslDrugId = mslDrugId; }

    public String getMedicationId() { return medicationId; }
    public void setMedicationId(String medicationId) { this.medicationId = medicationId; }

    public Double getQuantityDispensed() { return quantityDispensed; }
    public void setQuantityDispensed(Double quantityDispensed) { this.quantityDispensed = quantityDispensed; }

    public Double getUnitQtyPerDose() { return unitQtyPerDose; }
    public void setUnitQtyPerDose(Double unitQtyPerDose) { this.unitQtyPerDose = unitQtyPerDose; }

    public String getFrequency() { return frequency; }
    public void setFrequency(String frequency) { this.frequency = frequency; }

    public String getUnitOfMeasurement() { return unitOfMeasurement; }
    public void setUnitOfMeasurement(String unitOfMeasurement) { this.unitOfMeasurement = unitOfMeasurement; }

    public String getMshTimestamp() { return mshTimestamp; }
    public void setMshTimestamp(String mshTimestamp) { this.mshTimestamp = mshTimestamp; }

    @Override
    public String toString() {
        return "DispensationRecord{" +
                "hmisCode='" + hmisCode + '\'' +
                ", refPrescription='" + refPrescription + '\'' +
                ", mslDrugId='" + mslDrugId + '\'' +
                ", medicationId='" + medicationId + '\'' +
                ", quantityDispensed=" + quantityDispensed +
                ", frequency='" + frequency + '\'' +
                '}';
    }
}