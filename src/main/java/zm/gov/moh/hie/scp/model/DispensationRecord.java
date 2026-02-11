package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

/**
 * Represents a dispensation record matching the crt.dispensation table schema
 *
 * Database columns:
 * - id (bigint, auto-generated primary key)
 * - hmis_code (varchar(50), NOT NULL)
 * - drug_count (smallint) - count of non-ARV drugs (Essential medicines only)
 * - arv_drug_count (smallint) - count of ARV drugs (HIV medications only)
 * - date (date, NOT NULL, defaults to CURRENT_DATE)
 * - time (time, NOT NULL, defaults to CURRENT_TIME)
 * - ref_prescription (varchar(50))
 * - patient_guid (varchar(100)) - patient identifier
 * - art_number (varchar(100)) - ART number
 * - next_visit_date (date) - next appointment date
 * - transaction_time (time) - transaction timestamp
 * - dispensation_date (date) - actual dispensation date
 *
 * Drug Classification:
 * - ARV drugs: mslDrugId contains "ARV" (e.g., ARV0092)
 * - Essential drugs: mslDrugId does NOT contain "ARV"
 */
public class DispensationRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Note: id is auto-generated, so not included in insert
    public String hmisCode;           // HMIS facility code
    public Integer drugCount;         // Count of non-ARV drugs only (Essential medicines)
    public Integer arvDrugCount;      // Count of ARV drugs only (HIV medications with mslDrugId containing "ARV")
    public String refPrescription;    // Reference to prescription UUID
    public String messageId;          // For logging/debugging (not stored in DB)

    // New fields from enhanced payload
    public String patientGuid;        // Patient identifier (nullable)
    public String artNumber;          // ART number (nullable)
    public String nextVisitDate;      // Next visit date as string "yyyy-MM-dd" (nullable)
    public String transactionTime;    // Transaction time as string "HH:mm:ss" (nullable)
    public String dispensationDate;   // Dispensation date as string "yyyy-MM-dd" (nullable)

    // Drug line items (for streaming to drug sink, not stored in parent DB row)
    public java.util.List<DispensationDrugRecord> drugs;  // List of drug records

    // Default constructor
    public DispensationRecord() {}

    // Constructor
    public DispensationRecord(String hmisCode, Integer drugCount, Integer arvDrugCount, String refPrescription, String messageId) {
        this.hmisCode = hmisCode;
        this.drugCount = drugCount;
        this.arvDrugCount = arvDrugCount;
        this.refPrescription = refPrescription;
        this.messageId = messageId;
        this.drugs = new java.util.ArrayList<>();
    }

    // Extended constructor with new fields
    public DispensationRecord(String hmisCode, Integer drugCount, Integer arvDrugCount, String refPrescription,
                              String messageId, String patientGuid, String artNumber, String nextVisitDate,
                              String transactionTime, String dispensationDate, java.util.List<DispensationDrugRecord> drugs) {
        this.hmisCode = hmisCode;
        this.drugCount = drugCount;
        this.arvDrugCount = arvDrugCount;
        this.refPrescription = refPrescription;
        this.messageId = messageId;
        this.patientGuid = patientGuid;
        this.artNumber = artNumber;
        this.nextVisitDate = nextVisitDate;
        this.transactionTime = transactionTime;
        this.dispensationDate = dispensationDate;
        this.drugs = drugs != null ? drugs : new java.util.ArrayList<>();
    }

    // Getters and Setters
    public String getHmisCode() {
        return hmisCode;
    }

    public void setHmisCode(String hmisCode) {
        this.hmisCode = hmisCode;
    }

    public Integer getDrugCount() {
        return drugCount;
    }

    public void setDrugCount(Integer drugCount) {
        this.drugCount = drugCount;
    }

    public Integer getArvDrugCount() {
        return arvDrugCount;
    }

    public void setArvDrugCount(Integer arvDrugCount) {
        this.arvDrugCount = arvDrugCount;
    }

    public String getRefPrescription() {
        return refPrescription;
    }

    public void setRefPrescription(String refPrescription) {
        this.refPrescription = refPrescription;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getPatientGuid() {
        return patientGuid;
    }

    public void setPatientGuid(String patientGuid) {
        this.patientGuid = patientGuid;
    }

    public String getArtNumber() {
        return artNumber;
    }

    public void setArtNumber(String artNumber) {
        this.artNumber = artNumber;
    }

    public String getNextVisitDate() {
        return nextVisitDate;
    }

    public void setNextVisitDate(String nextVisitDate) {
        this.nextVisitDate = nextVisitDate;
    }

    public String getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(String transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getDispensationDate() {
        return dispensationDate;
    }

    public void setDispensationDate(String dispensationDate) {
        this.dispensationDate = dispensationDate;
    }

    public java.util.List<DispensationDrugRecord> getDrugs() {
        return drugs;
    }

    public void setDrugs(java.util.List<DispensationDrugRecord> drugs) {
        this.drugs = drugs;
    }

    @Override
    public String toString() {
        return "DispensationRecord{" +
                "hmisCode='" + hmisCode + '\'' +
                ", drugCount=" + drugCount +
                ", arvDrugCount=" + arvDrugCount +
                ", refPrescription='" + refPrescription + '\'' +
                ", messageId='" + messageId + '\'' +
                ", patientGuid='" + patientGuid + '\'' +
                ", artNumber='" + artNumber + '\'' +
                ", nextVisitDate='" + nextVisitDate + '\'' +
                ", transactionTime='" + transactionTime + '\'' +
                ", dispensationDate='" + dispensationDate + '\'' +
                ", drugsCount=" + (drugs != null ? drugs.size() : 0) +
                '}';
    }
}