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
 *
 * Drug Classification:
 * - ARV drugs: mslDrugId contains "ARV" (e.g., ARV0092)
 * - Essential drugs: mslDrugId does NOT contain "ARV"
 */
public class DispensationRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Note: id is auto-generated, so not included in insert
    public String hmisCode;         // HMIS facility code
    public Integer drugCount;       // Count of non-ARV drugs only (Essential medicines)
    public Integer arvDrugCount;    // Count of ARV drugs only (HIV medications with mslDrugId containing "ARV")
    public String refPrescription;  // Reference to prescription UUID
    public String messageId;        // For logging/debugging (not stored in DB)

    // Default constructor
    public DispensationRecord() {}

    // Constructor
    public DispensationRecord(String hmisCode, Integer drugCount, Integer arvDrugCount, String refPrescription, String messageId) {
        this.hmisCode = hmisCode;
        this.drugCount = drugCount;
        this.arvDrugCount = arvDrugCount;
        this.refPrescription = refPrescription;
        this.messageId = messageId;
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

    @Override
    public String toString() {
        return "DispensationRecord{" +
                "hmisCode='" + hmisCode + '\'' +
                ", drugCount=" + drugCount +
                ", arvDrugCount=" + arvDrugCount +
                ", refPrescription='" + refPrescription + '\'' +
                ", messageId='" + messageId + '\'' +
                '}';
    }
}