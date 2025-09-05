package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

/**
 * Represents a dispensation record matching the crt.dispensation table schema
 * 
 * Database columns:
 * - id (bigint, auto-generated primary key)
 * - hmis_code (varchar(50), NOT NULL)
 * - drug_count (smallint)
 * - date (date, NOT NULL, defaults to CURRENT_DATE)
 * - time (time, NOT NULL, defaults to CURRENT_TIME)
 * - ref_prescription (varchar(50))
 */
public class DispensationRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Note: id is auto-generated, so not included in insert
    public String hmisCode;         // HMIS facility code
    public Integer drugCount;       // Number of dispensed drugs
    public String refPrescription;  // Reference to prescription UUID
    public String messageId;        // For logging/debugging (not stored in DB)
    
    // Default constructor
    public DispensationRecord() {}
    
    // Constructor
    public DispensationRecord(String hmisCode, Integer drugCount, String refPrescription, String messageId) {
        this.hmisCode = hmisCode;
        this.drugCount = drugCount;
        this.refPrescription = refPrescription;
        this.messageId = messageId;
    }
    
    @Override
    public String toString() {
        return "DispensationRecord{" +
                "hmisCode='" + hmisCode + '\'' +
                ", drugCount=" + drugCount +
                ", refPrescription='" + refPrescription + '\'' +
                ", messageId='" + messageId + '\'' +
                '}';
    }
}