package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

/**
 * Represents a single drug line item within a dispensation.
 * Each DispensationRecord can have multiple DispensationDrugRecord entries.
 *
 * Database table: crt.dispensation_drug
 * Columns:
 * - id (bigint, auto-generated PK)
 * - ref_prescription (varchar(50)) — links to parent crt.dispensation.ref_prescription
 * - msl_drug_id (varchar(100))
 * - quantity_dispensed (numeric(10,2))
 * - unit_quantity_per_dose (numeric(10,2))
 * - frequency (varchar(100))
 * - unit_of_measurement (varchar(50))
 * - medication_id (varchar(100))
 */
public class DispensationDrugRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String refPrescription;      // FK to parent dispensation (prescriptionUuid)
    private String mslDrugId;            // Drug identifier from ELMIS
    private Double quantityDispensed;    // Quantity dispensed (nullable)
    private Double unitQuantityPerDose;  // Unit quantity per dose (nullable)
    private String frequency;            // Dosage frequency (nullable)
    private String unitOfMeasurement;    // Unit of measurement (nullable)
    private String medicationId;         // Medication identifier (nullable)

    // Default constructor
    public DispensationDrugRecord() {}

    // Constructor
    public DispensationDrugRecord(String refPrescription, String mslDrugId,
                                  Double quantityDispensed, Double unitQuantityPerDose,
                                  String frequency, String unitOfMeasurement,
                                  String medicationId) {
        this.refPrescription = refPrescription;
        this.mslDrugId = mslDrugId;
        this.quantityDispensed = quantityDispensed;
        this.unitQuantityPerDose = unitQuantityPerDose;
        this.frequency = frequency;
        this.unitOfMeasurement = unitOfMeasurement;
        this.medicationId = medicationId;
    }

    // Getters and Setters
    public String getRefPrescription() {
        return refPrescription;
    }

    public void setRefPrescription(String refPrescription) {
        this.refPrescription = refPrescription;
    }

    public String getMslDrugId() {
        return mslDrugId;
    }

    public void setMslDrugId(String mslDrugId) {
        this.mslDrugId = mslDrugId;
    }

    public Double getQuantityDispensed() {
        return quantityDispensed;
    }

    public void setQuantityDispensed(Double quantityDispensed) {
        this.quantityDispensed = quantityDispensed;
    }

    public Double getUnitQuantityPerDose() {
        return unitQuantityPerDose;
    }

    public void setUnitQuantityPerDose(Double unitQuantityPerDose) {
        this.unitQuantityPerDose = unitQuantityPerDose;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    public String getUnitOfMeasurement() {
        return unitOfMeasurement;
    }

    public void setUnitOfMeasurement(String unitOfMeasurement) {
        this.unitOfMeasurement = unitOfMeasurement;
    }

    public String getMedicationId() {
        return medicationId;
    }

    public void setMedicationId(String medicationId) {
        this.medicationId = medicationId;
    }

    @Override
    public String toString() {
        return "DispensationDrugRecord{" +
                "refPrescription='" + refPrescription + '\'' +
                ", mslDrugId='" + mslDrugId + '\'' +
                ", quantityDispensed=" + quantityDispensed +
                ", unitQuantityPerDose=" + unitQuantityPerDose +
                ", frequency='" + frequency + '\'' +
                ", unitOfMeasurement='" + unitOfMeasurement + '\'' +
                ", medicationId='" + medicationId + '\'' +
                '}';
    }
}
