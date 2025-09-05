package zm.gov.moh.hie.scp.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DispensationMessage {
    
    @JsonProperty("msh")
    public MessageHeader msh;
    
    @JsonProperty("patientGuid")
    public String patientGuid;
    
    @JsonProperty("artNumber")
    public String artNumber;
    
    @JsonProperty("hmisCode")
    public String hmisCode;
    
    @JsonProperty("nextVisitDate")
    public String nextVisitDate;
    
    @JsonProperty("transactionTime")
    public String transactionTime;
    
    @JsonProperty("dispensationDate")
    public String dispensationDate;
    
    @JsonProperty("clinician")
    public String clinician;
    
    @JsonProperty("dispensedDrugs")
    public List<DispensedDrug> dispensedDrugs;
    
    @JsonProperty("submitted")
    public Integer submitted;
    
    @JsonProperty("clinicianId")
    public String clinicianId;
    
    @JsonProperty("prescriptionUuid")
    public String prescriptionUuid;
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MessageHeader {
        @JsonProperty("timestamp")
        public String timestamp;
        
        @JsonProperty("sendingApplication")
        public String sendingApplication;
        
        @JsonProperty("receivingApplication")
        public String receivingApplication;
        
        @JsonProperty("messageId")
        public String messageId;
        
        @JsonProperty("hmisCode")
        public String hmisCode;
        
        @JsonProperty("messageType")
        public String messageType;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DispensedDrug {
        @JsonProperty("mslDrugId")
        public String mslDrugId;
        
        @JsonProperty("quantityDispensed")
        public Double quantityDispensed;
        
        @JsonProperty("doseStrength")
        public String doseStrength;
        
        @JsonProperty("unitQuantityPerDose")
        public Double unitQuantityPerDose;
        
        @JsonProperty("frequency")
        public String frequency;
        
        @JsonProperty("unitOfMeasurement")
        public String unitOfMeasurement;
        
        @JsonProperty("medicationId")
        public String medicationId;
    }
}