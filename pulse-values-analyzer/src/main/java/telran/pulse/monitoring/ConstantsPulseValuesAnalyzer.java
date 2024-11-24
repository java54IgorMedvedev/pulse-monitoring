package telran.pulse.monitoring;

public interface ConstantsPulseValuesAnalyzer {
    String PATIENT_ID_ATTRIBUTE = "patientId";
    String TIMESTAMP_ATTRIBUTE = "timestamp";
    String VALUE_ATTRIBUTE = "value";
    
    String ABNORMAL_VALUES_TABLE_NAME = "pulse_abnormal_values";
    String LOGGER_LEVEL_ENV_VARIABLE = "LOGGER_LEVEL";
    String DEFAULT_LOGGER_LEVEL = "INFO";
    String BASE_URL_ENV_NAME="BASE_URL";
    String MIN_FIELD_NAME = "min";
    String MAX_FIELD_NAME = "max";
}