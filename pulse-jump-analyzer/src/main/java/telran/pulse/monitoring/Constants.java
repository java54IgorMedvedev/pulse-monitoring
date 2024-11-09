package telran.pulse.monitoring;


public interface Constants {
    String PATIENT_ID_ATTRIBUTE = "patientId";
    String TIMESTAMP_ATTRIBUTE = "timestamp";
    String VALUE_ATTRIBUTE = "value";
    String PREVIOUS_VALUE_ATTRIBUTE = "previousValue";
    String CURRENT_VALUE_ATTRIBUTE = "currentValue";
    String PULSE_JUMPS_TABLE_NAME = "pulse_jumps_values";
    String LAST_PULSE_VALUES_TABLE_NAME = "last_pulse_values";
    String LOGGER_LEVEL_ENV_VARIABLE = "LOGGER_LEVEL";
    String DEFAULT_LOGGER_LEVEL = "INFO";
    float DEFAULT_FACTOR_VALUE = 0.2f;
    String FACTOR_ENV_VARIABLE = "FACTOR";
}