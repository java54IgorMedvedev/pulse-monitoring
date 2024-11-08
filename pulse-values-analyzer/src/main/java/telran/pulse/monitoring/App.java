package telran.pulse.monitoring;

import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import static telran.pulse.monitoring.Constants.DEFAULT_LOGGER_LEVEL;
import static telran.pulse.monitoring.Constants.LOGGER_LEVEL_ENV_VARIABLE;
import static telran.pulse.monitoring.Constants.PATIENT_ID_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.TIMESTAMP_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.VALUE_ATTRIBUTE;

public class App {

    static DynamoDbClient client = DynamoDbClient.builder().build();
    static Logger logger = Logger.getLogger("pulse-value-analyzer");
    static float factor;

    static {
        loggerSetUp();
        factor = getFactor();
    }

    public void handleRequest(DynamodbEvent event, Context context) {
        event.getRecords().forEach(r -> {
            Map<String, AttributeValue> map = r.getDynamodb().getNewImage();
            if (map == null) {
                logger.warning("No new image found");
            } else if ("INSERT".equals(r.getEventName())) {
                processPulseValue(map);
            } else {
                logger.warning(String.format("The event isn't INSERT but %s", r.getEventName()));
            }
        });
    }

    private static void loggerSetUp() {
        Level loggerLevel = getLoggerLevel();
        LogManager.getLogManager().reset();
        Handler handler = new ConsoleHandler();
        logger.setLevel(loggerLevel);
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
    }

    private static Level getLoggerLevel() {
        String levelStr = System.getenv().getOrDefault(LOGGER_LEVEL_ENV_VARIABLE, DEFAULT_LOGGER_LEVEL);
        try {
            return Level.parse(levelStr);
        } catch (Exception e) {
            return Level.parse(DEFAULT_LOGGER_LEVEL);
        }
    }

    private static float getFactor() {
        String factorStr = System.getenv().getOrDefault("FACTOR", "0.2");
        try {
            return Float.parseFloat(factorStr);
        } catch (NumberFormatException e) {
            return 0.2f;
        }
    }

    private void processPulseValue(Map<String, AttributeValue> map) {
        int value = Integer.parseInt(map.get(VALUE_ATTRIBUTE).getN());
        String patientId = map.get(PATIENT_ID_ATTRIBUTE).getN();
        int lastValue = getLastPulseValue(patientId);
        logger.finer(getLogMessage(map));
        if (isJump(value, lastValue)) {
            recordJump(patientId, lastValue, value, map.get(TIMESTAMP_ATTRIBUTE).getN());
        }
        saveLastPulseValue(patientId, value);
    }

    private boolean isJump(int currentValue, int lastValue) {
        return (float) Math.abs(currentValue - lastValue) / lastValue > factor;
    }

    private String getLogMessage(Map<String, AttributeValue> map) {
        return String.format("patientId: %s, value: %s", map.get(PATIENT_ID_ATTRIBUTE).getN(), map.get(VALUE_ATTRIBUTE).getN());
    }

    private int getLastPulseValue(String patientId) {
        GetItemRequest request = GetItemRequest.builder()
            .tableName("pulse_last_value")
            .key(Map.of(PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build()))
            .build();
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = client.getItem(request).item();
        return item != null && item.containsKey(VALUE_ATTRIBUTE) ? Integer.parseInt(item.get(VALUE_ATTRIBUTE).n()) : 0;
    }

    private void saveLastPulseValue(String patientId, int value) {
        client.putItem(PutItemRequest.builder()
            .tableName("pulse_last_value")
            .item(Map.of(
                PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build(),
                VALUE_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(String.valueOf(value)).build()
            ))
            .build());
    }

    private void recordJump(String patientId, int lastValue, int currentValue, String timestamp) {
        client.putItem(PutItemRequest.builder()
            .tableName("pulse_jump_values")
            .item(Map.of(
                PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build(),
                "PreviousValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(String.valueOf(lastValue)).build(),
                "CurrentValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(String.valueOf(currentValue)).build(),
                TIMESTAMP_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(timestamp).build()
            ))
            .build());
    }
}
