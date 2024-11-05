package telran.pulse.monitoring;

import java.util.Map;
import java.util.logging.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class App {
    static DynamoDbClient client = DynamoDbClient.builder().build();
    static Logger logger = Logger.getLogger(App.class.getName());
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
                String patientId = map.get("patientId").getN();
                Integer currentValue = Integer.parseInt(map.get("value").getN());
                String timestamp = map.get("timestamp").getN();
                Integer lastValue = getLastPulseValue(patientId, currentValue);

                if (isJump(currentValue, lastValue)) {
                    recordJump(patientId, lastValue, currentValue, timestamp);
                }
                saveLastPulseValue(patientId, currentValue);
            } else {
                logger.warning("Unexpected event: " + r.getEventName());
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
        String levelStr = System.getenv().getOrDefault("LOGGER_LEVEL", "INFO");
        try {
            return Level.parse(levelStr);
        } catch (Exception e) {
            return Level.INFO;
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

    private boolean isJump(Integer currentValue, Integer lastValue) {
        return (float) Math.abs(currentValue - lastValue) / lastValue > factor;
    }

    private Integer getLastPulseValue(String patientId, Integer defaultValue) {
        GetItemRequest request = GetItemRequest.builder()
            .tableName("pulse_last_value")
            .key(Map.of("patientId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build()))
            .build();

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = client.getItem(request).item();
        return item != null && item.containsKey("value") ? Integer.parseInt(item.get("value").n()) : defaultValue;
    }

    private void saveLastPulseValue(String patientId, Integer value) {
        client.putItem(PutItemRequest.builder()
            .tableName("pulse_last_value")
            .item(Map.of(
                "patientId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build(),
                "value", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(value.toString()).build()
            ))
            .build());
    }

    private void recordJump(String patientId, Integer lastValue, Integer currentValue, String timestamp) {
        client.putItem(PutItemRequest.builder()
            .tableName("pulse_jump_values")
            .item(Map.of(
                "patientId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(patientId).build(),
                "PreviousValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(lastValue.toString()).build(),
                "CurrentValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(currentValue.toString()).build(),
                "timestamp", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(timestamp).build()
            ))
            .build());
        logger.info(String.format("Jump recorded: patientId=%s, lastValue=%d, currentValue=%d, timestamp=%s",
                patientId, lastValue, currentValue, timestamp));
    }
}
