package telran.pulse.monitoring;

import java.util.*;
import java.util.logging.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;
import static telran.pulse.monitoring.ConstantsPulseJumpAnalyzer.*;

public class PulseJumpAnalyzer {
	static DynamoDbClient clientDynamo = DynamoDbClient.builder().build();
	static Builder requestInsertLastValues;
	static Builder requestInsertJumpValues;
	static Logger logger = Logger.getLogger("pulse-jump-analyzer");
	static float factor;

	HashMap<String, Integer> lastValues = new HashMap<>();

	public void handleRequest(DynamodbEvent event, Context context) {
		loggerSetUp();
		factorSetUp();
		requestsSetUp();
		var records = event.getRecords();
		if (records == null) {
			logger.severe("no records in the event");
		} else {
			records.forEach(r -> {
				Map<String, com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> map = r
						.getDynamodb().getNewImage();
				if (map == null) {
					logger.warning("No new image found");
				} else if (r.getEventName().equals("INSERT")) {
					String patientId = map.get("patientId").getN();
					Integer currentValue = Integer.parseInt(map.get("value").getN());
					String timestamp = map.get("timestamp").getN();
					logger.finer(String.format("Received: record with patientId=%s, value=%d, timestamp=%s",
							patientId, currentValue, timestamp));
					Integer lastValue = getLastValue(patientId, currentValue);
					if (!lastValue.equals(currentValue)) {
						logger.fine(String.format("patient %s, current value %d, last value %d", patientId,
								currentValue, lastValue));
						if (isJump(currentValue, lastValue)) {

							jumpProcessing(patientId, currentValue, lastValue, timestamp);
						}
						
					}
					putLastValue(patientId, currentValue);
				} else {
					logger.warning(r.getEventName() + " event name but should be INSERT");
				}

			});
		}
	}

	private void putLastValue(String patientId, Integer lastValue) {
		Map<String, AttributeValue> itemMap = new HashMap<>();
		itemMap.put(PATIENT_ID_ATTRIBUTE, AttributeValue.builder().n(patientId).build());
		itemMap.put(VALUE_ATTRIBUTE, AttributeValue.builder().n(lastValue + "").build());
		clientDynamo.putItem(requestInsertLastValues.item(itemMap).build());
		logger.fine(String.format("put in table %s", patientId, lastValue));
	}

	private Integer getLastValue(String patientId, Integer currentValue) {
		HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
		Integer res = currentValue;
		keyToGet.put(PATIENT_ID_ATTRIBUTE, AttributeValue.builder()
				.n(patientId).build());
		GetItemRequest request = GetItemRequest.builder()
				.key(keyToGet)
				.tableName(LAST_PULSE_VALUES_TABLE_NAME)
				.build();
		Map<String, AttributeValue> returnedItem = clientDynamo.getItem(request).item();
		if (returnedItem == null || returnedItem.get(VALUE_ATTRIBUTE) == null) {
			logger.warning(String.format("no pulse value found in table %s for patient %s, taken current value %d",
			 LAST_PULSE_VALUES_TABLE_NAME, patientId, currentValue));
		} else {
			String resStr = returnedItem.get(VALUE_ATTRIBUTE).n();
			res = Integer.parseInt(resStr);

		}
		return res;
	}

	private void requestsSetUp() {
		requestInsertJumpValues = PutItemRequest.builder()
				.tableName(PULSE_JUMPS_TABLE_NAME);
		requestInsertLastValues = PutItemRequest.builder()
				.tableName(LAST_PULSE_VALUES_TABLE_NAME);
	}

	private static void factorSetUp() {
		factor = DEFAULT_FACTOR_VALUE;
		String factorStr = System.getenv(FACTOR_ENV_VARIABLE);
		if (factorStr != null) {
			try {
				factor = Float.parseFloat(factorStr);
				logger.config("Factor for jump is " + factor);
			} catch (NumberFormatException e) {
				logger.warning(String.format("Env. variable %s contains %s, default value %f taken",
						FACTOR_ENV_VARIABLE, factorStr, factor));
			}
		}

	}

	private static void loggerSetUp() {
		Level loggerLevel = getLoggerLevel();
		LogManager.getLogManager().reset();
		Handler handler = new ConsoleHandler();
		logger.setLevel(loggerLevel);
		handler.setLevel(Level.FINEST);
		logger.addHandler(handler);
		logger.config("logger level is " + loggerLevel);
	}

	private static Level getLoggerLevel() {
		String levelStr = System.getenv()
				.getOrDefault(LOGGER_LEVEL_ENV_VARIABLE, DEFAULT_LOGGER_LEVEL);
		Level res = null;
		try {
			res = Level.parse(levelStr);
		} catch (Exception e) {
			logger.warning(levelStr + " wrong logger level take default value " + DEFAULT_LOGGER_LEVEL);
			res = Level.parse(DEFAULT_LOGGER_LEVEL);
		}
		return res;
	}

	private void jumpProcessing(String patientId, Integer currentValue, Integer lastValue,
	 String timestamp) {

		logger.fine(String.format("Jump: patientId is %s,lastValue is %d, currentValue is %d, timestamp is %s\n",
				patientId, lastValue, currentValue, timestamp));
		Map<String, AttributeValue> mapItem = new HashMap<>();
		mapItem.put(PATIENT_ID_ATTRIBUTE, AttributeValue.builder().n(patientId).build());
		mapItem.put(PREVIOUS_VALUE_ATTRIBUTE, AttributeValue.builder().n(lastValue + "").build());
		mapItem.put(CURRENT_VALUE_ATTRIBUTE, AttributeValue.builder().n(currentValue + "").build());
		mapItem.put(TIMESTAMP_ATTRIBUTE, AttributeValue.builder().n(timestamp).build());
		clientDynamo.putItem(requestInsertJumpValues.item(mapItem).build());

	}

	private boolean isJump(Integer currentValue, Integer lastValue) {

		return (float) Math.abs(currentValue - lastValue) / lastValue > factor;
	}
}