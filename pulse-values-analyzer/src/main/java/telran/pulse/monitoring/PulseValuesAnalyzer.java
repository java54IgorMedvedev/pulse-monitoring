package telran.pulse.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.ABNORMAL_VALUES_TABLE_NAME;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.BASE_URL_ENV_NAME;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.DEFAULT_LOGGER_LEVEL;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.LOGGER_LEVEL_ENV_VARIABLE;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.MAX_FIELD_NAME;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.MIN_FIELD_NAME;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.PATIENT_ID_ATTRIBUTE;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.TIMESTAMP_ATTRIBUTE;
import static telran.pulse.monitoring.ConstantsPulseValuesAnalyzer.VALUE_ATTRIBUTE;

record Range(int min, int max) {

}

public class PulseValuesAnalyzer {
	static HttpClient httpClient = HttpClient.newHttpClient();
	static String baseURL;
	static DynamoDbClient client = DynamoDbClient.builder().build();
	static Builder dynamoItemRequest;
	static Logger logger = Logger.getLogger("pulse-value-analyzer");
	static {
		loggerSetUp();
		baseURLSetUp();
	}
	static HashMap<String, Range> ranges = new HashMap<>();

	public void handleRequest(DynamodbEvent event, Context context) {
		dynamoItemRequest = PutItemRequest.builder().tableName(ABNORMAL_VALUES_TABLE_NAME);

		event.getRecords().forEach(r -> {
			Map<String, AttributeValue> map = r.getDynamodb().getNewImage();
			if (map == null) {
				logger.warning("No new image found");
			} else if (r.getEventName().equals("INSERT")) {
				processPulseValue(map);
			} else {
				logger.warning(String.format("The event isn't INSERT but %s", r.getEventName()));
			}

		});
	}

	private static void baseURLSetUp() {
		baseURL = System.getenv(BASE_URL_ENV_NAME);
		if (baseURL == null) {
			logger.severe("Range provider URL doesn't exist");
			throw new RuntimeException(BASE_URL_ENV_NAME + " Environment variable not set");
		}
		logger.config(BASE_URL_ENV_NAME + " is " + baseURL);

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
		String levelStr = System.getenv()
				.getOrDefault(LOGGER_LEVEL_ENV_VARIABLE, DEFAULT_LOGGER_LEVEL);
		Level res = null;
		try {
			res = Level.parse(levelStr);
		} catch (Exception e) {
			res = Level.parse(DEFAULT_LOGGER_LEVEL);
		}
		return res;
	}

	private void processPulseValue(Map<String, AttributeValue> map) {
		int value = Integer.parseInt(map.get(VALUE_ATTRIBUTE).getN());
		String patientIdStr = map.get(PATIENT_ID_ATTRIBUTE).getN();
		logger.finer(getLogMessage(map));
		Range range = getRange(patientIdStr);
		if (value > range.max() || value < range.min()) {
			processAbnormalPulseValue(map);
		}
	}

	private Range getRange(String patientIdStr) {
		Range res = ranges.get(patientIdStr);
		if (res != null) {
			logger.finer("Range taken from cache " + res);
		} else {
			res = getRangeFromProvider(patientIdStr);
			logger.fine("Range taken from provider API " + res);
		}
		return res;

	}

	private Range getRangeFromProvider(String patientIdStr) {

		HttpRequest request = HttpRequest.newBuilder(getURL(patientIdStr)).build();
		HttpResponse<String> response;
		try {
			response = httpClient.send(request, BodyHandlers.ofString());
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
		String bodyJSON = response.body();
		if (response.statusCode() >= 400) {
			throw new RuntimeException(bodyJSON);
		}
		return getRangeFromJSON(bodyJSON);
	}

	private Range getRangeFromJSON(String bodyJSON) {
		JSONObject jsonObj = new JSONObject(bodyJSON);
		return new Range(jsonObj.getInt(MIN_FIELD_NAME), jsonObj.getInt(MAX_FIELD_NAME));
	}

	private URI getURL(String patientIdStr) {
		String uriStr = baseURL + "?patientId=" + patientIdStr;
		logger.fine(uriStr + " is range provider URI");
		try {
			return new URI(uriStr);
		} catch (URISyntaxException e) {
			String errorMessage = uriStr + " wrong URI format";
			logger.severe(errorMessage);
			throw new RuntimeException(errorMessage);
		}
	}

	private void processAbnormalPulseValue(Map<String, AttributeValue> map) {
		logger.info(getLogMessage(map));
		client.putItem(dynamoItemRequest.item(getPutItemMap(map)).build());
	}

	private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> getPutItemMap(
			Map<String, AttributeValue> map) {
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> res = new HashMap<>();
		res.put(PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(map.get(PATIENT_ID_ATTRIBUTE).getN()).build());
		res.put(TIMESTAMP_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(map.get(TIMESTAMP_ATTRIBUTE).getN()).build());
		res.put(VALUE_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(map.get(VALUE_ATTRIBUTE).getN()).build());
		return res;
	}

	private String getLogMessage(Map<String, AttributeValue> map) {
		return String.format("patientId: %s, value: %s", map.get(PATIENT_ID_ATTRIBUTE).getN(),
				map.get(VALUE_ATTRIBUTE).getN());
	}
}