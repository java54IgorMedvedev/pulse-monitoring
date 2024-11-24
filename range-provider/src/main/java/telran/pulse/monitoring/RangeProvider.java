package telran.pulse.monitoring;
import java.util.*;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

/**
 * Handler for requests to Lambda function.
 */
record Range(int min, int max) {

}

public class RangeProvider implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    static HashMap<String, Range> ranges = new HashMap<>() {
        {
            put("1", new Range(60, 150));
            put("2", new Range(70, 160));
            put("3", new Range(50, 250));
            put("4", new Range(65, 170));
            put("5", new Range(50, 250));

        }
    };

    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        Map<String, String> mapParameters = input.getQueryStringParameters();
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);

        try {
            if (!mapParameters.containsKey("patientId")) {
                throw new IllegalArgumentException("no patientId parameter");
            }
            String patientIdStr = mapParameters.get("patientId");
            Range range = ranges.get(patientIdStr);
            if (range == null) {
                throw new IllegalStateException(patientIdStr + " not found in ranges");
            }

            response
                    .withStatusCode(200)
                    .withBody(getRangeJSON(range));
        } catch (IllegalArgumentException e) {
            String errorJSON = getErrorJSON(e.getMessage());
            response
                    .withBody(errorJSON)
                    .withStatusCode(400);
        } catch (IllegalStateException e) {
            String errorJSON = getErrorJSON(e.getMessage());
            response
                    .withBody(errorJSON)
                    .withStatusCode(404);
        }
        return response;
    }

    private String getErrorJSON(String message) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("error", message);
        return jsonObj.toString();
    }

    private String getRangeJSON(Range range) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("min", range.min());
        jsonObj.put("max", range.max());
        return jsonObj.toString();
    }

}