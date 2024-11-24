package telran.pulse.monitoring;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class AbnormalValuesNotifier {
    String topicARN;
    String awsRegion;
    SnsClient snsClient;

    public void handleRequest(DynamodbEvent event, Context context) {
        setUpEnvironment();
        setUpSnsClient();
        event.getRecords().forEach(r -> {
            Map<String, AttributeValue> image = r.getDynamodb().getNewImage();
            String message = getMessage(image);
            System.out.println("message is " + message);
            publishMessage(message);
        });
    }

    private void publishMessage(String message) {
        PublishRequest request = PublishRequest.builder()
                .message(message)
                .topicArn(topicARN)
                .build();
       snsClient.publish(request);
    }

    private String getMessage(Map<String, AttributeValue> image) {
        return String.format("patient %s\nabnormal pulse value %s\ndate-time %s",
                image.get("patientId").getN(), image.get("value").getN(),
                getDateTime(image.get("timestamp").getN()));
    }

    private Object getDateTime(String timestampStr) {
        long timestamp = Long.parseLong(timestampStr);
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime res = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return res;
    }

    private void setUpSnsClient() {
        snsClient = SnsClient.builder().region(Region.of(awsRegion)).build();
    }

    private void setUpEnvironment() {
        Map<String, String> env = System.getenv();
        topicARN = env.get("TOPIC_ARN");
        if (topicARN == null) {
            throw new NoSuchElementException("TOPIC_ARN not found");
        }
        awsRegion = env.getOrDefault("REGION", "us-east-1");
    }
}