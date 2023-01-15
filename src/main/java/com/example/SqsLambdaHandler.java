package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqsLambdaHandler implements RequestHandler<SQSEvent, String> {
    private static final Logger LOGGER = LogManager.getLogger(SqsLambdaHandler.class);
    private static final String FINISH_PROCESSING_FUNCTION = "Finish processing lambda function because sqs event is empty";

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        if (event.getRecords().isEmpty()) {
            LOGGER.info(FINISH_PROCESSING_FUNCTION + ": {}", event.getRecords().size());
            return FINISH_PROCESSING_FUNCTION;
        }

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            System.out.println(message.getBody());
        }
        return "";
    }
}
