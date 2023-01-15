package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.example.model.PhoneNumber;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.example.utils.LambdaUtils.PASSWORD;
import static com.example.utils.LambdaUtils.RDS_URL;
import static com.example.utils.LambdaUtils.USER_NAME;

public class SqsLambdaHandler implements RequestHandler<SQSEvent, String> {
    private static final Logger LOGGER = LogManager.getLogger(SqsLambdaHandler.class);
    private static final String FINISH_PROCESSING_FUNCTION = "Finish processing lambda function because sqs event is empty";
    private static final String SELECT_PHONE_NUMBERS = "SELECT numbers FROM phone_numbers";
    private static final String MESSAGE_BODY_TEMPLATE = "phones saved";

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        if (event.getRecords().isEmpty()) {
            LOGGER.info(FINISH_PROCESSING_FUNCTION + ": {}", event.getRecords().size());
            return FINISH_PROCESSING_FUNCTION;
        }
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            if (Objects.nonNull(message.getBody()) && message.getBody().equals(MESSAGE_BODY_TEMPLATE)) {
                PhoneNumber phoneNumbers = readPhoneNumbersFromRdsDb();
                if (Objects.nonNull(phoneNumbers)) {
                    PhoneNumber filteredNumbers = filteringPhoneNumbers(phoneNumbers);
                }
            }
        }
        return "Function executed successfully";
    }

    private static PhoneNumber filteringPhoneNumbers(PhoneNumber phoneNumbers) {
        LOGGER.info("Filtering numbers");
        List<Integer> filteredNumbers = phoneNumbers.getPhoneNumbers().stream()
                .map(String::valueOf)
                .filter((pn) -> pn.startsWith("98")
                        || pn.startsWith("63")
                        || pn.startsWith("93")
                        || pn.startsWith("97")
                        || pn.startsWith("68"))
                .map(Integer::valueOf)
                .collect(Collectors.toList());
        phoneNumbers.setPhoneNumbers(filteredNumbers);
        return phoneNumbers;
    }

    private static PhoneNumber readPhoneNumbersFromRdsDb() {
        PhoneNumber phoneNumbers = new PhoneNumber();
        try (Connection connection = DriverManager.getConnection(RDS_URL, USER_NAME, PASSWORD);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(SELECT_PHONE_NUMBERS);
            LOGGER.info("Try to read phone numbers from db");
            while (resultSet.next()) {
                String phoneNumber = resultSet.getString("numbers");
                phoneNumbers.addNumber(Integer.valueOf(phoneNumber));
            }
            LOGGER.info("Phone numbers were successfully read");
            return phoneNumbers;
        } catch (SQLException e) {
            LOGGER.error("Failed reading phone numbers from db rds phone numbers or connection to the db failed: {}", e.getMessage());
            return null;
        }
    }
}
