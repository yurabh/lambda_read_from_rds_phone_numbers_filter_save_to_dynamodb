package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.example.model.PhoneNumber;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.example.utils.LambdaUtils.CREDENTIALS;
import static com.example.utils.LambdaUtils.PASSWORD;
import static com.example.utils.LambdaUtils.RDS_URL;
import static com.example.utils.LambdaUtils.REGION;
import static com.example.utils.LambdaUtils.TOPIC_ARN;
import static com.example.utils.LambdaUtils.USER_NAME;

public class SqsLambdaHandler implements RequestHandler<SQSEvent, String> {
    private static final Logger LOGGER = LogManager.getLogger(SqsLambdaHandler.class);

    private static final String FINISH_PROCESSING_FUNCTION = "Finish processing lambda function because sqs event is empty";

    private static final String SELECT_PHONE_NUMBERS = "SELECT numbers FROM phone_numbers";

    private static final String MESSAGE_BODY_TEMPLATE = "phones saved";

    private static final String FUNCTION_EXECUTED_SUCCESSFULLY = "Function executed successfully";

    private static final String PHONE_NUMBER = "phoneNumber";

    private static final String TABLE_NAME = "phone_numbers";

    private static final String HASH_KEY_NAME = "Id";

    private static final String TOPIC = "phone-topic";

    private static final String MESSAGE = "Read phones number from Dynamodb";

    private static final CreateTopicRequest topicRequest = new CreateTopicRequest(TOPIC);

    private static final AmazonSNSClient amazonSNSClient = (AmazonSNSClient) AmazonSNSClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(CREDENTIALS))
            .withRegion(REGION)
            .build();

    private static final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(Regions.US_EAST_1)
            .withCredentials(new AWSStaticCredentialsProvider(CREDENTIALS))
            .build();

    private static final DynamoDB db = new DynamoDB(amazonDynamoDB);

    private static Table table;

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
                    createTable();
                    savePhoneNumbersInDynamoDb(filteredNumbers);
                    createSnsTopic();
                    publishMessageToTheSnsTopic();
                }
            }
        }
        return FUNCTION_EXECUTED_SUCCESSFULLY;
    }

    private static void createSnsTopic() {
        amazonSNSClient.createTopic(topicRequest);
    }

    private static void publishMessageToTheSnsTopic() {
        amazonSNSClient.publish(TOPIC_ARN, MESSAGE);
    }

    private static void createTable() {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(6L));
        try {
            LOGGER.info("Try to create DynamoDb table");
            table = db.createTable(request);
            LOGGER.info("Dynamo Db table successfully created");
        } catch (Exception e) {
            LOGGER.error("Failed create table in Dynamo Db or table already exist: {}", e.getMessage());
        }
    }

    private static void savePhoneNumbersInDynamoDb(PhoneNumber phoneNumbers) {
        table = db.getTable(TABLE_NAME);
        LOGGER.info("Start writing phone numbers in DynamoDb");
        for (int i = 0; i < phoneNumbers.getPhoneNumbers().size(); i++) {
            table.putItem(new Item().withPrimaryKey(HASH_KEY_NAME, i)
                    .with(PHONE_NUMBER, phoneNumbers.getPhoneNumbers().get(i)));
        }
        LOGGER.info("Phone number wrote in Dynamo db");
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
