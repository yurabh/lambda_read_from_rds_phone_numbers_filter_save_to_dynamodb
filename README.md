                                 lambda_read_from_rds_phone_numbers_filter_save_to_dynamodb
                                      
1)About the project.

This is a lambda function that is triggered by an event when sqs message were published in sqs queue. 
This function reads sqs message. After the message were read then function read phone numbers from rds mySql db 
and filtering phone numbers which is start with numbers like :98,63,97,93,67 after that filtered phone numbers is going to be save to the dynamodb. 
After the phone numbers were successfully saved in dynamo db, then sns topic would be created and published message in it - that phone numbers saved in dynamo db.

2)Start the project locally.

2.1 Required to install the project.

* Java 11

2.2 It is necessary to create the dynamodb table, sns topic, lambda function: in the aws console.

When creating a lambda, you need to specify a trigger (sqs queue), as well as set env variables: 
(you can see more details about env variables in the code package settings Class Settings).

Also, to deploy the function, you need to download the jar file from the target folder to the lambda console.

