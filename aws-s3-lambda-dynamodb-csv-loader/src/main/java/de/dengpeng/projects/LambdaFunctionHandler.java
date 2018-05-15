package de.dengpeng.projects;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;

import au.com.bytecode.opencsv.CSVReader;


/**
 * The Class LambdaFunctionHandler.
 * This application loads GZIPped CSV file to DynamoDB using AWS Lambda function.
 * 
 */
public class LambdaFunctionHandler implements RequestHandler<S3Event, Report> {
	
	/** Provide the AWS region which your DynamoDB table is hosted. */
	Region AWS_REGION = Region.getRegion(Regions.US_EAST_1);

	/** The DynamoDB table name. */
	String DYNAMO_TABLE_NAME = "def_specification";

	/* (non-Javadoc)
	 * @see com.amazonaws.services.lambda.runtime.RequestHandler#handleRequest(java.lang.Object, com.amazonaws.services.lambda.runtime.Context)
	 */
	public Report handleRequest(S3Event s3event, Context context) {
		long startTime = System.currentTimeMillis();
		Report statusReport = new Report();
		LambdaLogger logger = context.getLogger();

		logger.log("Lambda Function Started");
		logger.log("I am inside lambda function");
		Helper helper = new Helper();

		try {
			
			S3EventNotificationRecord record = s3event.getRecords().get(0);
			String srcBucket = record.getS3().getBucket().getName();
			// Object key may have spaces or unicode non-ASCII characters.
			String srcKey = record.getS3().getObject().getKey().replace('+', ' ');	
			srcKey = URLDecoder.decode(srcKey, "UTF-8");
			AmazonS3 s3Client = new AmazonS3Client();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
			statusReport.setFileSize(s3Object.getObjectMetadata().getContentLength());
			logger.log("S3 Event Received: " + srcBucket + "/" + srcKey);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent())); 
			CSVReader reader = new CSVReader(br);
			
			AmazonDynamoDB dynamoDBClient = new AmazonDynamoDBClient();
			
			dynamoDBClient.setRegion(AWS_REGION);
			DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
			TableWriteItems energyDataTableWriteItems = new TableWriteItems(DYNAMO_TABLE_NAME);
			List<Item> itemList = new ArrayList<Item>();
			String[] nextLine;
			//logger.log("I am inside DynamoDB-Table------>>"+br.readLine());
			//Item item = helper.splitColumname(br.readLine());
			while ((nextLine = reader.readNext()) != null) {
				Item newItem = helper.parseIt(nextLine);
				itemList.add(newItem);
				//logger.log("I am inside ClouldWatch Console------>>"+nextLine);
			}

			for (List<Item> partition : Lists.partition(itemList, 25)) {
				energyDataTableWriteItems.withItemsToPut(partition);
				BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(energyDataTableWriteItems);
				//Thread.sleep(1000);
				//logger.log("Insert data after 1 milisecond");
				logger.log("I am inside for loop");
				
				do {

					// Check for unprocessed keys which could happen if you
					// exceed provisioned throughput

					
					Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

					if (outcome.getUnprocessedItems().size() > 0) {
						logger.log("Retrieving the unprocessed " + String.valueOf(outcome.getUnprocessedItems().size())
								+ " items."+"i=====");
						outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
					}
				} while (outcome.getUnprocessedItems().size() > 0);

				
			}
			

			logger.log("Load finish in " + String.valueOf(System.currentTimeMillis() - startTime) + "ms");

			reader.close();
			br.close();
			s3Object.close();

			statusReport.setStatus(true);
		} catch (Exception ex) {
			logger.log(ex.getMessage());
		}

		statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
		return statusReport;
	}

}
