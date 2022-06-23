package com.interndemo.service;

import com.interndemo.dynamoDB.DynamoDBCRUD;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbServiceImpl implements DynamoDbService {
  ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
  Region region = Region.US_EAST_1;
  DynamoDbClient ddb = DynamoDbClient.builder()
      .credentialsProvider(credentialsProvider)
      .region(region)
      .build();
  DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
      .dynamoDbClient(ddb)
      .build();

  DynamoDBCRUD dynamoDBCRUD = new DynamoDBCRUD();
  private static final String NORMAL_FACTORY_DB = "NormalFactoryPickUpConfig";
  private static final String SERVICE_HUB_DB = "ServiceHubPickUpConfig";
  @Override
  public String queryNormalFactoryPickUp() {
    return dynamoDBCRUD.getDynamoDBItem(ddb, NORMAL_FACTORY_DB, "Id", "Id");
  }

  @Override
  public String queryServiceHubPickUp() {
    return dynamoDBCRUD.getDynamoDBItem(ddb, SERVICE_HUB_DB, "Id", "Id");
  }


  @Override
  public String updateNormalFactoryPickUpConfig(int fromTime, int toTime) {
    return dynamoDBCRUD.update(ddb, NORMAL_FACTORY_DB, "Id", "Id", fromTime, toTime);
  }

  @Override
  public String updateServiceHubPickUpConfig(int fromTime, int toTime) {
    return dynamoDBCRUD.update(ddb, SERVICE_HUB_DB, "Id", "Id", fromTime, toTime);
  }

  @Override
  public String createNormalFactoryPickUpConfigTable() {
    return dynamoDBCRUD.createTable(ddb, NORMAL_FACTORY_DB, "Id");
  }

  @Override
  public String createServiceHubPickUpConfigTable() {
    return dynamoDBCRUD.createTable(ddb, SERVICE_HUB_DB, "Id");
  }

  @Override
  public String writeItem(int isNormalFactoryPickUp, int fromTime, int toTime) {
    if (isNormalFactoryPickUp == 0) {
      dynamoDBCRUD.putItemInTable(ddb, NORMAL_FACTORY_DB, "Id", 90, 100);
    } else {
      dynamoDBCRUD.putItemInTable(ddb, SERVICE_HUB_DB, "Id", 0, 100);
    }
    return "test";
  }
}
