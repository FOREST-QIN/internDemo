package com.interndemo.dynamoDB;

import com.interndemo.bean.NormalFactoryPickUpConfig;
import com.interndemo.bean.ServiceHubPickUpConfig;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DynamoDBCRUD {

  public String createTable(DynamoDbClient ddb, String tableName, String key) {
    DynamoDbWaiter dbWaiter = ddb.waiter();
    CreateTableRequest request = CreateTableRequest.builder().attributeDefinitions(AttributeDefinition.builder()
            .attributeName(key)
            .attributeType(ScalarAttributeType.S).build())
        .keySchema(KeySchemaElement.builder()
            .attributeName(key)
            .keyType(KeyType.HASH)
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(new Long(10))
            .writeCapacityUnits(new Long(10))
            .build())
        .tableName(tableName)
        .build();
    String newTable = "";
    try {
      CreateTableResponse response = ddb.createTable(request);
      DescribeTableRequest tableRequest = DescribeTableRequest.builder()
          .tableName(tableName)
          .build();
      WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
      waiterResponse.matched().response().ifPresent(System.out::println);
      newTable = response.tableDescription().tableName();
      return newTable;
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return "";
  }

  public void writeNormalFactoryPickUpConfig(DynamoDbEnhancedClient enhancedClient, String id, Integer fromTime, Integer toTime) {
    try {
      DynamoDbTable<NormalFactoryPickUpConfig> normalFactoryPickUpConfigDynamoDbTable = enhancedClient.table("NormalFactoryPickUpConfig", TableSchema.fromBean(NormalFactoryPickUpConfig.class));
      NormalFactoryPickUpConfig config = new NormalFactoryPickUpConfig();
      config.setId(id);
      config.setFromTime(fromTime);
      config.setToTime(toTime);
      normalFactoryPickUpConfigDynamoDbTable.putItem(config);
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }

    System.out.println("Normal Factory Config data added to the table");
  }

  public void writeServiceHubFactoryPickUpConfig(DynamoDbEnhancedClient enhancedClient, String id, Integer fromTime, Integer toTime) {
    try {
      DynamoDbTable<ServiceHubPickUpConfig> serviceHubPickUpConfigDynamoDbTable = enhancedClient.table("ServiceHubPickUpConfig", TableSchema.fromBean(ServiceHubPickUpConfig.class));
      ServiceHubPickUpConfig config = new ServiceHubPickUpConfig();
      config.setId(id);
      config.setFromTime(fromTime);
      config.setToTime(toTime);
      serviceHubPickUpConfigDynamoDbTable.putItem(config);
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }

    System.out.println("Service Hub Config data added to the table");
  }

  public int queryTable(DynamoDbClient ddb,
                        String tableName,
                        String partitionKeyName,
                        String partitionKeyVal,
                        String partitionAlias) {
    HashMap<String, String> attrNameAlias = new HashMap<String, String>();
    attrNameAlias.put(partitionAlias, partitionKeyName);
    HashMap<String, AttributeValue> attrValues = new HashMap<>();
    attrValues.put(":" + partitionKeyName, AttributeValue.builder().s(partitionKeyVal).build());

    QueryRequest queryReq = QueryRequest.builder()
        .tableName(tableName)
        .keyConditionExpression(partitionAlias + " = :" + partitionKeyName)
        .expressionAttributeNames(attrNameAlias)
        .expressionAttributeValues(attrValues)
        .build();
    try {
      QueryResponse response = ddb.query(queryReq);
      return response.count();
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return -1;
  }

//  public Integer modifyServiceHubPickUpConfigItem(DynamoDbEnhancedClient enhancedClient, boolean isFromTime, String keyVal, Integer value) {
//    try {
//      DynamoDbTable<ServiceHubPickUpConfig> mappedTable = enhancedClient.table("ServiceHubFactoryPickUpConfig", TableSchema.fromBean(ServiceHubPickUpConfig.class));
//      Key key = Key.builder().partitionValue(keyVal).build();
//      ServiceHubPickUpConfig config = mappedTable.getItem(r -> r.key(key));
//      if (isFromTime) {
//        config.setFromTime(value);
//      } else {
//        config.setToTime(value);
//      }
//      mappedTable.updateItem(config);
//      return config.getFromTime();
//    } catch (DynamoDbException e) {
//      System.err.println(e.getMessage());
//      System.exit(1);
//    }
//    return -1;
//  }
//
//  public Integer modifyNormalFactoryPickUpConfigItem(DynamoDbEnhancedClient enhancedClient, boolean isFromTime, String keyVal, Integer value) {
//    try {
//      DynamoDbTable<NormalFactoryPickUpConfig> mappedTable = enhancedClient.table("NormalFactoryPickUpConfig", TableSchema.fromBean(NormalFactoryPickUpConfig.class));
//      Key key = Key.builder().partitionValue(keyVal).build();
//      NormalFactoryPickUpConfig config = mappedTable.getItem(r -> r.key(key));
//      if (isFromTime) {
//        config.setFromTime(value);
//      } else {
//        config.setToTime(value);
//      }
//      mappedTable.updateItem(config);
//      return config.getFromTime();
//    } catch (DynamoDbException e) {
//      System.err.println(e.getMessage());
//      System.exit(1);
//    }
//    return -1;
//  }

  public String update(DynamoDbClient ddb, String tableName, String key, String keyValue, int fromTime, int toTime) {
    HashMap<String, AttributeValue> itemKey = new HashMap<>();
    itemKey.put(key, AttributeValue.builder().s(keyValue).build());
    HashMap<String, AttributeValueUpdate> updateValues = new HashMap<>();
    updateValues.put("FromTime", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().s(String.valueOf(fromTime)).build())
        .action(AttributeAction.PUT)
        .build()
    );
    updateValues.put("ToTime", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().s(String.valueOf(toTime)).build())
        .action(AttributeAction.PUT)
        .build()
    );
    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(tableName)
        .key(itemKey)
        .attributeUpdates(updateValues)
        .build();

    try {
      ddb.updateItem(request);
    } catch (ResourceNotFoundException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    System.out.println("The Amazon DynamoDB table was updated!");
    return "yes";
  }

  public void putItemInTable(DynamoDbClient ddb, String tableName, String key, int fromTime, int toTime) {
    HashMap<String, AttributeValue> itemsValues = new HashMap<>();
    itemsValues.put(key, AttributeValue.builder().s(key).build());
    itemsValues.put("FromTime", AttributeValue.builder().s(String.valueOf(fromTime)).build());
    itemsValues.put("ToTime", AttributeValue.builder().s(String.valueOf(toTime)).build());
    PutItemRequest request = PutItemRequest.builder()
        .tableName(tableName)
        .item(itemsValues)
        .build();

    try {
      ddb.putItem(request);
      System.out.println(tableName + " was successfully updated");

    } catch (ResourceNotFoundException e) {
      System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
      System.err.println("Be sure that it exists and that you've typed its name correctly!");
      System.exit(1);
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  public String getDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {
    HashMap<String, AttributeValue> keyToGet = new HashMap<>();
    keyToGet.put(key, AttributeValue.builder()
        .s(keyVal).build());
    GetItemRequest request = GetItemRequest.builder()
        .key(keyToGet)
        .tableName(tableName)
        .build();
    try {
      Map<String, AttributeValue> returnedItem = ddb.getItem(request).item();

      if (returnedItem != null) {
        Set<String> keys = returnedItem.keySet();
        System.out.println("Amazon DynamoDB table attributes: \n");

        for (String key1 : keys) {
          System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
        }
      } else {
        System.out.format("No item found with the key %s!\n", key);
      }
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return "yes";
  }

}
