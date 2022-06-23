package com.interndemo.bean;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class ServiceHubPickUpConfig {
  private String id;
  private Integer fromTime;
  private Integer toTime;

  @DynamoDbPartitionKey
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getFromTime() {
    return fromTime;
  }

  public void setFromTime(Integer fromTime) {
    this.fromTime = fromTime;
  }

  public Integer getToTime() {
    return toTime;
  }

  public void setToTime(Integer toTime) {
    this.toTime = toTime;
  }
}
