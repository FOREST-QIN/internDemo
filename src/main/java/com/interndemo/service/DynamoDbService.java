package com.interndemo.service;

public interface DynamoDbService {
  public String queryNormalFactoryPickUp();
  public String queryServiceHubPickUp();
  public String updateNormalFactoryPickUpConfig(int fromTime, int toTime);
  public String updateServiceHubPickUpConfig(int fromTime, int toTime);
  public String createNormalFactoryPickUpConfigTable();
  public String createServiceHubPickUpConfigTable();
  public String writeItem(int isNormalFactoryPickUp,int fromTime, int toTime);
}
