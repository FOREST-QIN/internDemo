package com.interndemo.ocntroller;

import com.interndemo.service.DynamoDbService;
import com.interndemo.service.DynamoDbServiceImpl;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@ResponseBody
@RequestMapping("/config")
public class ConfiguratorController {
  DynamoDbService dynamoDbService = new DynamoDbServiceImpl();

  @GetMapping("/creatNTable")
  public String createNTable() {
    return dynamoDbService.createNormalFactoryPickUpConfigTable();
  }

  @GetMapping("/createSTable")
  public String createSTable() {
    return dynamoDbService.createServiceHubPickUpConfigTable();
  }

  @PostMapping("/putItem")
  public String putItem(@RequestParam(name = "isNormal") int isNormal, @RequestParam("fromTime") int fromTime, @RequestParam("toTime") int toTime) {
    return dynamoDbService.writeItem(isNormal,fromTime, toTime);
  }

  @PostMapping("/update")
  public String updateItem(@RequestParam(name = "isNormal") int isNormal, @RequestParam("fromTime") int fromTime, @RequestParam("toTime") int toTime) {
    if (isNormal == 0) {
      return dynamoDbService.updateNormalFactoryPickUpConfig(fromTime, toTime);
    } else {
      return dynamoDbService.updateServiceHubPickUpConfig(fromTime, toTime);
    }
  }

  @GetMapping("/query")
  public String query(@RequestParam(name = "isNormal") int isNormal) {
    if (isNormal == 0) {
      return dynamoDbService.queryNormalFactoryPickUp();
    } else {
      return dynamoDbService.queryServiceHubPickUp();
    }
  }

}
