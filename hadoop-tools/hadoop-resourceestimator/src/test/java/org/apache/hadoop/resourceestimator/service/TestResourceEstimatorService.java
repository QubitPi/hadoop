/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.resourceestimator.service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.resourceestimator.common.api.RecurrenceId;
import org.apache.hadoop.resourceestimator.common.api.ResourceSkyline;
import org.apache.hadoop.resourceestimator.common.serialization.RLESparseResourceAllocationSerDe;
import org.apache.hadoop.resourceestimator.common.serialization.ResourceSerDe;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;

/**
 * Test ResourceEstimatorService.
 */
public class TestResourceEstimatorService extends JerseyTest {
  private final String parseLogCommand = "resourceestimator/translator/"
      + "src/test/resources/resourceEstimatorService.txt";
  private final String getHistorySkylineCommand =
      "resourceestimator/skylinestore/history/tpch_q12/*";
  private final String getEstimatedSkylineCommand =
      "resourceestimator/skylinestore/estimation/tpch_q12";
  private final String makeEstimationCommand =
      "resourceestimator/estimator/tpch_q12";
  private final String deleteHistoryCommand =
      "resourceestimator/skylinestore/history/tpch_q12/tpch_q12_1";
  private static boolean setUpDone = false;
  private Resource containerSpec;
  private Gson gson;
  private long containerMemAlloc;
  private int containerCPUAlloc;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(ResourceEstimatorService.class);
    containerMemAlloc = 1024;
    containerCPUAlloc = 1;
    containerSpec = Resource.newInstance(containerMemAlloc, containerCPUAlloc);
    gson = new GsonBuilder()
            .registerTypeAdapter(Resource.class, new ResourceSerDe())
            .registerTypeAdapter(RLESparseResourceAllocation.class,
                    new RLESparseResourceAllocationSerDe())
            .enableComplexMapKeySerialization().create();
    return config;
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  private void compareResourceSkyline(final ResourceSkyline skyline1,
      final ResourceSkyline skyline2) {
    Assertions.assertEquals(skyline1.getJobId(), skyline2.getJobId());
    Assertions.assertEquals(skyline1.getJobInputDataSize(),
        skyline2.getJobInputDataSize(), 0);
    Assertions.assertEquals(skyline1.getJobSubmissionTime(),
        skyline2.getJobSubmissionTime());
    Assertions
        .assertEquals(skyline1.getJobFinishTime(), skyline2.getJobFinishTime());
    Assertions.assertEquals(skyline1.getContainerSpec().getMemorySize(),
        skyline2.getContainerSpec().getMemorySize());
    Assertions.assertEquals(skyline1.getContainerSpec().getVirtualCores(),
        skyline2.getContainerSpec().getVirtualCores());
    final RLESparseResourceAllocation skylineList1 = skyline1.getSkylineList();
    final RLESparseResourceAllocation skylineList2 = skyline2.getSkylineList();
    for (int i = (int) skylineList1.getEarliestStartTime();
         i < skylineList1.getLatestNonNullTime(); i++) {
      Assertions.assertEquals(skylineList1.getCapacityAtTime(i).getMemorySize(),
          skylineList2.getCapacityAtTime(i).getMemorySize());
      Assertions.assertEquals(skylineList1.getCapacityAtTime(i).getVirtualCores(),
          skylineList2.getCapacityAtTime(i).getVirtualCores());
    }
  }

  private ResourceSkyline getSkyline1() {
    final TreeMap<Long, Resource> resourceOverTime = new TreeMap<>();
    ReservationInterval riAdd;
    final RLESparseResourceAllocation skylineList =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    riAdd = new ReservationInterval(0, 10);
    Resource resource =
        Resource.newInstance(containerMemAlloc, containerCPUAlloc);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(10, 15);
    resource = Resource
        .newInstance(containerMemAlloc * 1074, containerCPUAlloc * 1074);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(15, 20);
    resource = Resource
        .newInstance(containerMemAlloc * 2538, containerCPUAlloc * 2538);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(20, 25);
    resource = Resource
        .newInstance(containerMemAlloc * 2468, containerCPUAlloc * 2468);
    skylineList.addInterval(riAdd, resource);
    final ResourceSkyline resourceSkyline1 =
        new ResourceSkyline("tpch_q12_0", 0, 0, 25, containerSpec, skylineList);

    return resourceSkyline1;
  }

  private ResourceSkyline getSkyline2() {
    final TreeMap<Long, Resource> resourceOverTime = new TreeMap<>();
    ReservationInterval riAdd;
    final RLESparseResourceAllocation skylineList =
        new RLESparseResourceAllocation(resourceOverTime,
            new DefaultResourceCalculator());
    riAdd = new ReservationInterval(0, 10);
    Resource resource =
        Resource.newInstance(containerMemAlloc, containerCPUAlloc);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(10, 15);
    resource =
        Resource.newInstance(containerMemAlloc * 794, containerCPUAlloc * 794);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(15, 20);
    resource = Resource
        .newInstance(containerMemAlloc * 2517, containerCPUAlloc * 2517);
    skylineList.addInterval(riAdd, resource);
    riAdd = new ReservationInterval(20, 25);
    resource = Resource
        .newInstance(containerMemAlloc * 2484, containerCPUAlloc * 2484);
    skylineList.addInterval(riAdd, resource);
    final ResourceSkyline resourceSkyline2 =
        new ResourceSkyline("tpch_q12_1", 0, 0, 25, containerSpec, skylineList);

    return resourceSkyline2;
  }

  private void checkResult(final String jobId,
      final Map<RecurrenceId, List<ResourceSkyline>> jobHistory) {
    switch (jobId) {
    case "tpch_q12_0": {
      final RecurrenceId recurrenceId =
          new RecurrenceId("tpch_q12", "tpch_q12_0");
      Assertions.assertEquals(1, jobHistory.get(recurrenceId).size());
      ResourceSkyline skylineReceive = jobHistory.get(recurrenceId).get(0);
      compareResourceSkyline(skylineReceive, getSkyline1());
      break;
    }
    case "tpch_q12_1": {
      final RecurrenceId recurrenceId =
          new RecurrenceId("tpch_q12", "tpch_q12_1");
      Assertions.assertEquals(1, jobHistory.get(recurrenceId).size());
      ResourceSkyline skylineReceive = jobHistory.get(recurrenceId).get(0);
      compareResourceSkyline(skylineReceive, getSkyline2());
      break;
    }
    default:
      break;
    }
  }

  private void compareRLESparseResourceAllocation(
      final RLESparseResourceAllocation rle1,
      final RLESparseResourceAllocation rle2) {
    for (int i = (int) rle1.getEarliestStartTime();
         i < rle1.getLatestNonNullTime(); i++) {
      Assertions.assertEquals(rle1.getCapacityAtTime(i), rle2.getCapacityAtTime(i));
    }
  }

  @Test
  public void testGetPrediction() {
    // first, parse the log
    final String logFile = "resourceEstimatorService.txt";
    WebTarget webResource = target();
    webResource.path(parseLogCommand).request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.entity(logFile, MediaType.TEXT_PLAIN_TYPE));
    webResource = target().path(getHistorySkylineCommand);
    String response = webResource.request().get(String.class);
    Map<RecurrenceId, List<ResourceSkyline>> jobHistory =
        gson.fromJson(response,
            new TypeToken<Map<RecurrenceId, List<ResourceSkyline>>>() {
            }.getType());
    checkResult("tpch_q12_0", jobHistory);
    checkResult("tpch_q12_1", jobHistory);
    // then, try to get estimated resource allocation from skyline store
    webResource = target().path(getEstimatedSkylineCommand);
    response = webResource.request().get(String.class);
    Assertions.assertEquals("null", response);
    // then, we call estimator module to make the prediction
    webResource = target().path(makeEstimationCommand);
    response = webResource.request().get(String.class);
    RLESparseResourceAllocation skylineList =
        gson.fromJson(response, new TypeToken<RLESparseResourceAllocation>() {
        }.getType());
    Assertions.assertEquals(1,
        skylineList.getCapacityAtTime(0).getMemorySize() / containerMemAlloc);
    Assertions.assertEquals(1058,
        skylineList.getCapacityAtTime(10).getMemorySize() / containerMemAlloc);
    Assertions.assertEquals(2538,
        skylineList.getCapacityAtTime(15).getMemorySize() / containerMemAlloc);
    Assertions.assertEquals(2484,
        skylineList.getCapacityAtTime(20).getMemorySize() / containerMemAlloc);
    // then, we get estimated resource allocation for tpch_q12
    webResource = target().path(getEstimatedSkylineCommand);
    response =  webResource.request().get(String.class);
    final RLESparseResourceAllocation skylineList2 =
        gson.fromJson(response, new TypeToken<RLESparseResourceAllocation>() {
        }.getType());
    compareRLESparseResourceAllocation(skylineList, skylineList2);
    // then, we call estimator module again to directly get estimated resource
    // allocation from skyline store
    webResource = target().path(makeEstimationCommand);
    response = webResource.request().get(String.class);
    final RLESparseResourceAllocation skylineList3 =
        gson.fromJson(response, new TypeToken<RLESparseResourceAllocation>() {
        }.getType());
    compareRLESparseResourceAllocation(skylineList, skylineList3);
    // finally, test delete
    webResource = target().path(deleteHistoryCommand);
    webResource.request().delete();
    webResource =  target().path(getHistorySkylineCommand);
    response = webResource.request().get(String.class);
    jobHistory = gson.fromJson(response,
        new TypeToken<Map<RecurrenceId, List<ResourceSkyline>>>() {
        }.getType());
    // jobHistory should only have info for tpch_q12_0
    Assertions.assertEquals(1, jobHistory.size());
    final String pipelineId =
        ((RecurrenceId) jobHistory.keySet().toArray()[0]).getRunId();
    Assertions.assertEquals("tpch_q12_0", pipelineId);
  }
}
