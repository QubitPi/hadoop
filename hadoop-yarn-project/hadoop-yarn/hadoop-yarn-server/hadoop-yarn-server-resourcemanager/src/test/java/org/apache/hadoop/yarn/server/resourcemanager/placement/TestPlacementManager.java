/**
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
 */

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.setupQueueConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestPlacementManager {

  public static final String USER = "user_";
  public static final String APP_NAME = "DistributedShell";
  public static final String APP_ID1 = "1";
  public static final String USER1 = USER + APP_ID1;
  public static final String APP_ID2 = "2";
  public static final String USER2 = USER + APP_ID2;
  public static final String PARENT_QUEUE = "c";

  private MockRM mockRM = null;
  private CapacitySchedulerConfiguration conf;

  private String getQueueMapping(String parentQueue, String leafQueue) {
    return parentQueue + DOT + leafQueue;
  }

  @BeforeEach
  public void setup() {
    conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
  }

  @Test
  public void testPlaceApplicationWithPlacementRuleChain() throws Exception {
    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    cs.start();

    PlacementManager pm = cs.getRMContext()
        .getQueuePlacementManager();

    List<PlacementRule> queuePlacementRules = new ArrayList<>();
    QueueMapping userQueueMapping = QueueMappingBuilder.create()
                                          .type(MappingType.USER)
                                          .source(USER1)
                                          .queue(
                                              getQueueMapping(PARENT_QUEUE,
                                                  USER1))
                                          .build();

    cs.getConfiguration().setQueueMappings(
        Lists.newArrayList(userQueueMapping));
    CSMappingPlacementRule ugRule = new CSMappingPlacementRule();
    ugRule.initialize(cs);
    queuePlacementRules.add(ugRule);

    pm.updateRules(queuePlacementRules);

    ApplicationSubmissionContext asc = Records.newRecord(
        ApplicationSubmissionContext.class);
    asc.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    asc.setApplicationName(APP_NAME);

    assertNull(pm.placeApplication(asc, USER2), "Placement should be null");
    QueueMapping queueMappingEntity = QueueMapping.QueueMappingBuilder.create()
      .type(MappingType.APPLICATION)
      .source(APP_NAME)
      .queue(USER1)
      .parentQueue(PARENT_QUEUE)
      .build();

    cs.getConfiguration().setAppNameMappings(
        Lists.newArrayList(queueMappingEntity));
    CSMappingPlacementRule anRule = new CSMappingPlacementRule();
    anRule.initialize(cs);
    queuePlacementRules.add(anRule);
    pm.updateRules(queuePlacementRules);
    ApplicationPlacementContext pc = pm.placeApplication(asc, USER2);
    assertNotNull(pc);
  }

  @Test
  public void testPlacementRuleUpdationOrder() throws Exception {
    List<QueueMapping> queueMappings = new ArrayList<>();
    QueueMapping userQueueMapping = QueueMappingBuilder.create()
        .type(MappingType.USER).source(USER1)
        .queue(getQueueMapping(PARENT_QUEUE, USER1)).build();

    CSMappingPlacementRule ugRule = new CSMappingPlacementRule();

    conf.set(YarnConfiguration.QUEUE_PLACEMENT_RULES, ugRule.getName());
    queueMappings.add(userQueueMapping);
    conf.setQueueMappings(queueMappings);

    mockRM = new MockRM(conf);
    CapacityScheduler cs = (CapacityScheduler) mockRM.getResourceScheduler();
    mockRM.start();
    PlacementManager pm = cs.getRMContext().getQueuePlacementManager();

    // As we are setting placement rule, It shouldn't update default
    // placement rule ie user-group. Number of placement rules should be 1.
    assertEquals(1, pm.getPlacementRules().size());
    // Verifying if placement rule set is same as the one we configured
    assertEquals(ugRule.getName(),
        pm.getPlacementRules().get(0).getName());
  }

  @AfterEach
  public void tearDown() {
    if (null != mockRM) {
      mockRM.stop();
    }
  }

}