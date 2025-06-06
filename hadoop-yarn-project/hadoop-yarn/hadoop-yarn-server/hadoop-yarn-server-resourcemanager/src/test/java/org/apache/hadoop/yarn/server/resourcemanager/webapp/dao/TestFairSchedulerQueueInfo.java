/*
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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFairSchedulerQueueInfo {

  @Test
  public void testEmptyChildQueues() {
    FairSchedulerConfiguration fsConf = new FairSchedulerConfiguration();
    RMContext rmContext = mock(RMContext.class);
    PlacementManager placementManager = new PlacementManager();
    SystemClock clock = SystemClock.getInstance();
    FairScheduler scheduler = mock(FairScheduler.class);
    when(scheduler.getConf()).thenReturn(fsConf);
    when(scheduler.getConfig()).thenReturn(fsConf);
    when(scheduler.getRMContext()).thenReturn(rmContext);
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    when(scheduler.getClusterResource()).thenReturn(
        Resource.newInstance(1, 1));
    when(scheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    when(scheduler.getClock()).thenReturn(clock);
    AllocationConfiguration allocConf = new AllocationConfiguration(scheduler);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    QueueManager queueManager = new QueueManager(scheduler);
    queueManager.initialize();

    FSQueue testQueue = queueManager.getLeafQueue("test", true);
    FairSchedulerQueueInfo queueInfo =
        new FairSchedulerQueueInfo(testQueue, scheduler);
    Collection<FairSchedulerQueueInfo> childQueues =
        queueInfo.getChildQueues();
    assertNotNull(childQueues);
    assertEquals(0, childQueues.size(), "Child QueueInfo was not empty");
  }
}
