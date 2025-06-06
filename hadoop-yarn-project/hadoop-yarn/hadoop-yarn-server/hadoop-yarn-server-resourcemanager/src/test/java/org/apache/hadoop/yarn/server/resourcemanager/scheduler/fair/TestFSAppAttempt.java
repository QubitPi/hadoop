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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;

import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFSAppAttempt extends FairSchedulerTestBase {

  @BeforeEach
  public void setup() {
    Configuration conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @Test
  public void testDelayScheduling() {
    FSLeafQueue queue = mock(FSLeafQueue.class);
    Priority pri = mock(Priority.class);
    SchedulerRequestKey prio = TestUtils.toSchedulerKey(pri);
    when(pri.getPriority()).thenReturn(1);
    double nodeLocalityThreshold = .5;
    double rackLocalityThreshold = .6;

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // First five scheduling opportunities should remain node local
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    // After five it should switch to rack local
    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio);
    assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    // Now escalate again to rack-local, then to off-switch
    for (int i = 0; i < 5; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.NODE_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));

    for (int i = 0; i < 6; i++) {
      schedulerApp.addSchedulingOpportunity(prio);
      assertEquals(NodeType.RACK_LOCAL, schedulerApp.getAllowedLocalityLevel(
          prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
    }

    schedulerApp.addSchedulingOpportunity(prio);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, nodeLocalityThreshold, rackLocalityThreshold));
  }

  @Test
  public void testDelaySchedulingForContinuousScheduling()
          throws InterruptedException {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue("queue", true);
    Priority pri = mock(Priority.class);
    SchedulerRequestKey prio = TestUtils.toSchedulerKey(pri);
    when(pri.getPriority()).thenReturn(1);

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    long nodeLocalityDelayMs = 5 * 1000L;    // 5 seconds
    long rackLocalityDelayMs = 6 * 1000L;    // 6 seconds

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
            new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue,
                    null, rmContext);

    // Default level should be node-local
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 4 seconds should remain node local
    clock.tickSec(4);
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // after 6 seconds should switch to rack local
    clock.tickSec(2);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // manually set back to node local
    schedulerApp.resetAllowedLocalityLevel(prio, NodeType.NODE_LOCAL);
    schedulerApp.resetSchedulingOpportunities(prio, clock.getTime());
    assertEquals(NodeType.NODE_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    // Now escalate again to rack-local, then to off-switch
    clock.tickSec(6);
    assertEquals(NodeType.RACK_LOCAL,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));

    clock.tickSec(7);
    assertEquals(NodeType.OFF_SWITCH,
            schedulerApp.getAllowedLocalityLevelByTime(prio,
                    nodeLocalityDelayMs, rackLocalityDelayMs, clock.getTime()));
  }

  @Test
  /**
   * Ensure that when negative paramaters are given (signaling delay scheduling
   * no tin use), the least restrictive locality level is returned.
   */
  public void testLocalityLevelWithoutDelays() {
    FSLeafQueue queue = mock(FSLeafQueue.class);
    Priority pri = mock(Priority.class);
    SchedulerRequestKey prio = TestUtils.toSchedulerKey(pri);
    when(pri.getPriority()).thenReturn(1);

    RMContext rmContext = resourceManager.getRMContext();
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue ,
            null, rmContext);
    assertEquals(NodeType.OFF_SWITCH, schedulerApp.getAllowedLocalityLevel(
        prio, 10, -1.0, -1.0));
  }

  @Test
  public void testHeadroom() {
    final FairScheduler mockScheduler = mock(FairScheduler.class);
    when(mockScheduler.getClock()).thenReturn(scheduler.getClock());

    final FSLeafQueue mockQueue = mock(FSLeafQueue.class);

    final Resource queueMaxResources = Resource.newInstance(5 * 1024, 3);
    final Resource queueFairShare = Resources.createResource(4096, 2);
    final Resource queueUsage = Resource.newInstance(2048, 2);

    final Resource queueStarvation =
        Resources.subtract(queueFairShare, queueUsage);
    final Resource queueMaxResourcesAvailable =
        Resources.subtract(queueMaxResources, queueUsage);

    final Resource clusterResource = Resources.createResource(8192, 8);
    final Resource clusterUsage = Resources.createResource(2048, 2);
    final Resource clusterAvailable =
        Resources.subtract(clusterResource, clusterUsage);

    final QueueMetrics fakeRootQueueMetrics = mock(QueueMetrics.class);

    when(mockQueue.getMaxShare()).thenReturn(queueMaxResources);
    when(mockQueue.getFairShare()).thenReturn(queueFairShare);
    when(mockQueue.getResourceUsage()).thenReturn(queueUsage);
    when(mockScheduler.getClusterResource()).thenReturn
        (clusterResource);
    when(fakeRootQueueMetrics.getAllocatedResources()).thenReturn
        (clusterUsage);
    when(mockScheduler.getRootQueueMetrics()).thenReturn
        (fakeRootQueueMetrics);
    when(mockScheduler.getConf()).thenReturn(
        mock(FairSchedulerConfiguration.class));

    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = resourceManager.getRMContext();
    FSAppAttempt schedulerApp =
        new FSAppAttempt(mockScheduler, applicationAttemptId, "user1", mockQueue ,
            null, rmContext);

    // Min of Memory and CPU across cluster and queue is used in
    // DominantResourceFairnessPolicy
    when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(DominantResourceFairnessPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemorySize(),
            clusterAvailable.getMemorySize(),
            queueMaxResourcesAvailable.getMemorySize()),
        min(queueStarvation.getVirtualCores(),
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    // Fair and Fifo ignore CPU of queue, so use cluster available CPU
    when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FairSharePolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemorySize(),
            clusterAvailable.getMemorySize(),
            queueMaxResourcesAvailable.getMemorySize()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );

    when(mockQueue.getPolicy()).thenReturn(SchedulingPolicy
        .getInstance(FifoPolicy.class));
    verifyHeadroom(schedulerApp,
        min(queueStarvation.getMemorySize(),
            clusterAvailable.getMemorySize(),
            queueMaxResourcesAvailable.getMemorySize()),
        Math.min(
            clusterAvailable.getVirtualCores(),
            queueMaxResourcesAvailable.getVirtualCores())
    );
  }

  @Test
  public void testHeadroomWithBlackListedNodes() {
    // Add two nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(2, scheduler.getNumClusterNodes(),
        "We should have two alive nodes.");
    Resource clusterResource = scheduler.getClusterResource();
    Resource clusterUsage = scheduler.getRootQueueMetrics()
        .getAllocatedResources();
    assertEquals(12 * 1024, clusterResource.getMemorySize());
    assertEquals(12, clusterResource.getVirtualCores());
    assertEquals(0, clusterUsage.getMemorySize());
    assertEquals(0, clusterUsage.getVirtualCores());
    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("default");
    scheduler.addApplication(id11.getApplicationId(),
            "default", "user1", false, placementCtx);
    scheduler.addApplicationAttempt(id11, false, false);
    assertNotNull(scheduler.getSchedulerApplications().get(id11.
            getApplicationId()));
    FSAppAttempt app = scheduler.getSchedulerApp(id11);
    assertNotNull(app);
    Resource queueUsage = app.getQueue().getResourceUsage();
    assertEquals(0, queueUsage.getMemorySize());
    assertEquals(0, queueUsage.getVirtualCores());
    SchedulerNode n1 = scheduler.getSchedulerNode(node1.getNodeID());
    SchedulerNode n2 = scheduler.getSchedulerNode(node2.getNodeID());
    assertNotNull(n1);
    assertNotNull(n2);
    List<String> blacklistAdditions = new ArrayList<String>(1);
    List<String> blacklistRemovals = new ArrayList<String>(1);
    blacklistAdditions.add(n1.getNodeName());
    FSAppAttempt spyApp = spy(app);
    doReturn(false)
        .when(spyApp).isWaitingForAMContainer();
    spyApp.updateBlacklist(blacklistAdditions, blacklistRemovals);
    spyApp.getQueue().setFairShare(clusterResource);
    assertTrue(spyApp.isPlaceBlacklisted(n1.getNodeName()));
    assertFalse(spyApp.isPlaceBlacklisted(n2.getNodeName()));
    assertEquals(n2.getUnallocatedResource(), spyApp.getHeadroom());

    blacklistAdditions.clear();
    blacklistAdditions.add(n2.getNodeName());
    blacklistRemovals.add(n1.getNodeName());
    spyApp.updateBlacklist(blacklistAdditions, blacklistRemovals);
    assertFalse(spyApp.isPlaceBlacklisted(n1.getNodeName()));
    assertTrue(spyApp.isPlaceBlacklisted(n2.getNodeName()));
    assertEquals(n1.getUnallocatedResource(), spyApp.getHeadroom());

    blacklistAdditions.clear();
    blacklistRemovals.clear();
    blacklistRemovals.add(n2.getNodeName());
    spyApp.updateBlacklist(blacklistAdditions, blacklistRemovals);
    assertFalse(spyApp.isPlaceBlacklisted(n1.getNodeName()));
    assertFalse(spyApp.isPlaceBlacklisted(n2.getNodeName()));
    assertEquals(clusterResource, spyApp.getHeadroom());
  }

  /**
   * Ensure that no pending ask request inside appSchedulingInfo
   * does not result in an error.
   */
  @Test
  public void testNoNextPendingAsk() {
    FSLeafQueue queue = mock(FSLeafQueue.class);
    ApplicationAttemptId applicationAttemptId = createAppAttemptId(1, 1);
    RMContext rmContext = mock(RMContext.class);
    ConcurrentMap<ApplicationId, RMApp> rmApps = new ConcurrentHashMap<>();
    RMApp rmApp = mock(RMApp.class);
    rmApps.put(applicationAttemptId.getApplicationId(), rmApp);
    ApplicationSubmissionContext appContext =
        mock(ApplicationSubmissionContext.class);
    when(appContext.getUnmanagedAM()).thenReturn(false);
    when(appContext.getLogAggregationContext())
        .thenReturn(mock(LogAggregationContext.class));
    when(rmApp.getApplicationSchedulingEnvs())
        .thenReturn(new HashMap<>());
    when(rmApp.getApplicationSubmissionContext())
      .thenReturn(appContext);
    when(rmContext.getRMApps()).thenReturn(rmApps);
    when(rmContext.getYarnConfiguration()).thenReturn(conf);
    FSAppAttempt schedulerApp =
        new FSAppAttempt(scheduler, applicationAttemptId, "user1", queue,
            null, rmContext);
    schedulerApp.setAmRunning(false);
    FSSchedulerNode schedulerNode = mock(FSSchedulerNode.class);

    Resource resource = schedulerApp.assignContainer(schedulerNode);

    assertEquals(Resources.none(), resource);
  }

  private static long min(long value1, long value2, long value3) {
    return Math.min(Math.min(value1, value2), value3);
  }

  protected void verifyHeadroom(FSAppAttempt schedulerApp,
                                long expectedMemory, long expectedCPU) {
    Resource headroom = schedulerApp.getHeadroom();
    assertEquals(expectedMemory, headroom.getMemorySize());
    assertEquals(expectedCPU, headroom.getVirtualCores());
  }
}
