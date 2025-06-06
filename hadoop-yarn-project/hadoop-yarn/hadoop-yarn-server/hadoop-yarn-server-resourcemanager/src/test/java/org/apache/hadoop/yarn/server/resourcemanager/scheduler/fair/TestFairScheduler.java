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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueuePlacementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueuePlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.UserSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestFairScheduler extends FairSchedulerTestBase {
  private final int GB = 1024;
  private final static String ALLOC_FILE =
      new File(TEST_DIR, "test-queues").getAbsolutePath();

  @BeforeEach
  public void setUp() throws IOException {
    DefaultMetricsSystem.setMiniClusterMode(true);
    scheduler = new FairScheduler();
    conf = createConfiguration();
    resourceManager = new MockRM(conf);

    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();

    scheduler.setRMContext(resourceManager.getRMContext());
  }

  @AfterEach
  public void tearDown() {
    if (scheduler != null) {
      scheduler.stop();
      scheduler = null;
    }
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
    YarnAuthorizationProvider.destroy();
  }


  @Test
  @Timeout(value = 30)
  public void testConfValidation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.serviceInit(conf);
      fail("Exception is expected because the min memory allocation is" +
        " larger than the max memory allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue(e.getMessage().startsWith(
          "Invalid resource scheduler memory"),
          "The thrown exception is not the expected one.");
    }

    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 2);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 1);
    try {
      scheduler.serviceInit(conf);
      fail("Exception is expected because the min vcores allocation is" +
        " larger than the max vcores allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue(e.getMessage().startsWith(
          "Invalid resource scheduler vcores"),
          "The thrown exception is not the expected one.");
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  @Timeout(value = 2)
  public void testLoadConfigurationOnInitialize() throws IOException {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setInt(FairSchedulerConfiguration.MAX_ASSIGN, 3);
    conf.setBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT, true);
    conf.setDouble(FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE, .5);
    conf.setDouble(FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK, .7);
    conf.setBoolean(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
            true);
    conf.setInt(FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_SLEEP_MS,
            10);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_RACK_MS,
            5000);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_NODE_MS,
            5000);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setInt(FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 
      128);
    ResourceUtils.resetResourceTypes(conf);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    assertEquals(true, scheduler.assignMultiple);
    assertEquals(3, scheduler.maxAssign);
    assertEquals(true, scheduler.sizeBasedWeight);
    assertEquals(.5, scheduler.nodeLocalityThreshold, .01);
    assertEquals(.7, scheduler.rackLocalityThreshold, .01);
    assertTrue(scheduler.continuousSchedulingEnabled,
        "The continuous scheduling should be enabled");
    assertEquals(10, scheduler.continuousSchedulingSleepMs);
    assertEquals(5000, scheduler.nodeLocalityDelayMs);
    assertEquals(5000, scheduler.rackLocalityDelayMs);
    assertEquals(1024, scheduler.getMaximumResourceCapability().getMemorySize());
    assertEquals(512, scheduler.getMinimumResourceCapability().getMemorySize());
    assertEquals(128, scheduler.getIncrementResourceCapability().getMemorySize());
  }
  
  @Test  
  public void testNonMinZeroResourcesSettings() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    ResourceUtils.resetResourceTypes(conf);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    assertEquals(256, scheduler.getMinimumResourceCapability().getMemorySize());
    assertEquals(1, scheduler.getMinimumResourceCapability().getVirtualCores());
    assertEquals(512, scheduler.getIncrementResourceCapability().getMemorySize());
    assertEquals(2, scheduler.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test  
  public void testMinZeroResourcesSettings() throws IOException {  
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 0);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB, 512);
    conf.setInt(
      FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES, 2);
    ResourceUtils.resetResourceTypes(conf);
    scheduler.init(conf);
    scheduler.reinitialize(conf, null);
    assertEquals(0, scheduler.getMinimumResourceCapability().getMemorySize());
    assertEquals(0, scheduler.getMinimumResourceCapability().getVirtualCores());
    assertEquals(512, scheduler.getIncrementResourceCapability().getMemorySize());
    assertEquals(2, scheduler.getIncrementResourceCapability().getVirtualCores());
  }  
  
  @Test
  public void testAggregateCapacityTracking() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(1024, scheduler.getClusterResource().getMemorySize());

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterResource().getMemorySize());

    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterResource().getMemorySize());
  }

  @Test
  public void testSimpleFairShareCalculation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(10 * 1024), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "queue1", "user1");
    createSchedulingRequest(10 * 1024, "queue2", "user1");
    createSchedulingRequest(10 * 1024, "root.default", "user1");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());
    
    // Divided three ways - between the two queues and the default queue
    for (FSLeafQueue p : queues) {
      assertEquals(3414, p.getFairShare().getMemorySize());
      assertEquals(3414, p.getMetrics().getFairShareMB());
      assertEquals(3414, p.getSteadyFairShare().getMemorySize());
      assertEquals(3414, p.getMetrics().getSteadyFairShareMB());
    }
  }

  @Test
  public void testQueueMaximumCapacityAllocations() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    int tooHighQueueAllocation = RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE +1;

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .maxContainerAllocation("512 mb 1 vcores").build())
        .addQueue(new AllocationFileQueue.Builder("queueB").build())
        .addQueue(new AllocationFileQueue.Builder("queueC")
            .maxContainerAllocation("2048 mb 3 vcores")
            .subQueue(new AllocationFileQueue.Builder("queueD").build())
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueE")
            .maxContainerAllocation(tooHighQueueAllocation + " mb 1 vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);

    assertEquals(1, scheduler.getMaximumResourceCapability("root.queueA")
        .getVirtualCores());
    assertEquals(512,
        scheduler.getMaximumResourceCapability("root.queueA").getMemorySize());

    assertEquals(DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        scheduler.getMaximumResourceCapability("root.queueB")
            .getVirtualCores());
    assertEquals(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE,
        scheduler.getMaximumResourceCapability("root.queueB").getMemorySize());

    assertEquals(3, scheduler.getMaximumResourceCapability("root.queueC")
        .getVirtualCores());
    assertEquals(2048,
        scheduler.getMaximumResourceCapability("root.queueC").getMemorySize());

    assertEquals(3, scheduler
        .getMaximumResourceCapability("root.queueC.queueD").getVirtualCores());
    assertEquals(2048, scheduler
        .getMaximumResourceCapability("root.queueC.queueD").getMemorySize());

    assertEquals(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, scheduler
        .getMaximumResourceCapability("root.queueE").getMemorySize());
  }

  @Test
  public void testNormalizationUsingQueueMaximumAllocation()
      throws IOException {

    int queueMaxAllocation = 4096;
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .maxContainerAllocation(queueMaxAllocation + " mb 1 vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB").build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    allocateAppAttempt("root.queueA", 1, queueMaxAllocation + 1024);
    allocateAppAttempt("root.queueB", 2,
        RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE + 1024);

    scheduler.update();
    FSQueue queueToCheckA = scheduler.getQueueManager().getQueue("root.queueA");
    FSQueue queueToCheckB = scheduler.getQueueManager().getQueue("root.queueB");

    assertEquals(queueMaxAllocation, queueToCheckA.getDemand().getMemorySize());
    assertEquals(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE,
        queueToCheckB.getDemand().getMemorySize());
  }

  private void allocateAppAttempt(String queueName, int id, int memorySize) {
    ApplicationAttemptId id11 = createAppAttemptId(id, id);
    createMockRMApp(id11);
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queueName);

    scheduler.addApplication(id11.getApplicationId(), queueName, "user1",
        false, placementCtx);
    scheduler.addApplicationAttempt(id11, false, false);
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 =
        createResourceRequest(memorySize, ResourceRequest.ANY, 1, 1, true);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(), null,
        null, NULL_UPDATE_REQUESTS);
  }

  /**
   * Test fair shares when max resources are set but are too high to impact
   * the shares.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testFairShareWithHighMaxResources() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB maxResources,
    // the sum of queueA and queueB maxResources is more than
    // Integer.MAX_VALUE.

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .maxResources("1073741824 mb 1000 vcores")
            .weight(.25f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .maxResources("1073741824 mb 1000 vcores")
            .weight(.75f)
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 1 * 1024.
    createSchedulingRequest(1 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.25, so its fair share should be 2 * 1024.
    assertEquals(2 * 1024, queue.getFairShare().getMemorySize(),
        "Queue A did not get its expected fair share");
    // queueB's weight is 0.75, so its fair share should be 6 * 1024.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(6 * 1024, queue.getFairShare().getMemorySize(),
        "Queue B did not get its expected fair share");
  }

  /**
   * Test fair shares when max resources are set and are low enough to impact
   * the shares.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testFairShareWithLowMaxResources() throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .maxResources("1024 mb 1 vcores")
            .weight(.75f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .maxResources("3072 mb 3 vcores")
            .weight(.25f)
            .build())
        .writeToFile(ALLOC_FILE);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, 1, "queueA", "user1", 2);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, 1, "queueB", "user1", 4);

    scheduler.update();

    FSLeafQueue queue =
        scheduler.getQueueManager().getLeafQueue("queueA", false);
    // queueA's weight is 0.5, so its fair share should be 6GB, but it's
    // capped at 1GB.
    assertEquals(1 * 1024, queue.getFairShare().getMemorySize(),
        "Queue A did not get its expected fair share");
    // queueB's weight is 0.5, so its fair share should be 2GB, but the
    // other queue is capped at 1GB, so queueB's share is 7GB,
    // capped at 3GB.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(3 * 1024, queue.getFairShare().getMemorySize(),
        "Queue B did not get its expected fair share");

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);
    scheduler.handle(updateEvent);

    // App 1 should be running with 1 container
    assertEquals(1, scheduler.getSchedulerApp(attId1).getLiveContainers().size(),
        "App 1 is not running with the correct number of containers");
    // App 2 should be running with 3 containers
    assertEquals(3, scheduler.getSchedulerApp(attId2).getLiveContainers().size(),
        "App 2 is not running with the correct number of containers");
  }

  /**
   * Test the child max resource settings.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testChildMaxResources() throws IOException {
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .parent(true)
            .maxChildResources("2048mb,2vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, 1, "queueA.queueB", "user1", 8);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, 1, "queueA.queueC", "user1", 8);

    scheduler.update();

    NodeUpdateSchedulerEvent nodeEvent = new NodeUpdateSchedulerEvent(node1);

    // Send 4 node heartbeats, this should be enough to allocate 4 containers
    // As we have 2 queues with capacity: 2GB,2cores, we could only have
    // 4 containers at most
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    drainEventsOnRM();

    // Apps should be running with 2 containers
    assertEquals(2, scheduler.getSchedulerApp(attId1).getLiveContainers().size(),
        "App 1 is not running with the correct number of containers");
    assertEquals(2, scheduler.getSchedulerApp(attId2).getLiveContainers().size(),
        "App 2 is not running with the correct number of containers");

    //ensure that a 5th node heartbeat does not allocate more containers
    scheduler.handle(nodeEvent);
    drainEventsOnRM();

    // Apps should be running with 2 containers
    assertEquals(2, scheduler.getSchedulerApp(attId1).getLiveContainers().size(),
        "App 1 is not running with the correct number of containers");
    assertEquals(2, scheduler.getSchedulerApp(attId2).getLiveContainers().size(),
        "App 2 is not running with the correct number of containers");

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .parent(true)
            .maxChildResources("3072mb,3vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.reinitialize(conf, resourceManager.getRMContext());
    scheduler.update();

    // Send 2 node heartbeats, this should be enough to allocate 2
    // more containers.
    // As we have 2 queues with capacity: 3GB,3cores, we could only have
    // 6 containers at most
    scheduler.handle(nodeEvent);
    scheduler.handle(nodeEvent);
    drainEventsOnRM();

    // Apps should be running with 3 containers now
    assertEquals(3, scheduler.getSchedulerApp(attId1).getLiveContainers().size(),
        "App 1 is not running with the correct number of containers");
    assertEquals(3, scheduler.getSchedulerApp(attId2).getLiveContainers().size(),
        "App 2 is not running with the correct number of containers");

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .parent(true)
            .maxChildResources("1024mb,1vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    //ensure that a 7th node heartbeat does not allocate more containers
    scheduler.handle(nodeEvent);
    drainEventsOnRM();
    assertEquals(6, scheduler.getRootQueueMetrics().getAllocatedContainers());

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(nodeEvent);
    drainEventsOnRM();

    // Apps still should be running with 3 containers because we don't preempt
    assertEquals(3, scheduler.getSchedulerApp(attId1).getLiveContainers().size(),
        "App 1 is not running with the correct number of containers");
    assertEquals(3, scheduler.getSchedulerApp(attId2).getLiveContainers().size(),
        "App 2 is not running with the correct number of containers");
  }

  private void drainEventsOnRM() {
    if (resourceManager instanceof MockRM) {
      ((MockRM) resourceManager).drainEvents();
    }
  }

  @Test
  public void testFairShareWithZeroWeight() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight zero.

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .weight(0.0f).build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .weight(0.0f).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 2 * 1024.
    createSchedulingRequest(2 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.0, so its fair share should be 0.
    assertEquals(0, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.0, so its fair share should be 0.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(0, queue.getFairShare().getMemorySize());
  }

  /**
   * Test if we compute the maximum AM resource correctly.
   *
   * @throws IOException if scheduler reinitialization fails
   */
  @Test
  public void testComputeMaxAMResource() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueFSZeroWithMax")
            .weight(0)
            .maxAMShare(0.5)
            .maxResources("4096 mb 4 vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueFSZeroWithAVL")
            .weight(0.0f)
            .maxAMShare(0.5)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueFSNonZero")
            .weight(1)
            .maxAMShare(0.5)
            .build())
        .drfDefaultQueueSchedulingPolicy()
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    long memCapacity = 20 * GB;
    int cpuCapacity = 20;
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(memCapacity,
            cpuCapacity), 0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    Resource amResource = Resource.newInstance(1 * GB, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();

    // queueFSZeroWithMax
    FSLeafQueue queueFSZeroWithMax = scheduler.getQueueManager().
        getLeafQueue("queueFSZeroWithMax", true);
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queueFSZeroWithMax", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSZeroWithMax's weight is 0.0, so its fair share should be 0, we use
    // the min(maxShare, available resource) to compute maxAMShare, in this
    // case, we use maxShare, since it is smaller than available resource.
    assertEquals(0, queueFSZeroWithMax.getFairShare().getMemorySize(),
        "QueueFSZeroWithMax's fair share should be zero");
    Resource expectedAMResource = Resources.multiplyAndRoundUp(
        queueFSZeroWithMax.getMaxShare(), queueFSZeroWithMax.getMaxAMShare());
    assertEquals(expectedAMResource.getMemorySize(),
        queueFSZeroWithMax.getMetrics().getMaxAMShareMB(),
        "QueueFSZeroWithMax's maximum AM resource should be "
        + "maxShare * maxAMShare");
    assertEquals(expectedAMResource.getVirtualCores(),
        queueFSZeroWithMax.getMetrics().getMaxAMShareVCores(),
        "QueueFSZeroWithMax's maximum AM resource should be "
        + "maxShare * maxAMShare");
    assertEquals(amResource.getMemorySize(),
        queueFSZeroWithMax.getMetrics().getAMResourceUsageMB(),
        "QueueFSZeroWithMax's AM resource usage should be the same to "
        + "AM resource request");

    // queueFSZeroWithAVL
    amResource = Resources.createResource(1 * GB, 1);
    FSLeafQueue queueFSZeroWithAVL = scheduler.getQueueManager().
        getLeafQueue("queueFSZeroWithAVL", true);
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queueFSZeroWithAVL", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId2);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSZeroWithAVL's weight is 0.0, so its fair share is 0, and we use
    // the min(maxShare, available resource) to compute maxAMShare, in this
    // case, we use available resource since it is smaller than the
    // default maxShare.
    expectedAMResource = Resources.multiplyAndRoundUp(
        Resources.createResource(memCapacity - amResource.getMemorySize(),
            cpuCapacity - amResource.getVirtualCores()),
        queueFSZeroWithAVL.getMaxAMShare());
    assertEquals(0, queueFSZeroWithAVL.getFairShare().getMemorySize(),
        "QueueFSZeroWithAVL's fair share should be zero");
    assertEquals(expectedAMResource.getMemorySize(),
        queueFSZeroWithAVL.getMetrics().getMaxAMShareMB(),
        "QueueFSZeroWithAVL's maximum AM resource should be "
        + " available resource * maxAMShare");
    assertEquals(expectedAMResource.getVirtualCores(),
        queueFSZeroWithAVL.getMetrics().getMaxAMShareVCores(),
        "QueueFSZeroWithAVL's maximum AM resource should be "
        + " available resource * maxAMShare");
    assertEquals(amResource.getMemorySize(),
        queueFSZeroWithAVL.getMetrics().getAMResourceUsageMB(),
        "QueueFSZeroWithMax's AM resource usage should be the same to "
        + "AM resource request");

    // queueFSNonZero
    amResource = Resources.createResource(1 * GB, 1);
    FSLeafQueue queueFSNonZero = scheduler.getQueueManager().
        getLeafQueue("queueFSNonZero", true);
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queueFSNonZero", "user1",
        amResource);
    createSchedulingRequestExistingApplication(1 * GB, 1, amPriority, attId3);
    scheduler.update();
    scheduler.handle(updateEvent);

    // queueFSNonZero's weight is 1, so its fair share is not 0, and we use the
    // fair share to compute maxAMShare
    assertNotEquals(0, queueFSNonZero.getFairShare().getMemorySize(),
        "QueueFSNonZero's fair share shouldn't be zero");
    expectedAMResource = Resources.multiplyAndRoundUp(
        queueFSNonZero.getFairShare(), queueFSNonZero.getMaxAMShare());
    assertEquals(expectedAMResource.getMemorySize(),
        queueFSNonZero.getMetrics().getMaxAMShareMB(),
        "QueueFSNonZero's maximum AM resource should be "
        + " fair share * maxAMShare");
    assertEquals(expectedAMResource.getVirtualCores(),
        queueFSNonZero.getMetrics().getMaxAMShareVCores(),
        "QueueFSNonZero's maximum AM resource should be "
        + " fair share * maxAMShare");
    assertEquals(amResource.getMemorySize(),
        queueFSNonZero.getMetrics().getAMResourceUsageMB(),
        "QueueFSNonZero's AM resource usage should be the same to "
        + "AM resource request");
  }

  @Test
  public void testFairShareWithZeroWeightNoneZeroMinRes() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight zero.
    // set queueA and queueB minResources 1.
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .weight(0)
            .minResources("1 mb 1 vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .minResources("1 mb 1 vcores")
            .weight(0.0f)
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 2 * 1024.
    createSchedulingRequest(2 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.0 and minResources is 1,
    // so its fair share should be 1 (minShare).
    assertEquals(1, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.0 and minResources is 1,
    // so its fair share should be 1 (minShare).
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(1, queue.getFairShare().getMemorySize());
  }

  @Test
  public void testFairShareWithNoneZeroWeightNoneZeroMinRes()
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set queueA and queueB weight 0.5.
    // set queueA and queueB minResources 1024.
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .weight(0.5f)
            .minResources("1024 mb 1 vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .weight(0.5f)
            .minResources("1024 mb 1 vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 4 * 1024.
    createSchedulingRequest(4 * 1024, "queueA", "user1");
    // Queue B wants 4 * 1024
    createSchedulingRequest(4 * 1024, "queueB", "user1");

    scheduler.update();

    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(
        "queueA", false);
    // queueA's weight is 0.5 and minResources is 1024,
    // so its fair share should be 4096.
    assertEquals(4096, queue.getFairShare().getMemorySize());
    // queueB's weight is 0.5 and minResources is 1024,
    // so its fair share should be 4096.
    queue = scheduler.getQueueManager().getLeafQueue(
        "queueB", false);
    assertEquals(4096, queue.getFairShare().getMemorySize());
  }

  @Test
  public void testQueueInfo() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .weight(0.25f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .weight(0.75f)
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 1 * 1024.
    createSchedulingRequest(1 * 1024, "queueA", "user1");
    // Queue B wants 6 * 1024
    createSchedulingRequest(6 * 1024, "queueB", "user1");

    scheduler.update();

    // Capacity should be the same as weight of Queue,
    // because the sum of all active Queues' weight are 1.
    // Before NodeUpdate Event, CurrentCapacity should be 0
    QueueInfo queueInfo = scheduler.getQueueInfo("queueA", false, false);
    assertEquals(0.25f, queueInfo.getCapacity(), 0.0f);
    assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);
    // test queueMetrics
    assertEquals(0, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    assertEquals(0, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
    queueInfo = scheduler.getQueueInfo("queueB", false, false);
    assertEquals(0.75f, queueInfo.getCapacity(), 0.0f);
    assertEquals(0.0f, queueInfo.getCurrentCapacity(), 0.0f);

    // Each NodeUpdate Event will only assign one container.
    // To assign two containers, call handle NodeUpdate Event twice.
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);
    scheduler.handle(nodeEvent2);

    // After NodeUpdate Event, CurrentCapacity for queueA should be 1/2=0.5
    // and CurrentCapacity for queueB should be 6/6=1.
    queueInfo = scheduler.getQueueInfo("queueA", false, false);
    assertEquals(0.25f, queueInfo.getCapacity(), 0.0f);
    assertEquals(0.5f, queueInfo.getCurrentCapacity(), 0.0f);
    // test queueMetrics
    assertEquals(1, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    assertEquals(1024, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
    queueInfo = scheduler.getQueueInfo("queueB", false, false);
    assertEquals(0.75f, queueInfo.getCapacity(), 0.0f);
    assertEquals(1.0f, queueInfo.getCurrentCapacity(), 0.0f);
    // test queueMetrics
    assertEquals(1, queueInfo.getQueueStatistics()
        .getAllocatedContainers());
    assertEquals(6144, queueInfo.getQueueStatistics()
        .getAllocatedMemoryMB());
  }

  @Test
  public void testSimpleHierarchicalFairShareCalculation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    int capacity = 10 * 24;
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(capacity), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "parent.queue2", "user1");
    createSchedulingRequest(10 * 1024, "parent.queue3", "user1");
    createSchedulingRequest(10 * 1024, "root.default", "user1");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> queues = queueManager.getLeafQueues();
    assertEquals(3, queues.size());
    
    FSLeafQueue queue1 = queueManager.getLeafQueue("default", true);
    FSLeafQueue queue2 = queueManager.getLeafQueue("parent.queue2", true);
    FSLeafQueue queue3 = queueManager.getLeafQueue("parent.queue3", true);
    assertEquals(capacity / 2, queue1.getFairShare().getMemorySize());
    assertEquals(capacity / 2, queue1.getMetrics().getFairShareMB());
    assertEquals(capacity / 2, queue1.getSteadyFairShare().getMemorySize());
    assertEquals(capacity / 2, queue1.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue2.getFairShare().getMemorySize());
    assertEquals(capacity / 4, queue2.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue2.getSteadyFairShare().getMemorySize());
    assertEquals(capacity / 4, queue2.getMetrics().getSteadyFairShareMB());
    assertEquals(capacity / 4, queue3.getFairShare().getMemorySize());
    assertEquals(capacity / 4, queue3.getMetrics().getFairShareMB());
    assertEquals(capacity / 4, queue3.getSteadyFairShare().getMemorySize());
    assertEquals(capacity / 4, queue3.getMetrics().getSteadyFairShareMB());
  }

  @Test
  public void testHierarchicalQueuesSimilarParents() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueManager = scheduler.getQueueManager();
    FSLeafQueue leafQueue = queueManager.getLeafQueue("parent.child", true);
    assertEquals(1, queueManager.getLeafQueues().size());
    assertNotNull(leafQueue);
    assertEquals("root.parent.child", leafQueue.getName());

    FSLeafQueue leafQueue2 = queueManager.getLeafQueue("parent", true);
    assertNull(leafQueue2);
    assertEquals(1, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue3 = queueManager.getLeafQueue("parent.child.grandchild", true);
    assertNull(leafQueue3);
    assertEquals(1, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue4 = queueManager.getLeafQueue("parent.sister", true);
    assertNotNull(leafQueue4);
    assertEquals("root.parent.sister", leafQueue4.getName());
    assertEquals(2, queueManager.getLeafQueues().size());
  }

  @Test
  public void testSchedulerRootQueueMetrics() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure reserved memory gets updated correctly
    assertEquals(1024, scheduler.rootMetrics.getReservedMB());
    
    // Now another node checks in with capacity
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);


    // The old reservation should still be there...
    assertEquals(1024, scheduler.rootMetrics.getReservedMB());

    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.rootMetrics.getReservedMB());
  }

  @Test
  @Timeout(value = 5)
  public void testSimpleContainerAllocation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Add another node
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(512, 2), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    createSchedulingRequest(512, 2, "queue1", "user1", 2);

    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Asked for less than increment allocation.
    assertEquals(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);

    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getMemorySize());
    assertEquals(2, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getVirtualCores());

    // verify metrics
    QueueMetrics queue1Metrics = scheduler.getQueueManager().getQueue("queue1")
        .getMetrics();
    assertEquals(1024, queue1Metrics.getAllocatedMB());
    assertEquals(2, queue1Metrics.getAllocatedVirtualCores());
    assertEquals(1024, scheduler.getRootQueueMetrics().getAllocatedMB());
    assertEquals(2, scheduler.getRootQueueMetrics().getAllocatedVirtualCores());
    assertEquals(512, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
  }

  @Test
  @Timeout(value = 5)
  public void testSimpleContainerReservation() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());

    // Now another node checks in with capacity
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to queue 2
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());

    // The old reservation should still be there...
    assertEquals(1024, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.getSchedulerApp(attId).getCurrentReservation().getMemorySize());

  }

  @Test
  @Timeout(value = 5)
  public void testOffSwitchAppReservationThreshold() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add three node
    RMNode node1 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Ensure capacity on all nodes are allocated
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // Verify capacity allocation
    assertEquals(6144, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Create new app with a resource request that can be satisfied by any
    // node but would be
    ApplicationAttemptId attId = createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));

    assertEquals(1,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // No new reservations should happen since it exceeds threshold
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));

    // Add 1 more node
    RMNode node4 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // New node satisfies resource request
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    scheduler.update();

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
  }

  @Test
  @Timeout(value = 5)
  public void testRackLocalAppReservationThreshold() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add four node
    RMNode node1 =
            MockNodes
                    .newNodeInfo(1, Resources.createResource(3072), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // These 3 on different rack
    RMNode node2 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);

    RMNode node4 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent4 = new NodeAddedSchedulerEvent(node4);
    scheduler.handle(nodeEvent4);

    // Ensure capacity on all nodes are allocated
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));

    // Verify capacity allocation
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Create new app with a resource request that can be satisfied by any
    // node but would be
    ApplicationAttemptId attemptId =
            createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(attemptId);

    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("queue1");
    scheduler.addApplication(attemptId.getApplicationId(), "queue1", "user1",
            false, placementCtx);
    scheduler.addApplicationAttempt(attemptId, false, false);
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(2048, node2.getRackName(), 1, 1, false));

    scheduler.allocate(attemptId, asks, null, new ArrayList<ContainerId>(), null,
            null, NULL_UPDATE_REQUESTS);

    ApplicationAttemptId attId = createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));

    assertEquals(1,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));

    // No new reservations should happen since it exceeds threshold
    assertEquals(2,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));

    // Add 1 more node
    RMNode node5 =
            MockNodes
                    .newNodeInfo(2, Resources.createResource(3072), 1, "127.0.0.4");
    NodeAddedSchedulerEvent nodeEvent5 = new NodeAddedSchedulerEvent(node5);
    scheduler.handle(nodeEvent5);

    // New node satisfies resource request
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    assertEquals(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE,
        scheduler.getQueueManager().getQueue("queue1").getResourceUsage()
            .getMemorySize());

    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    scheduler.handle(new NodeUpdateSchedulerEvent(node3));
    scheduler.handle(new NodeUpdateSchedulerEvent(node4));
    scheduler.update();

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
  }

  @Test
  @Timeout(value = 5)
  public void testReservationThresholdWithAssignMultiple() throws Exception {
    // set reservable-nodes to 0 which make reservation exceed
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0f);
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two node
    RMNode node1 =
        MockNodes
                .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes
                .newNodeInfo(2, Resources.createResource(4096, 4), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    //create one request and assign containers
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue1", "user1", 10);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    // Verify capacity allocation
    assertEquals(8192, scheduler.getQueueManager().getQueue("queue1").
            getResourceUsage().getMemorySize());

    // Verify number of reservations have decremented
    assertEquals(0,
            scheduler.getSchedulerApp(attId).getNumReservations(null, true));
  }

  @Test
  @Timeout(value = 500)
  public void testContainerReservationAttemptExceedingQueueMax()
      throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(new AllocationFileQueue.Builder("queue1")
            .maxResources("2048mb,5vcores").build())
            .subQueue(new AllocationFileQueue.Builder("queue2")
                .maxResources("2048mb,10vcores").build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 5), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of the queue
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Ensure the reservation does not get created as allocated memory of
    // queue1 exceeds max
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());
  }

  /**
   * The test verifies that zero-FairShare queues (because of zero/tiny
   * weight) can get resources for the AM.
   */
  @Test
  public void testRequestAMResourceInZeroFairShareQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .weight(0)
            .maxAMShare(0.5)
            .maxResources("4096mb,10vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queue2")
            .weight(2.0f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queue3")
            .weight(0.000001f)
            .build())
        .drfDefaultQueueSchedulingPolicy()
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
            MockNodes.newNodeInfo(1, Resources.createResource(8192, 20),
                    0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    //create request for non-zero weight queue
    createSchedulingRequest(1024, "root.queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // A managed AM which need 3G memory will not get resource,
    // since it request more than the maxAMShare (4G * 0.5 = 2G).
    Resource amResource = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "root.queue1", "user1", amResource);
    createSchedulingRequestExistingApplication(3 * 1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app1.getLiveContainers().size(),
        "Application 1 should not be running");

    // A managed AM which need 2G memory will get resource,
    // since it request no more than the maxAMShare (4G * 0.5 = 2G).
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "root.queue1", "user1", amResource);
    createSchedulingRequestExistingApplication(2 * 1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1, app2.getLiveContainers().size(),
        "Application 2 should be running");

    // A managed AM which need 1G memory will get resource, even thought its
    // fair share is 0 because its weight is tiny(0.000001).
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "root.queue3", "user1", amResource);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1, app3.getLiveContainers().size(),
        "Application 3 should be running");
  }

  @Test
  @Timeout(value = 500)
  public void testContainerReservationNotExceedingQueueMax() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(new AllocationFileQueue.Builder("queue1")
                .maxResources("3072mb,10vcores").build())
            .subQueue(new AllocationFileQueue.Builder("queue2")
                .maxResources("2048mb,10vcores").build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 5), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of the queue
    createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // Now queue 2 requests likewise
    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
      getResourceUsage().getMemorySize());
    
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 1 is waiting with a reservation
    assertEquals(1024, scheduler.getSchedulerApp(attId1)
        .getCurrentReservation().getMemorySize());

    // Exercise checks that reservation fits
    scheduler.handle(updateEvent);

    // Ensure the reservation still exists as allocated memory of queue1 doesn't
    // exceed max
    assertEquals(1024, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());

    // Now reduce max Resources of queue1 down to 2048
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(new AllocationFileQueue.Builder("queue1")
                .maxResources("2048mb,10vcores").build())
            .subQueue(new AllocationFileQueue.Builder("queue2")
                .maxResources("2048mb,10vcores").build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    createSchedulingRequest(1024, "queue2", "user2", 1);
    scheduler.handle(updateEvent);

    // Make sure allocated memory of queue1 doesn't exceed its maximum
    assertEquals(2048, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());
    //the reservation of queue1 should be reclaim
    assertEquals(0, scheduler.getSchedulerApp(attId1).
        getCurrentReservation().getMemorySize());
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
  }

  @Test
  public void testReservationThresholdGatesReservations() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        .writeToFile(ALLOC_FILE);

    // Set threshold to 2 * 1024 ==> 2048 MB & 2 * 1 ==> 2 vcores (test will
    // use vcores)
    conf.setFloat(FairSchedulerConfiguration.
            RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
        2f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(4096, 4, "queue1", "user1", 1, 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(4096, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // Now queue 2 requests below threshold
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 has no reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(0,
        scheduler.getSchedulerApp(attId).getReservedContainers().size());

    // Now queue requests CPU above threshold
    createSchedulingRequestExistingApplication(1024, 3, 1, attId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemorySize());
    assertEquals(3, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());

    // Now another node checks in with capacity
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024, 4), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to queue 2
    assertEquals(3, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getVirtualCores());

    // The old reservation should still be there...
    assertEquals(3, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.getSchedulerApp(attId).getCurrentReservation()
        .getVirtualCores());
  }

  @Test
  public void testEmptyQueueName() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // no queue by default
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());

    // Submit app with empty queue
    // Submit fails before we reach the placement check.
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(appAttemptId.getApplicationId(), "",
            "user1");
    scheduler.handle(appAddedEvent);

    // submission rejected
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());
  }

  @Test
  public void testQueueuNameWithPeriods() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // no queue by default
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());

    // Submit app with queue name (.A)
    // Submit fails before we reach the placement check.
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    AppAddedSchedulerEvent appAddedEvent1 =
        new AppAddedSchedulerEvent(appAttemptId1.getApplicationId(), ".A",
            "user1");
    scheduler.handle(appAddedEvent1);
    // submission rejected
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId1));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());

    // Submit app with queue name (A.)
    // Submit fails before we reach the placement check.
    ApplicationAttemptId appAttemptId2 = createAppAttemptId(2, 1);
    AppAddedSchedulerEvent appAddedEvent2 =
        new AppAddedSchedulerEvent(appAttemptId2.getApplicationId(), "A.",
            "user1");
    scheduler.handle(appAddedEvent2);
    // submission rejected
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId2));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());

    // submit app with queue name (A.B)
    // Submit does not fail we must have a placement context.
    ApplicationAttemptId appAttemptId3 = createAppAttemptId(3, 1);
    AppAddedSchedulerEvent appAddedEvent3 =
        new AppAddedSchedulerEvent(appAttemptId3.getApplicationId(), "A.B",
            "user1", new ApplicationPlacementContext("A.B"));
    scheduler.handle(appAddedEvent3);
    // submission accepted
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApp(appAttemptId3));
    assertEquals(0, resourceManager.getRMContext().getRMApps().size());
  }

  @Test
  public void testFairShareWithMinAlloc() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .minResources("1024mb,0vcores").build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .minResources("2048mb,0vcores")
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one big node (only care about aggregate capacity)
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(2 * 1024, "queueA", "user1");
    createSchedulingRequest(2 * 1024, "queueB", "user1");

    scheduler.update();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(2, queues.size());

    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(1024, p.getFairShare().getMemorySize());
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(2048, p.getFairShare().getMemorySize());
      }
    }
  }

  @Test
  public void testFairShareAndWeightsInNestedUserQueueRule() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("parentq")
            .parent(true)
            .minResources("1024mb,0vcores")
            .build())
        .queuePlacementPolicy(new AllocationFileQueuePlacementPolicy()
            .addRule(new AllocationFileQueuePlacementRule(
                AllocationFileQueuePlacementRule.RuleName.NESTED)
                .addNestedRule(
                    new AllocationFileQueuePlacementRule(
                        AllocationFileQueuePlacementRule.RuleName.SPECIFIED)
                        .create(false)))
            .addRule(new AllocationFileQueuePlacementRule(
                AllocationFileQueuePlacementRule.RuleName.DEFAULT)))
        .writeToFile(ALLOC_FILE);

    RMApp rmApp1 = new MockRMApp(0, 0, RMAppState.NEW);
    RMApp rmApp2 = new MockRMApp(1, 1, RMAppState.NEW);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    int capacity = 16 * 1024;
    // create node with 16 G
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(capacity),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // user1,user2 submit their apps to parentq and create user queues
    createSchedulingRequest(10 * 1024, "root.parentq", "user1");
    createSchedulingRequest(10 * 1024, "root.parentq", "user2");
    // user3 submits app in default queue
    createSchedulingRequest(10 * 1024, "root.default", "user3");

    scheduler.update();
    scheduler.getQueueManager().getRootQueue()
        .setSteadyFairShare(scheduler.getClusterResource());
    scheduler.getQueueManager().getRootQueue().recomputeSteadyShares();

    Collection<FSLeafQueue> leafQueues = scheduler.getQueueManager()
        .getLeafQueues();

    for (FSLeafQueue leaf : leafQueues) {
      if (leaf.getName().equals("root.parentq.user1")
          || leaf.getName().equals("root.parentq.user2")) {
        // assert that the fair share is 1/4th node1's capacity
        assertEquals(capacity / 4, leaf.getFairShare().getMemorySize());
        // assert that the steady fair share is 1/4th node1's capacity
        assertEquals(capacity / 4, leaf.getSteadyFairShare().getMemorySize());
        // assert weights are equal for both the user queues
        assertEquals(1.0, leaf.getWeight(), 0);
      }
    }
  }

  @Test
  public void testSteadyFairShareWithReloadAndNodeAddRemove() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .fairDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("root")
            .schedulingPolicy("drf")
            .subQueue(new AllocationFileQueue.Builder("default")
                .weight(1).build())
            .subQueue(new AllocationFileQueue.Builder("child1")
                .weight(1).build())
            .subQueue(new AllocationFileQueue.Builder("child2")
                .weight(1).build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // The steady fair share for all queues should be 0
    QueueManager queueManager = scheduler.getQueueManager();
    assertEquals(0, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());

    // Add one node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(6144), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(6144, scheduler.getClusterResource().getMemorySize());

    // The steady fair shares for all queues should be updated
    assertEquals(2048, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());

    // Reload the allocation configuration file
    AllocationFileWriter.create()
        .fairDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("root")
            .schedulingPolicy("drf")
            .subQueue(new AllocationFileQueue.Builder("default")
                .weight(1).build())
            .subQueue(new AllocationFileQueue.Builder("child1")
                .weight(1).build())
            .subQueue(new AllocationFileQueue.Builder("child2")
                .weight(2).build())
            .subQueue(new AllocationFileQueue.Builder("child3")
                .weight(2).build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // The steady fair shares for all queues should be updated
    assertEquals(1024, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(2048, queueManager.getLeafQueue("child3", false)
        .getSteadyFairShare().getMemorySize());

    // Remove the node, steady fair shares should back to 0
    NodeRemovedSchedulerEvent nodeEvent2 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);
    assertEquals(0, scheduler.getClusterResource().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child1", false)
        .getSteadyFairShare().getMemorySize());
    assertEquals(0, queueManager.getLeafQueue("child2", false)
        .getSteadyFairShare().getMemorySize());
  }

  @Test
  public void testSteadyFairShareWithQueueCreatedRuntime() throws Exception {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .fairDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("root")
            .schedulingPolicy("drf")
            .subQueue(new AllocationFileQueue.Builder("default")
                .weight(1).build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add one node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(6144), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(6144, scheduler.getClusterResource().getMemorySize());
    assertEquals(6144, scheduler.getQueueManager().getRootQueue()
        .getSteadyFairShare().getMemorySize());
    assertEquals(6144, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemorySize());

    // Submit one application
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId1, "user1", "user1", null);
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("default", false).getSteadyFairShare().getMemorySize());
    assertEquals(3072, scheduler.getQueueManager()
        .getLeafQueue("user1", false).getSteadyFairShare().getMemorySize());
  }

  /**
   * Make allocation requests and ensure they are reflected in queue demand.
   */
  @Test
  public void testQueueDemandCalculation() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    int minReqSize =
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB;

    // First ask, queue1 requests 1 large (minReqSize * 2).
    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("root.queue1");
    scheduler.addApplication(id11.getApplicationId(),
        "root.queue1", "user1", false, placementCtx);
    scheduler.addApplicationAttempt(id11, false, false);
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 = createResourceRequest(minReqSize * 2,
        ResourceRequest.ANY, 1, 1, true);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    // Second ask, queue2 requests 1 large.
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    createMockRMApp(id21);
    placementCtx = new ApplicationPlacementContext("root.queue2");
    scheduler.addApplication(id21.getApplicationId(),
        "root.queue2", "user1", false, placementCtx);
    scheduler.addApplicationAttempt(id21, false, false);
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ResourceRequest request2 = createResourceRequest(2 * minReqSize,
        "foo", 1, 1, false);
    ResourceRequest request3 = createResourceRequest(2 * minReqSize,
        ResourceRequest.ANY, 1, 1, false);
    ask2.add(request2);
    ask2.add(request3);
    scheduler.allocate(id21, ask2, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    // Third ask, queue2 requests 2 small (minReqSize).
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    createMockRMApp(id22);
    scheduler.addApplication(id22.getApplicationId(),
        "root.queue2", "user1", false, placementCtx);
    scheduler.addApplicationAttempt(id22, false, false);
    List<ResourceRequest> ask3 = new ArrayList<ResourceRequest>();
    ResourceRequest request4 = createResourceRequest(minReqSize,
        "bar", 2, 2, true);
    ResourceRequest request5 = createResourceRequest(minReqSize,
        ResourceRequest.ANY, 2, 2, true);
    ask3.add(request4);
    ask3.add(request5);
    scheduler.allocate(id22, ask3, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);

    scheduler.update();

    assertEquals(2 * minReqSize, scheduler.getQueueManager().getQueue("root.queue1")
        .getDemand().getMemorySize());
    assertEquals(2 * minReqSize + 2 * minReqSize, scheduler
        .getQueueManager().getQueue("root.queue2").getDemand()
        .getMemorySize());
  }

  @Test
  public void testHierarchicalQueueAllocationFileParsing() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .minResources("2048mb,0vcores").build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .minResources("2048mb,0vcores")
            .subQueue(new AllocationFileQueue.Builder("queueC")
                .minResources("2048mb,0vcores").build())
            .subQueue(new AllocationFileQueue.Builder("queueD")
                .minResources("2048mb,0vcores").build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> leafQueues = queueManager.getLeafQueues();
    assertEquals(3, leafQueues.size());
    assertNotNull(queueManager.getLeafQueue("queueA", false));
    assertNotNull(queueManager.getLeafQueue("queueB.queueC", false));
    assertNotNull(queueManager.getLeafQueue("queueB.queueD", false));
    // Make sure querying for queues didn't create any new ones:
    assertEquals(3, leafQueues.size());
  }
  
  @Test
  public void testConfigureRootQueue() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .fairDefaultQueueSchedulingPolicy()
        .defaultFairSharePreemptionTimeout(300)
        .defaultMinSharePreemptionTimeout(200)
        .defaultFairSharePreemptionThreshold(.6)
        .addQueue(new AllocationFileQueue.Builder("root")
            .schedulingPolicy("drf")
            .fairSharePreemptionTimeout(100)
            .fairSharePreemptionThreshold(.5)
            .minSharePreemptionTimeout(120)
            .subQueue(new AllocationFileQueue.Builder("child1")
                .minResources("1024mb,1vcores").build())
            .subQueue(new AllocationFileQueue.Builder("child2")
                .minResources("1024mb,4vcores").build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueManager queueManager = scheduler.getQueueManager();
    
    FSQueue root = queueManager.getRootQueue();
    assertTrue(root.getPolicy() instanceof DominantResourceFairnessPolicy);
    
    assertNotNull(queueManager.getLeafQueue("child1", false));
    assertNotNull(queueManager.getLeafQueue("child2", false));

    assertEquals(100000, root.getFairSharePreemptionTimeout());
    assertEquals(120000, root.getMinSharePreemptionTimeout());
    assertEquals(0.5f, root.getFairSharePreemptionThreshold(), 0.01);
  }

  @Test
  @Timeout(value = 5)
  public void testMultipleContainersWaitingForReservation() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Request full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue2", "user2", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue3", "user3", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // One container should get reservation and the other should get nothing
    assertEquals(1024,
        scheduler.getSchedulerApp(attId1).getCurrentReservation().getMemorySize());
    assertEquals(0,
        scheduler.getSchedulerApp(attId2).getCurrentReservation().getMemorySize());
  }

  @Test
  @Timeout(value = 5)
  public void testUserMaxRunningApps() throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(1).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Request for app 1
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    // App 1 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
    
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // App 2 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    
    // Request another container for app 1
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // Request should be fulfilled
    assertEquals(2, scheduler.getSchedulerApp(attId1).getLiveContainers().size());
  }

  @Test
  @Timeout(value = 5)
  public void testIncreaseQueueMaxRunningAppsOnTheFly() throws Exception {
    AllocationFileWriter allocBefore = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(1)
                    .build())
            .build());

    AllocationFileWriter allocAfter = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(3)
                    .build())
            .build());

    testIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  @Test
  @Timeout(value = 5)
  public void testIncreaseUserMaxRunningAppsOnTheFly() throws Exception {
    AllocationFileWriter allocBefore = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(10)
                    .build())
            .build())
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(1).build());

    AllocationFileWriter allocAfter = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(10)
                    .build())
            .build())
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(3).build());

    testIncreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  private void testIncreaseQueueSettingOnTheFlyInternal(
      AllocationFileWriter allocBefore,
      AllocationFileWriter allocAfter) throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    allocBefore.writeToFile(ALLOC_FILE);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Request for app 1
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // App 1 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId1).getLiveContainers().size());

    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    // App 3 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId3).getLiveContainers().size());
    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    allocAfter.writeToFile(ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 3 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 1
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId4).getLiveContainers().size());
  }

  @Test
  @Timeout(value = 5)
  public void testDecreaseQueueMaxRunningAppsOnTheFly() throws Exception {
    AllocationFileWriter allocBefore = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(3)
                    .build())
            .build());

    AllocationFileWriter allocAfter = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(1)
                    .build())
            .build());

    testDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  @Test
  @Timeout(value = 5)
  public void testDecreaseUserMaxRunningAppsOnTheFly() throws Exception {
    AllocationFileWriter allocBefore = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(10)
                    .build())
            .build())
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(3).build());

    AllocationFileWriter allocAfter = AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(
                new AllocationFileQueue.Builder("queue1")
                    .maxRunningApps(10)
                    .build())
            .build())
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(1).build());

    testDecreaseQueueSettingOnTheFlyInternal(allocBefore, allocAfter);
  }

  private void testDecreaseQueueSettingOnTheFlyInternal(
      AllocationFileWriter allocBefore,
      AllocationFileWriter allocAfter) throws Exception {
    // Set max running apps
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    allocBefore.writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Request for app 1
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // App 1 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId1).getLiveContainers().size());

    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1",
        "user1", 1);

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());
    // App 3 should be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());
    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    allocAfter.writeToFile(ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 2 should still be running
    assertEquals(1, scheduler.getSchedulerApp(attId2).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 3 should still be running
    assertEquals(1, scheduler.getSchedulerApp(attId3).getLiveContainers().size());

    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 1
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 2
    appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId2, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should not be running
    assertEquals(0, scheduler.getSchedulerApp(attId4).getLiveContainers().size());

    // Now remove app 3
    appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        attId3, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // App 4 should be running now
    assertEquals(1, scheduler.getSchedulerApp(attId4).getLiveContainers().size());
  }

  /**
   * Reserve at a lower priority and verify the lower priority request gets
   * allocated
   */
  @Test
  @Timeout(value = 5)
  public void testReservationWithMultiplePriorities() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    // Create first app and take up half resources so the second app that asks
    // for the entire node won't have enough.
    FSAppAttempt app1 = scheduler.getSchedulerApp(
        createSchedulingRequest(1024, 1, "queue", "user", 1));
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size(), "Basic allocation failed");

    // Create another app and reserve at a lower priority first
    ApplicationAttemptId attId =
        createSchedulingRequest(2048, 2, "queue1", "user1", 1, 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1, app2.getReservedContainers().size(),
        "Reservation at lower priority failed");

    // Request container on the second app at a higher priority
    createSchedulingRequestExistingApplication(2048, 2, 1, attId);

    // Complete the first container so we can trigger allocation for app2
    ContainerId containerId =
        app1.getLiveContainers().iterator().next().getContainerId();
    scheduler.allocate(app1.getApplicationAttemptId(), new ArrayList<>(), null,
        Arrays.asList(containerId), null, null, NULL_UPDATE_REQUESTS);

    // Trigger allocation for app2
    scheduler.handle(updateEvent);

    // Reserved container (at lower priority) should be run
    Collection<RMContainer> liveContainers = app2.getLiveContainers();
    assertEquals(1, liveContainers.size(), "Allocation post completion failed");
    assertEquals(2, liveContainers.iterator().next().getContainer().
        getPriority().getPriority(),
        "High prio container allocated against low prio reservation");
  }
  
  @Test
  public void testAclSubmitApplication() throws Exception {
    // Set acl's
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .aclSubmitApps(" ")
            .aclAdministerApps(" ")
            .subQueue(new AllocationFileQueue.Builder("queue1")
                .aclSubmitApps("norealuserhasthisname")
                .aclAdministerApps("norealuserhasthisname")
                .build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname2", 1);

    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    assertNotNull(app1, "The application was not allowed");
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    assertNull(app2, "The application was allowed");
  }
  
  @Test
  @Timeout(value = 5)
  public void testMultipleNodesSingleRackRequest() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    RMNode node3 =
        MockNodes
            .newNodeInfo(2, Resources.createResource(1024), 3, "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    
    ApplicationAttemptId attemptId =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(attemptId);

    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("queue1");
    scheduler.addApplication(attemptId.getApplicationId(), "queue1", "user1",
        false, placementCtx);
    scheduler.addApplicationAttempt(attemptId, false, false);
    
    // 1 request with 2 nodes on the same rack. another request with 1 node on
    // a different rack
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(1024, node1.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node2.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getHostName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node1.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, node3.getRackName(), 1, 1, true));
    asks.add(createResourceRequest(1024, ResourceRequest.ANY, 1, 2, true));

    scheduler.allocate(attemptId, asks, null, new ArrayList<>(), null,
        null, NULL_UPDATE_REQUESTS);
    
    // node 1 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent1);
    // should assign node local
    assertEquals(1, scheduler.getSchedulerApp(attemptId).getLiveContainers()
        .size());

    // node 2 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);
    // should assign rack local
    assertEquals(2, scheduler.getSchedulerApp(attemptId).getLiveContainers()
        .size());
  }
  
  @Test
  @Timeout(value = 5)
  public void testFifoWithinQueue() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(3072, 3), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Even if submitted at exact same time, apps will be deterministically
    // ordered by name.
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1", true);
    queue1.setPolicy(new FifoPolicy());
    
    scheduler.update();

    // First two containers should go to app 1, third should go to app 2.
    // Because tests set assignmultiple to false, each heartbeat assigns a single
    // container.
    
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());
  }

  @Test
  @Timeout(value = 3)
  public void testFixedMaxAssign() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16384, 16), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(1024, "root.default", "user", 8);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // set maxAssign to 2: only 2 containers should be allocated
    scheduler.maxAssign = 2;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(2, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");

    // set maxAssign to -1: all remaining containers should be allocated
    scheduler.maxAssign = -1;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(8, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");
  }


  /**
   * Test to verify the behavior of dynamic-max-assign.
   * 1. Verify the value of maxassign doesn't affect number of containers
   * affected.
   * 2. Verify the node is fully allocated.
   */
  @Test
  @Timeout(value = 3)
  public void testDynamicMaxAssign() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(8192, 8), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(1024, 1, "root.default", "user", 12);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // Set maxassign to a value smaller than half the remaining resources
    scheduler.maxAssign = 2;
    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be floor(8/2) + 1 = 5
    assertEquals(5, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");

    // Set maxassign to a value larger than half the remaining resources
    scheduler.maxAssign = 4;
    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be floor(3/2) + 1 = 2
    assertEquals(7, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");

    scheduler.update();
    scheduler.handle(updateEvent);
    // New container allocations should be 1
    assertEquals(8, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");
  }

  @Test
  @Timeout(value = 3)
  public void testMaxAssignWithZeroMemoryContainers() throws Exception {
    conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
    conf.setBoolean(FairSchedulerConfiguration.DYNAMIC_MAX_ASSIGN, false);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
    
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16384, 16), 0,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId attId =
        createSchedulingRequest(0, 1, "root.default", "user", 8);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // set maxAssign to 2: only 2 containers should be allocated
    scheduler.maxAssign = 2;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(2, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");

    // set maxAssign to -1: all remaining containers should be allocated
    scheduler.maxAssign = -1;
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(8, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");
  }

  /**
   * Test to verify the behavior of
   * {@link FSQueue#assignContainer(FSSchedulerNode)})
   * 
   * Create two queues under root (fifoQueue and fairParent), and two queues
   * under fairParent (fairChild1 and fairChild2). Submit two apps to the
   * fifoQueue and one each to the fairChild* queues, all apps requiring 4
   * containers each of the total 16 container capacity
   * 
   * Assert the number of containers for each app after 4, 8, 12 and 16 updates.
   * 
   * @throws Exception
   */
  @Test
  @Timeout(value = 5)
  public void testAssignContainer() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    final String user = "user1";
    final String fifoQueue = "fifo";
    final String fairParent = "fairParent";
    final String fairChild1 = fairParent + ".fairChild1";
    final String fairChild2 = fairParent + ".fairChild2";

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 1, "127.0.0.1");
    RMNode node2 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(8192, 8), 2, "127.0.0.2");

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);

    scheduler.handle(nodeEvent1);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 =
        createSchedulingRequest(1024, fifoQueue, user, 4);
    ApplicationAttemptId attId2 =
        createSchedulingRequest(1024, fairChild1, user, 4);
    ApplicationAttemptId attId3 =
        createSchedulingRequest(1024, fairChild2, user, 4);
    ApplicationAttemptId attId4 =
        createSchedulingRequest(1024, fifoQueue, user, 4);

    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);

    scheduler.getQueueManager().getLeafQueue(fifoQueue, true)
        .setPolicy(SchedulingPolicy.parse("fifo"));
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);

    for (int i = 0; i < 8; i++) {
      scheduler.handle(updateEvent1);
      scheduler.handle(updateEvent2);
      if ((i + 1) % 2 == 0) {
        // 4 node updates: fifoQueue should have received 2, and fairChild*
        // should have received one each
        String ERR =
            "Wrong number of assigned containers after " + (i + 1) + " updates";
        if (i < 4) {
          // app1 req still not met
          assertEquals((i + 1), app1.getLiveContainers().size(), ERR);
          assertEquals(0, app4.getLiveContainers().size(), ERR);
        } else {
          // app1 req has been met, app4 should be served now
          assertEquals(4, app1.getLiveContainers().size(), ERR);
          assertEquals((i - 3), app4.getLiveContainers().size(), ERR);
        }
        assertEquals((i + 1) / 2, app2.getLiveContainers().size(), ERR);
        assertEquals((i + 1) / 2, app3.getLiveContainers().size(), ERR);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testNotAllowSubmitApplication() throws Exception {
    // Set acl's
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .aclSubmitApps(" ")
            .aclAdministerApps(" ")
            .subQueue(new AllocationFileQueue.Builder("queue1")
                .aclSubmitApps("userallow")
                .aclAdministerApps("userallow")
                .build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    int appId = this.APP_ID++;
    String user = "usernotallow";
    String queue = "queue1";
    ApplicationId applicationId = MockApps.newAppID(appId);
    String name = MockApps.newAppName();
    ApplicationMasterService masterService =
        new ApplicationMasterService(resourceManager.getRMContext(), scheduler);
    ApplicationSubmissionContext submissionContext = new ApplicationSubmissionContextPBImpl();
    ContainerLaunchContext clc =
        BuilderUtils.newContainerLaunchContext(null, null, null, null,
            null, null);
    submissionContext.setApplicationId(applicationId);
    submissionContext.setAMContainerSpec(clc);
    RMApp application =
        new RMAppImpl(applicationId, resourceManager.getRMContext(), conf,
            name, user, queue, submissionContext, scheduler, masterService,
          System.currentTimeMillis(), "YARN", null, null);
    resourceManager.getRMContext().getRMApps().
        putIfAbsent(applicationId, application);
    application.handle(new RMAppEvent(applicationId, RMAppEventType.START));

    final int MAX_TRIES=20;
    int numTries = 0;
    while (!application.getState().equals(RMAppState.SUBMITTED) &&
        numTries < MAX_TRIES) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {ex.printStackTrace();}
      numTries++;
    }
    assertEquals(RMAppState.SUBMITTED, application.getState(),
        "The application doesn't reach SUBMITTED.");

    ApplicationAttemptId attId =
        ApplicationAttemptId.newInstance(applicationId, this.ATTEMPT_ID++);
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext(queue);
    scheduler.addApplication(attId.getApplicationId(), queue, user, false,
        placementCtx);

    numTries = 0;
    while (application.getFinishTime() == 0 && numTries < MAX_TRIES) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
      numTries++;
    }
    assertEquals(FinalApplicationStatus.FAILED,
        application.getFinalApplicationStatus());
  }

  @Test
  public void testRemoveNodeUpdatesRootQueueMetrics() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
	assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 4), 1,
        "127.0.0.1");
    NodeAddedSchedulerEvent addEvent = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(addEvent);
    
    assertEquals(1024, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    scheduler.update(); // update shouldn't change things
    assertEquals(1024, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(4, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    
    NodeRemovedSchedulerEvent removeEvent = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeEvent);
    
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
    scheduler.update(); // update shouldn't change things
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableMB());
    assertEquals(0, scheduler.getRootQueueMetrics().getAvailableVirtualCores());
}

  @Test
  public void testStrictLocality() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    
    ResourceRequest nodeRequest = createResourceRequest(1024, node1.getHostName(), 1, 1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, node1.getRackName(), 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node1UpdateEvent = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, app.getReservedContainers().size());
    }
    // then node1 should get the container
    scheduler.handle(node1UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testCancelStrictLocality() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 2, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 0);
    
    ResourceRequest nodeRequest = createResourceRequest(1024, node1.getHostName(), 1, 1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, "rack1", 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, ResourceRequest.ANY,
        1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node2UpdateEvent = new NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
    }
    
    // relax locality
    List<ResourceRequest> update = Arrays.asList(
        createResourceRequest(1024, node1.getHostName(), 1, 0, true),
        createResourceRequest(1024, "rack1", 1, 0, true),
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, true));
    scheduler.allocate(attId1, update, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    
    // then node2 should get the container
    scheduler.handle(node2UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testAMStrictLocalityRack() throws IOException {
    testAMStrictLocality(false, false);
  }

  @Test
  public void testAMStrictLocalityNode() throws IOException {
    testAMStrictLocality(true, false);
  }

  @Test
  public void testAMStrictLocalityRackInvalid() throws IOException {
    testAMStrictLocality(false, true);
  }

  @Test
  public void testAMStrictLocalityNodeInvalid() throws IOException {
    testAMStrictLocality(true, true);
  }

  private void testAMStrictLocality(boolean node, boolean invalid)
      throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024), 1,
        "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(2, Resources.createResource(1024), 2,
        "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    List<ResourceRequest> reqs = new ArrayList<>();
    ResourceRequest nodeRequest = createResourceRequest(1024,
        node2.getHostName(), 1, 1, true);
    if (node && invalid) {
      nodeRequest.setResourceName("invalid");
    }
    ResourceRequest rackRequest = createResourceRequest(1024,
        node2.getRackName(), 1, 1, !node);
    if (!node && invalid) {
      rackRequest.setResourceName("invalid");
    }
    ResourceRequest anyRequest = createResourceRequest(1024,
        ResourceRequest.ANY, 1, 1, false);
    reqs.add(anyRequest);
    reqs.add(rackRequest);
    if (node) {
      reqs.add(nodeRequest);
    }

    ApplicationAttemptId attId1 =
        createSchedulingRequest("queue1", "user1", reqs);

    scheduler.update();

    NodeUpdateSchedulerEvent node2UpdateEvent =
        new NodeUpdateSchedulerEvent(node2);

    FSAppAttempt app = scheduler.getSchedulerApp(attId1);

    // node2 should get the container
    scheduler.handle(node2UpdateEvent);
    if (invalid) {
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, scheduler.getNode(node2.getNodeID()).getNumContainers());
      assertEquals(0, scheduler.getNode(node1.getNodeID()).getNumContainers());
    } else {
      assertEquals(1, app.getLiveContainers().size());
      assertEquals(1, scheduler.getNode(node2.getNodeID()).getNumContainers());
      assertEquals(0, scheduler.getNode(node1.getNodeID()).getNumContainers());
    }
  }

  /**
   * Strict locality requests shouldn't reserve resources on another node.
   */
  @Test
  public void testReservationsStrictLocality() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two nodes
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 1));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024, 1));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // Submit application without container requests
    ApplicationAttemptId attId =
        createSchedulingRequest(1024, "queue1", "user1", 0);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);

    // Request a container on node2
    ResourceRequest nodeRequest =
        createResourceRequest(1024, node2.getHostName(), 1, 1, true);
    ResourceRequest rackRequest =
        createResourceRequest(1024, "rack1", 1, 1, false);
    ResourceRequest anyRequest =
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId);
    createSchedulingRequestExistingApplication(rackRequest, attId);
    createSchedulingRequestExistingApplication(anyRequest, attId);
    scheduler.update();

    // Heartbeat from node1. App shouldn't get an allocation or reservation
    NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeUpdateEvent);
    assertEquals(0, app.getLiveContainers().size(),
        "App assigned a container on the wrong node");
    scheduler.handle(nodeUpdateEvent);
    assertEquals(0, app.getReservedContainers().size(),
        "App reserved a container on the wrong node");
  }
  
  @Test
  public void testNoMoreCpuOnNode() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 1),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    ApplicationAttemptId attId = createSchedulingRequest(1024, 1, "default",
        "user1", 2);
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  @Test
  public void testBasicDRFAssignment() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(8192, 5));
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(2048, 1, "queue1",
        "user1", 2);
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 2, "queue1",
        "user1", 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);

    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getContext());
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.update();

    // First both apps get a container
    // Then the first gets another container because its dominant share of
    // 2048/8192 is less than the other's of 2/5
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());

    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());

    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());
  }

  /**
   * Two apps on one queue, one app on another
   */
  @Test
  public void testBasicDRFWithQueues() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(8192, 7),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3072, 1, "queue1",
        "user1", 2);
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(2048, 2, "queue1",
        "user1", 2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    FSAppAttempt app3 = scheduler.getSchedulerApp(appAttId3);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getContext());
    scheduler.getQueueManager().getQueue("root").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(1, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(2, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    assertEquals(1, app2.getLiveContainers().size());
  }

  @Test
  public void testDRFHierarchicalQueues() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(12288, 12),
        1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttId1 = createSchedulingRequest(3074, 1, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app1 = scheduler.getSchedulerApp(appAttId1);
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 3, "queue1.subqueue1",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app2 = scheduler.getSchedulerApp(appAttId2);
    ApplicationAttemptId appAttId3 = createSchedulingRequest(2048, 2, "queue1.subqueue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app3 = scheduler.getSchedulerApp(appAttId3);
    ApplicationAttemptId appAttId4 = createSchedulingRequest(1024, 2, "queue2",
        "user1", 2);
    Thread.sleep(3); // so that start times will be different
    FSAppAttempt app4 = scheduler.getSchedulerApp(appAttId4);
    
    DominantResourceFairnessPolicy drfPolicy = new DominantResourceFairnessPolicy();
    drfPolicy.initialize(scheduler.getContext());
    scheduler.getQueueManager().getQueue("root").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1").setPolicy(drfPolicy);
    scheduler.getQueueManager().getQueue("queue1.subqueue1").setPolicy(drfPolicy);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(updateEvent);
    // app1 gets first container because it asked first
    assertEquals(1, app1.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 gets second container because it's on queue2
    assertEquals(1, app4.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 gets another container because queue2's dominant share of memory
    // is still less than queue1's of cpu
    assertEquals(2, app4.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app3 gets one because queue1 gets one and queue1.subqueue2 is behind
    // queue1.subqueue1
    assertEquals(1, app3.getLiveContainers().size());
    scheduler.handle(updateEvent);
    // app4 would get another one, but it doesn't have any requests
    // queue1.subqueue2 is still using less than queue1.subqueue1, so it
    // gets another
    assertEquals(2, app3.getLiveContainers().size());
    // queue1.subqueue1 is behind again, so it gets one, which it gives to app2
    scheduler.handle(updateEvent);
    assertEquals(1, app2.getLiveContainers().size());
    
    // at this point, we've used all our CPU up, so nobody else should get a container
    scheduler.handle(updateEvent);

    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());
    assertEquals(2, app3.getLiveContainers().size());
    assertEquals(2, app4.getLiveContainers().size());
  }

  @Test
  @Timeout(value = 30)
  public void testHostPortNodeName() throws Exception {
    conf.setBoolean(YarnConfiguration
        .RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf,
        resourceManager.getRMContext());
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024),
        1, "127.0.0.1", 1);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024),
        2, "127.0.0.1", 2);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", 
        "user1", 0);

    ResourceRequest nodeRequest = createResourceRequest(1024, 
        node1.getNodeID().getHost() + ":" + node1.getNodeID().getPort(), 1,
        1, true);
    ResourceRequest rackRequest = createResourceRequest(1024, 
        node1.getRackName(), 1, 1, false);
    ResourceRequest anyRequest = createResourceRequest(1024, 
        ResourceRequest.ANY, 1, 1, false);
    createSchedulingRequestExistingApplication(nodeRequest, attId1);
    createSchedulingRequestExistingApplication(rackRequest, attId1);
    createSchedulingRequestExistingApplication(anyRequest, attId1);

    scheduler.update();

    NodeUpdateSchedulerEvent node1UpdateEvent = new 
        NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent node2UpdateEvent = new 
        NodeUpdateSchedulerEvent(node2);

    // no matter how many heartbeats, node2 should never get a container  
    FSAppAttempt app = scheduler.getSchedulerApp(attId1);
    for (int i = 0; i < 10; i++) {
      scheduler.handle(node2UpdateEvent);
      assertEquals(0, app.getLiveContainers().size());
      assertEquals(0, app.getReservedContainers().size());
    }
    // then node1 should get the container  
    scheduler.handle(node1UpdateEvent);
    assertEquals(1, app.getLiveContainers().size());
  }

  private void verifyAppRunnable(ApplicationAttemptId attId, boolean runnable) {
    FSAppAttempt app = scheduler.getSchedulerApp(attId);
    FSLeafQueue queue = app.getQueue();
    assertEquals(runnable, queue.isRunnableApp(app));
    assertEquals(!runnable, queue.isNonRunnableApp(app));
  }
  
  private void verifyQueueNumRunnable(String queueName, int numRunnableInQueue,
      int numNonRunnableInQueue) {
    FSLeafQueue queue = scheduler.getQueueManager().getLeafQueue(queueName, false);
    assertEquals(numRunnableInQueue, queue.getNumRunnableApps());
    assertEquals(numNonRunnableInQueue, queue.getNumNonRunnableApps());
  }
  
  @Test
  public void testUserAndQueueMaxRunningApps() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .maxRunningApps(2).build())
        .userSettings(new UserSettings.Builder("user1")
            .maxRunningApps(1).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // exceeds no limits
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1", "user1");
    verifyAppRunnable(attId1, true);
    verifyQueueNumRunnable("queue1", 1, 0);
    // exceeds user limit
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue2", "user1");
    verifyAppRunnable(attId2, false);
    verifyQueueNumRunnable("queue2", 0, 1);
    // exceeds no limits
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1", 2, 0);
    // exceeds queue limit
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1", 2, 1);
    
    // Remove app 1 and both app 2 and app 4 should becomes runnable in its place
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue2", 1, 0);
    verifyAppRunnable(attId4, true);
    verifyQueueNumRunnable("queue1", 2, 0);
    
    // A new app to queue1 should not be runnable
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1", "user2");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1", 2, 1);
  }

  @Test
  public void testMultipleCompletedEvent() throws Exception {
    // Set up a fair scheduler
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .maxAMShare(0.2).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create a node
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(20480, 20),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    // Launch an app
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(
        attId1, "queue1", "user1",
        Resource.newInstance(1024, 1));
    createSchedulingRequestExistingApplication(
        1024, 1,
        RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority(), attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);

    RMContainer container = app1.getLiveContainersMap().
        values().iterator().next();
    scheduler.completedContainer(container, SchedulerUtils
        .createAbnormalContainerStatus(container.getContainerId(),
            SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
    scheduler.completedContainer(container, SchedulerUtils
        .createAbnormalContainerStatus(container.getContainerId(),
            SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.FINISHED);
    assertEquals(Resources.none(), app1.getResourceUsage());
  }

  @Test
  public void testQueueMaxAMShare() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .maxAMShare(0.2).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(20480, 20),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1", true);
    assertEquals(0, queue1.getFairShare().getMemorySize(),
        "Queue queue1's fair share should be 0");

    createSchedulingRequest(1 * 1024, "default", "user1");
    scheduler.update();
    scheduler.handle(updateEvent);

    Resource amResource1 = Resource.newInstance(1024, 1);
    Resource amResource2 = Resource.newInstance(2048, 2);
    Resource amResource3 = Resource.newInstance(1860, 2);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    // Exceeds no limits
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1024, app1.getAMResource().getMemorySize(),
        "Application1's AM requests 1024 MB memory");
    assertEquals(1, app1.getLiveContainers().size(),
        "Application1's AM should be running");
    assertEquals(1024, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 1024 MB memory");

    // Exceeds no limits
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1024, app2.getAMResource().getMemorySize(),
        "Application2's AM requests 1024 MB memory");
    assertEquals(1, app2.getLiveContainers().size(),
        "Application2's AM should be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Exceeds queue limit
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app3.getAMResource().getMemorySize(),
        "Application3's AM resource shouldn't be updated");
    assertEquals(0, app3.getLiveContainers().size(),
        "Application3's AM should not be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Still can run non-AM container
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size(),
        "Application1 should have two running containers");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Remove app1, app3's AM should become running
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1, RMAppAttemptState.FINISHED, false);
    scheduler.update();
    scheduler.handle(appRemovedEvent1);
    scheduler.handle(updateEvent);
    assertEquals(0, app1.getLiveContainers().size(),
        "Application1's AM should be finished");
    assertEquals(Resources.none(), app1.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(1, app3.getLiveContainers().size(),
        "Application3's AM should be running");
    assertEquals(1024, app3.getAMResource().getMemorySize(),
        "Application3's AM requests 1024 MB memory");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Exceeds queue limit
    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app4.getAMResource().getMemorySize(),
        "Application4's AM resource shouldn't be updated");
    assertEquals(0, app4.getLiveContainers().size(),
        "Application4's AM should not be running");
    assertEquals(Resources.none(), app4.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Exceeds queue limit
    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(2048, 2, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app5.getAMResource().getMemorySize(),
        "Application5's AM resource shouldn't be updated");
    assertEquals(0, app5.getLiveContainers().size(),
        "Application5's AM should not be running");
    assertEquals(Resources.none(), app5.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Remove un-running app doesn't affect others
    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4, RMAppAttemptState.KILLED, false);
    scheduler.handle(appRemovedEvent4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app5.getLiveContainers().size(),
        "Application5's AM should not be running");
    assertEquals(Resources.none(), app5.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Remove app2 and app3, app5's AM should become running
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2, RMAppAttemptState.FINISHED, false);
    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId3, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent2);
    scheduler.handle(appRemovedEvent3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app2.getLiveContainers().size(),
        "Application2's AM should be finished");
    assertEquals(Resources.none(), app2.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(0, app3.getLiveContainers().size(),
        "Application3's AM should be finished");
    assertEquals(Resources.none(), app3.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(1, app5.getLiveContainers().size(),
        "Application5's AM should be running");
    assertEquals(2048, app5.getAMResource().getMemorySize(),
        "Application5's AM requests 2048 MB memory");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // request non-AM container for app5
    createSchedulingRequestExistingApplication(1024, 1, attId5);
    assertEquals(1, app5.getLiveContainers().size(),
        "Application5's AM should have 1 container");
    // complete AM container before non-AM container is allocated.
    // spark application hit this situation.
    RMContainer amContainer5 = (RMContainer)app5.getLiveContainers().toArray()[0];
    ContainerExpiredSchedulerEvent containerExpired =
        new ContainerExpiredSchedulerEvent(amContainer5.getContainerId());
    scheduler.handle(containerExpired);
    assertEquals(0, app5.getLiveContainers().size(), "Application5's AM should have 0 container");
    assertEquals(Resources.none(), app5.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");
    scheduler.update();
    scheduler.handle(updateEvent);
    // non-AM container should be allocated
    // check non-AM container allocation is not rejected
    // due to queue MaxAMShare limitation.
    assertEquals(1, app5.getLiveContainers().size(),
        "Application5 should have 1 container");
    // check non-AM container allocation won't affect queue AmResourceUsage
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Check amResource normalization
    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(1860, 2, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app6.getLiveContainers().size(),
        "Application6's AM should not be running");
    assertEquals(Resources.none(), app6.getResourceUsage(),
        "Finished application usage should be none");
    assertEquals(0, app6.getAMResource().getMemorySize(),
        "Application6's AM resource shouldn't be updated");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    // Remove all apps
    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5, RMAppAttemptState.FINISHED, false);
    AppAttemptRemovedSchedulerEvent appRemovedEvent6 =
        new AppAttemptRemovedSchedulerEvent(attId6, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent5);
    scheduler.handle(appRemovedEvent6);
    scheduler.update();
    assertEquals(0, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 0");
  }

  @Test
  public void testQueueMaxAMShareDefault() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 6);

    AllocationFileWriter.create()
        .fairDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("queue1").build())
        .addQueue(new AllocationFileQueue.Builder("queue2")
            .maxAMShare(0.4f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queue3")
            .maxResources("10240 mb 4 vcores")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queue4").build())
        .addQueue(new AllocationFileQueue.Builder("queue5").build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(8192, 10),
            0, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);
    scheduler.update();

    FSLeafQueue queue1 =
        scheduler.getQueueManager().getLeafQueue("queue1", true);
    assertEquals(0, queue1.getFairShare().getMemorySize(),
        "Queue queue1's fair share should be 0");
    FSLeafQueue queue2 =
        scheduler.getQueueManager().getLeafQueue("queue2", true);
    assertEquals(0, queue2.getFairShare().getMemorySize(),
        "Queue queue2's fair share should be 0");
    FSLeafQueue queue3 =
        scheduler.getQueueManager().getLeafQueue("queue3", true);
    assertEquals(0, queue3.getFairShare().getMemorySize(),
        "Queue queue3's fair share should be 0");
    FSLeafQueue queue4 =
        scheduler.getQueueManager().getLeafQueue("queue4", true);
    assertEquals(0, queue4.getFairShare().getMemorySize(),
        "Queue queue4's fair share should be 0");
    FSLeafQueue queue5 =
        scheduler.getQueueManager().getLeafQueue("queue5", true);
    assertEquals(0, queue5.getFairShare().getMemorySize(),
        "Queue queue5's fair share should be 0");

    List<String> queues = Arrays.asList("root.queue3", "root.queue4",
        "root.queue5");
    for (String queue : queues) {
      createSchedulingRequest(1 * 1024, queue, "user1");
      scheduler.update();
      scheduler.handle(updateEvent);
    }

    Resource amResource1 = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();

    // The fair share is 2048 MB, and the default maxAMShare is 0.5f,
    // so the AM is accepted.
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1024, app1.getAMResource().getMemorySize(),
        "Application1's AM requests 1024 MB memory");
    assertEquals(1, app1.getLiveContainers().size(),
        "Application1's AM should be running");
    assertEquals(1024, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 1024 MB memory");

    // Now the fair share is 1639 MB, and the maxAMShare is 0.4f,
    // so the AM is not accepted.
    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue2", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app2.getAMResource().getMemorySize(),
        "Application2's AM resource shouldn't be updated");
    assertEquals(0, app2.getLiveContainers().size(),
        "Application2's AM should not be running");
    assertEquals(0, queue2.getAmResourceUsage().getMemorySize(),
        "Queue2's AM resource usage should be 0 MB memory");

    // Remove the app2
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2,
                RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent2);
    scheduler.update();

    // AM3 can pass the fair share checking, but it takes all available VCore,
    // So the AM3 is not accepted.
    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue3", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 6, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app3.getAMResource().getMemorySize(),
        "Application3's AM resource shouldn't be updated");
    assertEquals(0, app3.getLiveContainers().size(),
        "Application3's AM should not be running");
    assertEquals(0, queue3.getAmResourceUsage().getMemorySize(),
        "Queue3's AM resource usage should be 0 MB memory");

    // AM4 can pass the fair share checking and it doesn't takes all
    // available VCore, but it need 5 VCores which are more than
    // maxResources(4 VCores). So the AM4 is not accepted.
    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue3", "test1", amResource1);
    createSchedulingRequestExistingApplication(1024, 5, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app4.getAMResource().getMemorySize(),
        "Application4's AM resource shouldn't be updated");
    assertEquals(0, app4.getLiveContainers().size(),
        "Application4's AM should not be running");
    assertEquals(0, queue3.getAmResourceUsage().getMemorySize(),
        "Queue3's AM resource usage should be 0 MB memory");
  }

  /**
   * The test verifies container gets reserved when not over maxAMShare,
   * reserved container gets unreserved when over maxAMShare,
   * container doesn't get reserved when over maxAMShare,
   * reserved container is turned into an allocation and
   * superfluously reserved container gets unreserved.
   * 1. create three nodes: Node1 is 10G, Node2 is 10G and Node3 is 5G.
   * 2. APP1 allocated 1G on Node1 and APP2 allocated 1G on Node2.
   * 3. APP3 reserved 10G on Node1 and Node2.
   * 4. APP4 allocated 5G on Node3, which makes APP3 over maxAMShare.
   * 5. Remove APP1 to make Node1 have 10G available resource.
   * 6. APP3 unreserved its container on Node1 because it is over maxAMShare.
   * 7. APP5 allocated 1G on Node1 after APP3 unreserved its container.
   * 8. Remove APP3.
   * 9. APP6 failed to reserve a 10G container on Node1 due to AMShare limit.
   * 10. APP7 allocated 1G on Node1.
   * 11. Remove APP4 and APP5.
   * 12. APP6 reserved 10G on Node1 and Node2.
   * 13. APP8 failed to allocate a 1G container on Node1 and Node2 because
   *     APP6 reserved Node1 and Node2.
   * 14. Remove APP2.
   * 15. APP6 turned the 10G reservation into an allocation on node2.
   * 16. APP6 unreserved its container on node1, APP8 allocated 1G on Node1.
   */
  @Test
  public void testQueueMaxAMShareWithContainerReservation() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 1f);
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .maxAMShare(0.5).build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 10),
        1, "127.0.0.1");
    RMNode node2 = MockNodes.newNodeInfo(1,
        Resources.createResource(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 10),
        2, "127.0.0.2");
    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(5120, 5),
            3, "127.0.0.3");
    NodeAddedSchedulerEvent nodeE1 = new NodeAddedSchedulerEvent(node1);
    NodeUpdateSchedulerEvent updateE1 = new NodeUpdateSchedulerEvent(node1);
    NodeAddedSchedulerEvent nodeE2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateE2 = new NodeUpdateSchedulerEvent(node2);
    NodeAddedSchedulerEvent nodeE3 = new NodeAddedSchedulerEvent(node3);
    NodeUpdateSchedulerEvent updateE3 = new NodeUpdateSchedulerEvent(node3);
    scheduler.handle(nodeE1);
    scheduler.handle(nodeE2);
    scheduler.handle(nodeE3);
    scheduler.update();
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1",
        true);
    Resource amResource1 = Resource.newInstance(1024, 1);
    Resource amResource2 = Resource.newInstance(1024, 1);
    Resource amResource3 =
        Resource.newInstance(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 1);
    Resource amResource4 = Resource.newInstance(5120, 1);
    Resource amResource5 = Resource.newInstance(1024, 1);
    Resource amResource6 =
        Resource.newInstance(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 1);
    Resource amResource7 = Resource.newInstance(1024, 1);
    Resource amResource8 = Resource.newInstance(1024, 1);
    int amPriority = RMAppAttemptImpl.AM_CONTAINER_PRIORITY.getPriority();
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "queue1", "user1", amResource1);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId1);
    FSAppAttempt app1 = scheduler.getSchedulerApp(attId1);
    scheduler.update();
    // Allocate app1's AM container on node1.
    scheduler.handle(updateE1);
    assertEquals(1024, app1.getAMResource().getMemorySize(),
        "Application1's AM requests 1024 MB memory");
    assertEquals(1, app1.getLiveContainers().size(),
        "Application1's AM should be running");
    assertEquals(1024, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 1024 MB memory");

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "queue1", "user1", amResource2);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId2);
    FSAppAttempt app2 = scheduler.getSchedulerApp(attId2);
    scheduler.update();
    // Allocate app2's AM container on node2.
    scheduler.handle(updateE2);
    assertEquals(1024, app2.getAMResource().getMemorySize(),
        "Application2's AM requests 1024 MB memory");
    assertEquals(1, app2.getLiveContainers().size(),
        "Application2's AM should be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "queue1", "user1", amResource3);
    createSchedulingRequestExistingApplication(
        RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 1, amPriority, attId3);
    FSAppAttempt app3 = scheduler.getSchedulerApp(attId3);
    scheduler.update();
    // app3 reserves a container on node1 because node1's available resource
    // is less than app3's AM container resource.
    scheduler.handle(updateE1);
    // Similarly app3 reserves a container on node2.
    scheduler.handle(updateE2);
    assertEquals(0, app3.getAMResource().getMemorySize(),
        "Application3's AM resource shouldn't be updated");
    assertEquals(0, app3.getLiveContainers().size(),
        "Application3's AM should not be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    ApplicationAttemptId attId4 = createAppAttemptId(4, 1);
    createApplicationWithAMResource(attId4, "queue1", "user1", amResource4);
    createSchedulingRequestExistingApplication(5120, 1, amPriority, attId4);
    FSAppAttempt app4 = scheduler.getSchedulerApp(attId4);
    scheduler.update();
    // app4 can't allocate its AM container on node1 because
    // app3 already reserved its container on node1.
    scheduler.handle(updateE1);
    assertEquals(0, app4.getAMResource().getMemorySize(),
        "Application4's AM resource shouldn't be updated");
    assertEquals(0, app4.getLiveContainers().size(),
        "Application4's AM should not be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    scheduler.update();
    // Allocate app4's AM container on node3.
    scheduler.handle(updateE3);
    assertEquals(5120, app4.getAMResource().getMemorySize(),
        "Application4's AM requests 5120 MB memory");
    assertEquals(1, app4.getLiveContainers().size(),
        "Application4's AM should be running");
    assertEquals(7168, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 7168 MB memory");

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId1,
            RMAppAttemptState.FINISHED, false);
    // Release app1's AM container on node1.
    scheduler.handle(appRemovedEvent1);
    assertEquals(6144, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 6144 MB memory");

    ApplicationAttemptId attId5 = createAppAttemptId(5, 1);
    createApplicationWithAMResource(attId5, "queue1", "user1", amResource5);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId5);
    FSAppAttempt app5 = scheduler.getSchedulerApp(attId5);
    scheduler.update();
    // app5 can allocate its AM container on node1 after
    // app3 unreserve its container on node1 due to
    // exceeding queue MaxAMShare limit.
    scheduler.handle(updateE1);
    assertEquals(1024, app5.getAMResource().getMemorySize(),
        "Application5's AM requests 1024 MB memory");
    assertEquals(1, app5.getLiveContainers().size(),
        "Application5's AM should be running");
    assertEquals(7168, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 7168 MB memory");

    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId3,
            RMAppAttemptState.FINISHED, false);
    // Remove app3.
    scheduler.handle(appRemovedEvent3);
    assertEquals(7168, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 7168 MB memory");

    ApplicationAttemptId attId6 = createAppAttemptId(6, 1);
    createApplicationWithAMResource(attId6, "queue1", "user1", amResource6);
    createSchedulingRequestExistingApplication(
        RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE, 1, amPriority, attId6);
    FSAppAttempt app6 = scheduler.getSchedulerApp(attId6);
    scheduler.update();
    // app6 can't reserve a container on node1 because
    // it exceeds queue MaxAMShare limit.
    scheduler.handle(updateE1);
    assertEquals(0, app6.getAMResource().getMemorySize(),
        "Application6's AM resource shouldn't be updated");
    assertEquals(0, app6.getLiveContainers().size(),
        "Application6's AM should not be running");
    assertEquals(7168, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 7168 MB memory");

    ApplicationAttemptId attId7 = createAppAttemptId(7, 1);
    createApplicationWithAMResource(attId7, "queue1", "user1", amResource7);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId7);
    FSAppAttempt app7 = scheduler.getSchedulerApp(attId7);
    scheduler.update();
    // Allocate app7's AM container on node1 to prove
    // app6 didn't reserve a container on node1.
    scheduler.handle(updateE1);
    assertEquals(1024, app7.getAMResource().getMemorySize(),
        "Application7's AM requests 1024 MB memory");
    assertEquals(1, app7.getLiveContainers().size(),
        "Application7's AM should be running");
    assertEquals(8192, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 8192 MB memory");

    AppAttemptRemovedSchedulerEvent appRemovedEvent4 =
        new AppAttemptRemovedSchedulerEvent(attId4,
            RMAppAttemptState.FINISHED, false);
    // Release app4's AM container on node3.
    scheduler.handle(appRemovedEvent4);
    assertEquals(3072, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 3072 MB memory");

    AppAttemptRemovedSchedulerEvent appRemovedEvent5 =
        new AppAttemptRemovedSchedulerEvent(attId5,
            RMAppAttemptState.FINISHED, false);
    // Release app5's AM container on node1.
    scheduler.handle(appRemovedEvent5);
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    scheduler.update();
    // app6 reserves a container on node1 because node1's available resource
    // is less than app6's AM container resource and
    // app6 is not over AMShare limit.
    scheduler.handle(updateE1);
    // Similarly app6 reserves a container on node2.
    scheduler.handle(updateE2);

    ApplicationAttemptId attId8 = createAppAttemptId(8, 1);
    createApplicationWithAMResource(attId8, "queue1", "user1", amResource8);
    createSchedulingRequestExistingApplication(1024, 1, amPriority, attId8);
    FSAppAttempt app8 = scheduler.getSchedulerApp(attId8);
    scheduler.update();
    // app8 can't allocate a container on node1 because
    // app6 already reserved a container on node1.
    scheduler.handle(updateE1);
    assertEquals(0, app8.getAMResource().getMemorySize(),
        "Application8's AM resource shouldn't be updated");
    assertEquals(0, app8.getLiveContainers().size(),
        "Application8's AM should not be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");
    scheduler.update();
    // app8 can't allocate a container on node2 because
    // app6 already reserved a container on node2.
    scheduler.handle(updateE2);
    assertEquals(0, app8.getAMResource().getMemorySize(),
        "Application8's AM resource shouldn't be updated");
    assertEquals(0, app8.getLiveContainers().size(),
        "Application8's AM should not be running");
    assertEquals(2048, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 2048 MB memory");

    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId2,
            RMAppAttemptState.FINISHED, false);
    // Release app2's AM container on node2.
    scheduler.handle(appRemovedEvent2);
    assertEquals(1024, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 1024 MB memory");

    scheduler.update();
    // app6 turns the reservation into an allocation on node2.
    scheduler.handle(updateE2);
    assertEquals(RM_SCHEDULER_MAXIMUM_ALLOCATION_MB_VALUE,
        app6.getAMResource().getMemorySize(), "Application6's AM requests 10240 MB memory");
    assertEquals(1, app6.getLiveContainers().size(),
        "Application6's AM should be running");
    assertEquals(11264, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 11264 MB memory");

    scheduler.update();
    // app6 unreserve its container on node1 because
    // it already got a container on node2.
    // Now app8 can allocate its AM container on node1.
    scheduler.handle(updateE1);
    assertEquals(1024, app8.getAMResource().getMemorySize(),
        "Application8's AM requests 1024 MB memory");
    assertEquals(1, app8.getLiveContainers().size(),
        "Application8's AM should be running");
    assertEquals(12288, queue1.getAmResourceUsage().getMemorySize(),
        "Queue1's AM resource usage should be 12288 MB memory");
  }

  @Test
  public void testMaxRunningAppsHierarchicalQueues() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queue1")
            .maxRunningApps(3)
            .subQueue(new AllocationFileQueue.Builder("sub1").build())
            .subQueue(new AllocationFileQueue.Builder("sub2").build())
            .subQueue(new AllocationFileQueue.Builder("sub3")
                .maxRunningApps(1)
                .build())
            .build())
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // exceeds no limits
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1.sub1", "user1");
    verifyAppRunnable(attId1, true);
    verifyQueueNumRunnable("queue1.sub1", 1, 0);
    clock.tickSec(10);
    // exceeds no limits
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId2, true);
    verifyQueueNumRunnable("queue1.sub3", 1, 0);
    clock.tickSec(10);
    // exceeds no limits
    ApplicationAttemptId attId3 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId3, true);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    clock.tickSec(10);
    // exceeds queue1 limit
    ApplicationAttemptId attId4 = createSchedulingRequest(1024, "queue1.sub2", "user1");
    verifyAppRunnable(attId4, false);
    verifyQueueNumRunnable("queue1.sub2", 1, 1);
    clock.tickSec(10);
    // exceeds sub3 limit
    ApplicationAttemptId attId5 = createSchedulingRequest(1024, "queue1.sub3", "user1");
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 1, 1);
    clock.tickSec(10);

    // Even though the app was removed from sub3, the app from sub2 gets to go
    // because it came in first
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(attId2, RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
    verifyAppRunnable(attId4, true);
    verifyQueueNumRunnable("queue1.sub2", 2, 0);
    verifyAppRunnable(attId5, false);
    verifyQueueNumRunnable("queue1.sub3", 0, 1);

    // Now test removal of a non-runnable app
    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(attId5, RMAppAttemptState.KILLED, true);
    scheduler.handle(appRemovedEvent2);
    assertEquals(0, scheduler.maxRunningEnforcer.usersNonRunnableApps
        .get("user1").size());
    // verify app gone in queue accounting
    verifyQueueNumRunnable("queue1.sub3", 0, 0);
    // verify it doesn't become runnable when there would be space for it
    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(attId4, RMAppAttemptState.FINISHED, true);
    scheduler.handle(appRemovedEvent3);
    verifyQueueNumRunnable("queue1.sub2", 1, 0);
    verifyQueueNumRunnable("queue1.sub3", 0, 0);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSchedulingOnRemovedNode() throws Exception {
    // Disable continuous scheduling, will invoke continuous scheduling manually
    scheduler.init(conf);
    scheduler.start();
    assertTrue(!scheduler.isContinuousSchedulingEnabled(),
        "Continuous scheduling should be disabled.");

    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);

    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("root.queue1");
    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1",
        false, placementCtx);
    scheduler.addApplicationAttempt(id11, false, false);

    List<ResourceRequest> ask1 = new ArrayList<>();
    ResourceRequest request1 =
        createResourceRequest(1024, 8, ResourceRequest.ANY, 1, 1, true);

    ask1.add(request1);
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(), null,
        null, NULL_UPDATE_REQUESTS);

    String hostName = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(8 * 1024, 8), 1, hostName);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    FSSchedulerNode node = scheduler.getSchedulerNode(node1.getNodeID());

    NodeRemovedSchedulerEvent removeNode1 =
        new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeNode1);

    scheduler.attemptScheduling(node);

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(id11,
            RMAppAttemptState.FINISHED, false);
    scheduler.handle(appRemovedEvent1);
  }

  @Test
  public void testDefaultRuleInitializesProperlyWhenPolicyNotConfigured()
      throws IOException {
    // This test verifies if default rule in queue placement policy
    // initializes properly when policy is not configured and
    // undeclared pools is not allowed.
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);

    // Create an alloc file with no queue placement policy
    AllocationFileWriter.create()
        .writeToFile(ALLOC_FILE);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    List<PlacementRule> rules = scheduler.getRMContext()
        .getQueuePlacementManager().getPlacementRules();

    for (PlacementRule rule : rules) {
      if (rule instanceof DefaultPlacementRule) {
        DefaultPlacementRule defaultRule = (DefaultPlacementRule) rule;
        assertNotNull(defaultRule.defaultQueueName);
      }
    }
  }
  
  @Test
  public void testBlacklistNodes() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    final int GB = 1024;
    String host = "127.0.0.1";
    RMNode node =
        MockNodes.newNodeInfo(1, Resources.createResource(16 * GB, 16),
            0, host);
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttemptId =
        createSchedulingRequest(GB, "root.default", "user", 1);
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

    // Verify the blacklist can be updated independent of requesting containers
    scheduler.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        null, Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    assertTrue(app.isPlaceBlacklisted(host));
    scheduler.allocate(appAttemptId, Collections.<ResourceRequest>emptyList(),
        null, Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    assertFalse(scheduler.getSchedulerApp(appAttemptId)
        .isPlaceBlacklisted(host));

    List<ResourceRequest> update = Arrays.asList(
        createResourceRequest(GB, node.getHostName(), 1, 0, true));

    // Verify a container does not actually get placed on the blacklisted host
    scheduler.allocate(appAttemptId, update, null, Collections.<ContainerId>emptyList(),
        Collections.singletonList(host), null, NULL_UPDATE_REQUESTS);
    assertTrue(app.isPlaceBlacklisted(host));
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(0, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");

    // Verify a container gets placed on the empty blacklist
    scheduler.allocate(appAttemptId, update, null, Collections.<ContainerId>emptyList(), null,
        Collections.singletonList(host), NULL_UPDATE_REQUESTS);
    assertFalse(app.isPlaceBlacklisted(host));
    createSchedulingRequest(GB, "root.default", "user", 1);
    scheduler.update();
    scheduler.handle(updateEvent);
    assertEquals(1, app.getLiveContainers().size(),
        "Incorrect number of containers allocated");
  }
  
  @Test
  public void testGetAppsInQueue() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId appAttId1 =
        createSchedulingRequest(1024, 1, "queue1.subqueue1", "user1");
    ApplicationAttemptId appAttId2 =
        createSchedulingRequest(1024, 1, "queue1.subqueue2", "user1");
    ApplicationAttemptId appAttId3 =
        createSchedulingRequest(1024, 1, "user1", "user1");
    
    List<ApplicationAttemptId> apps =
        scheduler.getAppsInQueue("queue1.subqueue1");
    assertEquals(1, apps.size());
    assertEquals(appAttId1, apps.get(0));
    // with and without root prefix should work
    apps = scheduler.getAppsInQueue("root.queue1.subqueue1");
    assertEquals(1, apps.size());
    assertEquals(appAttId1, apps.get(0));
    
    apps = scheduler.getAppsInQueue("user1");
    assertEquals(1, apps.size());
    assertEquals(appAttId3, apps.get(0));
    // with and without root prefix should work
    apps = scheduler.getAppsInQueue("root.user1");
    assertEquals(1, apps.size());
    assertEquals(appAttId3, apps.get(0));

    // apps in subqueues should be included
    apps = scheduler.getAppsInQueue("queue1");
    assertEquals(2, apps.size());
    Set<ApplicationAttemptId> appAttIds = Sets.newHashSet(apps.get(0), apps.get(1));
    assertTrue(appAttIds.contains(appAttId1));
    assertTrue(appAttIds.contains(appAttId2));
  }

  @Test
  public void testAddAndRemoveAppFromFairScheduler() throws Exception {
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> scheduler =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) resourceManager
          .getResourceScheduler();
    TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
      scheduler.getSchedulerApplications(), scheduler, "default");
  }

  @Test
  public void testResourceUsageByMoveApp() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(1 * GB, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId appAttId =
        createSchedulingRequest(1 * GB, 2, "parent1.queue1", "user1", 2);
    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    QueueManager queueMgr = scheduler.getQueueManager();
    FSQueue parent1 = queueMgr.getParentQueue("parent1", true);
    FSQueue parent2 = queueMgr.getParentQueue("parent2", true);
    FSQueue queue2 = queueMgr.getLeafQueue("parent2.queue2", true);
    FSQueue queue1 = queueMgr.getLeafQueue("parent1.queue1", true);

    assertThat(parent2.getResourceUsage().getMemorySize()).isEqualTo(0);
    assertThat(queue2.getResourceUsage().getMemorySize()).isEqualTo(0);
    assertThat(parent1.getResourceUsage().getMemorySize()).isEqualTo(1 * GB);
    assertThat(queue1.getResourceUsage().getMemorySize()).isEqualTo(1 * GB);

    scheduler.moveApplication(appAttId.getApplicationId(), "parent2.queue2");

    assertThat(parent2.getResourceUsage().getMemorySize()).isEqualTo(1 * GB);
    assertThat(queue2.getResourceUsage().getMemorySize()).isEqualTo(1 * GB);
    assertThat(parent1.getResourceUsage().getMemorySize()).isEqualTo(0);
    assertThat(queue1.getResourceUsage().getMemorySize()).isEqualTo(0);
  }
    
  @Test
  public void testMoveWouldViolateMaxAppsConstraints() throws Exception {
    assertThrows(YarnException.class, () -> {
      scheduler.init(conf);
      scheduler.start();
      scheduler.reinitialize(conf, resourceManager.getRMContext());

      QueueManager queueMgr = scheduler.getQueueManager();
      FSQueue queue2 = queueMgr.getLeafQueue("queue2", true);
      queue2.setMaxRunningApps(0);

      ApplicationAttemptId appAttId =
          createSchedulingRequest(1024, 1, "queue1", "user1", 3);

      scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    });
  }
  
  @Test
  public void testMoveWouldViolateMaxResourcesConstraints() throws Exception {
    assertThrows(YarnException.class, () -> {
      scheduler.init(conf);
      scheduler.start();
      scheduler.reinitialize(conf, resourceManager.getRMContext());

      QueueManager queueMgr = scheduler.getQueueManager();
      FSLeafQueue oldQueue = queueMgr.getLeafQueue("queue1", true);
      FSQueue queue2 = queueMgr.getLeafQueue("queue2", true);
      queue2.setMaxShare(
           new ConfigurableResource(Resource.newInstance(1024, 1)));

      ApplicationAttemptId appAttId =
          createSchedulingRequest(1024, 1, "queue1", "user1", 3);
      RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
      NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node);
      NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
      scheduler.handle(nodeEvent);
      scheduler.handle(updateEvent);
      scheduler.handle(updateEvent);

      assertEquals(Resource.newInstance(2048, 2), oldQueue.getResourceUsage());
      scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    });
  }
  
  @Test
  public void testMoveToNonexistentQueue() throws Exception {
    assertThrows(YarnException.class, () -> {
      scheduler.init(conf);
      scheduler.start();
      scheduler.reinitialize(conf, resourceManager.getRMContext());

      scheduler.getQueueManager().getLeafQueue("queue1", true);

      ApplicationAttemptId appAttId =
          createSchedulingRequest(1024, 1, "queue1", "user1", 3);
      scheduler.moveApplication(appAttId.getApplicationId(), "queue2");
    });
  }

  @Test
  public void testLowestCommonAncestorForNonRootParent() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue aQueue = mock(FSLeafQueue.class);
    FSLeafQueue bQueue = mock(FSLeafQueue.class);
    when(aQueue.getName()).thenReturn("root.queue1.a");
    when(bQueue.getName()).thenReturn("root.queue1.b");

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(aQueue, bQueue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testLowestCommonAncestorRootParent() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue aQueue = mock(FSLeafQueue.class);
    FSLeafQueue bQueue = mock(FSLeafQueue.class);
    when(aQueue.getName()).thenReturn("root.a");
    when(bQueue.getName()).thenReturn("root.b");

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("root", false);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(aQueue, bQueue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testLowestCommonAncestorDeeperHierarchy() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSQueue aQueue = mock(FSLeafQueue.class);
    FSQueue bQueue = mock(FSLeafQueue.class);
    FSQueue a1Queue = mock(FSLeafQueue.class);
    FSQueue b1Queue = mock(FSLeafQueue.class);
    when(a1Queue.getName()).thenReturn("root.queue1.a.a1");
    when(b1Queue.getName()).thenReturn("root.queue1.b.b1");
    when(aQueue.getChildQueues()).thenReturn(Arrays.asList(a1Queue));
    when(bQueue.getChildQueues()).thenReturn(Arrays.asList(b1Queue));

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    FSQueue ancestorQueue =
        scheduler.findLowestCommonAncestorQueue(a1Queue, b1Queue);
    assertEquals(ancestorQueue, queue1);
  }

  @Test
  public void testDoubleRemoval() throws Exception {
    String testUser = "user1"; // convenience var
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    ApplicationAttemptId attemptId = createAppAttemptId(1, 1);
    // The placement rule should add it to the user based queue but the
    // passed in queue must exist.
    ApplicationPlacementContext apc =
        new ApplicationPlacementContext(testUser);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(attemptId.getApplicationId(), testUser,
            testUser, apc);
    scheduler.handle(appAddedEvent);
    AppAttemptAddedSchedulerEvent attemptAddedEvent =
        new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
    scheduler.handle(attemptAddedEvent);

    // Get a handle on the attempt.
    FSAppAttempt attempt = scheduler.getSchedulerApp(attemptId);

    AppAttemptRemovedSchedulerEvent attemptRemovedEvent =
        new AppAttemptRemovedSchedulerEvent(createAppAttemptId(1, 1),
            RMAppAttemptState.FINISHED, false);

    // Make sure the app attempt is in the queue.
    List<ApplicationAttemptId> attemptList =
        scheduler.getAppsInQueue(testUser);
    assertNotNull(attemptList, "Queue missing");
    assertTrue(attemptList.contains(attemptId),
        "Attempt should be in the queue");
    assertFalse(attempt.isStopped(), "Attempt is stopped");

    // Now remove the app attempt
    scheduler.handle(attemptRemovedEvent);
    // The attempt is not in the queue, and stopped
    attemptList = scheduler.getAppsInQueue(testUser);
    assertFalse(attemptList.contains(attemptId),
        "Attempt should not be in the queue");
    assertTrue(attempt.isStopped(), "Attempt should have been stopped");

    // Now remove the app attempt again, since it is stopped nothing happens.
    scheduler.handle(attemptRemovedEvent);
    // The attempt should still show the original queue info.
    assertTrue(attempt.getQueue().getName().endsWith(testUser),
        "Attempt queue has changed");
  }

  @Test
  public void testMoveAfterRemoval() throws Exception {
    assertThrows(YarnException.class, () -> {
      String testUser = "user1"; // convenience var
      scheduler.init(conf);
      scheduler.start();
      scheduler.reinitialize(conf, resourceManager.getRMContext());

      ApplicationAttemptId attemptId = createAppAttemptId(1, 1);
      ApplicationPlacementContext apc =
          new ApplicationPlacementContext(testUser);
      AppAddedSchedulerEvent appAddedEvent =
          new AppAddedSchedulerEvent(attemptId.getApplicationId(), testUser,
          testUser, apc);
      scheduler.handle(appAddedEvent);
      AppAttemptAddedSchedulerEvent attemptAddedEvent =
           new AppAttemptAddedSchedulerEvent(createAppAttemptId(1, 1), false);
      scheduler.handle(attemptAddedEvent);

      // Get a handle on the attempt.
      FSAppAttempt attempt = scheduler.getSchedulerApp(attemptId);

      AppAttemptRemovedSchedulerEvent attemptRemovedEvent =
          new AppAttemptRemovedSchedulerEvent(createAppAttemptId(1, 1),
          RMAppAttemptState.FINISHED, false);

      // Remove the app attempt
      scheduler.handle(attemptRemovedEvent);
      // Make sure the app attempt is not in the queue and stopped.
      List<ApplicationAttemptId> attemptList =
          scheduler.getAppsInQueue(testUser);
      assertNotNull(attemptList, "Queue missing");
      assertFalse(attemptList.contains(attemptId),
          "Attempt should not be in the queue");
      assertTrue(attempt.isStopped(), "Attempt should have been stopped");
      // The attempt should still show the original queue info.
      assertTrue(attempt.getQueue().getName().endsWith(testUser),
          "Attempt queue has changed");

      // Now move the app: not using an event since there is none
      // in the scheduler. This should throw.
      scheduler.moveApplication(attemptId.getApplicationId(), "default");
    });
  }

  @Test
  public void testPerfMetricsInited() {
    scheduler.init(conf);
    scheduler.start();
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    scheduler.fsOpDurations.getMetrics(collector, true);
    assertEquals(1, collector.getRecords().size(),
        "Incorrect number of perf metrics");
  }

  @Test
  public void testQueueNameWithTrailingSpace() throws Exception {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // no queue by default
    assertEquals(0, scheduler.getQueueManager().getLeafQueues().size());

    // submit app with queue name "A"
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);
    ApplicationPlacementContext apc =
        new ApplicationPlacementContext("A");
    AppAddedSchedulerEvent appAddedEvent1 = new AppAddedSchedulerEvent(
        appAttemptId1.getApplicationId(), "A", "user1", apc);
    scheduler.handle(appAddedEvent1);
    // submission accepted
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNotNull(scheduler.getSchedulerApplications().get(appAttemptId1.
        getApplicationId()));

    AppAttemptAddedSchedulerEvent attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    scheduler.handle(attempAddedEvent);
    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("A", true)
        .getNumRunnableApps());
    assertNotNull(scheduler.getSchedulerApp(appAttemptId1));

    // submit app with queue name "A "
    ApplicationAttemptId appAttemptId2 = createAppAttemptId(2, 1);
    apc = new ApplicationPlacementContext("A ");
    AppAddedSchedulerEvent appAddedEvent2 = new AppAddedSchedulerEvent(
        appAttemptId2.getApplicationId(), "A ", "user1", apc);
    try {
      scheduler.handle(appAddedEvent2);
      fail("Submit should have failed with InvalidQueueNameException");
    } catch (InvalidQueueNameException iqne) {
      // expected ignore: rules should have filtered this out
    }
    // submission rejected
    assertEquals(1, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApplications().get(appAttemptId2.
        getApplicationId()));
    assertNull(scheduler.getSchedulerApp(appAttemptId2));

    // submit app with queue name "B.C"
    ApplicationAttemptId appAttemptId3 = createAppAttemptId(3, 1);
    apc = new ApplicationPlacementContext("B.C");
    AppAddedSchedulerEvent appAddedEvent3 = new AppAddedSchedulerEvent(
        appAttemptId3.getApplicationId(), "B.C", "user1", apc);
    scheduler.handle(appAddedEvent3);
    // submission accepted
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());
    assertNotNull(scheduler.getSchedulerApplications().get(appAttemptId3.
        getApplicationId()));

    attempAddedEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId3, false);
    scheduler.handle(attempAddedEvent);
    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("B.C", true)
        .getNumRunnableApps());
    assertNotNull(scheduler.getSchedulerApp(appAttemptId3));

    // submit app with queue name "A\u00a0" (non-breaking space)
    ApplicationAttemptId appAttemptId4 = createAppAttemptId(4, 1);
    apc = new ApplicationPlacementContext("A\u00a0");
    AppAddedSchedulerEvent appAddedEvent4 = new AppAddedSchedulerEvent(
        appAttemptId4.getApplicationId(), "A\u00a0", "user1", apc);
    try {
      scheduler.handle(appAddedEvent4);
    } catch (InvalidQueueNameException iqne) {
      // expected ignore: rules should have filtered this out
    }
    // submission rejected
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());
    assertNull(scheduler.getSchedulerApplications().get(appAttemptId4.
        getApplicationId()));
    assertNull(scheduler.getSchedulerApp(appAttemptId4));
  }

  @Test
  public void testEmptyQueueNameInConfigFile() {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    // set empty queue name
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("").build())
        .writeToFile(ALLOC_FILE);

    try {
      scheduler.init(conf);
      fail("scheduler init should fail because" +
          " empty queue name.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(
          "Failed to initialize FairScheduler"));
    }
  }

  @Test
  public void testUserAsDefaultQueueWithLeadingTrailingSpaceUserName()
      throws Exception {
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    ApplicationAttemptId attId1 = createAppAttemptId(1, 1);
    createApplicationWithAMResource(attId1, "root.user1", "  user1", null);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId1.getApplicationId()).getQueue());

    ApplicationAttemptId attId2 = createAppAttemptId(2, 1);
    createApplicationWithAMResource(attId2, "root.user1", "user1  ", null);
    assertEquals(2, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId2.getApplicationId()).getQueue());

    ApplicationAttemptId attId3 = createAppAttemptId(3, 1);
    createApplicationWithAMResource(attId3, "root.user1", "user1", null);
    assertEquals(3, scheduler.getQueueManager().getLeafQueue("user1", true)
        .getNumRunnableApps());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default", true)
        .getNumRunnableApps());
    assertEquals("root.user1", resourceManager.getRMContext().getRMApps()
        .get(attId3.getApplicationId()).getQueue());
  }

  @Test
  public void testRemovedNodeDecomissioningNode() throws Exception {
    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register nodemanager
    NodeManager nm = registerNode("host_decom", 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4),
        mockNodeStatus);

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // Force remove the node to simulate race condition
    ((FairScheduler) resourceManager.getResourceScheduler())
        .getNodeTracker().removeNode(nm.getNodeId());
    // Kick off another heartbeat with the node state mocked to decommissioning
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));
  }

  @Test
  public void testResourceUpdateDecommissioningNode() throws Exception {
    // Mock the RMNodeResourceUpdate event handler to update SchedulerNode
    // to have 0 available resource
    RMContext spyContext = Mockito.spy(resourceManager.getRMContext());
    Dispatcher mockDispatcher = mock(AsyncDispatcher.class);
    when(mockDispatcher.getEventHandler()).thenReturn(new EventHandler() {
      @Override
      public void handle(Event event) {
        if (event instanceof RMNodeResourceUpdateEvent) {
          RMNodeResourceUpdateEvent resourceEvent =
              (RMNodeResourceUpdateEvent) event;
          resourceManager
              .getResourceScheduler()
              .getSchedulerNode(resourceEvent.getNodeId())
              .updateTotalResource(resourceEvent.getResourceOption().getResource());
        }
      }
    });
    Mockito.doReturn(mockDispatcher).when(spyContext).getDispatcher();
    ((FairScheduler) resourceManager.getResourceScheduler())
        .setRMContext(spyContext);
    ((AsyncDispatcher) mockDispatcher).start();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node
    String host_0 = "host_0";
    NodeManager nm_0 = registerNode(host_0, 1234, 2345,
        NetworkTopology.DEFAULT_RACK, Resources.createResource(8 * GB, 4),
        mockNodeStatus);

    RMNode node =
        resourceManager.getRMContext().getRMNodes().get(nm_0.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    resourceManager.getResourceScheduler().handle(nodeUpdate);

    // Kick off another heartbeat with the node state mocked to decommissioning
    // This should update the schedulernodes to have 0 available resource
    RMNode spyNode =
        Mockito.spy(resourceManager.getRMContext().getRMNodes()
            .get(nm_0.getNodeId()));
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));

    // Check the used resource is 0 GB 0 core
    // assertEquals(1 * GB, nm_0.getUsed().getMemory());
    Resource usedResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getAllocatedResource();
    assertThat(usedResource.getMemorySize()).isEqualTo(0);
    assertThat(usedResource.getVirtualCores()).isEqualTo(0);
    // Check total resource of scheduler node is also changed to 0 GB 0 core
    Resource totalResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getTotalResource();
    assertThat(totalResource.getMemorySize()).isEqualTo(0 * GB);
    assertThat(totalResource.getVirtualCores()).isEqualTo(0);
    // Check the available resource is 0/0
    Resource availableResource =
        resourceManager.getResourceScheduler()
            .getSchedulerNode(nm_0.getNodeId()).getUnallocatedResource();
    assertThat(availableResource.getMemorySize()).isEqualTo(0);
    assertThat(availableResource.getVirtualCores()).isEqualTo(0);
    // Kick off another heartbeat where the RMNodeResourceUpdateEvent would
    // be skipped for DECOMMISSIONING state since the total resource is
    // already equal to used resource from the previous heartbeat.
    when(spyNode.getState()).thenReturn(NodeState.DECOMMISSIONING);
    resourceManager.getResourceScheduler().handle(
        new NodeUpdateSchedulerEvent(spyNode));
    verify(mockDispatcher, times(1)).getEventHandler();
  }

  private NodeManager registerNode(String hostName, int containerManagerPort,
      int httpPort, String rackName,
      Resource capability, NodeStatus nodeStatus)
      throws IOException, YarnException {
    NodeStatus mockNodeStatus = createMockNodeStatus();

    NodeManager nm = new NodeManager(hostName, containerManagerPort, httpPort,
        rackName, capability, resourceManager, mockNodeStatus);

    // after YARN-5375, scheduler event is processed in rm main dispatcher,
    // wait it processed, or may lead dead lock
    drainEventsOnRM();

    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }

  @Test
  @Timeout(value = 120)
  public void testContainerAllocationWithContainerIdLeap() throws Exception {
    conf.setFloat(FairSchedulerConfiguration.RESERVABLE_NODES, 0.50f);
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add two node
    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(3072, 10), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1,
        Resources.createResource(3072, 10), 1, "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    ApplicationAttemptId app1 =
        createSchedulingRequest(2048, "queue1", "user1", 2);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    ApplicationAttemptId app2 =
        createSchedulingRequest(2048, "queue1", "user1", 1);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node1));
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));

    assertEquals(4096, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    //container will be reserved at node1
    RMContainer reservedContainer1 =
        scheduler.getSchedulerNode(node1.getNodeID()).getReservedContainer();
    assertNotEquals(reservedContainer1, null);
    RMContainer reservedContainer2 =
        scheduler.getSchedulerNode(node2.getNodeID()).getReservedContainer();
    assertEquals(reservedContainer2, null);

    for (int i = 0; i < 10; i++) {
      scheduler.handle(new NodeUpdateSchedulerEvent(node1));
      scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    }

    // release resource
    scheduler.handle(new AppAttemptRemovedSchedulerEvent(
        app1, RMAppAttemptState.KILLED, false));

    assertEquals(0, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemorySize());

    // container will be allocated at node2
    scheduler.handle(new NodeUpdateSchedulerEvent(node2));
    assertThat(scheduler.getSchedulerApp(app2).getLiveContainers()).hasSize(1);

    long maxId = 0;
    for (RMContainer container :
        scheduler.getSchedulerApp(app2).getLiveContainers()) {
      assertTrue(
          container.getContainer().getNodeId().equals(node2.getNodeID()));
      if (container.getContainerId().getContainerId() > maxId) {
        maxId = container.getContainerId().getContainerId();
      }
    }

    long reservedId = reservedContainer1.getContainerId().getContainerId();
    assertEquals(reservedId + 1, maxId);
  }

  @Test
  @Timeout(value = 120)
  public void testRefreshQueuesWhenRMHA() throws Exception {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS, false);
    conf.setBoolean(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, false);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    HAServiceProtocol.StateChangeRequestInfo requestInfo =
        new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER);

    //ensure ALLOC_FILE contains an 'empty' config
    AllocationFileWriter.create().writeToFile(ALLOC_FILE);
    // 1. start a standby RM, file 'ALLOC_FILE' is empty, so there is no queues
    MockRM rm1 = new MockRM(conf, null);
    rm1.init(conf);
    rm1.start();
    rm1.getAdminService().transitionToStandby(requestInfo);

    // 2. add a new queue "test_queue"
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("test_queue")
            .maxRunningApps(3).build())
        .writeToFile(ALLOC_FILE);

    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    // 3. start a active RM
    MockRM rm2 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm2.getRMStateStore();
    rm2.start();

    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm2.getResourceTrackerService());
    nm.registerNode();

    rm2.getAdminService().transitionToActive(requestInfo);

    // 4. submit a app to the new added queue "test_queue"
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm2)
            .withAppName("test_app")
            .withUser("user")
            .withAcls(null)
            .withQueue("test_queue")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm2, data);
    RMAppAttempt attempt0 = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am0 = rm2.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    assertEquals("root.test_queue", app.getQueue());

    // 5. Transit rm1 to active, recover app
    ((RMContextImpl)rm1.getRMContext()).setStateStore(memStore);
    rm1.getAdminService().transitionToActive(requestInfo);
    rm1.drainEvents();
    assertEquals(1, rm1.getRMContext().getRMApps().size());
    RMApp recoveredApp =
        rm1.getRMContext().getRMApps().values().iterator().next();
    assertEquals("root.test_queue", recoveredApp.getQueue());

    rm1.stop();
    rm2.stop();
  }

  @Test
  public void testReservationMetrics() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    QueueMetrics metrics = scheduler.getRootQueueMetrics();

    RMNode node1 =
        MockNodes
            .newNodeInfo(1, Resources.createResource(4096, 4), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent);

    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createApplicationWithAMResource(appAttemptId, "default", "user1", null);

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.update();
    scheduler.handle(updateEvent);

    createSchedulingRequestExistingApplication(1024, 1, 1, appAttemptId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // no reservation yet
    assertEquals(0, metrics.getReservedContainers());
    assertEquals(0, metrics.getReservedMB());
    assertEquals(0, metrics.getReservedVirtualCores());

    // create reservation of {4096, 4}
    createSchedulingRequestExistingApplication(4096, 4, 1, appAttemptId);
    scheduler.update();
    scheduler.handle(updateEvent);

    // reservation created
    assertEquals(1, metrics.getReservedContainers());
    assertEquals(4096, metrics.getReservedMB());
    assertEquals(4, metrics.getReservedVirtualCores());

    // remove AppAttempt
    AppAttemptRemovedSchedulerEvent attRemoveEvent =
        new AppAttemptRemovedSchedulerEvent(
            appAttemptId,
            RMAppAttemptState.KILLED,
            false);
    scheduler.handle(attRemoveEvent);

    // The reservation metrics should be subtracted
    assertEquals(0, metrics.getReservedContainers());
    assertEquals(0, metrics.getReservedMB());
    assertEquals(0, metrics.getReservedVirtualCores());
  }


  @Test
  public void testUpdateDemand() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    Resource maxResource = Resources.createResource(1024 * 8);
    ConfigurableResource maxResourceConf =
        new ConfigurableResource(maxResource);

    FSAppAttempt app1 = mock(FSAppAttempt.class);
    Mockito.when(app1.getDemand()).thenReturn(maxResource);
    Mockito.when(app1.getResourceUsage()).thenReturn(Resources.none());
    FSAppAttempt app2 = mock(FSAppAttempt.class);
    Mockito.when(app2.getDemand()).thenReturn(maxResource);
    Mockito.when(app2.getResourceUsage()).thenReturn(Resources.none());

    QueueManager queueManager = scheduler.getQueueManager();
    FSParentQueue queue1 = queueManager.getParentQueue("queue1", true);

    FSLeafQueue aQueue =
        new FSLeafQueue("root.queue1.a", scheduler, queue1);
    aQueue.setMaxShare(maxResourceConf);
    aQueue.addApp(app1, true);

    FSLeafQueue bQueue =
        new FSLeafQueue("root.queue1.b", scheduler, queue1);
    bQueue.setMaxShare(maxResourceConf);
    bQueue.addApp(app2, true);

    queue1.setMaxShare(maxResourceConf);
    queue1.addChildQueue(aQueue);
    queue1.addChildQueue(bQueue);

    queue1.updateDemand();

    assertTrue(Resources.equals(queue1.getDemand(), maxResource),
        "Demand is greater than max allowed ");
    assertTrue(Resources.equals(aQueue.getDemand(), maxResource) &&
        Resources.equals(bQueue.getDemand(), maxResource),
        "Demand of child queue not updated ");
  }

  @Test
  public void testDumpState() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("parent")
            .subQueue(new AllocationFileQueue.Builder("child1")
                .weight(1).build())
            .build())
        .writeToFile(ALLOC_FILE);

    ControlledClock clock = new ControlledClock();
    scheduler.setClock(clock);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    FSLeafQueue child1 =
        scheduler.getQueueManager().getLeafQueue("parent.child1", false);
    Resource resource = Resource.newInstance(4 * GB, 4);
    child1.setMaxShare(new ConfigurableResource(resource));
    FSAppAttempt app = mock(FSAppAttempt.class);
    Mockito.when(app.getDemand()).thenReturn(resource);
    Mockito.when(app.getResourceUsage()).thenReturn(resource);
    child1.addApp(app, true);
    child1.updateDemand();

    String childQueueString = "{Name: root.parent.child1,"
        + " Weight: 1.0,"
        + " Policy: fair,"
        + " FairShare: <memory:0, vCores:0>,"
        + " SteadyFairShare: <memory:0, vCores:0>,"
        + " MaxShare: <memory:4096, vCores:4>,"
        + " MinShare: <memory:0, vCores:0>,"
        + " ResourceUsage: <memory:4096, vCores:4>,"
        + " Demand: <memory:4096, vCores:4>,"
        + " Runnable: 1,"
        + " NumPendingApps: 0,"
        + " NonRunnable: 0,"
        + " MaxAMShare: 0.5,"
        + " MaxAMResource: <memory:0, vCores:0>,"
        + " AMResourceUsage: <memory:0, vCores:0>,"
        + " LastTimeAtMinShare: " + clock.getTime()
        + "}";

    assertEquals(childQueueString, child1.dumpState(),
        "Unexpected state dump string");
    FSParentQueue parent =
        scheduler.getQueueManager().getParentQueue("parent", false);
    parent.setMaxShare(new ConfigurableResource(resource));
    parent.updateDemand();

    String parentQueueString = "{Name: root.parent,"
        + " Weight: 1.0,"
        + " Policy: fair,"
        + " FairShare: <memory:0, vCores:0>,"
        + " SteadyFairShare: <memory:0, vCores:0>,"
        + " MaxShare: <memory:4096, vCores:4>,"
        + " MinShare: <memory:0, vCores:0>,"
        + " ResourceUsage: <memory:4096, vCores:4>,"
        + " Demand: <memory:4096, vCores:4>,"
        + " MaxAMShare: 0.5,"
        + " Runnable: 0}";

    assertEquals(parentQueueString + ", " + childQueueString, parent.dumpState(),
        "Unexpected state dump string");
  }

  @Test
  public void testCompletedContainerOnRemovedNode() throws IOException {
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node = MockNodes.newNodeInfo(1, Resources.createResource(2048), 2,
        "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node));

    // Create application attempt
    ApplicationAttemptId appAttemptId = createAppAttemptId(1, 1);
    createMockRMApp(appAttemptId);
    ApplicationPlacementContext placementCtx =
        new ApplicationPlacementContext("root.queue1");
    scheduler.addApplication(appAttemptId.getApplicationId(), "root.queue1",
        "user1", false, placementCtx);
    scheduler.addApplicationAttempt(appAttemptId, false, false);

    // Create container request that goes to a specific node.
    // Without the 2nd and 3rd request we do not get live containers
    List<ResourceRequest> ask1 = new ArrayList<>();
    ResourceRequest request1 =
        createResourceRequest(1024, node.getHostName(), 1, 1, true);
    ask1.add(request1);
    ResourceRequest request2 =
        createResourceRequest(1024, node.getRackName(), 1, 1, false);
    ask1.add(request2);
    ResourceRequest request3 =
        createResourceRequest(1024, ResourceRequest.ANY, 1, 1, false);
    ask1.add(request3);

    // Perform allocation
    scheduler.allocate(appAttemptId, ask1, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    scheduler.update();
    scheduler.handle(new NodeUpdateSchedulerEvent(node));

    // Get the allocated containers for the application (list can not be null)
    Collection<RMContainer> clist = scheduler.getSchedulerApp(appAttemptId)
        .getLiveContainers();
    assertEquals(1, clist.size());

    // Make sure that we remove the correct node (should never fail)
    RMContainer rmc = clist.iterator().next();
    NodeId containerNodeID = rmc.getAllocatedNode();
    assertEquals(node.getNodeID(), containerNodeID);

    // Remove node
    scheduler.handle(new NodeRemovedSchedulerEvent(node));

    // Call completedContainer() should not fail even if the node has been
    // removed
    scheduler.completedContainer(rmc,
        SchedulerUtils.createAbnormalContainerStatus(rmc.getContainerId(),
            SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.EXPIRE);
  }

  @Test
  public void testAppRejectedToQueueWithZeroCapacityOfVcores()
      throws IOException {
    testAppRejectedToQueueWithZeroCapacityOfResource(
            ResourceInformation.VCORES_URI);
  }

  @Test
  public void testAppRejectedToQueueWithZeroCapacityOfMemory()
      throws IOException {
    testAppRejectedToQueueWithZeroCapacityOfResource(
            ResourceInformation.MEMORY_URI);
  }

  private void testAppRejectedToQueueWithZeroCapacityOfResource(String resource)
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    generateAllocationFileWithZeroResource(resource);

    final List<Event> recordedEvents = Lists.newArrayList();

    RMContext spyContext = Mockito.spy(resourceManager.getRMContext());
    Dispatcher mockDispatcher = mock(AsyncDispatcher.class);
    when(mockDispatcher.getEventHandler()).thenReturn((EventHandler) event -> {
      if (event instanceof RMAppEvent) {
        recordedEvents.add(event);
      }
    });
    Mockito.doReturn(mockDispatcher).when(spyContext).getDispatcher();
    ((AsyncDispatcher) mockDispatcher).start();

    scheduler.setRMContext(spyContext);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // submit app with queue name (queueA)
    ApplicationAttemptId appAttemptId1 = createAppAttemptId(1, 1);

    ResourceRequest amReqs = ResourceRequest.newBuilder()
        .capability(Resource.newInstance(5 * GB, 3)).build();
    createApplicationWithAMResource(appAttemptId1, "root.queueA", "user1",
        Resource.newInstance(GB, 1), Lists.newArrayList(amReqs));
    scheduler.update();

    assertEquals(1, recordedEvents.size(),
        "Exactly one APP_REJECTED event is expected");
    Event event = recordedEvents.get(0);
    RMAppEvent rmAppEvent = (RMAppEvent) event;
    assertEquals(RMAppEventType.APP_REJECTED, rmAppEvent.getType());
    assertTrue(rmAppEvent.getDiagnosticMsg()
        .matches("Cannot submit application application[\\d_]+ to queue "
        + "root.queueA because it has zero amount of resource "
        + "for a requested resource! " +
        "Invalid requested AM resources: .+, "
        + "maximum queue resources: .+"),
        "Diagnostic message does not match: " +
        rmAppEvent.getDiagnosticMsg());
  }

  private void generateAllocationFileWithZeroResource(String resource) {
    String resources = "";
    if (resource.equals(ResourceInformation.MEMORY_URI)) {
      resources = "0 mb,2vcores";
    } else if (resource.equals(ResourceInformation.VCORES_URI)) {
      resources = "10000 mb,0vcores";
    }

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("queueA")
            .minResources(resources)
            .maxResources(resources)
            .weight(2.0f)
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .minResources("1 mb 1 vcores")
            .weight(0.0f)
            .build())
        .writeToFile(ALLOC_FILE);
  }

  @Test
  public void testSchedulingRejectedToQueueWithZeroCapacityOfMemory()
      throws IOException {
    // This request is not valid as queue will have 0 capacity of memory and
    // the requests asks 2048M
    ResourceRequest invalidRequest =
        createResourceRequest(2048, 2, ResourceRequest.ANY, 1, 2, true);

    ResourceRequest validRequest =
        createResourceRequest(0, 0, ResourceRequest.ANY, 1, 2, true);
    testSchedulingRejectedToQueueZeroCapacityOfResource(
        ResourceInformation.MEMORY_URI,
        Lists.newArrayList(invalidRequest, validRequest));
  }

  @Test
  public void testSchedulingAllowedToQueueWithZeroCapacityOfMemory()
      throws IOException {
    testSchedulingAllowedToQueueZeroCapacityOfResource(
        ResourceInformation.MEMORY_URI, 0, 2);
  }

  @Test
  public void testSchedulingRejectedToQueueWithZeroCapacityOfVcores()
      throws IOException {
    // This request is not valid as queue will have 0 capacity of vCores and
    // the requests asks 1
    ResourceRequest invalidRequest =
        createResourceRequest(0, 1, ResourceRequest.ANY, 1, 2, true);

    ResourceRequest validRequest =
        createResourceRequest(0, 0, ResourceRequest.ANY, 1, 2, true);

    testSchedulingRejectedToQueueZeroCapacityOfResource(
        ResourceInformation.VCORES_URI,
        Lists.newArrayList(invalidRequest, validRequest));
  }

  @Test
  public void testSchedulingAllowedToQueueWithZeroCapacityOfVcores()
      throws IOException {
    testSchedulingAllowedToQueueZeroCapacityOfResource(
            ResourceInformation.VCORES_URI, 2048, 0);
  }

  private void testSchedulingRejectedToQueueZeroCapacityOfResource(
      String resource, Collection<ResourceRequest> requests)
      throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    generateAllocationFileWithZeroResource(resource);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    try {
      createSchedulingRequest(requests, "queueA", "user1");
      fail("Exception is expected because the queue has zero capacity of "
          + resource + " and requested resource capabilities are: "
          + requests.stream().map(ResourceRequest::getCapability)
              .collect(Collectors.toList()));
    } catch (SchedulerInvalidResourceRequestException e) {
      assertTrue(e.getMessage()
          .matches("Resource request is invalid for application "
          + "application[\\d_]+ because queue root\\.queueA has 0 "
          + "amount of resource for a resource type! "
          + "Validation result:.*"),
          "The thrown exception is not the expected one. Exception message: " + e.getMessage());

      List<ApplicationAttemptId> appsInQueue =
          scheduler.getAppsInQueue("queueA");
      assertEquals(1, appsInQueue.size(),
          "Number of apps in queue 'queueA' should be one!");

      ApplicationAttemptId appAttemptId =
          scheduler.getAppsInQueue("queueA").get(0);
      assertNotNull(scheduler.getSchedulerApp(appAttemptId),
          "Scheduler app for appAttemptId " + appAttemptId
          + " should not be null!");

      FSAppAttempt schedulerApp = scheduler.getSchedulerApp(appAttemptId);
      assertNotNull(schedulerApp.getAppSchedulingInfo(),
          "Scheduler app queueInfo for appAttemptId " + appAttemptId
          + " should not be null!");

      assertTrue(schedulerApp.getAppSchedulingInfo().getAllResourceRequests().isEmpty(),
          "There should be no requests accepted");
    }
  }

  private void testSchedulingAllowedToQueueZeroCapacityOfResource(
          String resource, int memory, int vCores) throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    generateAllocationFileWithZeroResource(resource);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2048, 2));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(memory, vCores, "queueA", "user1", 1, 2);
  }

  @Test
  public void testRestoreToExistingQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    generateAllocationFilePercentageResource();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // no nodes so the resource total should be zero for all queues
    // AM is using resources
    Resource amResource = Resources.createResource(1024, 1);
    // Add the app and the attempt
    ApplicationAttemptId appAttemptId = null;
    String queueId = "root.parent.queueA";
    try {
      appAttemptId = createRecoveringApplication(amResource, queueId, "user1");
    } catch (Exception e) {
      fail("The exception is not expected. Exception message: "
          + e.getMessage());
    }
    scheduler.addApplicationAttempt(appAttemptId, false, true);

    List<ApplicationAttemptId> appsInQueue =
        scheduler.getAppsInQueue(queueId);
    assertEquals(1, appsInQueue.size(),
        "Number of apps in queue 'root.parent.queueA' should be one!");

    appAttemptId = scheduler.getAppsInQueue(queueId).get(0);
    assertNotNull(scheduler.getSchedulerApp(appAttemptId),
        "Scheduler app for appAttemptId " + appAttemptId
        + " should not be null!");

    FSAppAttempt schedulerApp = scheduler.getSchedulerApp(appAttemptId);
    assertNotNull(schedulerApp.getAppSchedulingInfo(),
        "Scheduler app queueInfo for appAttemptId " + appAttemptId
        + " should not be null!");

    assertTrue(schedulerApp.getAppSchedulingInfo().getAllResourceRequests().isEmpty(),
        "There should be no requests accepted");

    // Restore an applications with a user that has no access to the queue
    try {
      appAttemptId = createRecoveringApplication(amResource, queueId,
        "usernotinacl");
    } catch (Exception e) {
      fail("The exception is not expected. Exception message: "
          + e.getMessage());
    }
    scheduler.addApplicationAttempt(appAttemptId, false, true);

    appsInQueue = scheduler.getAppsInQueue(queueId);
    assertEquals(2, appsInQueue.size(),
        "Number of apps in queue 'root.parent.queueA' should be two!");

    appAttemptId = scheduler.getAppsInQueue(queueId).get(1);
    assertNotNull(scheduler.getSchedulerApp(appAttemptId),
        "Scheduler app for appAttemptId " + appAttemptId
        + " should not be null!");

    schedulerApp = scheduler.getSchedulerApp(appAttemptId);
    assertNotNull(schedulerApp.getAppSchedulingInfo(),
        "Scheduler app queueInfo for appAttemptId " + appAttemptId
        + " should not be null!");

    assertTrue(schedulerApp.getAppSchedulingInfo().getAllResourceRequests().isEmpty(),
        "There should be no requests accepted");
  }

  @Test
  public void testRestoreToParentQueue() throws IOException {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    generateAllocationFilePercentageResource();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // no nodes so the resource total should be zero for all queues
    // AM is using resources
    Resource amResource = Resources.createResource(1024, 1);
    // Add the app and the attempt, use a queue that is now a parent
    ApplicationAttemptId appAttemptId = null;
    String queueId = "root.parent";
    try {
      appAttemptId = createRecoveringApplication(amResource, queueId, "user1");
    } catch (Exception e) {
      fail("The exception is not expected. Exception message: "
          + e.getMessage());
    }
    scheduler.addApplicationAttempt(appAttemptId, false, true);

    String recoveredQueue = "root.recovery";
    List<ApplicationAttemptId> appsInQueue =
        scheduler.getAppsInQueue(recoveredQueue);
    assertEquals(1, appsInQueue.size(),
        "Number of apps in queue 'root.recovery' should be one!");

    appAttemptId =
        scheduler.getAppsInQueue(recoveredQueue).get(0);
    assertNotNull(scheduler.getSchedulerApp(appAttemptId),
        "Scheduler app for appAttemptId " + appAttemptId
        + " should not be null!");

    FSAppAttempt schedulerApp = scheduler.getSchedulerApp(appAttemptId);
    assertNotNull(schedulerApp.getAppSchedulingInfo(),
        "Scheduler app queueInfo for appAttemptId " + appAttemptId
        + " should not be null!");

    assertTrue(schedulerApp.getAppSchedulingInfo().getAllResourceRequests().isEmpty(),
        "There should be no requests accepted");
  }

  private void generateAllocationFilePercentageResource() {
    AllocationFileWriter.create()
      .addQueue(new AllocationFileQueue.Builder("root")
          .parent(true)
          .aclSubmitApps(" ")
          .aclAdministerApps(" ")
          .subQueue(new AllocationFileQueue.Builder("parent")
              .parent(true)
              .maxChildResources("memory-mb=15.0%, vcores=15.0%")
              .subQueue(new AllocationFileQueue.Builder("queueA")
                  .aclSubmitApps("user1")
                  .build())
              .build())
          .build())
      .writeToFile(ALLOC_FILE);
  }
}
