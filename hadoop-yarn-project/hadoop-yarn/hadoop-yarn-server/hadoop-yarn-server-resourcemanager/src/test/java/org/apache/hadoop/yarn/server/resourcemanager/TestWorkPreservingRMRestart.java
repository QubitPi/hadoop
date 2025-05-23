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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart.TestSecurityMockRM;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueInvalidException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .TestCapacitySchedulerAutoCreatedQueueBase;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.event.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.TestCapacitySchedulerAutoCreatedQueueBase.USER1;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .RMWebServices.DEFAULT_QUEUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"rawtypes", "unchecked"})
public class TestWorkPreservingRMRestart extends ParameterizedSchedulerTestBase {

  private YarnConfiguration conf;
  MockRM rm1 = null;
  MockRM rm2 = null;

  public void initTestWorkPreservingRMRestart(SchedulerType type) throws IOException {
    initParameterizedSchedulerTestBase(type);
    setup();
  }

  public void setup() throws UnknownHostException {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @AfterEach
  public void tearDown() {
    if (rm1 != null) {
      rm1.stop();
    }
    if (rm2 != null) {
      rm2.stop();
    }
    conf = null;
  }

  // Test common scheduler state including SchedulerAttempt, SchedulerNode,
  // AppSchedulingInfo can be reconstructed via the container recovery reports
  // on NM re-registration.
  // Also test scheduler specific changes: i.e. Queue recovery-
  // CSQueue/FSQueue/FifoQueue recovery respectively.
  // Test Strategy: send 3 container recovery reports(AMContainer, running
  // container, completed container) on NM re-registration, check the states of
  // SchedulerAttempt, SchedulerNode etc. are updated accordingly.
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testSchedulerRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      DominantResourceCalculator.class.getName());

    int containerMemory = 1024;
    Resource containerResource = Resource.newInstance(containerMemory, 1);

    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    Resource amResources = app1.getAMResourceRequests().get(0).getCapability();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // clear queue metrics
    rm1.clearQueueMetrics(app1);

    // Re-start RM
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // recover app
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp1.getCurrentAppAttempt();
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);

    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
      completedContainer), null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
          .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // check RMContainers are re-recreated and the container state is correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
      RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
      RMContainerState.RUNNING);
    rm2.waitForContainerToComplete(loadedAttempt1, completedContainer);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    SchedulerNode schedulerNode1 = scheduler.getSchedulerNode(nm1.getNodeId());
    assertTrue(schedulerNode1
        .toString().contains(schedulerNode1.getUnallocatedResource().toString()),
        "SchedulerNode#toString is not in expected format");
    assertTrue(
        schedulerNode1
        .toString().contains(schedulerNode1.getAllocatedResource().toString()),
        "SchedulerNode#toString is not in expected format");

    // ********* check scheduler node state.*******
    // 2 running containers.
    Resource usedResources = Resources.multiply(containerResource, 2);
    Resource nmResource =
        Resource.newInstance(nm1.getMemory(), nm1.getvCores());

    assertTrue(schedulerNode1.isValidContainer(amContainer.getContainerId()));
    assertTrue(schedulerNode1.isValidContainer(runningContainer
      .getContainerId()));
    assertFalse(schedulerNode1.isValidContainer(completedContainer
      .getContainerId()));
    // 2 launched containers, 1 completed container
    assertEquals(2, schedulerNode1.getNumContainers());

    assertEquals(Resources.subtract(nmResource, usedResources),
      schedulerNode1.getUnallocatedResource());
    assertEquals(usedResources, schedulerNode1.getAllocatedResource());
    Resource availableResources = Resources.subtract(nmResource, usedResources);

    // ***** check queue state based on the underlying scheduler ********
    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
          .getSchedulerApplications();
    SchedulerApplication schedulerApp =
        schedulerApps.get(recoveredApp1.getApplicationId());

    if (getSchedulerType() == SchedulerType.CAPACITY) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    } else {
      checkFSQueue(rm2, schedulerApp, usedResources, availableResources,
          amResources);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers().contains(
      scheduler.getRMContainer(runningContainer.getContainerId())));
    assertThat(schedulerAttempt.getCurrentConsumption()).
        isEqualTo(usedResources);

    // *********** check appSchedulingInfo state ***********
    assertEquals((1L << 40) + 1L, schedulerAttempt.getNewContainerId());
  }

  private Configuration getSchedulerDynamicConfiguration() throws IOException {
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    conf.setTimeDuration(
        YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP, 1L,
        TimeUnit.SECONDS);
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      CapacitySchedulerConfiguration schedulerConf =
          new CapacitySchedulerConfiguration(conf);
      ReservationSystemTestUtil.setupDynamicQueueConfiguration(schedulerConf);
      return schedulerConf;
    } else {
      String allocFile = new File(FairSchedulerTestBase.TEST_DIR,
          TestWorkPreservingRMRestart.class.getSimpleName() + ".xml")
              .getAbsolutePath();
      ReservationSystemTestUtil.setupFSAllocationFile(allocFile);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
          ResourceScheduler.class);
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);
      return conf;
    }
  }

  private CapacitySchedulerConfiguration
      getSchedulerAutoCreatedQueueConfiguration(
      boolean overrideWithQueueMappings, boolean useFlexibleAQC) {
    CapacitySchedulerConfiguration schedulerConf =
        new CapacitySchedulerConfiguration(conf);
    if (useFlexibleAQC) {
      TestCapacitySchedulerAutoCreatedQueueBase
              .setupQueueConfigurationForSingleFlexibleAutoCreatedLeafQueue(schedulerConf);
    } else {
      TestCapacitySchedulerAutoCreatedQueueBase
              .setupQueueConfigurationForSingleAutoCreatedLeafQueue(schedulerConf);
    }
    TestCapacitySchedulerAutoCreatedQueueBase.setupQueueMappings(schedulerConf,
        "c", overrideWithQueueMappings, new int[] {0, 1});
    return schedulerConf;
  }

  // Test work preserving recovery of apps running under reservation.
  // This involves:
  // 1. Setting up a dynamic reservable queue,
  // 2. Submitting an app to it,
  // 3. Failing over RM,
  // 4. Validating that the app is recovered post failover,
  // 5. Check if all running containers are recovered,
  // 6. Verify the scheduler state like attempt info,
  // 7. Verify the queue/user metrics for the dynamic reservable queue.
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDynamicQueueRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());

    // 1. Set up dynamic reservable queue.
    Configuration schedulerConf = getSchedulerDynamicConfiguration();
    int containerMemory = 1024;
    Resource containerResource = Resource.newInstance(containerMemory, 1);

    rm1 = new MockRM(schedulerConf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    // 2. Run plan follower to update the added node & then submit app to
    // dynamic queue.
    rm1.getRMContext().getReservationSystem()
        .synchronizePlan(ReservationSystemTestUtil.reservationQ, true);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppName("dynamicQApp")
            .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
            .withAcls(null)
            .withQueue(ReservationSystemTestUtil.getReservationQueueName())
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    Resource amResources = app1.getAMResourceRequests().get(0).getCapability();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // clear queue metrics
    rm1.clearQueueMetrics(app1);

    // 3. Fail over (restart) RM.
    rm2 = new MockRM(schedulerConf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // 4. Validate app is recovered post failover.
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp1.getCurrentAppAttempt();
    NMContainerStatus amContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    NMContainerStatus runningContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
            ContainerState.COMPLETE);

    nm1.registerNode(
        Arrays.asList(amContainer, runningContainer, completedContainer), null);

    // Wait for RM to settle down on recovering containers.
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
            .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // 5. Check RMContainers are re-recreated and the container state is
    // correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForContainerToComplete(loadedAttempt1, completedContainer);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    SchedulerNode schedulerNode1 = scheduler.getSchedulerNode(nm1.getNodeId());

    // ********* check scheduler node state.*******
    // 2 running containers.
    Resource usedResources = Resources.multiply(containerResource, 2);
    Resource nmResource =
        Resource.newInstance(nm1.getMemory(), nm1.getvCores());

    assertTrue(schedulerNode1.isValidContainer(amContainer.getContainerId()));
    assertTrue(
        schedulerNode1.isValidContainer(runningContainer.getContainerId()));
    assertFalse(
        schedulerNode1.isValidContainer(completedContainer.getContainerId()));
    // 2 launched containers, 1 completed container
    assertEquals(2, schedulerNode1.getNumContainers());

    assertEquals(Resources.subtract(nmResource, usedResources),
        schedulerNode1.getUnallocatedResource());
    assertEquals(usedResources, schedulerNode1.getAllocatedResource());
    Resource availableResources = Resources.subtract(nmResource, usedResources);

    // 6. Verify the scheduler state like attempt info.
    Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> sa =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
            .getSchedulerApplications();
    SchedulerApplication<SchedulerApplicationAttempt> schedulerApp =
        sa.get(recoveredApp1.getApplicationId());

    // 7. Verify the queue/user metrics for the dynamic reservable queue.
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    } else {
      checkFSQueue(rm2, schedulerApp, usedResources, availableResources,
          amResources);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(runningContainer.getContainerId())));
    assertThat(schedulerAttempt.getCurrentConsumption()).
        isEqualTo(usedResources);

    // *********** check appSchedulingInfo state ***********
    assertEquals((1L << 40) + 1L, schedulerAttempt.getNewContainerId());
  }

  private void checkCSQueue(MockRM rm,
      SchedulerApplication<SchedulerApplicationAttempt> app,
      Resource clusterResource, Resource queueResource, Resource usedResource,
      int numContainers)
      throws Exception {
    checkCSLeafQueue(rm, app, clusterResource, queueResource, usedResource,
        numContainers);

    AbstractLeafQueue queue = (AbstractLeafQueue) app.getQueue();
    Resource availableResources =
        Resources.subtract(queueResource, usedResource);
    // ************ check app headroom ****************
    SchedulerApplicationAttempt schedulerAttempt = app.getCurrentAppAttempt();
    assertEquals(availableResources, schedulerAttempt.getHeadroom());

    // ************* check Queue metrics ************
    QueueMetrics queueMetrics = queue.getMetrics();
    assertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResource.getMemorySize(),
        usedResource.getVirtualCores());

    // ************ check user metrics ***********
    QueueMetrics userMetrics =
        queueMetrics.getUserMetrics(app.getUser());
    assertMetrics(userMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResource.getMemorySize(),
        usedResource.getVirtualCores());
  }

  private void checkCSLeafQueue(MockRM rm,
      SchedulerApplication<SchedulerApplicationAttempt> app,
      Resource clusterResource, Resource queueResource, Resource usedResource,
      int numContainers) {
    AbstractLeafQueue leafQueue = (AbstractLeafQueue) app.getQueue();
    // assert queue used resources.
    assertEquals(usedResource, leafQueue.getUsedResources());
    assertEquals(numContainers, leafQueue.getNumContainers());

    ResourceCalculator calc =
        ((CapacityScheduler) rm.getResourceScheduler()).getResourceCalculator();
    float usedCapacity =
        Resources.divide(calc, clusterResource, usedResource, queueResource);
    // assert queue used capacity
    assertEquals(usedCapacity, leafQueue.getUsedCapacity(), 1e-8);
    float absoluteUsedCapacity =
        Resources.divide(calc, clusterResource, usedResource, clusterResource);
    // assert queue absolute capacity
    assertEquals(absoluteUsedCapacity, leafQueue.getAbsoluteUsedCapacity(),
      1e-8);
    // assert user consumed resources.
    assertEquals(usedResource, leafQueue.getUser(app.getUser())
      .getUsed());
  }

  private void checkFSQueue(ResourceManager rm,
      SchedulerApplication  schedulerApp, Resource usedResources,
      Resource availableResources, Resource amResources) throws Exception {
    // waiting for RM's scheduling apps
    int retry = 0;
    Resource assumedFairShare = Resource.newInstance(8192, 8);
    while (true) {
      Thread.sleep(100);
      if (assumedFairShare.equals(((FairScheduler)rm.getResourceScheduler())
          .getQueueManager().getRootQueue().getFairShare())) {
        break;
      }
      retry++;
      if (retry > 30) {
        fail("Apps are not scheduled within assumed timeout");
      }
    }

    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    FSParentQueue root = scheduler.getQueueManager().getRootQueue();
    // ************ check cluster used Resources ********
    assertTrue(root.getPolicy() instanceof DominantResourceFairnessPolicy);
    assertEquals(usedResources,root.getResourceUsage());

    // ************ check app headroom ****************
    FSAppAttempt schedulerAttempt =
        (FSAppAttempt) schedulerApp.getCurrentAppAttempt();
    assertEquals(availableResources, schedulerAttempt.getHeadroom());

    // ************ check queue metrics ****************
    QueueMetrics queueMetrics = scheduler.getRootQueueMetrics();
    assertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.getMemorySize(),
        availableResources.getVirtualCores(), usedResources.getMemorySize(),
        usedResources.getVirtualCores());

    // ************ check AM resources ****************
    assertEquals(amResources,
        schedulerApp.getCurrentAppAttempt().getAMResource());
    FSQueueMetrics fsQueueMetrics =
        (FSQueueMetrics) schedulerApp.getQueue().getMetrics();
    assertEquals(amResources.getMemorySize(),
        fsQueueMetrics.getAMResourceUsageMB());
    assertEquals(amResources.getVirtualCores(),
        fsQueueMetrics.getAMResourceUsageVCores());
  }

  // create 3 container reports for AM
  public static List<NMContainerStatus>
      createNMContainerStatusForApp(MockAM am) {
    List<NMContainerStatus> list =
        new ArrayList<NMContainerStatus>();
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    list.add(amContainer);
    list.add(runningContainer);
    list.add(completedContainer);
    return list;
  }

  private static final String R = "Default";
  private static final String A = "QueueA";
  private static final String B = "QueueB";
  private static final String B1 = "QueueB1";
  private static final String B2 = "QueueB2";
  //don't ever create the below queue ;-)
  private static final String QUEUE_DOESNT_EXIST = "NoSuchQueue";
  private static final String USER_1 = "user1";
  private static final String USER_2 = "user2";
  private static final String Q_R_PATH = CapacitySchedulerConfiguration.ROOT + "." + R;
  private static final String Q_A_PATH = Q_R_PATH + "." + A;
  private static final String Q_B_PATH = Q_R_PATH + "." + B;
  private static final String Q_B1_PATH = Q_B_PATH + "." + B1;
  private static final String Q_B2_PATH = Q_B_PATH + "." + B2;

  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath R_QUEUE_PATH = new QueuePath(Q_R_PATH);
  private static final QueuePath A_QUEUE_PATH = new QueuePath(Q_A_PATH);
  private static final QueuePath B_QUEUE_PATH = new QueuePath(Q_B_PATH);
  private static final QueuePath B1_QUEUE_PATH = new QueuePath(Q_B1_PATH);
  private static final QueuePath B2_QUEUE_PATH = new QueuePath(Q_B2_PATH);

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    conf.setQueues(ROOT, new String[] {R});
    conf.setCapacity(R_QUEUE_PATH, 100);
    conf.setQueues(R_QUEUE_PATH, new String[] {A, B});
    conf.setCapacity(A_QUEUE_PATH, 50);
    conf.setCapacity(B_QUEUE_PATH, 50);
    conf.setDouble(CapacitySchedulerConfiguration
      .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.5f);
  }
  
  private void setupQueueConfigurationOnlyA(
      CapacitySchedulerConfiguration conf) {
    conf.setQueues(ROOT, new String[] {R});
    conf.setCapacity(R_QUEUE_PATH, 100);
    conf.setQueues(R_QUEUE_PATH, new String[] {A});
    conf.setCapacity(A_QUEUE_PATH, 100);
    conf.setDouble(CapacitySchedulerConfiguration
      .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 1.0f);
  }

  private void setupQueueConfigurationChildOfB(CapacitySchedulerConfiguration conf) {
    conf.setQueues(ROOT, new String[] {R});
    conf.setCapacity(R_QUEUE_PATH, 100);
    conf.setQueues(R_QUEUE_PATH, new String[] {A, B});
    conf.setCapacity(A_QUEUE_PATH, 50);
    conf.setCapacity(B_QUEUE_PATH, 50);
    conf.setQueues(B_QUEUE_PATH, new String[] {B1, B2});
    conf.setCapacity(B1_QUEUE_PATH, 50);
    conf.setCapacity(B2_QUEUE_PATH, 50);
    conf.setDouble(CapacitySchedulerConfiguration
        .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.5f);
  }

  // 1. submit an app to default queue and let it finish
  // 2. restart rm with no default queue
  // 3. getApplicationReport call should succeed (with no NPE)
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testRMRestartWithRemovedQueue(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "");
    rm1 = new MockRM(conf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1")
            .withUser(USER_1)
            .withAcls(null)
            .build();
    final RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1,rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    csConf.setQueues(ROOT, new String[]{QUEUE_DOESNT_EXIST});
    final String noQueue = CapacitySchedulerConfiguration.ROOT + "." + QUEUE_DOESNT_EXIST;
    csConf.setCapacity(new QueuePath(noQueue), 100);
    rm2 = new MockRM(csConf, memStore);

    rm2.start();
    UserGroupInformation user2 = UserGroupInformation.createRemoteUser("user2");

    ApplicationReport report =
        user2.doAs(new PrivilegedExceptionAction<ApplicationReport>() {
          @Override
          public ApplicationReport run() throws Exception {
            return rm2.getApplicationReport(app1.getApplicationId());
          }
    });
    assertNotNull(report);
  }

  // Test CS recovery with multi-level queues and multi-users:
  // 1. setup 2 NMs each with 8GB memory;
  // 2. setup 2 level queues: Default -> (QueueA, QueueB)
  // 3. User1 submits 2 apps on QueueA
  // 4. User2 submits 1 app  on QueueB
  // 5. AM and each container has 1GB memory
  // 6. Restart RM.
  // 7. nm1 re-syncs back containers belong to user1
  // 8. nm2 re-syncs back containers belong to user2.
  // 9. Assert the parent queue and 2 leaf queues state and the metrics.
  // 10. Assert each user's consumption inside the queue.
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testCapacitySchedulerRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    if (getSchedulerType() != SchedulerType.CAPACITY) {
      return;
    }
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      DominantResourceCalculator.class.getName());
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    rm1 = new MockRM(csConf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1_1")
            .withUser(USER_1)
            .withAcls(null)
            .withQueue(A)
            .withUnmanagedAM(false)
            .build();
    RMApp app1_1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1_1 = MockRM.launchAndRegisterAM(app1_1, rm1, nm1);
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1_2")
            .withUser(USER_1)
            .withAcls(null)
            .withQueue(A)
            .withUnmanagedAM(false)
            .build();
    RMApp app1_2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1_2 = MockRM.launchAndRegisterAM(app1_2, rm1, nm2);

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app2")
            .withUser(USER_2)
            .withAcls(null)
            .withQueue(B)
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // clear queue metrics
    rm1.clearQueueMetrics(app1_1);
    rm1.clearQueueMetrics(app1_2);
    rm1.clearQueueMetrics(app2);

    csConf.set(PREFIX + "root.Default.QueueB.state", "STOPPED");

    // Re-start RM
    rm2 = new MockRM(csConf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());

    List<NMContainerStatus> am1_1Containers =
        createNMContainerStatusForApp(am1_1);
    List<NMContainerStatus> am1_2Containers =
        createNMContainerStatusForApp(am1_2);
    am1_1Containers.addAll(am1_2Containers);
    nm1.registerNode(am1_1Containers, null);

    List<NMContainerStatus> am2Containers =
        createNMContainerStatusForApp(am2);
    nm2.registerNode(am2Containers, null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1_1.getApplicationAttemptId());
    waitForNumContainersToRecover(2, rm2, am1_2.getApplicationAttemptId());
    waitForNumContainersToRecover(2, rm2, am2.getApplicationAttemptId());

    // Calculate each queue's resource usage.
    Resource containerResource = Resource.newInstance(1024, 1);
    Resource nmResource =
        Resource.newInstance(nm1.getMemory(), nm1.getvCores());
    Resource clusterResource = Resources.multiply(nmResource, 2);
    Resource q1Resource = Resources.multiply(clusterResource, 0.5);
    Resource q2Resource = Resources.multiply(clusterResource, 0.5);
    Resource q1UsedResource = Resources.multiply(containerResource, 4);
    Resource q2UsedResource = Resources.multiply(containerResource, 2);
    Resource totalUsedResource = Resources.add(q1UsedResource, q2UsedResource);
    Resource q1availableResources =
        Resources.subtract(q1Resource, q1UsedResource);
    Resource q2availableResources =
        Resources.subtract(q2Resource, q2UsedResource);
    Resource totalAvailableResource =
        Resources.add(q1availableResources, q2availableResources);

    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
          .getSchedulerApplications();
    SchedulerApplication schedulerApp1_1 =
        schedulerApps.get(app1_1.getApplicationId());

    // assert queue A state.
    checkCSLeafQueue(rm2, schedulerApp1_1, clusterResource, q1Resource,
      q1UsedResource, 4);
    QueueMetrics queue1Metrics = schedulerApp1_1.getQueue().getMetrics();
    assertMetrics(queue1Metrics, 2, 0, 2, 0, 4,
        q1availableResources.getMemorySize(),
        q1availableResources.getVirtualCores(), q1UsedResource.getMemorySize(),
        q1UsedResource.getVirtualCores());

    // assert queue B state.
    SchedulerApplication schedulerApp2 =
        schedulerApps.get(app2.getApplicationId());
    checkCSLeafQueue(rm2, schedulerApp2, clusterResource, q2Resource,
      q2UsedResource, 2);
    QueueMetrics queue2Metrics = schedulerApp2.getQueue().getMetrics();
    assertMetrics(queue2Metrics, 1, 0, 1, 0, 2,
        q2availableResources.getMemorySize(),
        q2availableResources.getVirtualCores(), q2UsedResource.getMemorySize(),
        q2UsedResource.getVirtualCores());

    // assert parent queue state.
    LeafQueue leafQueue = (LeafQueue) schedulerApp2.getQueue();
    ParentQueue parentQueue = (ParentQueue) leafQueue.getParent();
    checkParentQueue(parentQueue, 6, totalUsedResource, (float) 6 / 16,
      (float) 6 / 16);
    assertMetrics(parentQueue.getMetrics(), 3, 0, 3, 0, 6,
        totalAvailableResource.getMemorySize(),
        totalAvailableResource.getVirtualCores(), totalUsedResource.getMemorySize(),
        totalUsedResource.getVirtualCores());
  }

  private void verifyAppRecoveryWithWrongQueueConfig(
      CapacitySchedulerConfiguration csConf, RMApp app, String diagnostics,
      MockMemoryRMStateStore memStore, RMState state) throws Exception {
    // Restart RM with fail-fast as false. App should be killed.
    csConf.setBoolean(YarnConfiguration.RM_FAIL_FAST, false);
    csConf.setBoolean(CapacitySchedulerConfiguration.APP_FAIL_FAST, false);
    rm2 = new MockRM(csConf, memStore);
    rm2.start();

    MockMemoryRMStateStore memStore2 =
        (MockMemoryRMStateStore) rm2.getRMStateStore();

    // Wait for app to be killed.
    rm2.waitForState(app.getApplicationId(), RMAppState.KILLED);
    ApplicationReport report = rm2.getApplicationReport(app.getApplicationId());
    assertEquals(report.getFinalApplicationStatus(),
        FinalApplicationStatus.KILLED);
    assertThat(report.getYarnApplicationState()).
        isEqualTo(YarnApplicationState.KILLED);
    assertThat(report.getDiagnostics()).isEqualTo(diagnostics);

    //Reload previous state with cloned app sub context object
    RMState newState = memStore2.reloadStateWithClonedAppSubCtxt(state);

    // Remove updated app info(app being KILLED) from state store and reinstate
    // state store to previous state i.e. which indicates app is RUNNING.
    // This is to simulate app recovery with fail fast config as true.
    for(Map.Entry<ApplicationId, ApplicationStateData> entry :
        newState.getApplicationState().entrySet()) {
      ApplicationStateData appState = mock(ApplicationStateData.class);
      ApplicationSubmissionContext ctxt =
          mock(ApplicationSubmissionContext.class);
      when(appState.getApplicationSubmissionContext()).thenReturn(ctxt);
      when(ctxt.getApplicationId()).thenReturn(entry.getKey());
      memStore2.removeApplicationStateInternal(appState);
      memStore2.storeApplicationStateInternal(
          entry.getKey(), entry.getValue());
    }

    // Now restart RM with fail-fast as true. QueueException should be thrown.
    csConf.setBoolean(YarnConfiguration.RM_FAIL_FAST, true);
    csConf.setBoolean(CapacitySchedulerConfiguration.APP_FAIL_FAST, true);
    MockRM rm = new MockRM(csConf, memStore2);
    try {
      rm.start();
      fail("QueueException must have been thrown");
    } catch (QueueInvalidException e) {
    } finally {
      rm.close();
    }
  }

  //Test behavior of an app if queue is changed from leaf to parent during
  //recovery. Test case does following:
  //1. Add an app to QueueB and start the attempt.
  //2. Add 2 subqueues(QueueB1 and QueueB2) to QueueB, restart the RM, once with
  //   fail fast config as false and once with fail fast as true.
  //3. Verify that app was killed if fail fast is false.
  //4. Verify that QueueException was thrown if fail fast is true.
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testCapacityLeafQueueBecomesParentOnRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    if (getSchedulerType() != SchedulerType.CAPACITY) {
      return;
    }
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    rm1 = new MockRM(csConf);
    rm1.start();

    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm =
        new MockNM("127.1.1.1:4321", 8192, rm1.getResourceTrackerService());
    nm.registerNode();

    // Submit an app to QueueB.
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withUser(USER_2)
            .withAcls(null)
            .withQueue(B)
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm1, data);
    MockRM.launchAndRegisterAM(app, rm1, nm);
    assertEquals(rm1.getApplicationReport(app.getApplicationId()).
        getYarnApplicationState(), YarnApplicationState.RUNNING);

    // Take a copy of state store so that it can be reset to this state.
    RMState state = rm1.getRMStateStore().loadState();

    // Change scheduler config with child queues added to QueueB.
    csConf = new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationChildOfB(csConf);

    String diags = "Application killed on recovery as it was submitted to " +
        "queue QueueB which is no longer a leaf queue after restart.";
    verifyAppRecoveryWithWrongQueueConfig(csConf, app, diags,
        memStore, state);
  }

  //Test behavior of an app if queue is removed during recovery. Test case does
  //following:
  //1. Add some apps to two queues, attempt to add an app to a non-existant
  //   queue to verify that the new logic is not in effect during normal app
  //   submission
  //2. Remove one of the queues, restart the RM, once with fail fast config as
  //   false and once with fail fast as true.
  //3. Verify that app was killed if fail fast is false.
  //4. Verify that QueueException was thrown if fail fast is true.
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testCapacitySchedulerQueueRemovedRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    if (getSchedulerType() != SchedulerType.CAPACITY) {
      return;
    }
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
      DominantResourceCalculator.class.getName());
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfiguration(csConf);
    rm1 = new MockRM(csConf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1_1")
            .withUser(USER_1)
            .withAcls(null)
            .withQueue(A)
            .withUnmanagedAM(false)
            .build();
    RMApp app1_1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1_1 = MockRM.launchAndRegisterAM(app1_1, rm1, nm1);
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1_2")
            .withUser(USER_1)
            .withAcls(null)
            .withQueue(A)
            .withUnmanagedAM(false)
            .build();
    RMApp app1_2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am1_2 = MockRM.launchAndRegisterAM(app1_2, rm1, nm2);

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app2")
            .withUser(USER_2)
            .withAcls(null)
            .withQueue(B)
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);
    assertEquals(rm1.getApplicationReport(app2.getApplicationId()).
        getYarnApplicationState(), YarnApplicationState.RUNNING);

    //Submit an app with a non existant queue to make sure it does not
    //cause a fatal failure in the non-recovery case
    RMApp appNA = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app1_2")
            .withUser(USER_1)
            .withAcls(null)
            .withQueue(QUEUE_DOESNT_EXIST)
            .withWaitForAppAcceptedState(false)
            .build());

    // clear queue metrics
    rm1.clearQueueMetrics(app1_1);
    rm1.clearQueueMetrics(app1_2);
    rm1.clearQueueMetrics(app2);

    // Take a copy of state store so that it can be reset to this state.
    RMState state = memStore.loadState();

    // Set new configuration with QueueB removed.
    csConf = new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationOnlyA(csConf);

    String diags = "Application killed on recovery as it was submitted to " +
        "queue QueueB which no longer exists after restart.";
    verifyAppRecoveryWithWrongQueueConfig(csConf, app2, diags,
        memStore, state);
  }

  private void checkParentQueue(ParentQueue parentQueue, int numContainers,
      Resource usedResource, float UsedCapacity, float absoluteUsedCapacity) {
    assertEquals(numContainers, parentQueue.getNumContainers());
    assertEquals(usedResource, parentQueue.getUsedResources());
    assertEquals(UsedCapacity, parentQueue.getUsedCapacity(), 1e-8);
    assertEquals(absoluteUsedCapacity, parentQueue.getAbsoluteUsedCapacity(), 1e-8);
  }

  // Test RM shuts down, in the meanwhile, AM fails. Restarted RM scheduler
  // should not recover the containers that belong to the failed AM.
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAMfailedBetweenRMRestart(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.COMPLETE);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
      completedContainer), null);
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);

    YarnScheduler scheduler = rm2.getResourceScheduler();
    // Previous AM failed, The failed AM should once again release the
    // just-recovered containers.
    assertNull(scheduler.getRMContainer(runningContainer.getContainerId()));
    assertNull(scheduler.getRMContainer(completedContainer.getContainerId()));

    rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);

    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8192, rm2.getResourceTrackerService());
    NMContainerStatus previousAttemptContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 4,
          ContainerState.RUNNING);
    nm2.registerNode(Arrays.asList(previousAttemptContainer), null);
    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);
    // check containers from previous failed attempt should not be recovered.
    assertNull(scheduler.getRMContainer(previousAttemptContainer.getContainerId()));
  }

  // Apps already completed before RM restart. Restarted RM scheduler should not
  // recover containers for completed apps.
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testContainersNotRecoveredForCompletedApps(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    rm1 = new MockRM(conf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
          ContainerState.COMPLETE);
    nm1.registerNode(Arrays.asList(runningContainer, completedContainer), null);
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    assertEquals(RMAppState.FINISHED, recoveredApp1.getState());

    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);

    YarnScheduler scheduler = rm2.getResourceScheduler();

    // scheduler should not recover containers for finished apps.
    assertNull(scheduler.getRMContainer(runningContainer.getContainerId()));
    assertNull(scheduler.getRMContainer(completedContainer.getContainerId()));
  }

  @Timeout(600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAppReregisterOnRMWorkPreservingRestart(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);

    // start RM
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am0 = MockRM.launchAM(app0, rm1, nm1);
    // Issuing registerAppAttempt() before and after RM restart to confirm
    // registerApplicationMaster() is idempotent.
    am0.registerAppAttempt();

    // start new RM
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    rm2.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.LAUNCHED);

    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    // retry registerApplicationMaster() after RM restart.
    am0.registerAppAttempt(true);

    rm2.waitForState(app0.getApplicationId(), RMAppState.RUNNING);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.RUNNING);
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAMContainerStatusWithRMRestart(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1_1 = MockRMAppSubmitter.submitWithMemory(1024, rm1);
    MockAM am1_1 = MockRM.launchAndRegisterAM(app1_1, rm1, nm1);
    
    RMAppAttempt attempt0 = app1_1.getCurrentAppAttempt();
    YarnScheduler scheduler = rm1.getResourceScheduler();

    assertTrue(scheduler.getRMContainer(
        attempt0.getMasterContainer().getId()).isAMContainer());

    // Re-start RM
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    List<NMContainerStatus> am1_1Containers =
        createNMContainerStatusForApp(am1_1);
    nm1.registerNode(am1_1Containers, null);

    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am1_1.getApplicationAttemptId());

    scheduler = rm2.getResourceScheduler();
    assertTrue(scheduler.getRMContainer(
        attempt0.getMasterContainer().getId()).isAMContainer());
  }

  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testRecoverSchedulerAppAndAttemptSynchronously(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    // start RM
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // scheduler app/attempt is immediately available after RM is re-started.
    assertNotNull(rm2.getResourceScheduler().getSchedulerAppInfo(
      am0.getApplicationAttemptId()));

    // getTransferredContainers should not throw NPE.
    rm2.getResourceScheduler()
      .getTransferredContainers(am0.getApplicationAttemptId());

    List<NMContainerStatus> containers = createNMContainerStatusForApp(am0);
    nm1.registerNode(containers, null);
    waitForNumContainersToRecover(2, rm2, am0.getApplicationAttemptId());
  }

  // Test if RM on recovery receives the container release request from AM
  // before it receives the container status reported by NM for recovery. this
  // container should not be recovered.
  @Timeout(50)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testReleasedContainerNotRecovered(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    rm1 = new MockRM(conf);
    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    rm1.start();

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(1024, rm1);
    final MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Re-start RM
    conf.setInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS, 8000);
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);

    // try to release a container before the container is actually recovered.
    final ContainerId runningContainer =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    am1.allocate(null, Arrays.asList(runningContainer));

    // send container statuses to recover the containers
    List<NMContainerStatus> containerStatuses =
        createNMContainerStatusForApp(am1);
    nm1.registerNode(containerStatuses, null);

    // only the am container should be recovered.
    waitForNumContainersToRecover(1, rm2, am1.getApplicationAttemptId());

    final AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    // cached release request is cleaned.
    // assertFalse(scheduler.getPendingRelease().contains(runningContainer));

    AllocateResponse response = am1.allocate(null, null);
    // AM gets notified of the completed container.
    boolean receivedCompletedContainer = false;
    for (ContainerStatus status : response.getCompletedContainersStatuses()) {
      if (status.getContainerId().equals(runningContainer)) {
        receivedCompletedContainer = true;
      }
    }
    assertTrue(receivedCompletedContainer);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        // release cache is cleaned up and previous running container is not
        // recovered
        return scheduler.getApplicationAttempt(am1.getApplicationAttemptId())
          .getPendingRelease().isEmpty()
            && scheduler.getRMContainer(runningContainer) == null;
      }
    }, 1000, 20000);
  }

  private void assertMetrics(QueueMetrics qm, int appsSubmitted,
      int appsPending, int appsRunning, int appsCompleted,
      int allocatedContainers, long availableMB, long availableVirtualCores,
      long allocatedMB, long allocatedVirtualCores) {
    assertEquals(appsSubmitted, qm.getAppsSubmitted());
    assertEquals(appsPending, qm.getAppsPending());
    assertEquals(appsRunning, qm.getAppsRunning());
    assertEquals(appsCompleted, qm.getAppsCompleted());
    assertEquals(allocatedContainers, qm.getAllocatedContainers());
    assertEquals(availableMB, qm.getAvailableMB());
    assertEquals(availableVirtualCores, qm.getAvailableVirtualCores());
    assertEquals(allocatedMB, qm.getAllocatedMB());
    assertEquals(allocatedVirtualCores, qm.getAllocatedVirtualCores());
  }

  public static void waitForNumContainersToRecover(int num, MockRM rm,
      ApplicationAttemptId attemptId) throws Exception {
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    SchedulerApplicationAttempt attempt =
        scheduler.getApplicationAttempt(attemptId);
    while (attempt == null) {
      System.out.println("Wait for scheduler attempt " + attemptId
          + " to be created");
      Thread.sleep(200);
      attempt = scheduler.getApplicationAttempt(attemptId);
    }
    while (attempt.getLiveContainers().size() < num) {
      System.out.println("Wait for " + num
          + " containers to recover. currently: "
          + attempt.getLiveContainers().size());
      Thread.sleep(200);
    }
  }

  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testNewContainersNotAllocatedDuringSchedulerRecovery(SchedulerType type)
      throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setLong(
      YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 4000);
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Restart RM
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode();
    ControlledClock clock = new ControlledClock();
    long startTime = System.currentTimeMillis();
    ((RMContextImpl)rm2.getRMContext()).setSystemClock(clock);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);
    rm2.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // AM request for new containers
    am1.allocate("127.0.0.1", 1000, 1, new ArrayList<ContainerId>());

    List<Container> containers = new ArrayList<Container>();
    clock.setTime(startTime + 2000);
    nm1.nodeHeartbeat(true);

    // sleep some time as allocation happens asynchronously.
    Thread.sleep(3000);
    containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
      new ArrayList<ContainerId>()).getAllocatedContainers());
    // container is not allocated during scheduling recovery.
    assertTrue(containers.isEmpty());

    clock.setTime(startTime + 8000);
    nm1.nodeHeartbeat(true);
    // Container is created after recovery is done.
    while (containers.isEmpty()) {
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
  }

  /**
   * Testing to confirm that retried finishApplicationMaster() doesn't throw
   * InvalidApplicationMasterRequest before and after RM restart.
   */
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testRetriedFinishApplicationMasterRequest(SchedulerType type)
      throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    // start RM
    rm1 = new MockRM(conf);
    rm1.start();

    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am0 = MockRM.launchAM(app0, rm1, nm1);

    am0.registerAppAttempt();

    // Emulating following a scenario:
    // RM1 saves the app in RMStateStore and then crashes,
    // FinishApplicationMasterResponse#isRegistered still return false,
    // so AM still retry the 2nd RM
    MockRM.finishAMAndVerifyAppState(app0, rm1, nm1, am0);


    // start new RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();

    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am0.unregisterAppAttempt(false);
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAppFailedToRenewTokenOnRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    UserGroupInformation.setConfiguration(conf);
    MockRM rm1 = new TestSecurityMockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    MockRM rm2 = new TestSecurityMockRM(conf, rm1.getRMStateStore()) {
      protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer() {
          @Override
          public void addApplicationSync(ApplicationId applicationId,
              Credentials ts, boolean shouldCancelAtEnd, String user)
              throws IOException {
            throw new IOException("Token renew failed !!");
          }
        };
      }
    };
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
    NMContainerStatus containerStatus =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(containerStatus), null);

    // am re-register
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt(true);
    rm2.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // Because the token expired, am could crash.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm2.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
  }

  /**
   * Test validateAndCreateResourceRequest fails on recovery, app should ignore
   * this Exception and continue
   */
  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAppFailToValidateResourceRequestOnRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Change the config so that validateAndCreateResourceRequest throws
    // exception on recovery
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 50);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 100);

    rm2 = new MockRM(conf, rm1.getRMStateStore());
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
  }

  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testContainerCompleteMsgNotLostAfterAMFailedAndRMRestart(SchedulerType type)
      throws Exception {
    initTestWorkPreservingRMRestart(type);
    rm1 = new MockRM(conf);
    rm1.start();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();

    // submit app with keepContainersAcrossApplicationAttempts true
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(200);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithResource(resource, rm1)
        .withKeepContainers(true)
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .build();
    RMApp app0 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    am0.allocate("127.0.0.1", 1000, 2, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    List<Container> conts = am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() < 2) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(100);
    }

    // am failed,and relaunch it
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am1 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    // rm failover
    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // container launched by first am completed
    NMContainerStatus amContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 1,
          ContainerState.RUNNING);
    NMContainerStatus completedContainer=
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 2,
          ContainerState.COMPLETE);
    NMContainerStatus runningContainer =
        TestRMRestart.createNMContainerStatus(am0.getApplicationAttemptId(), 3,
          ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(amContainer, runningContainer,
        completedContainer), null);
    Thread.sleep(200);

    // check whether current am could get containerCompleteMsg
    RMApp recoveredApp0 =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp0.getCurrentAppAttempt();
    assertEquals(1,loadedAttempt1.getJustFinishedContainers().size());
  }

  // Test that if application state was saved, but attempt state was not saved.
  // RM should start correctly.
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAppStateSavedButAttemptStateNotSaved(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    MockMemoryRMStateStore memStore = new MockMemoryRMStateStore() {
      @Override public synchronized void updateApplicationAttemptStateInternal(
          ApplicationAttemptId appAttemptId,
          ApplicationAttemptStateData attemptState) {
        // do nothing;
        // simulate the failure that attempt final state is not saved.
      }
    };
    memStore.init(conf);
    rm1 = new MockRM(conf, memStore);
    rm1.start();

    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am1);

    ApplicationStateData appSavedState =
        memStore.getState().getApplicationState().get(app1.getApplicationId());

    // check that app state is  saved.
    assertEquals(RMAppState.FINISHED, appSavedState.getState());
    // check that attempt state is not saved.
    assertNull(appSavedState.getAttempt(am1.getApplicationAttemptId()).getState());

    rm2 = new MockRM(conf, memStore);
    rm2.start();
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());

    assertEquals(RMAppState.FINISHED, recoveredApp1.getState());
    // check that attempt state is recovered correctly.
    assertEquals(RMAppAttemptState.FINISHED, recoveredApp1.getCurrentAppAttempt().getState());
  }

  @Timeout(600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUAMRecoveryOnRMWorkPreservingRestart(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);

    // start RM
    rm1 = new MockRM(conf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    QueueMetrics qm1 = rm1.getResourceScheduler().getRootQueueMetrics();
    assertUnmanagedAMQueueMetrics(qm1, 0, 0, 0, 0);
    // create app and launch the UAM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withUnmanagedAM(true)
            .build();
    RMApp app0 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am0 = MockRM.launchUAM(app0, rm1, nm1);
    am0.registerAppAttempt();
    assertUnmanagedAMQueueMetrics(qm1, 1, 1, 0, 0);

    // Allocate containers to UAM
    int numContainers = 2;
    am0.allocate("127.0.0.1", 1000, numContainers,
        new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    List<Container> conts = am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() < 2) {
      nm1.nodeHeartbeat(true);
      conts.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(100);
    }
    assertUnmanagedAMQueueMetrics(qm1, 1, 0, 1, 0);

    // start new RM
    rm2 = new MockRM(conf, memStore);
    rm2.start();
    MockMemoryRMStateStore memStore2 =
        (MockMemoryRMStateStore) rm2.getRMStateStore();
    QueueMetrics qm2 = rm2.getResourceScheduler().getRootQueueMetrics();
    rm2.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.LAUNCHED);
    // recover app
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    assertUnmanagedAMQueueMetrics(qm2, 1, 1, 0, 0);
    RMApp recoveredApp =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    NMContainerStatus container1 = TestRMRestart
        .createNMContainerStatus(am0.getApplicationAttemptId(), 1,
            ContainerState.RUNNING);
    NMContainerStatus container2 = TestRMRestart
        .createNMContainerStatus(am0.getApplicationAttemptId(), 2,
            ContainerState.RUNNING);
    nm1.registerNode(Arrays.asList(container1, container2), null);
    // Wait for RM to settle down on recovering containers;
    waitForNumContainersToRecover(2, rm2, am0.getApplicationAttemptId());

    // retry registerApplicationMaster() after RM restart.
    am0.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am0.registerAppAttempt(true);
    assertUnmanagedAMQueueMetrics(qm2, 1, 0, 1, 0);

    // Check if UAM is correctly recovered on restart
    rm2.waitForState(app0.getApplicationId(), RMAppState.RUNNING);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // Check if containers allocated to UAM are recovered
    Map<ApplicationId, SchedulerApplication> schedulerApps =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
            .getSchedulerApplications();
    SchedulerApplication schedulerApp =
        schedulerApps.get(recoveredApp.getApplicationId());
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertEquals(numContainers,
        schedulerAttempt.getLiveContainers().size());

    // Check if UAM is able to heart beat
    assertNotNull(am0.doHeartbeat());
    assertUnmanagedAMQueueMetrics(qm2, 1, 0, 1, 0);

    // Complete the UAM
    am0.unregisterAppAttempt(false);
    rm2.waitForState(am0.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
    rm2.waitForState(app0.getApplicationId(), RMAppState.FINISHED);
    assertEquals(FinalApplicationStatus.SUCCEEDED,
        recoveredApp.getFinalApplicationStatus());
    assertUnmanagedAMQueueMetrics(qm2, 1, 0, 0, 1);

    // Restart RM once more to check UAM is not re-run
    MockRM rm3 = new MockRM(conf, memStore2);
    rm3.start();
    recoveredApp = rm3.getRMContext().getRMApps().get(app0.getApplicationId());
    QueueMetrics qm3 = rm3.getResourceScheduler().getRootQueueMetrics();
    assertEquals(RMAppState.FINISHED, recoveredApp.getState());
    assertUnmanagedAMQueueMetrics(qm2, 1, 0, 0, 1);
  }

  //   Test behavior of an app if two same name leaf queue with different queuePath
  //   during work preserving rm restart with %specified mapping Placement Rule.
  //   Test case does following:
  //1. Submit an apps to queue root.joe.test.
  //2. While the applications is running, restart the rm and
  //   check whether the app submitted to the queue it was submitted initially.
  //3. Verify that application running successfully.
  @Timeout(60)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testQueueRecoveryOnRMWorkPreservingRestart(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    if (getSchedulerType() != SchedulerType.CAPACITY) {
      return;
    }
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
    final QueuePath defaultPath = new QueuePath(ROOT + "." + "default");
    final QueuePath joe = new QueuePath(ROOT + "." + "joe");
    final QueuePath john = new QueuePath(ROOT + "." + "john");

    csConf.setQueues(ROOT, new String[] {"default", "joe", "john"});
    csConf.setCapacity(joe, 25);
    csConf.setCapacity(john, 25);
    csConf.setCapacity(defaultPath, 50);

    csConf.setQueues(joe, new String[] {"test"});
    csConf.setQueues(john, new String[] {"test"});
    csConf.setCapacity(new QueuePath(joe.getFullPath(), "test"), 100);
    csConf.setCapacity(new QueuePath(john.getFullPath(), "test"), 100);

    csConf.set(CapacitySchedulerConfiguration.MAPPING_RULE_JSON,
        "{\"rules\" : [{\"type\": \"user\", \"policy\" : \"specified\", " +
        "\"fallbackResult\" : \"skip\", \"matches\" : \"*\"}]}");

    // start RM
    rm1 = new MockRM(csConf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMContext newMockRMContext = rm1.getRMContext();
    newMockRMContext.setQueuePlacementManager(TestAppManager.createMockPlacementManager(
        "user1|user2", "test", "root.joe"));

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm1)
            .withAppName("app")
            .withQueue("root.joe.test")
            .withUser("user1")
            .withAcls(null)
            .build();

    RMApp app = MockRMAppSubmitter.submit(rm1, data);
    MockAM am = MockRM.launchAndRegisterAM(app, rm1, nm1);
    rm1.waitForState(app.getApplicationId(), RMAppState.RUNNING);

    MockRM rm2 = new MockRM(csConf, memStore) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new RMAppManager(this.rmContext, this.scheduler,
            this.masterService, this.applicationACLsManager, conf) {
          @Override
          ApplicationPlacementContext placeApplication(
              PlacementManager placementManager,
              ApplicationSubmissionContext context, String user,
              boolean isRecovery) throws YarnException {
            return super.placeApplication(
                    newMockRMContext.getQueuePlacementManager(), context, user, isRecovery);
          }
        };
      }
    };

    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    rm2.start();
    RMApp recoveredApp0 =
        rm2.getRMContext().getRMApps().get(app.getApplicationId());

    rm2.waitForState(recoveredApp0.getApplicationId(), RMAppState.ACCEPTED);
    am.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am.registerAppAttempt(true);
    rm2.waitForState(recoveredApp0.getApplicationId(), RMAppState.RUNNING);

    assertEquals("root.joe.test", recoveredApp0.getQueue());
  }

  private void assertUnmanagedAMQueueMetrics(QueueMetrics qm, int appsSubmitted,
      int appsPending, int appsRunning, int appsCompleted) {
    assertEquals(appsSubmitted, qm.getUnmanagedAppsSubmitted());
    assertEquals(appsPending, qm.getUnmanagedAppsPending());
    assertEquals(appsRunning, qm.getUnmanagedAppsRunning());
    assertEquals(appsCompleted, qm.getUnmanagedAppsCompleted());
  }


  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUnknownUserOnRecovery(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the UAM
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withUnmanagedAM(true)
            .build();
    RMApp app0 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am0 = MockRM.launchUAM(app0, rm1, nm1);
    am0.registerAppAttempt();
    rm1.killApp(app0.getApplicationId());
    PlacementManager placementMgr = mock(PlacementManager.class);
    doThrow(new YarnException("No groups for user")).when(placementMgr)
        .placeApplication(any(ApplicationSubmissionContext.class),
            any(String.class));
    MockRM rm2 = new MockRM(conf, memStore) {
      @Override
      protected RMAppManager createRMAppManager() {
        return new RMAppManager(this.rmContext, this.scheduler,
            this.masterService, this.applicationACLsManager, conf) {
          @Override
          ApplicationPlacementContext placeApplication(
              PlacementManager placementManager,
              ApplicationSubmissionContext context, String user,
              boolean isRecovery) throws YarnException {
            return super
                .placeApplication(placementMgr, context, user, isRecovery);
          }
        };
      }
    };
    rm2.start();
    RMApp recoveredApp =
        rm2.getRMContext().getRMApps().get(app0.getApplicationId());
    assertEquals(RMAppState.KILLED, recoveredApp.getState());
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDynamicFlexibleAutoCreatedQueueRecoveryWithDefaultQueue(
      SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    //if queue name is not specified, it should submit to 'default' queue
    testDynamicAutoCreatedQueueRecovery(USER1, null, true);
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDynamicAutoCreatedQueueRecoveryWithDefaultQueue(
      SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    //if queue name is not specified, it should submit to 'default' queue
    testDynamicAutoCreatedQueueRecovery(USER1, null, false);
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDynamicFlexibleAutoCreatedQueueRecoveryWithOverrideQueueMappingFlag(
      SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    testDynamicAutoCreatedQueueRecovery(USER1, USER1, true);
  }

  @Timeout(30)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDynamicAutoCreatedQueueRecoveryWithOverrideQueueMappingFlag(
      SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    testDynamicAutoCreatedQueueRecovery(USER1, USER1, false);
  }

  // Test work preserving recovery of apps running on auto-created queues.
  // This involves:
  // 1. Setting up a dynamic auto-created queue,
  // 2. Submitting an app to it,
  // 3. Failing over RM,
  // 4. Validating that the app is recovered post failover,
  // 5. Check if all running containers are recovered,
  // 6. Verify the scheduler state like attempt info,
  // 7. Verify the queue/user metrics for the dynamic auto-created queue.

  public void testDynamicAutoCreatedQueueRecovery(
      String user, String queueName, boolean useFlexibleAQC)
      throws Exception {
    conf.setBoolean(CapacitySchedulerConfiguration.ENABLE_USER_METRICS, true);
    conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    conf.setBoolean(YarnConfiguration.RM_FAIL_FAST, true);

    // 1. Set up dynamic auto-created queue.
    CapacitySchedulerConfiguration schedulerConf = null;
    if (queueName == null || queueName.equals(DEFAULT_QUEUE)) {
      schedulerConf = getSchedulerAutoCreatedQueueConfiguration(false, useFlexibleAQC);
    } else{
      schedulerConf = getSchedulerAutoCreatedQueueConfiguration(true, useFlexibleAQC);
    }
    int containerMemory = 1024;
    Resource containerResource = Resource.newInstance(containerMemory, 1);

    rm1 = new MockRM(schedulerConf);
    rm1.start();
    MockNM nm1 = new MockNM("127.0.0.1:1234", 8192,
        rm1.getResourceTrackerService());
    nm1.registerNode();
    // 2. submit app to queue which is auto-created.
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppName("autoCreatedQApp")
            .withUser(user)
            .withAcls(null)
            .withQueue(queueName)
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    Resource amResources = app1.getAMResourceRequests().get(0).getCapability();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // clear queue metrics
    rm1.clearQueueMetrics(app1);

    // 3. Fail over (restart) RM.
    rm2 = new MockRM(schedulerConf, rm1.getRMStateStore());
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    // 4. Validate app is recovered post failover.
    RMApp recoveredApp1 = rm2.getRMContext().getRMApps().get(
        app1.getApplicationId());
    RMAppAttempt loadedAttempt1 = recoveredApp1.getCurrentAppAttempt();
    NMContainerStatus amContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    NMContainerStatus runningContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    NMContainerStatus completedContainer =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
            ContainerState.COMPLETE);

    nm1.registerNode(
        Arrays.asList(amContainer, runningContainer, completedContainer), null);

    // Wait for RM to settle down on recovering containers.
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
            .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // 5. Check RMContainers are re-recreated and the container state is
    // correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForContainerToComplete(loadedAttempt1, completedContainer);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm2.getResourceScheduler();
    SchedulerNode schedulerNode1 = scheduler.getSchedulerNode(nm1.getNodeId());

    // ********* check scheduler node state.*******
    // 2 running containers.
    Resource usedResources = Resources.multiply(containerResource, 2);
    Resource nmResource = Resource.newInstance(nm1.getMemory(),
        nm1.getvCores());

    assertTrue(schedulerNode1.isValidContainer(amContainer.getContainerId()));
    assertTrue(
        schedulerNode1.isValidContainer(runningContainer.getContainerId()));
    assertFalse(
        schedulerNode1.isValidContainer(completedContainer.getContainerId()));
    // 2 launched containers, 1 completed container
    assertEquals(2, schedulerNode1.getNumContainers());

    assertEquals(Resources.subtract(nmResource, usedResources),
        schedulerNode1.getUnallocatedResource());
    assertEquals(usedResources, schedulerNode1.getAllocatedResource());
    //    Resource availableResources = Resources.subtract(nmResource,
    // usedResources);

    // 6. Verify the scheduler state like attempt info.
    Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> sa =
        ((AbstractYarnScheduler) rm2.getResourceScheduler())
            .getSchedulerApplications();
    SchedulerApplication<SchedulerApplicationAttempt> schedulerApp = sa.get(
        recoveredApp1.getApplicationId());

    // 7. Verify the queue/user metrics for the dynamic reservable queue.
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      checkCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
    }

    // *********** check scheduler attempt state.********
    SchedulerApplicationAttempt schedulerAttempt =
        schedulerApp.getCurrentAppAttempt();
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(amContainer.getContainerId())));
    assertTrue(schedulerAttempt.getLiveContainers()
        .contains(scheduler.getRMContainer(runningContainer.getContainerId())));
    assertThat(schedulerAttempt.getCurrentConsumption()).
        isEqualTo(usedResources);

    // *********** check appSchedulingInfo state ***********
    assertEquals((1L << 40) + 1L, schedulerAttempt.getNewContainerId());
  }

  // Apps already completed before RM restart. Make sure we restore the queue
  // correctly
  @Timeout(20)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testFairSchedulerCompletedAppsQueue(SchedulerType type) throws Exception {
    initTestWorkPreservingRMRestart(type);
    if (getSchedulerType() != SchedulerType.FAIR) {
      return;
    }

    rm1 = new MockRM(conf);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app = MockRMAppSubmitter.submitWithMemory(200, rm1);
    MockAM am1 = MockRM.launchAndRegisterAM(app, rm1, nm1);
    MockRM.finishAMAndVerifyAppState(app, rm1, nm1, am1);

    String fsQueueContext = app.getApplicationSubmissionContext().getQueue();
    String fsQueueApp = app.getQueue();
    assertEquals(fsQueueApp,
        fsQueueContext, "Queue in app not equal to submission context");
    RMAppAttempt rmAttempt = app.getCurrentAppAttempt();
    assertNotNull(rmAttempt, "No AppAttempt found");

    rm2 = new MockRM(conf, rm1.getRMStateStore());
    rm2.start();

    RMApp recoveredApp =
        rm2.getRMContext().getRMApps().get(app.getApplicationId());
    RMAppAttempt rmAttemptRecovered = recoveredApp.getCurrentAppAttempt();
    assertNotNull(rmAttemptRecovered, "No AppAttempt found after recovery");
    String fsQueueContextRecovered =
        recoveredApp.getApplicationSubmissionContext().getQueue();
    String fsQueueAppRecovered = recoveredApp.getQueue();
    assertEquals(RMAppState.FINISHED, recoveredApp.getState());
    assertEquals(fsQueueAppRecovered, fsQueueContextRecovered,
        "Recovered app queue is not the same as context queue");
  }
}
