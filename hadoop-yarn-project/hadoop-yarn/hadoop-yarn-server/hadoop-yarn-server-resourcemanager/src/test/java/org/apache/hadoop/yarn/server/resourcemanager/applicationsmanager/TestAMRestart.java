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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockMemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.Records;

import org.apache.hadoop.thirdparty.com.google.common.base.Throwables;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test AM restart functions.
 */
public class TestAMRestart extends ParameterizedSchedulerTestBase {

  public void initTestAMRestart(SchedulerType type) throws IOException {
    initParameterizedSchedulerTestBase(type);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 30)
  @SuppressWarnings("checkstyle:methodlength")
  public void testAMRestartWithExistingContainers(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("name")
        .withUser("user")
        .withAcls(new HashMap<ApplicationAccessType, String>())
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(-1)
        .withCredentials(null)
        .withAppType("MAPREDUCE")
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(true)
        .build();
    RMApp app1 =
        MockRMAppSubmitter.submit(rm1, data);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockNM nm2 =
        new MockNM("127.0.0.1:2351", 4089, rm1.getResourceTrackerService());
    nm2.registerNode();

    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    int NUM_CONTAINERS = 3;
    allocateContainers(nm1, am1, NUM_CONTAINERS);

    // launch the 2nd container, for testing running container transferred.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // launch the 3rd container, for testing container allocated by previous
    // attempt is completed by the next new attempt/
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.RUNNING);
    ContainerId containerId3 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    rm1.waitForState(nm1, containerId3, RMContainerState.RUNNING);

    // 4th container still in AQUIRED state. for testing Acquired container is
    // always killed.
    ContainerId containerId4 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 4);
    rm1.waitForState(nm1, containerId4, RMContainerState.ACQUIRED);

    // 5th container is in Allocated state. for testing allocated container is
    // always killed.
    am1.allocate("127.0.0.1", 1024, 1, new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);
    ContainerId containerId5 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 5);
    rm1.waitForState(nm1, containerId5, RMContainerState.ALLOCATED);

    // 6th container is in Reserved state.
    am1.allocate("127.0.0.1", 6000, 1, new ArrayList<ContainerId>());
    ContainerId containerId6 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 6);
    nm1.nodeHeartbeat(true);
    SchedulerApplicationAttempt schedulerAttempt =
        ((AbstractYarnScheduler) rm1.getResourceScheduler())
          .getCurrentAttemptForContainer(containerId6);
    while (schedulerAttempt.getReservedContainers().isEmpty()) {
      System.out.println("Waiting for container " + containerId6
          + " to be reserved.");
      nm1.nodeHeartbeat(true);
      Thread.sleep(200);
    }
    // assert containerId6 is reserved.
    assertEquals(containerId6, schedulerAttempt.getReservedContainers()
      .get(0).getContainerId());

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);

    // wait for some time. previous AM's running containers should still remain
    // in scheduler even though am failed
    Thread.sleep(3000);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);
    // acquired/allocated containers are cleaned up.
    assertNull(rm1.getResourceScheduler().getRMContainer(containerId4));
    assertNull(rm1.getResourceScheduler().getRMContainer(containerId5));

    // wait for app to start a new attempt.
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    // assert this is a new AM.
    ApplicationAttemptId newAttemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    assertFalse(newAttemptId.equals(am1.getApplicationAttemptId()));

    // launch the new AM
    MockAM am2 = rm1.launchAM(app1, rm1, nm1);
    RegisterApplicationMasterResponse registerResponse =
        am2.registerAppAttempt();


    // Assert two containers are running: container2 and container3;
    assertEquals(2, registerResponse.getContainersFromPreviousAttempts()
      .size());
    boolean containerId2Exists = false, containerId3Exists = false;
    for (Container container : registerResponse
      .getContainersFromPreviousAttempts()) {
      if (container.getId().equals(containerId2)) {
        containerId2Exists = true;
      }
      if (container.getId().equals(containerId3)) {
        containerId3Exists = true;
      }
    }
    assertTrue(containerId2Exists && containerId3Exists);
    rm1.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    // complete container by sending the container complete event which has earlier
    // attempt's attemptId
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.COMPLETE);

    // Even though the completed container containerId3 event was sent to the
    // earlier failed attempt, new RMAppAttempt can also capture this container
    // info.
    // completed containerId4 is also transferred to the new attempt.
    RMAppAttempt newAttempt =
        app1.getRMAppAttempt(am2.getApplicationAttemptId());
    // 4 containers finished, acquired/allocated/reserved/completed.
    waitForContainersToFinish(4, newAttempt);
    boolean container3Exists = false, container4Exists = false, container5Exists =
        false, container6Exists = false;
    for(ContainerStatus status :  newAttempt.getJustFinishedContainers()) {
      if(status.getContainerId().equals(containerId3)) {
        // containerId3 is the container ran by previous attempt but finished by the
        // new attempt.
        container3Exists = true;
      }
      if (status.getContainerId().equals(containerId4)) {
        // containerId4 is the Acquired Container killed by the previous attempt,
        // it's now inside new attempt's finished container list.
        container4Exists = true;
      }
      if (status.getContainerId().equals(containerId5)) {
        // containerId5 is the Allocated container killed by previous failed attempt.
        container5Exists = true;
      }
      if (status.getContainerId().equals(containerId6)) {
        // containerId6 is the reserved container killed by previous failed attempt.
        container6Exists = true;
      }
    }
    assertTrue(container3Exists && container4Exists && container5Exists
        && container6Exists);

    // New SchedulerApplicationAttempt also has the containers info.
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // record the scheduler attempt for testing.
    SchedulerApplicationAttempt schedulerNewAttempt =
        ((AbstractYarnScheduler) rm1.getResourceScheduler())
          .getCurrentAttemptForContainer(containerId2);
    // finish this application
    MockRM.finishAMAndVerifyAppState(app1, rm1, nm1, am2);

    // the 2nd attempt released the 1st attempt's running container, when the
    // 2nd attempt finishes.
    assertFalse(schedulerNewAttempt.getLiveContainers().contains(
      containerId2));
    // all 4 normal containers finished.
    System.out.println("New attempt's just finished containers: "
        + newAttempt.getJustFinishedContainers());
    waitForContainersToFinish(5, newAttempt);
    rm1.stop();
  }

  public static List<Container> allocateContainers(MockNM nm1, MockAM am1,
      int NUM_CONTAINERS) throws Exception {
    // allocate NUM_CONTAINERS containers
    am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
      new ArrayList<ContainerId>());
    nm1.nodeHeartbeat(true);

    // wait for containers to be allocated.
    List<Container> containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    while (containers.size() != NUM_CONTAINERS) {
      nm1.nodeHeartbeat(true);
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }

    assertEquals(NUM_CONTAINERS, containers.size(),
        "Did not get all containers allocated");
    return containers;
  }

  private void waitForContainersToFinish(int expectedNum, RMAppAttempt attempt)
      throws InterruptedException {
    int count = 0;
    while (attempt.getJustFinishedContainers().size() < expectedNum
        && count < 500) {
      Thread.sleep(100);
      count++;
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 30)
  public void testNMTokensRebindOnAMRestart(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 3);
    // To prevent test from blacklisting nm1 for AM, we sit threshold to half
    // of 2 nodes which is 1
    getConf().setFloat(
        YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD,
        0.5f);

    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("myname")
        .withUser("myuser")
        .withAcls(new HashMap<ApplicationAccessType, String>())
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(-1)
        .withCredentials(null)
        .withAppType("MAPREDUCE")
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(true)
        .build();
    RMApp app1 =
        MockRMAppSubmitter.submit(rm1, data);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockNM nm2 =
        new MockNM("127.1.1.1:4321", 8000, rm1.getResourceTrackerService());
    nm2.registerNode();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    List<Container> containers = new ArrayList<Container>();
    // nmTokens keeps track of all the nmTokens issued in the allocate call.
    List<NMToken> expectedNMTokens = new ArrayList<NMToken>();

    // am1 allocate 2 container on nm1.
    // first container
    while (true) {
      AllocateResponse response =
          am1.allocate("127.0.0.1", 2000, 2,
            new ArrayList<ContainerId>());
      nm1.nodeHeartbeat(true);
      containers.addAll(response.getAllocatedContainers());
      expectedNMTokens.addAll(response.getNMTokens());
      if (containers.size() == 2) {
        break;
      }
      Thread.sleep(200);
      System.out.println("Waiting for container to be allocated.");
    }
    // launch the container-2
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);
    // launch the container-3
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 3, ContainerState.RUNNING);
    ContainerId containerId3 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    rm1.waitForState(nm1, containerId3, RMContainerState.RUNNING);
    
    // fail am1
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // restart the am
    MockAM am2 = MockRM.launchAM(app1, rm1, nm1);
    RegisterApplicationMasterResponse registerResponse =
        am2.registerAppAttempt();
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // check am2 get the nm token from am1.
    assertEquals(expectedNMTokens.size(),
        registerResponse.getNMTokensFromPreviousAttempts().size());
    for (int i = 0; i < expectedNMTokens.size(); i++) {
      assertTrue(expectedNMTokens.get(i)
          .equals(registerResponse.getNMTokensFromPreviousAttempts().get(i)));
    }

    // am2 allocate 1 container on nm2
    containers = new ArrayList<Container>();
    while (true) {
      AllocateResponse allocateResponse =
          am2.allocate("127.1.1.1", 4000, 1,
            new ArrayList<ContainerId>());
      nm2.nodeHeartbeat(true);
      containers.addAll(allocateResponse.getAllocatedContainers());
      expectedNMTokens.addAll(allocateResponse.getNMTokens());
      if (containers.size() == 1) {
        break;
      }
      Thread.sleep(200);
      System.out.println("Waiting for container to be allocated.");
    }
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId am2ContainerId2 =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, am2ContainerId2, RMContainerState.RUNNING);

    // fail am2.
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // restart am
    MockAM am3 = MockRM.launchAM(app1, rm1, nm1);
    registerResponse = am3.registerAppAttempt();
    rm1.waitForState(am3.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // check am3 get the NM token from both am1 and am2;
    List<NMToken> transferredTokens = registerResponse.getNMTokensFromPreviousAttempts();
    assertEquals(2, transferredTokens.size());
    assertTrue(transferredTokens.containsAll(expectedNMTokens));
    rm1.stop();
  }

  /**
   * AM container preempted, nm disk failure
   * should not be counted towards AM max retry count.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 100)
  public void testShouldNotCountFailureToMaxAttemptRetry(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm1.getResourceScheduler();
    ContainerId amContainer =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    // Preempt the next attempt;
    scheduler.killContainer(scheduler.getRMContainer(amContainer));

    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am1.getApplicationAttemptId());

    assertFalse(attempt1.shouldCountTowardsMaxAttemptRetry());
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    ApplicationStateData appState =
        ((MemoryRMStateStore) rm1.getRMStateStore()).getState()
            .getApplicationState().get(app1.getApplicationId());

    // AM should be restarted even though max-am-attempt is 1.
    MockAM am2 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();

    // Preempt the second attempt.
    ContainerId amContainer2 =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 1);
    scheduler.killContainer(scheduler.getRMContainer(amContainer2));

    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am2.getApplicationAttemptId());

    assertFalse(attempt2.shouldCountTowardsMaxAttemptRetry());
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am3 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 3, nm1);
    RMAppAttempt attempt3 = app1.getCurrentAppAttempt();

    // mimic NM disk_failure
    ContainerStatus containerStatus = Records.newRecord(ContainerStatus.class);
    containerStatus.setContainerId(attempt3.getMasterContainer().getId());
    containerStatus.setDiagnostics("mimic NM disk_failure");
    containerStatus.setState(ContainerState.COMPLETE);
    containerStatus.setExitStatus(ContainerExitStatus.DISKS_FAILED);
    Map<ApplicationId, List<ContainerStatus>> conts =
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(app1.getApplicationId(),
      Collections.singletonList(containerStatus));
    nm1.nodeHeartbeat(conts, true);

    rm1.waitForState(am3.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am3.getApplicationAttemptId());

    assertFalse(attempt3.shouldCountTowardsMaxAttemptRetry());
    assertEquals(ContainerExitStatus.DISKS_FAILED,
      appState.getAttempt(am3.getApplicationAttemptId())
        .getAMContainerExitStatus());

    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am4 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 4, nm1);
    RMAppAttempt attempt4 = app1.getCurrentAppAttempt();

    // create second NM, and register to rm1
    MockNM nm2 =
        new MockNM("127.0.0.1:2234", 8000, rm1.getResourceTrackerService());
    nm2.registerNode();
    // nm1 heartbeats to report unhealthy
    // This will mimic ContainerExitStatus.ABORT
    nm1.nodeHeartbeat(false);
    rm1.waitForState(am4.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am4.getApplicationAttemptId());

    assertFalse(attempt4.shouldCountTowardsMaxAttemptRetry());
    assertEquals(ContainerExitStatus.ABORTED,
      appState.getAttempt(am4.getApplicationAttemptId())
        .getAMContainerExitStatus());
    // launch next AM in nm2
    MockAM am5 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 5, nm2);
    RMAppAttempt attempt5 = app1.getCurrentAppAttempt();
    // fail the AM normally
    nm2
      .nodeHeartbeat(am5.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am5.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am5.getApplicationAttemptId());

    assertTrue(attempt5.shouldCountTowardsMaxAttemptRetry());

    // launch next AM in nm2
    MockAM am6 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 6, nm2);
    RMAppAttempt attempt6 = app1.getCurrentAppAttempt();

    // fail the AM normally
    nm2
      .nodeHeartbeat(am6.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am6.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am6.getApplicationAttemptId());

    assertTrue(attempt6.shouldCountTowardsMaxAttemptRetry());

    // AM should not be restarted.
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    assertEquals(6, app1.getAppAttempts().size());
    rm1.stop();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 100)
  public void testMaxAttemptOneMeansOne(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm1.getResourceScheduler();
    ContainerId amContainer =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    // Preempt the attempt;
    scheduler.killContainer(scheduler.getRMContainer(amContainer));

    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am1.getApplicationAttemptId());

    // AM should not be restarted.
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    assertEquals(1, app1.getAppAttempts().size());
    rm1.stop();
  }

  /**
   * Test RM restarts after AM container is preempted, new RM should not count
   * AM preemption failure towards the max-retry-account and should be able to
   * re-launch the AM.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 60)
  public void testPreemptedAMRestartOnRMRestart(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);

    getConf().set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(getConf());
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm1.getResourceScheduler();
    ContainerId amContainer =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);

    // fail the AM normally
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1,
        ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am1.getApplicationAttemptId());
    assertTrue(attempt1.shouldCountTowardsMaxAttemptRetry());

    // wait for the next AM to start
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am2 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();

    // Forcibly preempt the am container;
    amContainer = ContainerId.newContainerId(am2.getApplicationAttemptId(), 1);
    scheduler.killContainer(scheduler.getRMContainer(amContainer));

    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    assertFalse(attempt2.shouldCountTowardsMaxAttemptRetry());
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // state store has 2 attempts stored.
    ApplicationStateData appState =
        memStore.getState().getApplicationState().get(app1.getApplicationId());
    assertEquals(2, appState.getAttemptCount());
    if (getSchedulerType().equals(SchedulerType.FAIR)) {
      // attempt stored has the preempted container exit status.
      assertEquals(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
          appState.getAttempt(am2.getApplicationAttemptId())
              .getAMContainerExitStatus());
    } else {
      // attempt stored has the preempted container exit status.
      assertEquals(ContainerExitStatus.PREEMPTED,
          appState.getAttempt(am2.getApplicationAttemptId())
              .getAMContainerExitStatus());
    }
    // Restart rm.
    MockRM rm2 = new MockRM(getConf(), memStore);
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode();
    rm2.start();

    // Restarted RM should re-launch the am.
    MockAM am3 =
        rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm2, nm1, am3);
    RMAppAttempt attempt3 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId())
          .getCurrentAppAttempt();
    assertTrue(attempt3.shouldCountTowardsMaxAttemptRetry());
    assertEquals(ContainerExitStatus.INVALID,
        appState.getAttempt(am3.getApplicationAttemptId())
            .getAMContainerExitStatus());
    rm1.stop();
    rm2.stop();
  }

  /**
   * Test regular RM restart/failover, new RM should not count
   * AM failure towards the max-retry-account and should be able to
   * re-launch the AM.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 50)
  public void testRMRestartOrFailoverNotCountedForAMFailures(SchedulerType type)
      throws Exception {
    initTestAMRestart(type);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);

    getConf().set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    // explicitly set max-am-retry count as 2.
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(getConf());
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm1.getResourceScheduler();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(200, rm1);
    // AM should be restarted even though max-am-attempt is 1.
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();

    // fail the AM normally
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1,
        ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am1.getApplicationAttemptId());
    assertTrue(attempt1.shouldCountTowardsMaxAttemptRetry());

    // wait for the next AM to start
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am2 =
        rm1.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 2, nm1);
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();

    // Restart rm.
    MockRM rm2 = new MockRM(getConf(), memStore);
    rm2.start();
    ApplicationStateData appState =
        memStore.getState().getApplicationState().get(app1.getApplicationId());
    // re-register the NM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus status = Records.newRecord(NMContainerStatus.class);
    status
      .setContainerExitStatus(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER);
    status.setContainerId(attempt2.getMasterContainer().getId());
    status.setContainerState(ContainerState.COMPLETE);
    status.setDiagnostics("");
    nm1.registerNode(Collections.singletonList(status), null);

    rm2.waitForState(attempt2.getAppAttemptId(), RMAppAttemptState.FAILED);
    assertEquals(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
        appState.getAttempt(am2.getApplicationAttemptId())
            .getAMContainerExitStatus());
    // Will automatically start a new AppAttempt in rm2
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    MockAM am3 =
        rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 3, nm1);
    MockRM.finishAMAndVerifyAppState(app1, rm2, nm1, am3);
    RMAppAttempt attempt3 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId())
          .getCurrentAppAttempt();
    assertTrue(attempt3.shouldCountTowardsMaxAttemptRetry());
    assertEquals(ContainerExitStatus.INVALID,
        appState.getAttempt(am3.getApplicationAttemptId())
            .getAMContainerExitStatus());

    rm1.stop();
    rm2.stop();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 120)
  public void testRMAppAttemptFailuresValidityInterval(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);

    getConf().set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    // explicitly set max-am-retry count as 2.
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    MockRM rm1 = new MockRM(getConf());
    rm1.start();

    MockMemoryRMStateStore memStore =
        (MockMemoryRMStateStore) rm1.getRMStateStore();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();

    // set window size to a larger number : 60s
    // we will verify the app should be failed if
    // two continuous attempts failed in 60s.
    RMApp app = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
            .withKeepContainers(false)
            .withAttemptFailuresValidityInterval((long) 60000)
            .build());
    
    MockAM am = MockRM.launchAM(app, rm1, nm1);
    // Fail current attempt normally
    nm1.nodeHeartbeat(am.getApplicationAttemptId(),
      1, ContainerState.COMPLETE);
    rm1.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // launch the second attempt
    rm1.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(2, app.getAppAttempts().size());

    MockAM am_2 = MockRM.launchAndRegisterAM(app, rm1, nm1);
    rm1.waitForState(am_2.getApplicationAttemptId(), RMAppAttemptState.RUNNING);
    nm1.nodeHeartbeat(am_2.getApplicationAttemptId(),
      1, ContainerState.COMPLETE);
    rm1.waitForState(am_2.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    // current app should be failed.
    rm1.waitForState(app.getApplicationId(), RMAppState.FAILED);

    ControlledClock clock = new ControlledClock();
    // set window size to 10s
    RMAppImpl app1 = (RMAppImpl) MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
            .withKeepContainers(false)
            .withAttemptFailuresValidityInterval((long) 10000)
            .build());
    app1.setSystemClock(clock);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    // Fail attempt1 normally
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(),
      1, ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    //Wait to make sure attempt1 be removed in State Store
    //TODO explore a better way than sleeping for a while (YARN-4929)
    Thread.sleep(15 * 1000);

    // launch the second attempt
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(2, app1.getAppAttempts().size());

    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();
    MockAM am2 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // wait for 10 seconds
    clock.setTime(System.currentTimeMillis() + 10*1000);
    // Fail attempt2 normally
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(),
      1, ContainerState.COMPLETE);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);

    // can launch the third attempt successfully
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(3, app1.getAppAttempts().size());
    RMAppAttempt attempt3 = app1.getCurrentAppAttempt();
    clock.reset();
    MockAM am3 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    rm1.waitForState(am3.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // Restart rm.
    @SuppressWarnings("resource")
    MockRM rm2 = new MockRM(getConf(), memStore);
    rm2.start();

    MockMemoryRMStateStore memStore1 =
        (MockMemoryRMStateStore) rm2.getRMStateStore();
    ApplicationStateData app1State =
        memStore1.getState().getApplicationState().
        get(app1.getApplicationId());
    assertEquals(1, app1State.getFirstAttemptId());

    // re-register the NM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus status = Records.newRecord(NMContainerStatus.class);
    status
      .setContainerExitStatus(ContainerExitStatus.KILLED_BY_RESOURCEMANAGER);
    status.setContainerId(attempt3.getMasterContainer().getId());
    status.setContainerState(ContainerState.COMPLETE);
    status.setDiagnostics("");
    nm1.registerNode(Collections.singletonList(status), null);

    rm2.waitForState(attempt3.getAppAttemptId(), RMAppAttemptState.FAILED);
    //Wait to make sure attempt3 be removed in State Store
    //TODO explore a better way than sleeping for a while (YARN-4929)
    Thread.sleep(15 * 1000);
    assertEquals(2, app1State.getAttemptCount());

    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    // Lauch Attempt 4
    MockAM am4 =
        rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 4, nm1);

    // wait for 10 seconds
    clock.setTime(System.currentTimeMillis() + 10*1000);
    // Fail attempt4 normally
    nm1
      .nodeHeartbeat(am4.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm2.waitForState(am4.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    assertEquals(2, app1State.getAttemptCount());

    // can launch the 5th attempt successfully
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);

    MockAM am5 =
        rm2.waitForNewAMToLaunchAndRegister(app1.getApplicationId(), 5, nm1);
    clock.reset();
    rm2.waitForState(am5.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // Fail attempt5 normally
    nm1
      .nodeHeartbeat(am5.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm2.waitForState(am5.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    assertEquals(2, app1State.getAttemptCount());

    rm2.waitForState(app1.getApplicationId(), RMAppState.FAILED);
    rm1.stop();
    rm2.stop();
  }

  private boolean isContainerIdInContainerStatus(
      List<ContainerStatus> containerStatuses, ContainerId containerId) {
    for (ContainerStatus status : containerStatuses) {
      if (status.getContainerId().equals(containerId)) {
        return true;
      }
    }
    return false;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 40)
  public void testAMRestartNotLostContainerCompleteMsg(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("name")
        .withUser("user")
        .withAcls(new HashMap<ApplicationAccessType, String>())
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(-1)
        .withCredentials(null)
        .withAppType("MAPREDUCE")
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(true)
        .build();
    RMApp app1 =
        MockRMAppSubmitter.submit(rm1, data);
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 10240, rm1.getResourceTrackerService());
    nm1.registerNode();

    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    allocateContainers(nm1, am1, 1);

    nm1.nodeHeartbeat(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // container complete
    nm1.nodeHeartbeat(
        am1.getApplicationAttemptId(), 2, ContainerState.COMPLETE);
    rm1.waitForState(nm1, containerId2, RMContainerState.COMPLETED);

    // make sure allocate() get complete container,
    // before this msg pass to AM, AM may crash
    while (true) {
      AllocateResponse response = am1.allocate(
          new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      List<ContainerStatus> containerStatuses =
          response.getCompletedContainersStatuses();
      if (isContainerIdInContainerStatus(
          containerStatuses, containerId2) == false) {
        Thread.sleep(100);
        continue;
      }

      // is containerId still in justFinishedContainer?
      containerStatuses =
          app1.getCurrentAppAttempt().getJustFinishedContainers();
      if (isContainerIdInContainerStatus(containerStatuses,
          containerId2)) {
        fail();
      }
      break;
    }

    // fail the AM by sending CONTAINER_FINISHED event without registering.
    nm1.nodeHeartbeat(
        am1.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);

    // wait for app to start a new attempt.
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    // assert this is a new AM.
    ApplicationAttemptId newAttemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    assertFalse(newAttemptId.equals(am1.getApplicationAttemptId()));

    // launch the new AM
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();
    MockAM am2 = rm1.launchAndRegisterAM(app1, rm1, nm1);

    // whether new AM could get container complete msg
    AllocateResponse allocateResponse = am2.allocate(
        new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
    List<ContainerStatus> containerStatuses =
        allocateResponse.getCompletedContainersStatuses();
    if (isContainerIdInContainerStatus(containerStatuses,
        containerId2) == false) {
      fail();
    }
    containerStatuses = attempt2.getJustFinishedContainers();
    if (isContainerIdInContainerStatus(containerStatuses, containerId2)) {
      fail();
    }

    // the second allocate should not get container complete msg
    allocateResponse = am2.allocate(
        new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
    containerStatuses =
        allocateResponse.getCompletedContainersStatuses();
    if (isContainerIdInContainerStatus(containerStatuses, containerId2)) {
      fail();
    }

    rm1.stop();
  }

  /**
   * Test restarting AM launched with the KeepContainers and AM reset window.
   * after AM reset window, even if AM who was the last is failed,
   * all containers are launched by previous AM should be kept.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 20)
  public void testAMRestartNotLostContainerAfterAttemptFailuresValidityInterval(SchedulerType type)
      throws Exception {
    initTestAMRestart(type);
    // explicitly set max-am-retry count as 2.
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);

    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockNM nm1 =
            new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();

    // set window size to 10s and enable keepContainers
    RMAppImpl app1 = (RMAppImpl) MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
            .withKeepContainers(true)
            .withAttemptFailuresValidityInterval((long) 10000)
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    int NUM_CONTAINERS = 2;
    allocateContainers(nm1, am1, NUM_CONTAINERS);

    // launch the 2nd container, for testing running container transferred.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);
    ContainerId containerId2 =
            ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // Fail attempt1 normally
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(),
            1, ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);

    // launch the second attempt
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(2, app1.getAppAttempts().size());

    // It will be the last attempt.
    RMAppAttempt attempt2 = app1.getCurrentAppAttempt();
    MockAM am2 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.RUNNING);

    // wait for 10 seconds to reset AM failure count
    Thread.sleep(10 * 1000);

    // Fail attempt2 normally
    nm1.nodeHeartbeat(am2.getApplicationAttemptId(),
            1, ContainerState.COMPLETE);
    rm1.waitForState(am2.getApplicationAttemptId(), RMAppAttemptState.FAILED);

    // can launch the third attempt successfully
    rm1.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(3, app1.getAppAttempts().size());
    MockAM am3 = rm1.launchAM(app1, rm1, nm1);
    RegisterApplicationMasterResponse registerResponse =
            am3.registerAppAttempt();

    // keepContainers is applied, even if attempt2 was the last attempt.
    assertEquals(1, registerResponse.getContainersFromPreviousAttempts()
            .size());
    boolean containerId2Exists = false;
    Container container = registerResponse.getContainersFromPreviousAttempts().get(0);
    if (container.getId().equals(containerId2)) {
      containerId2Exists = true;
    }
    assertTrue(containerId2Exists);

    rm1.waitForState(app1.getApplicationId(), RMAppState.RUNNING);
    rm1.stop();
  }

  /**
   * Test to verify that the containers of previous attempt are returned in
   * the RM response to the heartbeat of AM if these containers were not
   * recovered by the time AM registered.
   *
   * 1. App is started with 2 containers running on 2 different nodes-
   *    container 2 on the NM1 node and container 3 on the NM2 node.
   * 2. Fail the AM of the application.
   * 3. Simulate RM restart.
   * 4. NM1 connects to the restarted RM immediately. It sends the RM the status
   *    of container 2.
   * 5. 2nd attempt of the app is launched and the app master registers with RM.
   * 6. Verify that app master receives container 2 in the RM response to
   *    register request.
   * 7. NM2 connects to the RM after a delay. It sends the RM the status of
   *    container 3.
   * 8. Verify that the app master receives container 3 in the RM response to
   *    its heartbeat.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 200)
  public void testContainersFromPreviousAttemptsWithRMRestart(SchedulerType type)
      throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf().setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    getConf().setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
    getConf()
        .set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    MockRM rm1 = new MockRM(getConf());
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    YarnScheduler scheduler = rm1.getResourceScheduler();

    String nm1Address = "127.0.0.1:1234";
    MockNM nm1 = new MockNM(nm1Address, 10240, rm1.getResourceTrackerService());
    nm1.registerNode();

    String nm2Address = "127.0.0.1:2351";
    MockNM nm2 = new MockNM(nm2Address, 4089, rm1.getResourceTrackerService());
    nm2.registerNode();

    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
        .withAppName("name")
        .withUser("user")
        .withAcls(new HashMap<>())
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(-1)
        .withCredentials(null)
        .withAppType("MAPREDUCE")
        .withWaitForAppAcceptedState(false)
        .withKeepContainers(true)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);

    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    allocateContainers(nm1, am1, 1);
    allocateContainers(nm2, am1, 1);

    // container 2 launched and running on node 1
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
        ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    // container 3 launched and running node 2
    nm2.nodeHeartbeat(am1.getApplicationAttemptId(), 3,
        ContainerState.RUNNING);
    ContainerId containerId3 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    rm1.waitForState(nm2, containerId3, RMContainerState.RUNNING);

    // fail the AM normally
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 1,
        ContainerState.COMPLETE);
    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(
        (AbstractYarnScheduler)scheduler, am1.getApplicationAttemptId());

    // restart rm
    MockRM rm2 = new MockRM(getConf(), memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus container2Status =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
            ContainerState.RUNNING);
    nm1.registerNode(Lists.newArrayList(container2Status), null);


    // Wait for RM to settle down on recovering containers;
    Thread.sleep(3000);
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
        ContainerState.RUNNING);
    rm2.waitForState(nm1, containerId2, RMContainerState.RUNNING);
    assertNotNull(rm2.getResourceScheduler()
        .getRMContainer(containerId2));

    // wait for app to start a new attempt.
    rm2.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    // assert this is a new AM.
    ApplicationAttemptId newAttemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    assertFalse(newAttemptId.equals(am1.getApplicationAttemptId()));

    // launch the new AM
    MockAM am2 = MockRM.launchAMWhenAsyncSchedulingEnabled(app1, rm2);
    RegisterApplicationMasterResponse registerResponse =
        am2.registerAppAttempt();

    // container2 is recovered from previous attempt
    assertEquals(1,
        registerResponse.getContainersFromPreviousAttempts().size());
    assertEquals(containerId2, registerResponse.getContainersFromPreviousAttempts().get(0).getId(),
        "container 2");
    List<NMToken> prevNMTokens = registerResponse
        .getNMTokensFromPreviousAttempts();
    assertEquals(1, prevNMTokens.size());
    // container 2 is running on node 1
    assertEquals(nm1Address, prevNMTokens.get(0).getNodeId().toString());

    rm2.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    //NM2 is back
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());
    NMContainerStatus container3Status =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 3,
            ContainerState.RUNNING);
    nm2.registerNode(Lists.newArrayList(container3Status), null);

    nm2.nodeHeartbeat(am1.getApplicationAttemptId(), 3,
        ContainerState.RUNNING);
    rm2.waitForState(nm2, containerId3, RMContainerState.RUNNING);
    assertNotNull(rm2.getResourceScheduler()
        .getRMContainer(containerId3));

    List<Container> containersFromPreviousAttempts = new ArrayList<>();
    GenericTestUtils.waitFor(() -> {
      try {
        AllocateResponse allocateResponse = am2.doHeartbeat();
        if (allocateResponse.getContainersFromPreviousAttempts().size() > 0){
          containersFromPreviousAttempts.addAll(
              allocateResponse.getContainersFromPreviousAttempts());
          assertEquals(0, allocateResponse.getAllocatedContainers().size(),
              "new containers should not be allocated");
          List<NMToken> nmTokens = allocateResponse.getNMTokens();
          assertEquals(1, nmTokens.size());
          // container 3 is running on node 2
          assertEquals(nm2Address,
              nmTokens.get(0).getNodeId().toString());
          return true;
        }
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
      return false;
    }, 2000, 200000);
    assertEquals(containerId3,
        containersFromPreviousAttempts.get(0).getId(), "container 3");
    rm2.stop();
    rm1.stop();
  }

  /**
   * Test to verify that there is no queue resource leak after app fail.
   *
   * 1. Submit an app which is configured to keep containers across app
   *    attempts and should fail after AM finished (am-max-attempts=1).
   * 2. App is started with 2 containers running on NM1 node.
   * 3. Preempt the AM of the application which should not count towards max
   *    attempt retry but app will fail immediately.
   * 4. Verify that the used resource of queue should be cleaned up normally
   *    after app fail.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(value = 30)
  public void testQueueResourceDoesNotLeak(SchedulerType type) throws Exception {
    initTestAMRestart(type);
    getConf().setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    getConf().setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    getConf()
        .set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    MockRM rm1 = new MockRM(getConf());
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8000, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app1 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAttemptFailuresValidityInterval(0)
            .withKeepContainers(true)
            .build());
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    allocateContainers(nm1, am1, 1);

    // launch the 2nd container, for testing running container transferred.
    nm1.nodeHeartbeat(am1.getApplicationAttemptId(), 2,
        ContainerState.RUNNING);
    ContainerId containerId2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm1.waitForState(nm1, containerId2, RMContainerState.RUNNING);

    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm1.getResourceScheduler();
    ContainerId amContainer =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    // Preempt AM container
    scheduler.killContainer(scheduler.getRMContainer(amContainer));

    rm1.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.FAILED);
    TestSchedulerUtils.waitSchedulerApplicationAttemptStopped(scheduler,
        am1.getApplicationAttemptId());

    assertFalse(attempt1.shouldCountTowardsMaxAttemptRetry());

    // AM should not be restarted.
    rm1.waitForState(app1.getApplicationId(), RMAppState.FAILED);

    // After app1 failed, used resource of this queue should
    // be cleaned up, otherwise resource leak happened.
    if (getSchedulerType() == SchedulerType.CAPACITY) {
      LeafQueue queue =
          (LeafQueue) ((CapacityScheduler) scheduler).getQueue("default");
      assertEquals(0,
          queue.getQueueResourceUsage().getUsed().getMemorySize());
      assertEquals(0,
          queue.getQueueResourceUsage().getUsed().getVirtualCores());
    } else if (getSchedulerType() == SchedulerType.FAIR) {
      // The default queue is not auto created after YARN-7769 so
      // user-as-default-queue option is used
      Collection<FSLeafQueue> queues = ((FairScheduler) scheduler)
          .getQueueManager().getLeafQueues();
      assertEquals(1, queues.size());

      FSLeafQueue queue = queues.iterator().next();
      assertEquals(0, queue.getResourceUsage().getMemorySize());
      assertEquals(0, queue.getResourceUsage().getVirtualCores());
    }

    rm1.stop();
  }
}
