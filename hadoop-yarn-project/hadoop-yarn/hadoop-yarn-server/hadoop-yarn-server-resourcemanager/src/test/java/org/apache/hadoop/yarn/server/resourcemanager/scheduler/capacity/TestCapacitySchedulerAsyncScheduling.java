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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.contrib.java.lang.system.internal.NoExitSecurityManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestCapacitySchedulerAsyncScheduling {
  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  private NMHeartbeatThread nmHeartbeatThread = null;

  private static final String POLICY_CLASS_NAME =
          "org.apache.hadoop.yarn.server.resourcemanager.scheduler" +
                  ".placement.ResourceUsageMultiNodeLookupPolicy";

  @BeforeEach
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  @Timeout(value = 300)
  public void testSingleThreadAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(1);
  }

  @Test
  @Timeout(value = 300)
  public void testTwoThreadsAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(2);
  }

  @Test
  @Timeout(value = 300)
  public void testThreeThreadsAsyncContainerAllocation() throws Exception {
    testAsyncContainerAllocation(3);
  }

  @Test
  @Timeout(value = 300)
  public void testAsyncContainerAllocationWithMultiNode() throws Exception {
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
            "resource-based");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
            "resource-based");
    String policyName =
            CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
                    + ".resource-based" + ".class";
    conf.set(policyName, POLICY_CLASS_NAME);
    conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
            true);
    testAsyncContainerAllocation(2);
  }

  @Test
  @Timeout(value = 300)
  public void testAsyncThreadNames() throws Exception {
    conf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        1);
    conf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 0);
    final RMNodeLabelsManager mg = new NullRMNodeLabelsManager();
    mg.init(conf);

    // inject node label manager
    MockRM rm = new MockRM(TestUtils.getConfigurationWithMultipleQueues(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mg;
      }
    };

    rm.getRMContext().setNodeLabelManager(mg);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    for (CapacityScheduler.AsyncScheduleThread thread :
        cs.getAsyncSchedulerThreads()) {
      assertTrue(thread.getName()
          .startsWith("AsyncCapacitySchedulerThread"));
    }
  }

  public void testAsyncContainerAllocation(int numThreads) throws Exception {
    conf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        numThreads);
    conf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 0);
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    // inject node label manager
    MockRM rm = new MockRM(TestUtils.getConfigurationWithMultipleQueues(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    List<MockNM> nms = new ArrayList<>();
    // Add 10 nodes to the cluster, in the cluster we have 200 GB resource
    for (int i = 0; i < 10; i++) {
      nms.add(rm.registerNode("127.0.0." + i + ":1234", 20 * GB));
    }

    keepNMHeartbeat(nms, 1000);

    List<MockAM> ams = new ArrayList<>();
    // Add 3 applications to the cluster, one app in one queue
    // the i-th app ask (20 * i) containers. So in total we will have
    // 123G container allocated
    int totalAsked = 3 * GB; // 3 AMs

    for (int i = 0; i < 3; i++) {
      RMApp rmApp = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppName("app")
              .withUser("user")
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(Character.toString((char) (i % 34 + 97)))
              .withMaxAppAttempts(1)
              .withCredentials(null)
              .withAppType(null)
              .withWaitForAppAcceptedState(false)
              .build());
      MockAM am = MockRM.launchAMWhenAsyncSchedulingEnabled(rmApp, rm);
      am.registerAppAttempt();
      ams.add(am);
    }

    for (int i = 0; i < 3; i++) {
      ams.get(i).allocate("*", 1024, 20 * (i + 1), new ArrayList<>());
      totalAsked += 20 * (i + 1) * GB;
    }
    // Wait for at most 15000 ms
    int waitTime = 15000; // ms
    while (waitTime > 0) {
      if (rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB()
          == totalAsked) {
        break;
      }
      Thread.sleep(50);
      waitTime -= 50;
    }

    assertEquals(
        rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB(),
        totalAsked);

    // Wait for another 2 sec to make sure we will not allocate more than
    // required
    waitTime = 2000; // ms
    while (waitTime > 0) {
      assertEquals(
          rm.getResourceScheduler().getRootQueueMetrics().getAllocatedMB(),
          totalAsked);
      waitTime -= 50;
      Thread.sleep(50);
    }

    rm.close();
  }

  // Testcase for YARN-6714
  @Test
  @Timeout(value = 30)
  public void testCommitProposalForFailedAppAttempt()
      throws Exception {
    // disable async-scheduling for simulating complex since scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs & Nodes
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("192.168.0.1:1234", 9 * GB);
    final MockNM nm2 = rm.registerNode("192.168.0.2:2234", 9 * GB);
    List<MockNM> nmLst = new ArrayList<>();
    nmLst.add(nm1);
    nmLst.add(nm2);

    // init scheduler & nodes
    while (
        ((CapacityScheduler) rm.getRMContext().getScheduler()).getNodeTracker()
            .nodeCount() < 2) {
      Thread.sleep(10);
    }
    assertEquals(2,
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getNodeTracker().nodeCount());
    CapacityScheduler scheduler =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode sn1 = scheduler.getSchedulerNode(nm1.getNodeId());
    SchedulerNode sn2 = scheduler.getSchedulerNode(nm2.getNodeId());

    // launch app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
        .withAppName("app")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    FiCaSchedulerApp schedulerApp =
        scheduler.getApplicationAttempt(am.getApplicationAttemptId());

    // allocate and launch 1 containers and running on nm2
    allocateAndLaunchContainers(am, nm2, rm, 1,
        Resources.createResource(5 * GB), 0, 2);

    // nm1 runs 1 container(app1-container_01/AM)
    // nm2 runs 1 container(app1-container_02)
    assertEquals(1, sn1.getNumContainers());
    assertEquals(1, sn2.getNumContainers());

    // kill app attempt1
    scheduler.handle(
        new AppAttemptRemovedSchedulerEvent(am.getApplicationAttemptId(),
            RMAppAttemptState.KILLED, true));
    // wait until app attempt1 removed on nm1
    while (sn1.getCopiedListOfRunningContainers().size() == 1) {
      Thread.sleep(100);
    }
    // wait until app attempt2 launched on nm1
    while (sn1.getCopiedListOfRunningContainers().size() == 0) {
      nm1.nodeHeartbeat(true);
      Thread.sleep(100);
    }

    // generate reserved proposal of stopped app attempt
    // and it could be committed for async-scheduling
    // this kind of proposal should be skipped
    Resource reservedResource = Resources.createResource(5 * GB);
    Container container = Container.newInstance(
        ContainerId.newContainerId(am.getApplicationAttemptId(), 3),
        sn2.getNodeID(), sn2.getHttpAddress(), reservedResource,
        Priority.newInstance(0), null);
    RMContainer rmContainer = new RMContainerImpl(container, SchedulerRequestKey
        .create(ResourceRequest
            .newInstance(Priority.newInstance(0), "*", reservedResource, 1)),
        am.getApplicationAttemptId(), sn2.getNodeID(), "user",
        rm.getRMContext());
    SchedulerContainer reservedContainer =
        new SchedulerContainer(schedulerApp, scheduler.getNode(sn2.getNodeID()),
            rmContainer, "", false);
    ContainerAllocationProposal reservedForAttempt1Proposal =
        new ContainerAllocationProposal(reservedContainer, null,
            reservedContainer, NodeType.OFF_SWITCH, NodeType.OFF_SWITCH,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, reservedResource);
    List<ContainerAllocationProposal> reservedProposals = new ArrayList<>();
    reservedProposals.add(reservedForAttempt1Proposal);
    ResourceCommitRequest request =
        new ResourceCommitRequest(null, reservedProposals, null);
    scheduler.tryCommit(scheduler.getClusterResource(), request, true);
    assertNull(sn2.getReservedContainer(),
        "Outdated proposal should not be accepted!");

    rm.stop();
  }

  // Testcase for YARN-6678
  @Test
  @Timeout(value = 30)
  @SuppressWarnings("checkstyle:methodlength")
  public void testCommitOutdatedReservedProposal() throws Exception {
    // disable async-scheduling for simulating complex since scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs & Nodes
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("127.0.0.1:1234", 9 * GB);
    final MockNM nm2 = rm.registerNode("127.0.0.2:2234", 9 * GB);

    // init scheduler nodes
    int waitTime = 1000;
    while (waitTime > 0 &&
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getNodeTracker().nodeCount() < 2) {
      waitTime -= 10;
      Thread.sleep(10);
    }
    assertEquals(2,
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getNodeTracker().nodeCount());

    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    final SchedulerNode sn1 =
        ((CapacityScheduler) scheduler).getSchedulerNode(nm1.getNodeId());
    final SchedulerNode sn2 =
        ((CapacityScheduler) scheduler).getSchedulerNode(nm2.getNodeId());

    // submit app1, am1 is running on nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data1);
    final MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    // submit app2, am2 is running on nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
    final MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

    // allocate and launch 2 containers for app1
    allocateAndLaunchContainers(am, nm1, rm, 1,
        Resources.createResource(5 * GB), 0, 2);
    allocateAndLaunchContainers(am, nm2, rm, 1,
        Resources.createResource(5 * GB), 0, 3);

    // nm1 runs 3 containers(app1-container_01/AM, app1-container_02,
    //                       app2-container_01/AM)
    // nm2 runs 1 container(app1-container_03)
    assertEquals(3, sn1.getNumContainers());
    assertEquals(1, sn2.getNumContainers());

    // reserve 1 container(app1-container_04) for app1 on nm1
    ResourceRequest rr2 = ResourceRequest
        .newInstance(Priority.newInstance(0), "*",
            Resources.createResource(5 * GB), 1);
    am.allocate(Arrays.asList(rr2), null);
    nm1.nodeHeartbeat(true);
    // wait app1-container_04 reserved on nm1
    waitTime = 1000;
    while (waitTime > 0 && sn1.getReservedContainer() == null) {
      waitTime -= 10;
      Thread.sleep(10);
    }
    assertNotNull(sn1.getReservedContainer());

    final CapacityScheduler cs = (CapacityScheduler) scheduler;
    final CapacityScheduler spyCs = spy(cs);
    final AtomicBoolean isFirstReserve = new AtomicBoolean(true);
    final AtomicBoolean isChecked = new AtomicBoolean(false);
    // handle CapacityScheduler#tryCommit,
    // reproduce the process that can raise IllegalStateException before
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) throws Exception {
        ResourceCommitRequest request =
            (ResourceCommitRequest) invocation.getArguments()[1];
        if (request.getContainersToReserve().size() > 0 && isFirstReserve
            .compareAndSet(true, false)) {
          // release app1-container_03 on nm2
          RMContainer killableContainer =
              sn2.getCopiedListOfRunningContainers().get(0);
          cs.completedContainer(killableContainer, ContainerStatus
                  .newInstance(killableContainer.getContainerId(),
                      ContainerState.COMPLETE, "",
                      ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
              RMContainerEventType.KILL);
          assertEquals(0, sn2.getCopiedListOfRunningContainers().size());
          // unreserve app1-container_04 on nm1
          // and allocate app1-container_05 on nm2
          cs.handle(new NodeUpdateSchedulerEvent(sn2.getRMNode()));
          int waitTime = 1000;
          while (waitTime > 0
              && sn2.getCopiedListOfRunningContainers().size() == 0) {
            waitTime -= 10;
            Thread.sleep(10);
          }
          assertEquals(1, sn2.getCopiedListOfRunningContainers().size());
          assertNull(sn1.getReservedContainer());

          // reserve app2-container_02 on nm1
          ResourceRequest rr3 = ResourceRequest
              .newInstance(Priority.newInstance(0), "*",
                  Resources.createResource(5 * GB), 1);
          am2.allocate(Arrays.asList(rr3), null);
          cs.handle(new NodeUpdateSchedulerEvent(sn1.getRMNode()));
          waitTime = 1000;
          while (waitTime > 0 && sn1.getReservedContainer() == null) {
            waitTime -= 10;
            Thread.sleep(10);
          }
          assertNotNull(sn1.getReservedContainer());

          // call real apply
          try {
            cs.tryCommit((Resource) invocation.getArguments()[0],
                (ResourceCommitRequest) invocation.getArguments()[1], true);
          } catch (Exception e) {
            e.printStackTrace();
            fail();
          }
          isChecked.set(true);
        } else {
          cs.tryCommit((Resource) invocation.getArguments()[0],
              (ResourceCommitRequest) invocation.getArguments()[1], true);
        }
        return null;
      }
    }).when(spyCs).tryCommit(any(Resource.class),
        any(ResourceCommitRequest.class), anyBoolean());

    spyCs.handle(new NodeUpdateSchedulerEvent(sn1.getRMNode()));

    waitTime = 1000;
    while (waitTime > 0 && !isChecked.get()) {
      waitTime -= 10;
      Thread.sleep(10);
    }
    rm.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testNodeResourceOverAllocated()
      throws Exception {
    // disable async-scheduling for simulating complex scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs & Nodes
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("127.0.0.1:1234", 9 * GB);
    final MockNM nm2 = rm.registerNode("127.0.0.2:1234", 9 * GB);
    List<MockNM> nmLst = new ArrayList<>();
    nmLst.add(nm1);
    nmLst.add(nm2);

    // init scheduler & nodes
    while (
        ((CapacityScheduler) rm.getRMContext().getScheduler()).getNodeTracker()
            .nodeCount() < 2) {
      Thread.sleep(10);
    }
    assertEquals(2,
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getNodeTracker().nodeCount());
    CapacityScheduler scheduler =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode sn1 = scheduler.getSchedulerNode(nm1.getNodeId());

    // launch app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
        .withAppName("app")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    FiCaSchedulerApp schedulerApp =
        scheduler.getApplicationAttempt(am.getApplicationAttemptId());
    // allocate 2 containers and running on nm1
    Resource containerResource = Resources.createResource(5 * GB);
    am.allocate(Arrays.asList(ResourceRequest
            .newInstance(Priority.newInstance(0), "*", containerResource, 2)),
        null);

    // generate over-allocated proposals for nm1
    for (int containerNo = 2; containerNo <= 3; containerNo++) {
      Container container = Container.newInstance(
          ContainerId.newContainerId(am.getApplicationAttemptId(), containerNo),
          sn1.getNodeID(), sn1.getHttpAddress(), containerResource,
          Priority.newInstance(0), null);
      RMContainer rmContainer = new RMContainerImpl(container,
          SchedulerRequestKey.create(ResourceRequest
              .newInstance(Priority.newInstance(0), "*", containerResource, 1)),
          am.getApplicationAttemptId(), sn1.getNodeID(), "user",
          rm.getRMContext());
      SchedulerContainer newContainer = new SchedulerContainer(schedulerApp,
          scheduler.getNode(sn1.getNodeID()), rmContainer, "", true);
      ContainerAllocationProposal newContainerProposal =
          new ContainerAllocationProposal(newContainer, null, null,
              NodeType.OFF_SWITCH, NodeType.OFF_SWITCH,
              SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, containerResource);
      List<ContainerAllocationProposal> newProposals = new ArrayList<>();
      newProposals.add(newContainerProposal);
      ResourceCommitRequest request =
          new ResourceCommitRequest(newProposals, null, null);
      scheduler.tryCommit(scheduler.getClusterResource(), request, true);
    }
    // make sure node resource can't be over-allocated!
    assertTrue(sn1.getUnallocatedResource().getMemorySize() > 0,
        "Node resource is Over-allocated!");
    rm.stop();
  }

  /**
   * Make sure scheduler skips NMs which haven't heartbeat for a while.
   * @throws Exception
   */
  @Test
  public void testAsyncSchedulerSkipNoHeartbeatNMs() throws Exception {
    int heartbeatInterval = 100;
    conf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        1);
    conf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 100);
    // Heartbeat interval is 100 ms.
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, heartbeatInterval);
    mgr.init(conf);

    // inject node label manager
    MockRM rm = new MockRM(TestUtils.getConfigurationWithMultipleQueues(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    List<MockNM> nms = new ArrayList<>();
    // Add 10 nodes to the cluster, in the cluster we have 200 GB resource
    for (int i = 0; i < 10; i++) {
      nms.add(rm.registerNode("127.0.0." + i + ":1234", 20 * GB));
    }

    List<MockAM> ams = new ArrayList<>();

    keepNMHeartbeat(nms, heartbeatInterval);

    for (int i = 0; i < 3; i++) {
      RMApp rmApp = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppName("app")
              .withUser("user")
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(Character.toString((char) (i % 34 + 97)))
              .withMaxAppAttempts(1)
              .withCredentials(null)
              .withAppType(null)
              .withWaitForAppAcceptedState(false)
              .build());
      MockAM am = MockRM.launchAMWhenAsyncSchedulingEnabled(rmApp, rm);
      am.registerAppAttempt();
      ams.add(am);
    }

    pauseNMHeartbeat();

    Thread.sleep(heartbeatInterval * 3);

    // Applications request containers.
    for (int i = 0; i < 3; i++) {
      ams.get(i).allocate("*", 1024, 20 * (i + 1), new ArrayList<>());
    }

    for (int i = 0; i < 5; i++) {
      // Do heartbeat for NM 0-4
      nms.get(i).nodeHeartbeat(true);
    }

    // Wait for 2000 ms.
    Thread.sleep(2000);

    // Make sure that NM5-9 don't have non-AM containers.
    for (int i = 0; i < 9; i++) {
      if (i < 5) {
        assertTrue(checkNumNonAMContainersOnNode(cs, nms.get(i)) > 0);
      } else {
        assertTrue(checkNumNonAMContainersOnNode(cs, nms.get(i)) == 0);
      }
    }

    rm.close();
  }

  /**
   * Make sure scheduler skips NMs which are not RUNNING.
   * @throws Exception
   */
  @Test
  public void testAsyncSchedulerSkipNoRunningNMs() throws Exception {
    int heartbeatInterval = 100;
    conf.setInt(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
        1);
    conf.setInt(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
        + ".scheduling-interval-ms", 100);
    // Heartbeat interval is 100 ms.
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
        heartbeatInterval);
    conf.setInt(YarnConfiguration.SCHEDULER_SKIP_NODE_MULTIPLIER,
        5);
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    // inject node label manager
    MockRM rm = new MockRM(TestUtils.getConfigurationWithMultipleQueues(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    List<MockNM> nms = new ArrayList<>();
    // Add 10 nodes to the cluster, in the cluster we have 200 GB resource
    for (int i = 0; i < 10; i++) {
      nms.add(rm.registerNode("127.0.0." + i + ":1234", 20 * GB));
    }

    keepNMHeartbeat(nms, heartbeatInterval);

    List<MockAM> ams = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      RMApp rmApp = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppName("app")
              .withUser("user")
              .withAcls(null)
              .withUnmanagedAM(false)
              .withQueue(Character.toString((char) (i % 34 + 97)))
              .withMaxAppAttempts(1)
              .withCredentials(null)
              .withAppType(null)
              .withWaitForAppAcceptedState(false)
              .build());
      MockAM am = MockRM.launchAMWhenAsyncSchedulingEnabled(rmApp, rm);
      am.registerAppAttempt();
      ams.add(am);
    }

    // Test for no NodeState.RUNNING node
    for (int i = 0; i < 5; i++) {
      RMNode rmNode = cs.getNode(nms.get(i).getNodeId()).getRMNode();
      cs.getRMContext().getDispatcher().getEventHandler().handle(
          new RMNodeEvent(rmNode.getNodeID(),
              RMNodeEventType.GRACEFUL_DECOMMISSION));
      rm.drainEvents();
      assertEquals(NodeState.DECOMMISSIONING, rmNode.getState());
      boolean shouldSkip =
          cs.shouldSkipNodeSchedule(cs.getNode(nms.get(i).getNodeId()),
              cs, true);
      // make sure should skip
      assertTrue(shouldSkip);
    }

    for (int i = 5; i < 9; i++) {
      boolean shouldSkip =
          cs.shouldSkipNodeSchedule(cs.getNode(nms.get(i).getNodeId()),
              cs, true);
      // make sure should not skip
      assertFalse(shouldSkip);
    }

    pauseNMHeartbeat();

    //Not exceed configured 5
    Thread.sleep(heartbeatInterval * 3);

    // Applications request containers.
    for (int i = 0; i < 3; i++) {
      ams.get(i).allocate("*", 1024, 20 * (i + 1), new ArrayList<>());
    }

    // Wait for 2000 ms.
    Thread.sleep(2000);

    //Make sure that NM 0-5 don't have non-AM containers.
    for (int i = 0; i < 9; i++) {
      if (i < 5) {
        assertTrue(checkNumNonAMContainersOnNode(cs, nms.get(i)) == 0);
      } else {
        assertTrue(checkNumNonAMContainersOnNode(cs, nms.get(i)) > 0);
      }
    }
    rm.close();
  }

  public static class NMHeartbeatThread extends Thread {
    private List<MockNM> mockNMS;
    private int interval;
    private volatile boolean shouldStop = false;

    public NMHeartbeatThread(List<MockNM> mockNMs, int interval) {
      this.mockNMS = mockNMs;
      this.interval = interval;
    }

    public void run() {
      while (true) {
        if (shouldStop) {
          break;
        }
        for (MockNM nm : mockNMS) {
          try {
            nm.nodeHeartbeat(true);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void setShouldStop() {
      shouldStop = true;
    }
  }

  // Testcase for YARN-8127
  @Test
  @Timeout(value = 30)
  public void testCommitDuplicatedAllocateFromReservedProposals()
      throws Exception {
    // disable async-scheduling for simulating complex scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("192.168.0.1:1234", 8 * GB);
    rm.registerNode("192.168.0.2:2234", 8 * GB);

    // init scheduler & nodes
    while (
        ((CapacityScheduler) rm.getRMContext().getScheduler()).getNodeTracker()
            .nodeCount() < 2) {
      Thread.sleep(10);
    }
    assertEquals(2,
        ((AbstractYarnScheduler) rm.getRMContext().getScheduler())
            .getNodeTracker().nodeCount());
    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode sn1 = cs.getSchedulerNode(nm1.getNodeId());

    // launch app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
        .withAppName("app")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);
    FiCaSchedulerApp schedulerApp =
        cs.getApplicationAttempt(am.getApplicationAttemptId());

    // app asks 1 * 6G container
    // nm1 runs 2 container(container_01/AM, container_02)
    allocateAndLaunchContainers(am, nm1, rm, 1,
        Resources.createResource(6 * GB), 0, 2);
    assertEquals(2, sn1.getNumContainers());
    assertEquals(1 * GB, sn1.getUnallocatedResource().getMemorySize());

    // app asks 5 * 2G container
    // nm1 reserves 1 * 2G containers
    am.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(0), "*",
            Resources.createResource(2 * GB), 5)), null);
    cs.handle(new NodeUpdateSchedulerEvent(sn1.getRMNode()));
    assertEquals(1, schedulerApp.getReservedContainers().size());

    // rm kills 1 * 6G container_02
    for (RMContainer rmContainer : sn1.getCopiedListOfRunningContainers()) {
      if (rmContainer.getContainerId().getContainerId() != 1) {
        cs.completedContainer(rmContainer, ContainerStatus
                .newInstance(rmContainer.getContainerId(),
                    ContainerState.COMPLETE, "",
                    ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
            RMContainerEventType.KILL);
      }
    }
    assertEquals(7 * GB, sn1.getUnallocatedResource().getMemorySize());

    final CapacityScheduler spyCs = spy(cs);
    // handle CapacityScheduler#tryCommit, submit duplicated proposals
    // that do allocation for reserved container for three times,
    // to simulate that case in YARN-8127
    doAnswer(new Answer<Object>() {
      public Boolean answer(InvocationOnMock invocation) throws Exception {
        ResourceCommitRequest request =
            (ResourceCommitRequest) invocation.getArguments()[1];
        if (request.getFirstAllocatedOrReservedContainer()
            .getAllocateFromReservedContainer() != null) {
          for (int i=0; i<3; i++) {
            cs.tryCommit((Resource) invocation.getArguments()[0],
                (ResourceCommitRequest) invocation.getArguments()[1],
                (Boolean) invocation.getArguments()[2]);
          }
          assertEquals(2, sn1.getCopiedListOfRunningContainers().size());
          assertEquals(5 * GB,
              sn1.getUnallocatedResource().getMemorySize());
        }
        return true;
      }
    }).when(spyCs).tryCommit(any(Resource.class),
        any(ResourceCommitRequest.class), anyBoolean());

    spyCs.handle(new NodeUpdateSchedulerEvent(sn1.getRMNode()));

    rm.stop();
  }


  @Test
  @Timeout(value = 60)
  public void testReleaseOutdatedReservedContainer() throws Exception {
    /*
     * Submit a application, reserved container_02 on nm1,
     * submit two allocate proposals which contain the same reserved
     * container_02 as to-released container.
     * First proposal should be accepted, second proposal should be rejected
     * because it try to release an outdated reserved container
     */
    // disable async-scheduling for simulating complex scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);
    MockRM rm1 = new MockRM(disableAsyncConf);
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);
    MockNM nm3 = rm1.registerNode("h3:1234", 8 * GB);
    rm1.drainEvents();

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue defaultQueue = (LeafQueue) cs.getQueue("default");
    SchedulerNode sn1 = cs.getSchedulerNode(nm1.getNodeId());
    SchedulerNode sn2 = cs.getSchedulerNode(nm2.getNodeId());
    SchedulerNode sn3 = cs.getSchedulerNode(nm3.getNodeId());

    // launch another app to queue, AM container should be launched in nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(4 * GB, rm1)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    Resource allocateResource = Resources.createResource(5 * GB);
    am1.allocate("*", (int) allocateResource.getMemorySize(), 3, 0,
        new ArrayList<ContainerId>(), "");
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());

    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    assertEquals(1, schedulerApp1.getReservedContainers().size());
    assertEquals(9 * GB,
        defaultQueue.getQueueResourceUsage().getUsed().getMemorySize());

    RMContainer reservedContainer =
        schedulerApp1.getReservedContainers().get(0);
    ResourceCommitRequest allocateFromSameReservedContainerProposal1 =
        createAllocateFromReservedProposal(3, allocateResource, schedulerApp1,
            sn2, sn1, cs.getRMContext(), reservedContainer);
    boolean tryCommitResult = cs.tryCommit(cs.getClusterResource(),
        allocateFromSameReservedContainerProposal1, true);
    assertTrue(tryCommitResult);
    ResourceCommitRequest allocateFromSameReservedContainerProposal2 =
        createAllocateFromReservedProposal(4, allocateResource, schedulerApp1,
            sn3, sn1, cs.getRMContext(), reservedContainer);
    tryCommitResult = cs.tryCommit(cs.getClusterResource(),
        allocateFromSameReservedContainerProposal2, true);
    assertFalse(tryCommitResult, "This proposal should be rejected because "
        + "it try to release an outdated reserved container");

    rm1.close();
  }

  @Test
  @Timeout(value = 30)
  public void testCommitProposalsForUnusableNode() throws Exception {
    // disable async-scheduling for simulating complex scene
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("192.168.0.1:1234", 8 * GB);
    final MockNM nm2 = rm.registerNode("192.168.0.2:2234", 8 * GB);
    final MockNM nm3 = rm.registerNode("192.168.0.3:2234", 8 * GB);
    rm.drainEvents();
    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode sn1 = cs.getSchedulerNode(nm1.getNodeId());

    // launch app1-am on nm1
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
        .withAppName("app1")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // launch app2-am on nm2
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
        .withAppName("app2")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

    // app2 asks 1 * 8G container
    am2.allocate(ImmutableList.of(ResourceRequest
        .newInstance(Priority.newInstance(0), "*",
            Resources.createResource(8 * GB), 1)), null);

    List<Object> reservedProposalParts = new ArrayList<>();
    final CapacityScheduler spyCs = spy(cs);
    // handle CapacityScheduler#tryCommit
    doAnswer(new Answer<Object>() {
      public Boolean answer(InvocationOnMock invocation) throws Exception {
        for (Object argument : invocation.getArguments()) {
          reservedProposalParts.add(argument);
        }
        return false;
      }
    }).when(spyCs).tryCommit(any(Resource.class),
        any(ResourceCommitRequest.class), anyBoolean());

    spyCs.handle(new NodeUpdateSchedulerEvent(sn1.getRMNode()));

    // decommission nm1
    RMNode rmNode1 = cs.getNode(nm1.getNodeId()).getRMNode();
    cs.getRMContext().getDispatcher().getEventHandler().handle(
        new RMNodeEvent(nm1.getNodeId(), RMNodeEventType.DECOMMISSION));
    rm.drainEvents();
    assertEquals(NodeState.DECOMMISSIONED, rmNode1.getState());
    assertNull(cs.getNode(nm1.getNodeId()));

    // try commit after nm1 decommissioned
    boolean isSuccess =
        cs.tryCommit((Resource) reservedProposalParts.get(0),
            (ResourceCommitRequest) reservedProposalParts.get(1),
            (Boolean) reservedProposalParts.get(2));
    assertFalse(isSuccess);
    rm.stop();
  }

  @Test
  @Timeout(value = 30)
  public void testAsyncScheduleThreadExit() throws Exception {

    // Set no exit security manager to catch System.exit
    SecurityManager originalSecurityManager = System.getSecurityManager();
    NoExitSecurityManager noExitSecurityManager =
        new NoExitSecurityManager(originalSecurityManager);
    try {
      System.setSecurityManager(noExitSecurityManager);
    } catch (UnsupportedOperationException e) {
      Assumptions.assumeTrue(false,
          "Test is skipped because SecurityManager cannot be set (JEP411)");
    }

    // init RM & NM
    final MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("192.168.0.1:1234", 8 * GB);
    rm.drainEvents();

    // test async-scheduling thread exit
    try{
      // set resource calculator to be null to simulate
      // NPE in async-scheduling thread
      CapacityScheduler cs =
          (CapacityScheduler) rm.getRMContext().getScheduler();
      cs.setResourceCalculator(null);

      // wait for RM to be shutdown until timeout
      GenericTestUtils.waitFor(noExitSecurityManager::isCheckExitCalled,
          100, 5000);
    } finally {
      System.setSecurityManager(originalSecurityManager);
      rm.stop();
    }
  }

  private ResourceCommitRequest createAllocateFromReservedProposal(
      int containerId, Resource allocateResource, FiCaSchedulerApp schedulerApp,
      SchedulerNode allocateNode, SchedulerNode reservedNode,
      RMContext rmContext, RMContainer reservedContainer) {
    Container container = Container.newInstance(
        ContainerId.newContainerId(schedulerApp.getApplicationAttemptId(), containerId),
        allocateNode.getNodeID(), allocateNode.getHttpAddress(), allocateResource,
        Priority.newInstance(0), null);
    RMContainer rmContainer = new RMContainerImpl(container, SchedulerRequestKey
        .create(ResourceRequest
            .newInstance(Priority.newInstance(0), "*", allocateResource, 1)),
        schedulerApp.getApplicationAttemptId(), allocateNode.getNodeID(), "user",
        rmContext);
    SchedulerContainer allocateContainer =
        new SchedulerContainer(schedulerApp, allocateNode, rmContainer, "", true);
    SchedulerContainer reservedSchedulerContainer =
        new SchedulerContainer(schedulerApp, reservedNode, reservedContainer, "",
            false);
    List<SchedulerContainer> toRelease = new ArrayList<>();
    toRelease.add(reservedSchedulerContainer);
    ContainerAllocationProposal allocateFromReservedProposal =
        new ContainerAllocationProposal(allocateContainer, toRelease, null,
            NodeType.OFF_SWITCH, NodeType.OFF_SWITCH,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY, allocateResource);
    List<ContainerAllocationProposal> allocateProposals = new ArrayList<>();
    allocateProposals.add(allocateFromReservedProposal);
    return new ResourceCommitRequest(allocateProposals, null, null);
  }

  @Test
  @Timeout(value = 30)
  public void testReturnNullWhenGetSchedulerContainer() throws Exception {
    // disable async-scheduling for simulating complex scenario
    Configuration disableAsyncConf = new Configuration(conf);
    disableAsyncConf.setBoolean(
        CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, false);

    // init RM & NMs
    final MockRM rm = new MockRM(disableAsyncConf);
    rm.start();
    final MockNM nm1 = rm.registerNode("192.168.0.1:1234", 8 * GB);
    final MockNM nm2 = rm.registerNode("192.168.0.2:2234", 8 * GB);
    rm.drainEvents();
    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode sn1 = cs.getSchedulerNode(nm1.getNodeId());
    RMNode rmNode1 = cs.getNode(nm1.getNodeId()).getRMNode();
    SchedulerNode sn2 = cs.getSchedulerNode(nm2.getNodeId());

    // launch app1-am on nm1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
        .withAppName("app1")
        .withUser("user")
        .withAcls(null)
        .withUnmanagedAM(false)
        .withQueue("default")
        .withMaxAppAttempts(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(true)
        .withKeepContainers(true)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // app2 asks 1 * 1G container
    am1.allocate(ImmutableList.of(ResourceRequest
        .newInstance(Priority.newInstance(0), "*",
            Resources.createResource(1 * GB), 1)), null);
    RMContainer amContainer = cs.getRMContainer(
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 1));

    // spy CapacityScheduler
    final CapacityScheduler spyCs = spy(cs);
    // hook CapacityScheduler#submitResourceCommitRequest
    List<CSAssignment> assignmentSnapshots = new ArrayList<>();
    doAnswer(new Answer<Object>() {
      public Boolean answer(InvocationOnMock invocation) throws Exception {
        CSAssignment assignment = (CSAssignment) invocation.getArguments()[1];
        if (cs.getNode(nm1.getNodeId()) != null) {
          // decommission nm1 for first allocation on nm1
          cs.getRMContext().getDispatcher().getEventHandler().handle(
              new RMNodeEvent(nm1.getNodeId(), RMNodeEventType.DECOMMISSION));
          rm.drainEvents();
          assertEquals(NodeState.DECOMMISSIONED, rmNode1.getState());
          assertNull(cs.getNode(nm1.getNodeId()));
          assignmentSnapshots.add(assignment);
        } else {
          // add am container on nm1 to containersToKill
          // for second allocation on nm2
          assignment.setContainersToKill(ImmutableList.of(amContainer));
        }
        // check no NPE in actual submit, before YARN-8233 will throw NPE
        cs.submitResourceCommitRequest((Resource) invocation.getArguments()[0],
            assignment);
        return false;
      }
    }).when(spyCs).submitResourceCommitRequest(any(Resource.class),
        any(CSAssignment.class));

    // allocation on nm1, test return null when get scheduler container
    CandidateNodeSet<FiCaSchedulerNode> candidateNodeSet =
        new SimpleCandidateNodeSet(sn1);
    spyCs.allocateContainersToNode(candidateNodeSet, false);
    // make sure unconfirmed resource is decreased correctly
    assertTrue(spyCs.getApplicationAttempt(am1.getApplicationAttemptId())
        .hasPendingResourceRequest(RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));

    // allocation on nm2,
    // test return null when get scheduler container to release
    candidateNodeSet =
        new SimpleCandidateNodeSet(sn2);
    spyCs.allocateContainersToNode(candidateNodeSet, false);
    // make sure unconfirmed resource is decreased correctly
    assertTrue(spyCs.getApplicationAttempt(am1.getApplicationAttemptId())
        .hasPendingResourceRequest(RMNodeLabelsManager.NO_LABEL,
            SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));

    rm.stop();
  }

  private void keepNMHeartbeat(List<MockNM> mockNMs, int interval) {
    if (nmHeartbeatThread != null) {
      nmHeartbeatThread.setShouldStop();
      nmHeartbeatThread = null;
    }
    nmHeartbeatThread = new NMHeartbeatThread(mockNMs, interval);
    nmHeartbeatThread.start();
  }

  private void pauseNMHeartbeat() {
    if (nmHeartbeatThread != null) {
      nmHeartbeatThread.setShouldStop();
      nmHeartbeatThread = null;
    }
  }

  private int checkNumNonAMContainersOnNode(CapacityScheduler cs, MockNM nm) {
    SchedulerNode node = cs.getNode(nm.getNodeId());
    int nonAMContainer = 0;
    for (RMContainer c : node.getCopiedListOfRunningContainers()) {
      if (!c.isAMContainer()) {
         nonAMContainer++;
      }
    }
    return nonAMContainer;
  }

  private void allocateAndLaunchContainers(MockAM am, MockNM nm, MockRM rm,
      int nContainer, Resource resource, int priority, int startContainerId)
      throws Exception {
    am.allocate(Arrays.asList(ResourceRequest
        .newInstance(Priority.newInstance(priority), "*", resource,
            nContainer)), null);
    ContainerId lastContainerId = ContainerId
        .newContainerId(am.getApplicationAttemptId(),
            startContainerId + nContainer - 1);
    assertTrue(
        rm.waitForState(nm, lastContainerId, RMContainerState.ALLOCATED));
    // Acquire them, and NM report RUNNING
    am.allocate(null, null);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    for (int cId = startContainerId;
         cId < startContainerId + nContainer; cId++) {
      ContainerId containerId =
          ContainerId.newContainerId(am.getApplicationAttemptId(), cId);
      RMContainer rmContainer = cs.getRMContainer(containerId);
      if (rmContainer != null) {
        rmContainer.handle(
            new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
      } else {
        fail("Cannot find RMContainer");
      }
      rm.waitForState(nm,
          ContainerId.newContainerId(am.getApplicationAttemptId(), cId),
          RMContainerState.RUNNING);
    }
  }
}
