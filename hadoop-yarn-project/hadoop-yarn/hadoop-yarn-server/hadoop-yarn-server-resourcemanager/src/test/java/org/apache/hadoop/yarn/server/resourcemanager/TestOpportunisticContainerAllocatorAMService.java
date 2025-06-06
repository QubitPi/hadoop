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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterDistributedSchedulingAMResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Test cases for {@link OpportunisticContainerAllocatorAMService}.
 */
public class TestOpportunisticContainerAllocatorAMService {

  private static final int GB = 1024;

  private MockRM rm;
  private DrainDispatcher dispatcher;

  private OpportunisticContainersStatus oppContainersStatus =
      getOpportunisticStatus();

  @BeforeEach
  public void createAndStartRM() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS, 100);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(
        YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    startRM(conf);
  }

  public void createAndStartRMWithAutoUpdateContainer() {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS, true);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS, 100);
    startRM(conf);
  }

  private void startRM(final YarnConfiguration conf) {
    dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
  }

  @AfterEach
  public void stopRM() {
    if (rm != null) {
      rm.stop();
    }

    OpportunisticSchedulerMetrics.resetMetrics();
  }

  @Test
  @Timeout(value = 600)
  @SuppressWarnings("checkstyle:MethodLength")
  public void testContainerPromoteAndDemoteBeforeContainerStart() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h1:4321", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    MockNM nm3 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm3.getNodeId(), nm3);
    MockNM nm4 = new MockNM("h2:4321", 4096, rm.getResourceTrackerService());
    nodes.put(nm4.getNodeId(), nm4);
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);
    nm3.nodeHeartbeat(oppContainersStatus, true);
    nm4.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 - 4 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);
    nm3.nodeHeartbeat(oppContainersStatus, true);
    nm4.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        amservice.getLeastLoadedNodes().size() == 4, 10, 10 * 100);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());
    MockNM sameHostDiffNode = null;
    for (NodeId n : nodes.keySet()) {
      if (n.getHost().equals(allocNode.getNodeId().getHost()) &&
          n.getPort() != allocNode.getNodeId().getPort()) {
        sameHostDiffNode = nodes.get(n);
      }
    }

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    // Node on same host should not result in allocation
    sameHostDiffNode.nodeHeartbeat(oppContainersStatus, true);
    rm.drainEvents();
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    assertEquals(0, allocateResponse.getUpdatedContainers().size());

    // Wait for scheduler to process all events
    dispatcher.waitForEventThreadToWait();
    rm.drainEvents();
    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);

    // Send Promotion req again... this should result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    assertEquals(0, allocateResponse.getUpdatedContainers().size());
    assertEquals(1, allocateResponse.getUpdateErrors().size());
    assertEquals("UPDATE_OUTSTANDING_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Send Promotion req again with incorrect version...
    // this should also result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(1,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    assertEquals(0, allocateResponse.getUpdatedContainers().size());
    assertEquals(1, allocateResponse.getUpdateErrors().size());
    assertEquals("INCORRECT_CONTAINER_VERSION_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    assertEquals(0,
        allocateResponse.getUpdateErrors().get(0)
            .getCurrentContainerVersion());
    assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Ensure after correct node heartbeats, we should get the allocation
    allocNode.nodeHeartbeat(oppContainersStatus, true);
    rm.drainEvents();
    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    Container uc =
        allocateResponse.getUpdatedContainers().get(0).getContainer();
    assertEquals(ExecutionType.GUARANTEED, uc.getExecutionType());
    assertEquals(uc.getId(), container.getId());
    assertEquals(uc.getVersion(), container.getVersion() + 1);

    // Verify Metrics After OPP allocation :
    // Allocated cores+mem should have increased, available should decrease
    verifyMetrics(metrics, 14336, 14, 2048, 2, 2);

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);
    nm3.nodeHeartbeat(oppContainersStatus, true);
    nm4.nodeHeartbeat(oppContainersStatus, true);
    rm.drainEvents();

    // Verify that the container is still in ACQUIRED state wrt the RM.
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
        uc.getId().getApplicationAttemptId()).getRMContainer(uc.getId());
    assertEquals(RMContainerState.ACQUIRED, rmContainer.getState());

    // Now demote the container back..
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(uc.getVersion(),
            uc.getId(), ContainerUpdateType.DEMOTE_EXECUTION_TYPE,
            null, ExecutionType.OPPORTUNISTIC)));
    // This should happen in the same heartbeat..
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0).getContainer();
    assertEquals(ExecutionType.OPPORTUNISTIC, uc.getExecutionType());
    assertEquals(uc.getId(), container.getId());
    assertEquals(uc.getVersion(), container.getVersion() + 2);

    // Wait for scheduler to finish processing events
    dispatcher.waitForEventThreadToWait();
    rm.drainEvents();
    // Verify Metrics After OPP allocation :
    // Everything should have reverted to what it was
    verifyMetrics(metrics, 15360, 15, 1024, 1, 1);
  }

  @Test
  @Timeout(value = 60)
  public void testContainerPromoteAfterContainerStart() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        amservice.getLeastLoadedNodes().size() == 2, 10, 10 * 100);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    // Send Promotion req again... this should result in update error
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));
    assertEquals(0, allocateResponse.getUpdatedContainers().size());
    assertEquals(1, allocateResponse.getUpdateErrors().size());
    assertEquals("UPDATE_OUTSTANDING_ERROR",
        allocateResponse.getUpdateErrors().get(0).getReason());
    assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    rm.drainEvents();

    allocateResponse =  am1.allocate(new ArrayList<>(), new ArrayList<>());
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    Container uc =
        allocateResponse.getUpdatedContainers().get(0).getContainer();
    assertEquals(ExecutionType.GUARANTEED, uc.getExecutionType());
    assertEquals(uc.getId(), container.getId());
    assertEquals(uc.getVersion(), container.getVersion() + 1);

    // Verify that the Container is still in RUNNING state wrt RM..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            uc.getId().getApplicationAttemptId()).getRMContainer(uc.getId());
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Verify Metrics After OPP allocation :
    // Allocated cores+mem should have increased, available should decrease
    verifyMetrics(metrics, 6144, 6, 2048, 2, 2);
  }

  @Test
  @Timeout(value = 600)
  public void testContainerPromoteAfterContainerComplete() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();


    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        amservice.getLeastLoadedNodes().size() == 2, 10, 10 * 100);

    QueueMetrics metrics = ((CapacityScheduler) scheduler).getRootQueue()
        .getMetrics();

    // Verify Metrics
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    AllocateResponse allocateResponse = am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))),
        null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Container Completed in the NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.COMPLETE, "", 0)),
        true);
    rm.drainEvents();

    // Verify that container has been removed..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    assertNull(rmContainer);

    // Verify Metrics After OPP allocation (Nothing should change)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);

    // Send Promotion req... this should result in update error
    // Since the container doesn't exist anymore..
    allocateResponse = am1.sendContainerUpdateRequest(
        Arrays.asList(UpdateContainerRequest.newInstance(0,
            container.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED)));

    assertEquals(1,
        allocateResponse.getCompletedContainersStatuses().size());
    assertEquals(container.getId(),
        allocateResponse.getCompletedContainersStatuses().get(0)
            .getContainerId());
    assertEquals(0, allocateResponse.getUpdatedContainers().size());
    assertEquals(1, allocateResponse.getUpdateErrors().size());
    assertEquals("INVALID_CONTAINER_ID",
        allocateResponse.getUpdateErrors().get(0).getReason());
    assertEquals(container.getId(),
        allocateResponse.getUpdateErrors().get(0)
            .getUpdateContainerRequest().getContainerId());

    // Verify Metrics After OPP allocation (Nothing should change again)
    verifyMetrics(metrics, 7168, 7, 1024, 1, 1);
  }

  @Test
  @Timeout(value = 600)
  public void testContainerAutoUpdateContainer() throws Exception {
    rm.stop();
    createAndStartRMWithAutoUpdateContainer();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());

    nm1.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        amservice.getLeastLoadedNodes().size() == 1, 10, 10 * 100);

    AllocateResponse allocateResponse = am1.allocate(Arrays.asList(
        ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest
                .newInstance(ExecutionType.OPPORTUNISTIC, true))), null);
    List<Container> allocatedContainers =
        allocateResponse.getAllocatedContainers();
    allocatedContainers.addAll(
        am1.allocate(null, null).getAllocatedContainers());
    assertEquals(2, allocatedContainers.size());
    Container container = allocatedContainers.get(0);
    // Start Container in NM
    nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.RUNNING, "", 0)), true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(container.getId().getApplicationAttemptId())
        .getRMContainer(container.getId());
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Send Promotion req... this should result in update error
    // Since the container doesn't exist anymore..
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(0, container.getId(),
            ContainerUpdateType.PROMOTE_EXECUTION_TYPE, null,
            ExecutionType.GUARANTEED)));

    nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.OPPORTUNISTIC,
            ContainerState.RUNNING, "", 0)), true);
    rm.drainEvents();
    // Get the update response on next allocate
    allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    // Check the update response from YARNRM
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    UpdatedContainer uc = allocateResponse.getUpdatedContainers().get(0);
    assertEquals(container.getId(), uc.getContainer().getId());
    assertEquals(ExecutionType.GUARANTEED,
        uc.getContainer().getExecutionType());
    // Check that the container is updated in NM through NM heartbeat response
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(true);
    assertEquals(1, response.getContainersToUpdate().size());
    Container containersFromNM = response.getContainersToUpdate().get(0);
    assertEquals(container.getId(), containersFromNM.getId());
    assertEquals(ExecutionType.GUARANTEED,
        containersFromNM.getExecutionType());

    //Increase resources
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(1, container.getId(),
            ContainerUpdateType.INCREASE_RESOURCE,
            Resources.createResource(2 * GB, 1), null)));
    response = nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.RUNNING, "", 0)), true);

    rm.drainEvents();
    if (allocateResponse.getUpdatedContainers().size() == 0) {
      allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    }
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0);
    assertEquals(container.getId(), uc.getContainer().getId());
    assertEquals(Resource.newInstance(2 * GB, 1),
        uc.getContainer().getResource());
    rm.drainEvents();

    // Check that the container resources are increased in
    // NM through NM heartbeat response
    if (response.getContainersToUpdate().size() == 0) {
      response = nm1.nodeHeartbeat(true);
    }
    assertEquals(1, response.getContainersToUpdate().size());
    assertEquals(Resource.newInstance(2 * GB, 1),
        response.getContainersToUpdate().get(0).getResource());

    //Decrease resources
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(2, container.getId(),
            ContainerUpdateType.DECREASE_RESOURCE,
            Resources.createResource(1 * GB, 1), null)));
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    rm.drainEvents();

    // Check that the container resources are decreased
    // in NM through NM heartbeat response
    response = nm1.nodeHeartbeat(true);
    assertEquals(1, response.getContainersToUpdate().size());
    assertEquals(Resource.newInstance(1 * GB, 1),
        response.getContainersToUpdate().get(0).getResource());

    nm1.nodeHeartbeat(oppContainersStatus, true);
    // DEMOTE the container
    allocateResponse = am1.sendContainerUpdateRequest(Arrays.asList(
        UpdateContainerRequest.newInstance(3, container.getId(),
            ContainerUpdateType.DEMOTE_EXECUTION_TYPE, null,
            ExecutionType.OPPORTUNISTIC)));

    response = nm1.nodeHeartbeat(Arrays.asList(ContainerStatus
        .newInstance(container.getId(), ExecutionType.GUARANTEED,
            ContainerState.RUNNING, "", 0)), true);
    rm.drainEvents();
    if (allocateResponse.getUpdatedContainers().size() == 0) {
      // Get the update response on next allocate
      allocateResponse = am1.allocate(new ArrayList<>(), new ArrayList<>());
    }
    // Check the update response from YARNRM
    assertEquals(1, allocateResponse.getUpdatedContainers().size());
    uc = allocateResponse.getUpdatedContainers().get(0);
    assertEquals(ExecutionType.OPPORTUNISTIC,
        uc.getContainer().getExecutionType());
    // Check that the container is updated in NM through NM heartbeat response
    if (response.getContainersToUpdate().size() == 0) {
      response = nm1.nodeHeartbeat(oppContainersStatus, true);
    }
    assertEquals(1, response.getContainersToUpdate().size());
    assertEquals(ExecutionType.OPPORTUNISTIC,
        response.getContainersToUpdate().get(0).getExecutionType());
  }

  private void verifyMetrics(QueueMetrics metrics, long availableMB,
      int availableVirtualCores, long allocatedMB,
      int allocatedVirtualCores, int allocatedContainers) {
    assertEquals(availableMB, metrics.getAvailableMB());
    assertEquals(availableVirtualCores, metrics.getAvailableVirtualCores());
    assertEquals(allocatedMB, metrics.getAllocatedMB());
    assertEquals(allocatedVirtualCores, metrics.getAllocatedVirtualCores());
    assertEquals(allocatedContainers, metrics.getAllocatedContainers());
  }

  @Test
  @Timeout(value = 60)
  public void testOpportunisticSchedulerMetrics() throws Exception {
    HashMap<NodeId, MockNM> nodes = new HashMap<>();
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm1.getNodeId(), nm1);
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nodes.put(nm2.getNodeId(), nm2);
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticSchedulerMetrics metrics =
        OpportunisticSchedulerMetrics.getMetrics();

    int allocContainers = metrics.getAllocatedContainers();
    long aggrAllocatedContainers = metrics.getAggregatedAllocatedContainers();
    long aggrOffSwitchContainers = metrics.getAggregatedOffSwitchContainers();
    long aggrReleasedContainers = metrics.getAggregatedReleasedContainers();

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();

    // All nodes 1 to 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        amservice.getLeastLoadedNodes().size() == 2, 10, 10 * 100);

    AllocateResponse allocateResponse = am1.allocate(Arrays.asList(
        ResourceRequest.newInstance(Priority.newInstance(1), "*",
            Resources.createResource(1 * GB), 2, true, null,
            ExecutionTypeRequest
                .newInstance(ExecutionType.OPPORTUNISTIC, true))), null);

    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    assertEquals(2, allocatedContainers.size());

    assertEquals(allocContainers + 2, metrics.getAllocatedContainers());
    assertEquals(aggrAllocatedContainers + 2,
        metrics.getAggregatedAllocatedContainers());
    assertEquals(aggrOffSwitchContainers + 2,
        metrics.getAggregatedOffSwitchContainers());

    Container container = allocatedContainers.get(0);
    MockNM allocNode = nodes.get(container.getNodeId());

    // Start Container in NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.RUNNING, "", 0)),
        true);
    rm.drainEvents();

    // Verify that container is actually running wrt the RM..
    RMContainer rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    assertEquals(RMContainerState.RUNNING, rmContainer.getState());

    // Container Completed in the NM
    allocNode.nodeHeartbeat(Arrays.asList(
        ContainerStatus.newInstance(container.getId(),
            ExecutionType.OPPORTUNISTIC, ContainerState.COMPLETE, "", 0)),
        true);
    rm.drainEvents();

    // Verify that container has been removed..
    rmContainer = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(
            container.getId().getApplicationAttemptId()).getRMContainer(
            container.getId());
    assertNull(rmContainer);

    assertEquals(allocContainers + 1, metrics.getAllocatedContainers());
    assertEquals(aggrReleasedContainers + 1,
        metrics.getAggregatedReleasedContainers());
  }

  /**
   * Tests that, if a node has running opportunistic containers when the RM
   * is down, RM is able to reflect the opportunistic containers
   * in its metrics upon RM recovery.
   */
  @Test
  public void testMetricsRetainsAllocatedOpportunisticAfterRMRestart()
      throws Exception {
    final MockRMAppSubmissionData appSubmissionData =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build();

    MockNM nm1 = new MockNM("h:1234", 4096, rm.getResourceTrackerService());
    nm1.registerNode();

    final RMApp app = MockRMAppSubmitter.submit(rm, appSubmissionData);

    final ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();

    MockRM.launchAndRegisterAM(app, rm, nm1);

    final OpportunisticSchedulerMetrics metrics =
        OpportunisticSchedulerMetrics.getMetrics();

    // We start with ID 2, since AMContainer is ID 1
    final ContainerId recoverOContainerId2 = ContainerId.newContainerId(
        appAttemptId, 2);

    final Resource fakeResource = Resource.newInstance(1024, 1);
    final String fakeDiagnostics = "recover container";
    final Priority fakePriority = Priority.newInstance(0);

    final NMContainerStatus recoverOContainerReport1 =
        NMContainerStatus.newInstance(
            recoverOContainerId2, 0, ContainerState.RUNNING,
            fakeResource, fakeDiagnostics, 0,
            fakePriority, 0, null,
            ExecutionType.OPPORTUNISTIC, -1);

    // Make sure that numbers start with 0
    assertEquals(0, metrics.getAllocatedContainers());

    // Recover one OContainer only
    rm.registerNode("h2:1234", 4096, 1,
        Collections.singletonList(
            appAttemptId.getApplicationId()),
        Collections.singletonList(recoverOContainerReport1));

    assertEquals(1, metrics.getAllocatedContainers());

    // Recover two OContainers at once
    final ContainerId recoverOContainerId3 = ContainerId.newContainerId(
        appAttemptId, 3);

    final ContainerId recoverOContainerId4 = ContainerId.newContainerId(
        appAttemptId, 4);

    final NMContainerStatus recoverOContainerReport2 =
        NMContainerStatus.newInstance(
            recoverOContainerId2, 0, ContainerState.RUNNING,
            fakeResource, fakeDiagnostics, 0,
            fakePriority, 0, null,
            ExecutionType.OPPORTUNISTIC, -1);

    final NMContainerStatus recoverOContainerReport3 =
        NMContainerStatus.newInstance(
            recoverOContainerId3, 0, ContainerState.RUNNING,
            fakeResource, fakeDiagnostics, 0,
            fakePriority, 0, null,
            ExecutionType.OPPORTUNISTIC, -1);

    rm.registerNode(
        "h3:1234", 4096, 10,
        Collections.singletonList(
            appAttemptId.getApplicationId()),
        Arrays.asList(recoverOContainerReport2, recoverOContainerReport3));

    assertEquals(3, metrics.getAllocatedContainers());

    // Make sure that the recovered GContainer
    // does not increment OContainer count
    final ContainerId recoverGContainerId = ContainerId.newContainerId(
        appAttemptId, 5);

    final NMContainerStatus recoverGContainerReport =
        NMContainerStatus.newInstance(
            recoverGContainerId, 0, ContainerState.RUNNING,
            fakeResource, fakeDiagnostics, 0,
            fakePriority, 0, null,
            ExecutionType.GUARANTEED, -1);

    rm.registerNode(
        "h4:1234", 4096, 10,
        Collections.singletonList(
            appAttemptId.getApplicationId()),
        Collections.singletonList(recoverGContainerReport));

    assertEquals(3, metrics.getAllocatedContainers());

    final ContainerId completedOContainerId = ContainerId.newContainerId(
        appAttemptId, 6);

    final NMContainerStatus completedOContainerReport =
        NMContainerStatus.newInstance(
            completedOContainerId, 0, ContainerState.COMPLETE,
            fakeResource, fakeDiagnostics, 0,
            fakePriority, 0, null,
            ExecutionType.OPPORTUNISTIC, -1);

    // Tests that completed containers are not recorded
    rm.registerNode(
        "h5:1234", 4096, 10,
        Collections.singletonList(
            appAttemptId.getApplicationId()),
        Collections.singletonList(completedOContainerReport));

    assertEquals(3, metrics.getAllocatedContainers());
  }

  @Test
  @Timeout(value = 60)
  public void testAMCrashDuringAllocate() throws Exception {
    MockNM nm = new MockNM("h:1234", 4096, rm.getResourceTrackerService());
    nm.registerNode();

    RMApp app = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    ApplicationAttemptId attemptId0 =
        app.getCurrentAppAttempt().getAppAttemptId();
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm);

    //simulate AM crash by replacing the current attempt
    //Do not use rm.failApplicationAttempt, the bug will skip due to
    //ApplicationDoesNotExistInCacheException
    CapacityScheduler scheduler= ((CapacityScheduler) rm.getRMContext().
        getScheduler());
    SchedulerApplication<FiCaSchedulerApp> schApp =
        (SchedulerApplication<FiCaSchedulerApp>)scheduler.
        getSchedulerApplications().get(attemptId0.getApplicationId());
    final ApplicationAttemptId appAttemptId1 = TestUtils.
        getMockApplicationAttemptId(1, 1);
    schApp.setCurrentAppAttempt(new FiCaSchedulerApp(appAttemptId1,
        null, scheduler.getQueue("default"), null, rm.getRMContext()));

    //start to allocate
    am.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
        "*", Resources.createResource(1 * GB), 2)), null);
  }

  @Test
  @Timeout(value = 60)
  public void testNodeRemovalDuringAllocate() throws Exception {
    MockNM nm1 = new MockNM("h1:1234", 4096, rm.getResourceTrackerService());
    MockNM nm2 = new MockNM("h2:1234", 4096, rm.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();

    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    OpportunisticContainerAllocatorAMService amservice =
        (OpportunisticContainerAllocatorAMService) rm
            .getApplicationMasterService();
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .withUnmanagedAM(false)
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId attemptId =
        app1.getCurrentAppAttempt().getAppAttemptId();
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());

    OpportunisticContainerContext ctxt = ((CapacityScheduler) scheduler)
        .getApplicationAttempt(attemptId).getOpportunisticContainerContext();

    // Both node 1 and node 2 will be applicable for scheduling.
    nm1.nodeHeartbeat(oppContainersStatus, true);
    nm2.nodeHeartbeat(oppContainersStatus, true);

    for (int i = 0; i < 10; i++) {
      am1.allocate(
          Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
              "*", Resources.createResource(1 * GB), 2)),
          null);
      if (ctxt.getNodeMap().size() == 2) {
        break;
      }
      Thread.sleep(50);
    }
    assertEquals(2, ctxt.getNodeMap().size());
    // Remove node from scheduler but not from AM Service.
    scheduler.handle(new NodeRemovedSchedulerEvent(rmNode1));
    // After removal of node 1, only 1 node will be applicable for scheduling.
    for (int i = 0; i < 10; i++) {
      try {
        am1.allocate(
            Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
                "*", Resources.createResource(1 * GB), 2)),
            null);
      } catch (Exception e) {
        fail("Allocate request should be handled on node removal");
      }
      if (ctxt.getNodeMap().size() == 1) {
        break;
      }
      Thread.sleep(50);
    }
    assertEquals(1, ctxt.getNodeMap().size());
  }

  @Test
  @Timeout(value = 60)
  public void testAppAttemptRemovalAfterNodeRemoval() throws Exception {
    MockNM nm = new MockNM("h:1234", 4096, rm.getResourceTrackerService());

    nm.registerNode();
    nm.nodeHeartbeat(oppContainersStatus, true);

    RMApp app = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("default")
            .build());
    ApplicationAttemptId attemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm);
    ResourceScheduler scheduler = rm.getResourceScheduler();
    SchedulerApplicationAttempt schedulerAttempt =
        ((CapacityScheduler)scheduler).getApplicationAttempt(attemptId);
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm.getNodeId());

    nm.nodeHeartbeat(oppContainersStatus, true);

    GenericTestUtils.waitFor(() ->
        scheduler.getNumClusterNodes() == 1, 10, 200 * 100);

    AllocateResponse allocateResponse = am.allocate(
            Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
                "*", Resources.createResource(1 * GB), 2, true, null,
                ExecutionTypeRequest.newInstance(
                    ExecutionType.OPPORTUNISTIC, true))),
                null);
    List<Container> allocatedContainers = allocateResponse
        .getAllocatedContainers();
    Container container = allocatedContainers.get(0);
    scheduler.handle(new NodeRemovedSchedulerEvent(rmNode1));

    GenericTestUtils.waitFor(() ->
        scheduler.getNumClusterNodes() == 0, 10, 200 * 100);

    //test YARN-9165
    RMContainer rmContainer = null;
    rmContainer = SchedulerUtils.createOpportunisticRmContainer(
                    rm.getRMContext(), container, true);
    if (rmContainer == null) {
      rmContainer = new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container),
        schedulerAttempt.getApplicationAttemptId(), container.getNodeId(),
        schedulerAttempt.getUser(), rm.getRMContext(), true);
    }
    //test YARN-9164
    schedulerAttempt.addRMContainer(container.getId(), rmContainer);
    scheduler.handle(new AppAttemptRemovedSchedulerEvent(attemptId,
        RMAppAttemptState.FAILED, false));
  }

  private OpportunisticContainersStatus getOpportunisticStatus() {
    return getOppurtunisticStatus(-1, 100, 1000);
  }

  private OpportunisticContainersStatus getOppurtunisticStatus(int waitTime,
      int queueLength, int queueCapacity) {
    OpportunisticContainersStatus status =
        OpportunisticContainersStatus.newInstance();
    status.setEstimatedQueueWaitTime(waitTime);
    status.setOpportQueueCapacity(queueCapacity);
    status.setWaitQueueLength(queueLength);
    return status;
  }

  // Test if the OpportunisticContainerAllocatorAMService can handle both
  // DSProtocol as well as AMProtocol clients
  @Test
  public void testRPCWrapping() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class
        .getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    conf.setSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS, addr);
    final RecordFactory factory = RecordFactoryProvider.getRecordFactory(null);
    final RMContext rmContext = new RMContextImpl() {
      @Override
      public AMLivelinessMonitor getAMLivelinessMonitor() {
        return null;
      }

      @Override
      public Configuration getYarnConfiguration() {
        return new YarnConfiguration();
      }

      @Override
      public RMContainerTokenSecretManager getContainerTokenSecretManager() {
        return new RMContainerTokenSecretManager(conf);
      }

      @Override
      public ResourceScheduler getScheduler() {
        return new FifoScheduler();
      }
    };
    Container c = factory.newRecordInstance(Container.class);
    c.setExecutionType(ExecutionType.OPPORTUNISTIC);
    c.setId(
        ContainerId.newContainerId(
            ApplicationAttemptId.newInstance(
                ApplicationId.newInstance(12345, 1), 2), 3));
    AllocateRequest allReq =
        (AllocateRequestPBImpl)factory.newRecordInstance(AllocateRequest.class);
    allReq.setAskList(Arrays.asList(
        ResourceRequest.newInstance(Priority.UNDEFINED, "a",
            Resource.newInstance(1, 2), 1, true, "exp",
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true))));
    OpportunisticContainerAllocatorAMService service =
        createService(factory, rmContext, c);
    conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);
    Server server = service.getServer(rpc, conf, addr, null);
    server.start();

    // Verify that the OpportunisticContainerAllocatorAMSercvice can handle
    // vanilla ApplicationMasterProtocol clients
    RPC.setProtocolEngine(conf, ApplicationMasterProtocolPB.class,
        ProtobufRpcEngine2.class);
    ApplicationMasterProtocolPB ampProxy =
        RPC.getProxy(ApplicationMasterProtocolPB
            .class, 1, NetUtils.getConnectAddress(server), conf);
    RegisterApplicationMasterResponse regResp =
        new RegisterApplicationMasterResponsePBImpl(
            ampProxy.registerApplicationMaster(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        RegisterApplicationMasterRequest.class)).getProto()));
    assertEquals("dummyQueue", regResp.getQueue());
    FinishApplicationMasterResponse finishResp =
        new FinishApplicationMasterResponsePBImpl(
            ampProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(
                        FinishApplicationMasterRequest.class)).getProto()
            ));
    assertEquals(false, finishResp.getIsUnregistered());
    AllocateResponse allocResp =
        new AllocateResponsePBImpl(
            ampProxy.allocate(null,
                ((AllocateRequestPBImpl)factory
                    .newRecordInstance(AllocateRequest.class)).getProto())
        );
    List<Container> allocatedContainers = allocResp.getAllocatedContainers();
    assertEquals(1, allocatedContainers.size());
    assertEquals(ExecutionType.OPPORTUNISTIC,
        allocatedContainers.get(0).getExecutionType());
    assertEquals(12345, allocResp.getNumClusterNodes());


    // Verify that the DistrubutedSchedulingService can handle the
    // DistributedSchedulingAMProtocol clients as well
    RPC.setProtocolEngine(conf, DistributedSchedulingAMProtocolPB.class,
        ProtobufRpcEngine2.class);
    DistributedSchedulingAMProtocolPB dsProxy =
        RPC.getProxy(DistributedSchedulingAMProtocolPB
            .class, 1, NetUtils.getConnectAddress(server), conf);

    RegisterDistributedSchedulingAMResponse dsRegResp =
        new RegisterDistributedSchedulingAMResponsePBImpl(
            dsProxy.registerApplicationMasterForDistributedScheduling(null,
                ((RegisterApplicationMasterRequestPBImpl)factory
                    .newRecordInstance(RegisterApplicationMasterRequest.class))
                    .getProto()));
    assertEquals(54321L, dsRegResp.getContainerIdStart());
    assertEquals(4,
        dsRegResp.getMaxContainerResource().getVirtualCores());
    assertEquals(1024,
        dsRegResp.getMinContainerResource().getMemorySize());
    assertEquals(2,
        dsRegResp.getIncrContainerResource().getVirtualCores());

    DistributedSchedulingAllocateRequestPBImpl distAllReq =
        (DistributedSchedulingAllocateRequestPBImpl)factory.newRecordInstance(
            DistributedSchedulingAllocateRequest.class);
    distAllReq.setAllocateRequest(allReq);
    distAllReq.setAllocatedContainers(Arrays.asList(c));
    DistributedSchedulingAllocateResponse dsAllocResp =
        new DistributedSchedulingAllocateResponsePBImpl(
            dsProxy.allocateForDistributedScheduling(null,
                distAllReq.getProto()));
    assertEquals(
        "h1", dsAllocResp.getNodesForScheduling().get(0).getNodeId().getHost());
    assertEquals(
        "l1", dsAllocResp.getNodesForScheduling().get(1).getNodePartition());

    FinishApplicationMasterResponse dsfinishResp =
        new FinishApplicationMasterResponsePBImpl(
            dsProxy.finishApplicationMaster(null,
                ((FinishApplicationMasterRequestPBImpl) factory
                    .newRecordInstance(FinishApplicationMasterRequest.class))
                    .getProto()));
    assertEquals(
        false, dsfinishResp.getIsUnregistered());
  }

  private OpportunisticContainerAllocatorAMService createService(
      final RecordFactory factory, final RMContext rmContext,
      final Container c) {
    return new OpportunisticContainerAllocatorAMService(rmContext, null) {
      @Override
      public RegisterApplicationMasterResponse registerApplicationMaster(
          RegisterApplicationMasterRequest request) throws
          YarnException, IOException {
        RegisterApplicationMasterResponse resp = factory.newRecordInstance(
            RegisterApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setQueue("dummyQueue");
        return resp;
      }

      @Override
      public FinishApplicationMasterResponse finishApplicationMaster(
          FinishApplicationMasterRequest request) throws YarnException,
          IOException {
        FinishApplicationMasterResponse resp = factory.newRecordInstance(
            FinishApplicationMasterResponse.class);
        // Dummy Entry to Assert that we get this object back
        resp.setIsUnregistered(false);
        return resp;
      }

      @Override
      public AllocateResponse allocate(AllocateRequest request) throws
          YarnException, IOException {
        AllocateResponse response = factory.newRecordInstance(
            AllocateResponse.class);
        response.setNumClusterNodes(12345);
        response.setAllocatedContainers(Arrays.asList(c));
        return response;
      }

      @Override
      public RegisterDistributedSchedulingAMResponse
          registerApplicationMasterForDistributedScheduling(
          RegisterApplicationMasterRequest request)
          throws YarnException, IOException {
        RegisterDistributedSchedulingAMResponse resp = factory
            .newRecordInstance(RegisterDistributedSchedulingAMResponse.class);
        resp.setContainerIdStart(54321L);
        resp.setMaxContainerResource(Resource.newInstance(4096, 4));
        resp.setMinContainerResource(Resource.newInstance(1024, 1));
        resp.setIncrContainerResource(Resource.newInstance(2048, 2));
        return resp;
      }

      @Override
      public DistributedSchedulingAllocateResponse
          allocateForDistributedScheduling(
          DistributedSchedulingAllocateRequest request)
          throws YarnException, IOException {
        List<ResourceRequest> askList =
            request.getAllocateRequest().getAskList();
        List<Container> allocatedContainers = request.getAllocatedContainers();
        assertEquals(1, allocatedContainers.size());
        assertEquals(ExecutionType.OPPORTUNISTIC,
            allocatedContainers.get(0).getExecutionType());
        assertEquals(1, askList.size());
        assertTrue(askList.get(0)
            .getExecutionTypeRequest().getEnforceExecutionType());
        DistributedSchedulingAllocateResponse resp = factory
            .newRecordInstance(DistributedSchedulingAllocateResponse.class);
        RemoteNode remoteNode1 = RemoteNode.newInstance(
            NodeId.newInstance("h1", 1234), "http://h1:4321");
        RemoteNode remoteNode2 = RemoteNode.newInstance(
            NodeId.newInstance("h2", 1234), "http://h2:4321");
        remoteNode2.setNodePartition("l1");
        resp.setNodesForScheduling(
            Arrays.asList(remoteNode1, remoteNode2));
        return resp;
      }
    };
  }
}
