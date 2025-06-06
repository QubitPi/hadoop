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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClientRMService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestClientRMService.class);

  private RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private String appType = "MockApp";

  private final static String QUEUE_1 = "Q-1";
  private final static String QUEUE_2 = "Q-2";
  private final static String APPLICATION_TAG_SC_PREPROCESSOR ="mytag:foo";
  private File resourceTypesFile = null;

  private Configuration conf;
  private ResourceManager resourceManager;
  private YarnRPC rpc;
  private ApplicationClientProtocol client;

  @Test
  public void testGetDecommissioningClusterNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();

    int nodeMemory = 1024;
    MockNM nm1 = rm.registerNode("host1:1234", nodeMemory);
    rm.sendNodeStarted(nm1);
    nm1.nodeHeartbeat(true);
    rm.waitForState(nm1.getNodeId(), NodeState.RUNNING);
    Integer decommissioningTimeout = 600;
    rm.sendNodeGracefulDecommission(nm1, decommissioningTimeout);
    rm.waitForState(nm1.getNodeId(), NodeState.DECOMMISSIONING);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    List<NodeReport> nodeReports = client.getClusterNodes(
        GetClusterNodesRequest.newInstance(
            EnumSet.of(NodeState.DECOMMISSIONING)))
        .getNodeReports();
    assertEquals(1, nodeReports.size());
    NodeReport nr = nodeReports.iterator().next();
    assertEquals(decommissioningTimeout, nr.getDecommissioningTimeout());
    assertNull(nr.getNodeUpdateType());
  }

  @Test
  public void testGetClusterNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));

    // Add a healthy node with label = x
    MockNM node = rm.registerNode("host1:1234", 1024);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("x"));
    labelsMgr.replaceLabelsOnNode(map);
    rm.sendNodeStarted(node);
    node.nodeHeartbeat(true);

    // Add and lose a node with label = y
    MockNM lostNode = rm.registerNode("host2:1235", 1024);
    rm.sendNodeStarted(lostNode);
    lostNode.nodeHeartbeat(true);
    rm.waitForState(lostNode.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(lostNode);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetClusterNodesRequest request =
        GetClusterNodesRequest.newInstance(EnumSet.of(NodeState.RUNNING));
    List<NodeReport> nodeReports =
        client.getClusterNodes(request).getNodeReports();
    assertEquals(1, nodeReports.size());
    assertNotSame(NodeState.UNHEALTHY, nodeReports.get(0).getNodeState(),
        "Node is expected to be healthy!");

    // Check node's label = x
    assertTrue(nodeReports.get(0).getNodeLabels().contains("x"));
    assertNull(nodeReports.get(0).getDecommissioningTimeout());
    assertNull(nodeReports.get(0).getNodeUpdateType());

    // Now make the node unhealthy.
    node.nodeHeartbeat(false);
    rm.waitForState(node.getNodeId(), NodeState.UNHEALTHY);

    // Call again
    nodeReports = client.getClusterNodes(request).getNodeReports();
    assertEquals(0, nodeReports.size(),
        "Unhealthy nodes should not show up by default");

    // Change label of host1 to y
    map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("y"));
    labelsMgr.replaceLabelsOnNode(map);

    // Now query for UNHEALTHY nodes
    request = GetClusterNodesRequest.newInstance(EnumSet.of(NodeState.UNHEALTHY));
    nodeReports = client.getClusterNodes(request).getNodeReports();
    assertEquals(1, nodeReports.size());
    assertEquals(NodeState.UNHEALTHY, nodeReports.get(0).getNodeState(),
        "Node is expected to be unhealthy!");

    assertTrue(nodeReports.get(0).getNodeLabels().contains("y"));
    assertNull(nodeReports.get(0).getDecommissioningTimeout());
    assertNull(nodeReports.get(0).getNodeUpdateType());

    // Remove labels of host1
    map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("y"));
    labelsMgr.removeLabelsFromNode(map);

    // Query all states should return all nodes
    rm.registerNode("host3:1236", 1024);
    request = GetClusterNodesRequest.newInstance(EnumSet.allOf(NodeState.class));
    nodeReports = client.getClusterNodes(request).getNodeReports();
    assertEquals(3, nodeReports.size());

    // All host1-3's label should be empty (instead of null)
    for (NodeReport report : nodeReports) {
      assertTrue(report.getNodeLabels() != null
          && report.getNodeLabels().isEmpty());
      assertNull(report.getDecommissioningTimeout());
      assertNull(report.getNodeUpdateType());
    }
  }

  @Test
  public void testNonExistingApplicationReport() throws YarnException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null, null);
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(ApplicationId.newInstance(0, 0));
    try {
      rmService.getApplicationReport(request);
      fail();
    } catch (ApplicationNotFoundException ex) {
      assertEquals(ex.getMessage(),
          "Application with id '" + request.getApplicationId()
              + "' doesn't exist in RM. Please check that the "
              + "job submission was successful.");
    }
  }

  @Test
  public void testGetApplicationReport() throws Exception {
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);

    ApplicationId appId1 = getApplicationId(1);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    ClientRMService rmService = new ClientRMService(rmContext, scheduler,
        null, mockAclsManager, null, null);
    try {
      GetApplicationReportRequest request = recordFactory
          .newRecordInstance(GetApplicationReportRequest.class);
      request.setApplicationId(appId1);
      GetApplicationReportResponse response =
          rmService.getApplicationReport(request);
      ApplicationReport report = response.getApplicationReport();
      ApplicationResourceUsageReport usageReport =
          report.getApplicationResourceUsageReport();
      assertEquals(10, usageReport.getMemorySeconds());
      assertEquals(3, usageReport.getVcoreSeconds());
      assertEquals("<Not set>", report.getAmNodeLabelExpression());
      assertEquals("<Not set>", report.getAppNodeLabelExpression());

      // if application has am node label set to blank
      ApplicationId appId2 = getApplicationId(2);
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
          ApplicationAccessType.VIEW_APP, null, appId2)).thenReturn(true);
      request.setApplicationId(appId2);
      response = rmService.getApplicationReport(request);
      report = response.getApplicationReport();

      assertEquals(NodeLabel.DEFAULT_NODE_LABEL_PARTITION,
          report.getAmNodeLabelExpression());
      assertEquals(NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET,
          report.getAppNodeLabelExpression());

      // if application has am node label set to blank
      ApplicationId appId3 = getApplicationId(3);
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
          ApplicationAccessType.VIEW_APP, null, appId3)).thenReturn(true);

      request.setApplicationId(appId3);
      response = rmService.getApplicationReport(request);
      report = response.getApplicationReport();

      assertEquals("high-mem", report.getAmNodeLabelExpression());
      assertEquals("high-mem", report.getAppNodeLabelExpression());

      // if application id is null
      GetApplicationReportRequest invalidRequest = recordFactory
          .newRecordInstance(GetApplicationReportRequest.class);
      invalidRequest.setApplicationId(null);
      try {
        rmService.getApplicationReport(invalidRequest);
      } catch (YarnException e) {
        // rmService should return a ApplicationNotFoundException
        // when a null application id is provided
        assertTrue(e instanceof ApplicationNotFoundException);
      }
    } finally {
      rmService.close();
    }
  }

  @Test
  public void testGetApplicationAttemptReport() throws YarnException,
      IOException {
    ClientRMService rmService = createRMService();
    GetApplicationAttemptReportRequest request = recordFactory
        .newRecordInstance(GetApplicationAttemptReportRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    request.setApplicationAttemptId(attemptId);

    try {
      GetApplicationAttemptReportResponse response = rmService
          .getApplicationAttemptReport(request);
      assertEquals(attemptId, response.getApplicationAttemptReport()
          .getApplicationAttemptId());
    } catch (ApplicationNotFoundException ex) {
      fail(ex.getMessage());
    }
  }

  @Test
  public void testGetApplicationResourceUsageReportDummy() throws YarnException,
      IOException {
    ApplicationAttemptId attemptId = getApplicationAttemptId(1);
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });
    ApplicationSubmissionContext asContext =
        mock(ApplicationSubmissionContext.class);
    YarnConfiguration config = new YarnConfiguration();
    RMAppAttemptImpl rmAppAttemptImpl = new RMAppAttemptImpl(attemptId,
        rmContext, scheduler, null, asContext, config, null, null);
    ApplicationResourceUsageReport report = rmAppAttemptImpl
        .getApplicationResourceUsageReport();
    assertEquals(report, RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT);
  }

  @Test
  public void testGetApplicationAttempts() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    GetApplicationAttemptsRequest request = recordFactory
        .newRecordInstance(GetApplicationAttemptsRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    request.setApplicationId(ApplicationId.newInstance(123456, 1));

    try {
      GetApplicationAttemptsResponse response = rmService
          .getApplicationAttempts(request);
      assertEquals(1, response.getApplicationAttemptList().size());
      assertEquals(attemptId, response.getApplicationAttemptList()
          .get(0).getApplicationAttemptId());

    } catch (ApplicationNotFoundException ex) {
      fail(ex.getMessage());
    }
  }

  @Test
  public void testGetContainerReport() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    GetContainerReportRequest request = recordFactory
        .newRecordInstance(GetContainerReportRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    request.setContainerId(containerId);

    try {
      GetContainerReportResponse response = rmService
          .getContainerReport(request);
      assertEquals(containerId, response.getContainerReport()
          .getContainerId());
    } catch (ApplicationNotFoundException ex) {
      fail(ex.getMessage());
    }
  }

  @Test
  public void testGetContainers() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    GetContainersRequest request = recordFactory
        .newRecordInstance(GetContainersRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    request.setApplicationAttemptId(attemptId);
    try {
      GetContainersResponse response = rmService.getContainers(request);
      assertEquals(containerId, response.getContainerList().get(0)
          .getContainerId());
    } catch (ApplicationNotFoundException ex) {
      fail(ex.getMessage());
    }
  }

  public ClientRMService createRMService() throws IOException, YarnException {
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    ConcurrentHashMap<ApplicationId, RMApp> apps = getRMApps(rmContext,
        scheduler);
    when(rmContext.getRMApps()).thenReturn(apps);
    when(rmContext.getYarnConfiguration()).thenReturn(new Configuration());
    RMAppManager appManager = new RMAppManager(rmContext, scheduler, null,
        mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(
        mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
            any(QueueACL.class), any(RMApp.class), any(),
            any())).thenReturn(true);
    return new ClientRMService(rmContext, scheduler, appManager,
        mockAclsManager, mockQueueACLsManager, null);
  }

  @Test
  public void testForceKillNonExistingApplication() throws YarnException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null, null);
    ApplicationId applicationId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(), 0);
    KillApplicationRequest request =
        KillApplicationRequest.newInstance(applicationId);
    try {
      rmService.forceKillApplication(request);
      fail();
    } catch (ApplicationNotFoundException ex) {
      assertEquals(ex.getMessage(),
          "Trying to kill an absent " +
              "application " + request.getApplicationId());
    }
  }

  @Test
  public void testApplicationTagsValidation() throws IOException {
    conf = new YarnConfiguration();
    int maxtags = 3, appMaxTagLength = 5;
    conf.setInt(YarnConfiguration.RM_APPLICATION_MAX_TAGS, maxtags);
    conf.setInt(YarnConfiguration.RM_APPLICATION_MAX_TAG_LENGTH,
        appMaxTagLength);
    MockRM rm = new MockRM(conf);
    resourceManager = rm;
    rm.init(conf);
    rm.start();

    ClientRMService rmService = rm.getClientRMService();

    List<String> tags = Arrays.asList("Tag1", "Tag2", "Tag3", "Tag4");
    validateApplicationTag(rmService, tags,
        "Too many applicationTags, a maximum of only " + maxtags
            + " are allowed!");

    tags = Arrays.asList("ApplicationTag1", "ApplicationTag2",
        "ApplicationTag3");
    // tags are converted to lowercase in
    // ApplicationSubmissionContext#setApplicationTags
    validateApplicationTag(rmService, tags,
        "Tag applicationtag1 is too long, maximum allowed length of a tag is "
            + appMaxTagLength);

    tags = Arrays.asList("tãg1", "tag2#");
    validateApplicationTag(rmService, tags,
        "A tag can only have ASCII characters! Invalid tag - tãg1");
  }

  private void validateApplicationTag(ClientRMService rmService,
      List<String> tags, String errorMsg) {
    SubmitApplicationRequest submitRequest = mockSubmitAppRequest(
        getApplicationId(101), MockApps.newAppName(), QUEUE_1,
        new HashSet<String>(tags));
    try {
      rmService.submitApplication(submitRequest);
      fail();
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains(errorMsg));
    }
  }

  @Test
  public void testForceKillApplication() throws Exception {
    conf = new YarnConfiguration();
    conf.setBoolean(MockRM.ENABLE_WEBAPP, true);
    MockRM rm = new MockRM(conf);
    resourceManager = rm;
    rm.init(conf);
    rm.start();

    ClientRMService rmService = rm.getClientRMService();
    GetApplicationsRequest getRequest = GetApplicationsRequest.newInstance(
        EnumSet.of(YarnApplicationState.KILLED));

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(1024, rm);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withUnmanagedAM(true)
            .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);

    assertEquals(0, rmService.getApplications(getRequest).getApplicationList().size(),
        "Incorrect number of apps in the RM");

    KillApplicationRequest killRequest1 =
        KillApplicationRequest.newInstance(app1.getApplicationId());
    String diagnostic = "message1";
    killRequest1.setDiagnostics(diagnostic);
    KillApplicationRequest killRequest2 =
        KillApplicationRequest.newInstance(app2.getApplicationId());

    int killAttemptCount = 0;
    for (int i = 0; i < 100; i++) {
      KillApplicationResponse killResponse1 =
          rmService.forceKillApplication(killRequest1);
      killAttemptCount++;
      if (killResponse1.getIsKillCompleted()) {
        break;
      }
      Thread.sleep(10);
    }
    assertTrue(killAttemptCount > 1,
        "Kill attempt count should be greater than 1 for managed AMs");
    assertEquals(1,
        rmService.getApplications(getRequest).getApplicationList().size(),
        "Incorrect number of apps in the RM");
    assertTrue(app1.getDiagnostics().toString().contains(diagnostic),
        "Diagnostic message is incorrect");

    KillApplicationResponse killResponse2 =
        rmService.forceKillApplication(killRequest2);
    assertTrue(killResponse2.getIsKillCompleted(),
        "Killing UnmanagedAM should falsely acknowledge true");
    for (int i = 0; i < 100; i++) {
      if (2 ==
          rmService.getApplications(getRequest).getApplicationList().size()) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals(2, rmService.getApplications(getRequest).getApplicationList().size(),
        "Incorrect number of apps in the RM");
  }

  @Test
  public void testMoveAbsentApplication() throws YarnException {
    assertThrows(ApplicationNotFoundException.class, () -> {
      RMContext rmContext = mock(RMContext.class);
      when(rmContext.getRMApps()).thenReturn(
          new ConcurrentHashMap<ApplicationId, RMApp>());
      ClientRMService rmService = new ClientRMService(rmContext, null, null,
          null, null, null);
      ApplicationId applicationId =
          BuilderUtils.newApplicationId(System.currentTimeMillis(), 0);
      MoveApplicationAcrossQueuesRequest request =
          MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
          "newqueue");
      rmService.moveApplicationAcrossQueues(request);
    });
  }

  @Test
  public void testMoveApplicationSubmitTargetQueue() throws Exception {
    // move the application as the owner
    ApplicationId applicationId = getApplicationId(1);
    UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
    QueueACLsManager queueACLsManager = getQueueAclManager("allowed_queue",
        QueueACL.SUBMIT_APPLICATIONS, aclUGI);
    ApplicationACLsManager appAclsManager = getAppAclManager();

    ClientRMService rmService = createClientRMServiceForMoveApplicationRequest(
        applicationId, aclUGI.getShortUserName(), appAclsManager,
        queueACLsManager);

    // move as the owner queue in the acl
    MoveApplicationAcrossQueuesRequest moveAppRequest =
        MoveApplicationAcrossQueuesRequest.
        newInstance(applicationId, "allowed_queue");
    rmService.moveApplicationAcrossQueues(moveAppRequest);

    // move as the owner queue not in the acl
    moveAppRequest = MoveApplicationAcrossQueuesRequest.newInstance(
        applicationId, "not_allowed");

    try {
      rmService.moveApplicationAcrossQueues(moveAppRequest);
      fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      assertTrue(rex.getCause() instanceof AccessControlException,
          "AccessControlException is expected");
    }

    // ACL is owned by "moveuser", move is performed as a different user
    aclUGI = UserGroupInformation.createUserForTesting("moveuser",
        new String[]{});
    queueACLsManager = getQueueAclManager("move_queue",
        QueueACL.SUBMIT_APPLICATIONS, aclUGI);
    appAclsManager = getAppAclManager();
    ClientRMService rmService2 =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueACLsManager);

    // access to the queue not OK: user not allowed in this queue
    MoveApplicationAcrossQueuesRequest moveAppRequest2 =
        MoveApplicationAcrossQueuesRequest.
            newInstance(applicationId, "move_queue");
    try {
      rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      assertTrue(rex.getCause() instanceof AccessControlException,
          "AccessControlException is expected");
    }

    // execute the move as the acl owner
    // access to the queue OK: user allowed in this queue
    aclUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        return rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      }
    });
  }

  @Test
  public void testMoveApplicationAdminTargetQueue() throws Exception {
    ApplicationId applicationId = getApplicationId(1);
    UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
    QueueACLsManager queueAclsManager = getQueueAclManager("allowed_queue",
        QueueACL.ADMINISTER_QUEUE, aclUGI);
    ApplicationACLsManager appAclsManager = getAppAclManager();
    ClientRMService rmService =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueAclsManager);

    // user is admin move to queue in acl
    MoveApplicationAcrossQueuesRequest moveAppRequest =
        MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
            "allowed_queue");
    rmService.moveApplicationAcrossQueues(moveAppRequest);

    // user is admin move to queue not in acl
    moveAppRequest = MoveApplicationAcrossQueuesRequest.newInstance(
        applicationId, "not_allowed");

    try {
      rmService.moveApplicationAcrossQueues(moveAppRequest);
      fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      assertTrue(rex.getCause() instanceof AccessControlException,
          "AccessControlException is expected");
    }

    // ACL is owned by "moveuser", move is performed as a different user
    aclUGI = UserGroupInformation.createUserForTesting("moveuser",
        new String[]{});
    queueAclsManager = getQueueAclManager("move_queue",
        QueueACL.ADMINISTER_QUEUE, aclUGI);
    appAclsManager = getAppAclManager();
    ClientRMService rmService2 =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueAclsManager);

    // no access to this queue
    MoveApplicationAcrossQueuesRequest moveAppRequest2 =
        MoveApplicationAcrossQueuesRequest.
            newInstance(applicationId, "move_queue");

    try {
      rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      assertTrue(rex.getCause() instanceof AccessControlException,
          "AccessControlException is expected");
    }

    // execute the move as the acl owner
    // access to the queue OK: user allowed in this queue
    aclUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        return rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      }
    });
  }

  @Test
  public void testNonExistingQueue() throws Exception {
    assertThrows(YarnException.class, () -> {
      ApplicationId applicationId = getApplicationId(1);
      UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
      QueueACLsManager queueAclsManager = getQueueAclManager();
      ApplicationACLsManager appAclsManager = getAppAclManager();
      ClientRMService rmService =
          createClientRMServiceForMoveApplicationRequest(applicationId,
          aclUGI.getShortUserName(), appAclsManager, queueAclsManager);
      MoveApplicationAcrossQueuesRequest moveAppRequest =
          MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
          "unknown_queue");
      rmService.moveApplicationAcrossQueues(moveAppRequest);
    });
  }

  /**
   * Create an instance of ClientRMService for testing
   * moveApplicationAcrossQueues requests.
   * @param applicationId the application
   * @return ClientRMService
   */
  private ClientRMService createClientRMServiceForMoveApplicationRequest(
      ApplicationId applicationId, String appOwner,
      ApplicationACLsManager appAclsManager,
      QueueACLsManager queueAclsManager) {
    RMApp app = mock(RMApp.class);
    when(app.getUser()).thenReturn(appOwner);
    when(app.getState()).thenReturn(RMAppState.RUNNING);
    when(app.getApplicationId()).thenReturn(applicationId);
    ConcurrentHashMap<ApplicationId, RMApp> apps = new ConcurrentHashMap<>();
    apps.put(applicationId, app);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(apps);

    RMAppManager rmAppManager = mock(RMAppManager.class);
    return new ClientRMService(rmContext, null, rmAppManager, appAclsManager,
        queueAclsManager, null);
  }

  /**
   * Plain application acl manager that always returns true.
   * @return ApplicationACLsManager
   */
  private ApplicationACLsManager getAppAclManager() {
    ApplicationACLsManager aclsManager = mock(ApplicationACLsManager.class);
    when(aclsManager.checkAccess(
        any(UserGroupInformation.class),
        any(ApplicationAccessType.class),
        any(String.class),
        any(ApplicationId.class))).thenReturn(true);
    return aclsManager;
  }

  /**
   * Generate the Queue acl.
   * @param allowedQueue the queue to allow the move to
   * @param queueACL the acl to check: submit app or queue admin
   * @param aclUser the user to check
   * @return QueueACLsManager
   */
  private QueueACLsManager getQueueAclManager(String allowedQueue,
      QueueACL queueACL, UserGroupInformation aclUser) throws IOException {
    // ACL that checks the queue is allowed
    QueueACLsManager queueACLsManager = mock(QueueACLsManager.class);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(),
        any())).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) {
              final UserGroupInformation user =
                  (UserGroupInformation) invocationOnMock.getArguments()[0];
              final QueueACL acl =
                  (QueueACL) invocationOnMock.getArguments()[1];
              return (queueACL.equals(acl) &&
                  aclUser.getShortUserName().equals(user.getShortUserName()));
            }
        });

    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(),
        any(),
        any(String.class))).thenAnswer(new Answer<Boolean>() {
          @Override
          public Boolean answer(InvocationOnMock invocationOnMock) {
            final UserGroupInformation user =
                (UserGroupInformation) invocationOnMock.getArguments()[0];
            final QueueACL acl = (QueueACL) invocationOnMock.getArguments()[1];
            final String queue = (String) invocationOnMock.getArguments()[5];
            return (allowedQueue.equals(queue) && queueACL.equals(acl) &&
                aclUser.getShortUserName().equals(user.getShortUserName()));
          }
        });
    return queueACLsManager;
  }

  /**
   * QueueACLsManager that always returns false when a target queue is passed
   * in and true for other checks to simulate a missing queue.
   * @return QueueACLsManager
   */
  private QueueACLsManager getQueueAclManager() {
    QueueACLsManager queueACLsManager = mock(QueueACLsManager.class);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyList(),
        any(String.class))).thenReturn(false);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyList())).thenReturn(true);
    return queueACLsManager;
  }

  @Test
  public void testGetQueueInfo() throws Exception {
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);
    when(mockAclsManager.checkAccess(any(UserGroupInformation.class),
        any(ApplicationAccessType.class), any(),
        any(ApplicationId.class))).thenReturn(true);

    ClientRMService rmService = new ClientRMService(rmContext, scheduler,
        null, mockAclsManager, mockQueueACLsManager, null);
    GetQueueInfoRequest request = recordFactory
        .newRecordInstance(GetQueueInfoRequest.class);
    request.setQueueName("testqueue");
    request.setIncludeApplications(true);
    GetQueueInfoResponse queueInfo = rmService.getQueueInfo(request);
    List<ApplicationReport> applications = queueInfo.getQueueInfo()
        .getApplications();
    assertEquals(2, applications.size());
    Map<String, QueueConfigurations> queueConfigsByPartition =
        queueInfo.getQueueInfo().getQueueConfigurations();
    assertEquals(1, queueConfigsByPartition.size());
    assertTrue(queueConfigsByPartition.containsKey("*"));
    QueueConfigurations queueConfigs = queueConfigsByPartition.get("*");
    assertEquals(0.5f, queueConfigs.getCapacity(), 0.0001f);
    assertEquals(0.1f, queueConfigs.getAbsoluteCapacity(), 0.0001f);
    assertEquals(1.0f, queueConfigs.getMaxCapacity(), 0.0001f);
    assertEquals(1.0f, queueConfigs.getAbsoluteMaxCapacity(), 0.0001f);
    assertEquals(0.2f, queueConfigs.getMaxAMPercentage(), 0.0001f);

    request.setQueueName("nonexistentqueue");
    request.setIncludeApplications(true);
    // should not throw exception on nonexistent queue
    queueInfo = rmService.getQueueInfo(request);

    // Case where user does not have application access
    ApplicationACLsManager mockAclsManager1 =
        mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager1 =
        mock(QueueACLsManager.class);
    when(mockQueueACLsManager1.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(false);
    when(mockAclsManager1.checkAccess(any(UserGroupInformation.class),
        any(ApplicationAccessType.class), anyString(),
        any(ApplicationId.class))).thenReturn(false);

    ClientRMService rmService1 = new ClientRMService(rmContext, scheduler,
        null, mockAclsManager1, mockQueueACLsManager1, null);
    request.setQueueName("testqueue");
    request.setIncludeApplications(true);
    GetQueueInfoResponse queueInfo1 = rmService1.getQueueInfo(request);
    List<ApplicationReport> applications1 = queueInfo1.getQueueInfo()
        .getApplications();
    assertEquals(0, applications1.size());
  }

  @Test
  @Timeout(value = 30)
  @SuppressWarnings ("rawtypes")
  public void testAppSubmitWithSubmissionPreProcessor() throws Exception {
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    YarnConfiguration yConf = new YarnConfiguration();
    yConf.setBoolean(YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_ENABLED,
        true);
    yConf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    // Override the YARN configuration.
    when(rmContext.getYarnConfiguration()).thenReturn(yConf);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, scheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });
    ApplicationId appId1 = getApplicationId(100);
    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);

    ClientRMService rmService =
        new ClientRMService(rmContext, scheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);
    File rulesFile = File.createTempFile("submission_rules", ".tmp");
    rulesFile.deleteOnExit();
    rulesFile.createNewFile();

    yConf.set(YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_FILE_PATH,
        rulesFile.getAbsolutePath());
    rmService.serviceInit(yConf);
    rmService.serviceStart();

    BufferedWriter writer = new BufferedWriter(new FileWriter(rulesFile));
    writer.write("host.cluster1.com   NL=foo     Q=bar  TA=cluster:cluster1");
    writer.newLine();
    writer.write("host.cluster2.com   Q=hello  NL=zuess   TA=cluster:cluster2");
    writer.newLine();
    writer.write("host.cluster.*.com   Q=hello  NL=reg   TA=cluster:reg");
    writer.newLine();
    writer.write("host.cluster.*.com   Q=hello  NL=reg   TA=cluster:reg");
    writer.newLine();
    writer.write("*   TA=cluster:other    Q=default  NL=barfoo");
    writer.newLine();
    writer.write("host.testcluster1.com  Q=default");
    writer.flush();
    writer.close();
    rmService.getContextPreProcessor().refresh();
    setupCurrentCall("host.cluster1.com");
    SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(
        appId1, null, null);
    try {
      rmService.submitApplication(submitRequest1);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app1 = rmContext.getRMApps().get(appId1);
    assertNotNull(app1, "app doesn't exist");
    assertEquals(YarnConfiguration.DEFAULT_APPLICATION_NAME, app1.getName(),
        "app name doesn't match");
    assertTrue(app1.getApplicationTags().contains("cluster:cluster1"),
        "custom tag not present");
    assertEquals("bar", app1.getQueue(), "app queue doesn't match");
    assertEquals("foo",
        app1.getApplicationSubmissionContext().getNodeLabelExpression(),
        "app node label doesn't match");
    setupCurrentCall("host.cluster2.com");
    ApplicationId appId2 = getApplicationId(101);
    SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(
        appId2, null, null);
    submitRequest2.getApplicationSubmissionContext().setApplicationType(
        "matchType");
    Set<String> aTags = new HashSet<String>();
    aTags.add(APPLICATION_TAG_SC_PREPROCESSOR);
    submitRequest2.getApplicationSubmissionContext().setApplicationTags(aTags);
    try {
      rmService.submitApplication(submitRequest2);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app2 = rmContext.getRMApps().get(appId2);
    assertNotNull(app2, "app doesn't exist");
    assertEquals(YarnConfiguration.DEFAULT_APPLICATION_NAME, app2.getName(),
        "app name doesn't match");
    assertTrue(app2.getApplicationTags().contains(APPLICATION_TAG_SC_PREPROCESSOR),
        "client tag not present");
    assertTrue(app2.getApplicationTags().contains("cluster:cluster2"),
        "custom tag not present");
    assertEquals("hello", app2.getQueue(), "app queue doesn't match");
    assertEquals("zuess",
        app2.getApplicationSubmissionContext().getNodeLabelExpression(),
        "app node label doesn't match");
    // Test Default commands
    setupCurrentCall("host2.cluster3.com");
    ApplicationId appId3 = getApplicationId(102);
    SubmitApplicationRequest submitRequest3 = mockSubmitAppRequest(
        appId3, null, null);
    submitRequest3.getApplicationSubmissionContext().setApplicationType(
        "matchType");
    submitRequest3.getApplicationSubmissionContext().setApplicationTags(aTags);
    try {
      rmService.submitApplication(submitRequest3);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app3 = rmContext.getRMApps().get(appId3);
    assertNotNull(app3, "app doesn't exist");
    assertEquals(YarnConfiguration.DEFAULT_APPLICATION_NAME, app3.getName(),
        "app name doesn't match");
    assertTrue(app3.getApplicationTags().contains(APPLICATION_TAG_SC_PREPROCESSOR),
        "client tag not present");
    assertTrue(app3.getApplicationTags().contains("cluster:other"),
        "custom tag not present");
    assertEquals("default", app3.getQueue(), "app queue doesn't match");
    assertEquals("barfoo", app3.getApplicationSubmissionContext().getNodeLabelExpression(),
        "app node label doesn't match");
    // Test regex
    setupCurrentCall("host.cluster100.com");
    ApplicationId appId4 = getApplicationId(103);
    SubmitApplicationRequest submitRequest4 = mockSubmitAppRequest(
        appId4, null, null);
    try {
      rmService.submitApplication(submitRequest4);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app4 = rmContext.getRMApps().get(appId4);
    assertTrue(app4.getApplicationTags().contains("cluster:reg"),
        "custom tag not present");
    assertEquals("reg",
        app4.getApplicationSubmissionContext().getNodeLabelExpression(),
        "app node label doesn't match");
    testSubmissionContextWithAbsentTAG(rmService, rmContext);
    rmService.serviceStop();
  }

  private void testSubmissionContextWithAbsentTAG(ClientRMService rmService,
      RMContext rmContext) throws Exception {
    setupCurrentCall("host.testcluster1.com");
    ApplicationId appId5 = getApplicationId(104);
    SubmitApplicationRequest submitRequest5 = mockSubmitAppRequest(
        appId5, null, null);
    try {
      rmService.submitApplication(submitRequest5);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app5 = rmContext.getRMApps().get(appId5);
    assertEquals(app5.getApplicationTags().size(), 0, "custom tag  present");
    assertNull(app5.getApplicationSubmissionContext().getNodeLabelExpression(),
        "app node label present");
    assertEquals(app5.getQueue(), "default",
        "Queue name is not present");
  }
  private void setupCurrentCall(String hostName) throws UnknownHostException {
    Server.Call mockCall = mock(Server.Call.class);
    when(mockCall.getHostInetAddress()).thenReturn(
                InetAddress.getByAddress(hostName,
                        new byte[]{123, 123, 123, 123}));
    Server.getCurCall().set(mockCall);
  }

  @Test
  @Timeout(value = 30)
  @SuppressWarnings ("rawtypes")
  public void testAppSubmit() throws Exception {
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, scheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    ApplicationId appId1 = getApplicationId(100);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, scheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);
    rmService.init(new Configuration());

    // without name and queue

    SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(
        appId1, null, null);
    try {
      rmService.submitApplication(submitRequest1);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app1 = rmContext.getRMApps().get(appId1);
    assertNotNull(app1, "app doesn't exist");
    assertEquals(YarnConfiguration.DEFAULT_APPLICATION_NAME, app1.getName(),
        "app name doesn't match");
    assertEquals(YarnConfiguration.DEFAULT_QUEUE_NAME, app1.getQueue(),
        "app queue doesn't match");

    // with name and queue
    String name = MockApps.newAppName();
    String queue = MockApps.newQueue();
    ApplicationId appId2 = getApplicationId(101);
    SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(
        appId2, name, queue);
    submitRequest2.getApplicationSubmissionContext().setApplicationType(
        "matchType");
    try {
      rmService.submitApplication(submitRequest2);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }
    RMApp app2 = rmContext.getRMApps().get(appId2);
    assertNotNull(app2, "app doesn't exist");
    assertEquals(name, app2.getName(), "app name doesn't match");
    assertEquals(queue, app2.getQueue(), "app queue doesn't match");

    // duplicate appId
    try {
      rmService.submitApplication(submitRequest2);
    } catch (YarnException e) {
      fail("Exception is not expected.");
    }

    GetApplicationsRequest getAllAppsRequest =
        GetApplicationsRequest.newInstance(new HashSet<String>());
    GetApplicationsResponse getAllApplicationsResponse =
        rmService.getApplications(getAllAppsRequest);
    assertEquals(5,
        getAllApplicationsResponse.getApplicationList().size());

    Set<String> appTypes = new HashSet<String>();
    appTypes.add("matchType");

    getAllAppsRequest = GetApplicationsRequest.newInstance(appTypes);
    getAllApplicationsResponse =
        rmService.getApplications(getAllAppsRequest);
    assertEquals(1,
        getAllApplicationsResponse.getApplicationList().size());
    assertEquals(appId2,
        getAllApplicationsResponse.getApplicationList()
            .get(0).getApplicationId());

    // Test query with uppercase appType also works
    appTypes = new HashSet<String>();
    appTypes.add("MATCHTYPE");
    getAllAppsRequest = GetApplicationsRequest.newInstance(appTypes);
    getAllApplicationsResponse =
        rmService.getApplications(getAllAppsRequest);
    assertEquals(1,
        getAllApplicationsResponse.getApplicationList().size());
    assertEquals(appId2,
        getAllApplicationsResponse.getApplicationList()
            .get(0).getApplicationId());
  }

  @Test
  public void testGetApplications() throws Exception {
    /**
     * 1. Submit 3 applications alternately in two queues
     * 2. Test each of the filters
     */
    // Basic setup
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
    .getRMTimelineCollectorManager();

    RMAppManager appManager = new RMAppManager(rmContext, scheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(),
        any()))
        .thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, scheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);
    rmService.init(new Configuration());

    // Initialize appnames and queues
    String[] queues = {QUEUE_1, QUEUE_2};
    String[] appNames =
        {MockApps.newAppName(), MockApps.newAppName(), MockApps.newAppName()};
    ApplicationId[] appIds =
        {getApplicationId(101), getApplicationId(102), getApplicationId(103)};
    List<String> tags = Arrays.asList("Tag1", "Tag2", "Tag3");

    long[] submitTimeMillis = new long[3];
    // Submit applications
    for (int i = 0; i < appIds.length; i++) {
      ApplicationId appId = appIds[i];
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
              ApplicationAccessType.VIEW_APP, null, appId)).thenReturn(true);
      SubmitApplicationRequest submitRequest = mockSubmitAppRequest(
          appId, appNames[i], queues[i % queues.length],
          new HashSet<String>(tags.subList(0, i + 1)));
      // make sure each app is submitted at a different time
      Thread.sleep(1);
      rmService.submitApplication(submitRequest);
      submitTimeMillis[i] = rmService.getApplicationReport(
          GetApplicationReportRequest.newInstance(appId))
          .getApplicationReport().getStartTime();
    }

    // Test different cases of ClientRMService#getApplications()
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    assertEquals(6,
        rmService.getApplications(request).getApplicationList().size(),
        "Incorrect total number of apps");

    // Check limit
    request.setLimit(1L);
    assertEquals(1,
        rmService.getApplications(request).getApplicationList().size(),
        "Failed to limit applications");

    // Check start range
    request = GetApplicationsRequest.newInstance();
    request.setStartRange(submitTimeMillis[0] + 1, System.currentTimeMillis());

    // 2 applications are submitted after first timeMills
    assertEquals(2, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching start range");

    // 1 application is submitted after the second timeMills
    request.setStartRange(submitTimeMillis[1] + 1, System.currentTimeMillis());
    assertEquals(1, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching start range");

    // no application is submitted after the third timeMills
    request.setStartRange(submitTimeMillis[2] + 1, System.currentTimeMillis());
    assertEquals(0, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching start range");

    // Check queue
    request = GetApplicationsRequest.newInstance();
    Set<String> queueSet = new HashSet<String>();
    request.setQueues(queueSet);

    queueSet.add(queues[0]);
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications in queue");
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications in queue");

    queueSet.add(queues[1]);
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications in queue");

    // Check user
    request = GetApplicationsRequest.newInstance();
    Set<String> userSet = new HashSet<String>();
    request.setUsers(userSet);

    userSet.add("random-user-name");
    assertEquals(0, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for user");

    userSet.add(UserGroupInformation.getCurrentUser().getShortUserName());
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for user");

    rmService.setDisplayPerUserApps(true);
    userSet.clear();
    assertEquals(6, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for user");
    rmService.setDisplayPerUserApps(false);

    // Check tags
    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.ALL, null, null, null, null, null, null,
        null, null);
    Set<String> tagSet = new HashSet<String>();
    request.setApplicationTags(tagSet);
    assertEquals(6, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching tags");

    tagSet = Sets.newHashSet(tags.get(0));
    request.setApplicationTags(tagSet);
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching tags");

    tagSet = Sets.newHashSet(tags.get(1));
    request.setApplicationTags(tagSet);
    assertEquals(2, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching tags");

    tagSet = Sets.newHashSet(tags.get(2));
    request.setApplicationTags(tagSet);
    assertEquals(1, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of matching tags");

    // Check scope
    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.VIEWABLE);
    assertEquals(6, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for the scope");

    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.OWN);
    assertEquals(3, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for the scope");
  }

  @Test
  @Timeout(value = 4)
  public void testConcurrentAppSubmit()
      throws IOException, InterruptedException, BrokenBarrierException,
      YarnException {
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, scheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());

    final ApplicationId appId1 = getApplicationId(100);
    final ApplicationId appId2 = getApplicationId(101);
    final SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(
        appId1, null, null);
    final SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(
        appId2, null, null);

    final CyclicBarrier startBarrier = new CyclicBarrier(2);
    final CyclicBarrier endBarrier = new CyclicBarrier(2);

    EventHandler<Event> eventHandler = new EventHandler<Event>() {
      @Override
      public void handle(Event rawEvent) {
        if (rawEvent instanceof RMAppEvent) {
          RMAppEvent event = (RMAppEvent) rawEvent;
          if (event.getApplicationId().equals(appId1)) {
            try {
              startBarrier.await();
              endBarrier.await();
            } catch (BrokenBarrierException e) {
              LOG.warn("Broken Barrier", e);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted while awaiting barriers", e);
            }
          }
        }
      }
    };

    when(rmContext.getDispatcher().getEventHandler()).thenReturn(eventHandler);
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    final ClientRMService rmService =
        new ClientRMService(rmContext, scheduler, appManager, null, null,
            null);
    rmService.init(new Configuration());

    // submit an app and wait for it to block while in app submission
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          rmService.submitApplication(submitRequest1);
        } catch (YarnException | IOException e) {}
      }
    };
    t.start();

    // submit another app, so go through while the first app is blocked
    startBarrier.await();
    rmService.submitApplication(submitRequest2);
    endBarrier.await();
    t.join();
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue) {
    return mockSubmitAppRequest(appId, name, queue, null);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue, Set<String> tags) {
    return mockSubmitAppRequest(appId, name, queue, tags, false);
  }

  @SuppressWarnings("deprecation")
  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
        String name, String queue, Set<String> tags, boolean unmanaged) {

    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);

    Resource resource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext submissionContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    submissionContext.setAMContainerSpec(amContainerSpec);
    submissionContext.setApplicationName(name);
    submissionContext.setQueue(queue);
    submissionContext.setApplicationId(appId);
    submissionContext.setResource(resource);
    submissionContext.setApplicationType(appType);
    submissionContext.setApplicationTags(tags);
    submissionContext.setUnmanagedAM(unmanaged);
    submissionContext.setPriority(Priority.newInstance(0));

    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }

  private void mockRMContext(ResourceScheduler scheduler, RMContext rmContext)
      throws IOException {
    Dispatcher dispatcher = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(dispatcher);
    @SuppressWarnings("unchecked")
    EventHandler<Event> eventHandler = mock(EventHandler.class);
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);

    QueueInfo queInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queInfo.setQueueName("testqueue");
    QueueConfigurations queueConfigs =
        recordFactory.newRecordInstance(QueueConfigurations.class);
    queueConfigs.setCapacity(0.5f);
    queueConfigs.setAbsoluteCapacity(0.1f);
    queueConfigs.setMaxCapacity(1.0f);
    queueConfigs.setAbsoluteMaxCapacity(1.0f);
    queueConfigs.setMaxAMPercentage(0.2f);
    Map<String, QueueConfigurations> queueConfigsByPartition =
        new HashMap<>();
    queueConfigsByPartition.put("*", queueConfigs);
    queInfo.setQueueConfigurations(queueConfigsByPartition);

    when(scheduler.getQueueInfo(eq("testqueue"), anyBoolean(), anyBoolean()))
        .thenReturn(queInfo);
    when(scheduler.getQueueInfo(eq("nonexistentqueue"), anyBoolean(),
        anyBoolean())).thenThrow(new IOException("queue does not exist"));
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ConcurrentHashMap<ApplicationId, RMApp> apps =
        getRMApps(rmContext, scheduler);
    when(rmContext.getRMApps()).thenReturn(apps);
    when(scheduler.getAppsInQueue(eq("testqueue"))).thenReturn(
        getSchedulerApps(apps));
    when(rmContext.getScheduler()).thenReturn(scheduler);
  }

  private ConcurrentHashMap<ApplicationId, RMApp> getRMApps(
      RMContext rmContext, YarnScheduler yarnScheduler) {
    ConcurrentHashMap<ApplicationId, RMApp> apps =
        new ConcurrentHashMap<ApplicationId, RMApp>();
    ApplicationId applicationId1 = getApplicationId(1);
    ApplicationId applicationId2 = getApplicationId(2);
    ApplicationId applicationId3 = getApplicationId(3);
    YarnConfiguration config = new YarnConfiguration();
    apps.put(applicationId1, getRMApp(rmContext, yarnScheduler, applicationId1,
        config, "testqueue", 10, 3, null, null));
    apps.put(applicationId2, getRMApp(rmContext, yarnScheduler, applicationId2,
        config, "a", 20, 2, null, ""));
    apps.put(applicationId3, getRMApp(rmContext, yarnScheduler, applicationId3,
        config, "testqueue", 40, 5, "high-mem", "high-mem"));
    return apps;
  }

  private List<ApplicationAttemptId> getSchedulerApps(
      Map<ApplicationId, RMApp> apps) {
    List<ApplicationAttemptId> schedApps = new ArrayList<ApplicationAttemptId>();
    // Return app IDs for the apps in testqueue (as defined in getRMApps)
    schedApps.add(ApplicationAttemptId.newInstance(getApplicationId(1), 0));
    schedApps.add(ApplicationAttemptId.newInstance(getApplicationId(3), 0));
    return schedApps;
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }

  private static ApplicationAttemptId getApplicationAttemptId(int id) {
    return ApplicationAttemptId.newInstance(getApplicationId(id), 1);
  }

  private RMAppImpl getRMApp(RMContext rmContext, YarnScheduler yarnScheduler,
      ApplicationId applicationId3, YarnConfiguration config, String queueName,
      final long memorySeconds, final long vcoreSeconds,
      String appNodeLabelExpression, String amNodeLabelExpression) {
    ApplicationSubmissionContext asContext = mock(ApplicationSubmissionContext.class);
    when(asContext.getMaxAppAttempts()).thenReturn(1);
    when(asContext.getNodeLabelExpression()).thenReturn(appNodeLabelExpression);
    when(asContext.getPriority()).thenReturn(Priority.newInstance(0));
    RMAppImpl app =
        spy(new RMAppImpl(applicationId3, rmContext, config, null, null,
            queueName, asContext, yarnScheduler, null,
            System.currentTimeMillis(), "YARN", null,
            Collections.singletonList(BuilderUtils.newResourceRequest(
                RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                Resource.newInstance(1024, 1), 1))){
                  @Override
                  public ApplicationReport createAndGetApplicationReport(
                      String clientUserName, boolean allowAccess) {
                    ApplicationReport report = super.createAndGetApplicationReport(
                        clientUserName, allowAccess);
                    ApplicationResourceUsageReport usageReport =
                        report.getApplicationResourceUsageReport();
                    usageReport.setMemorySeconds(memorySeconds);
                    usageReport.setVcoreSeconds(vcoreSeconds);
                    report.setApplicationResourceUsageReport(usageReport);
                    return report;
                  }
              });
    app.getAMResourceRequests().get(0)
        .setNodeLabelExpression(amNodeLabelExpression);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    RMAppAttemptImpl rmAppAttemptImpl = spy(new RMAppAttemptImpl(attemptId,
        rmContext, yarnScheduler, null, asContext, config, null, app));
    Container container = Container.newInstance(
        ContainerId.newContainerId(attemptId, 1), null,
        "", null, null, null);
    RMContainerImpl containerimpl = spy(new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), attemptId, null, "",
        rmContext));
    Map<ApplicationAttemptId, RMAppAttempt> attempts = new HashMap<>();
    attempts.put(attemptId, rmAppAttemptImpl);
    when(app.getCurrentAppAttempt()).thenReturn(rmAppAttemptImpl);
    when(app.getAppAttempts()).thenReturn(attempts);
    when(app.getApplicationPriority()).thenReturn(Priority.newInstance(0));
    when(rmAppAttemptImpl.getMasterContainer()).thenReturn(container);
    ResourceScheduler rs = mock(ResourceScheduler.class);
    when(rmContext.getScheduler()).thenReturn(rs);
    when(rmContext.getScheduler().getRMContainer(any(ContainerId.class)))
        .thenReturn(containerimpl);
    SchedulerAppReport sAppReport = mock(SchedulerAppReport.class);
    when(
        rmContext.getScheduler().getSchedulerAppInfo(
            any(ApplicationAttemptId.class))).thenReturn(sAppReport);
    List<RMContainer> rmContainers = new ArrayList<RMContainer>();
    rmContainers.add(containerimpl);
    when(
        rmContext.getScheduler().getSchedulerAppInfo(attemptId)
            .getLiveContainers()).thenReturn(rmContainers);
    ContainerStatus cs = mock(ContainerStatus.class);
    when(containerimpl.completed()).thenReturn(false);
    when(containerimpl.getDiagnosticsInfo()).thenReturn("N/A");
    when(containerimpl.getContainerExitStatus()).thenReturn(0);
    when(containerimpl.getContainerState()).thenReturn(ContainerState.COMPLETE);
    return app;
  }

  private static ResourceScheduler mockResourceScheduler()
      throws YarnException {
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
    when(scheduler.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    when(scheduler.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(scheduler.getMaximumResourceCapability(anyString())).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(scheduler.getAppsInQueue(QUEUE_1)).thenReturn(
        Arrays.asList(getApplicationAttemptId(101), getApplicationAttemptId(102)));
    when(scheduler.getAppsInQueue(QUEUE_2)).thenReturn(
        Arrays.asList(getApplicationAttemptId(103)));
    ApplicationAttemptId attemptId = getApplicationAttemptId(1);
    when(scheduler.getAppResourceUsageReport(attemptId)).thenReturn(null);

    ResourceCalculator rs = mock(ResourceCalculator.class);
    when(scheduler.getResourceCalculator()).thenReturn(rs);

    when(scheduler.checkAndGetApplicationPriority(any(Priority.class),
        any(UserGroupInformation.class), anyString(), any(ApplicationId.class)))
            .thenReturn(Priority.newInstance(0));
    return scheduler;
  }

  private ResourceManager setupResourceManager() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    MockRM rm = new MockRM(conf);
    resourceManager = rm;
    rm.start();
    try {
      rm.registerNode("127.0.0.1:1", 102400, 100);
      // allow plan follower to synchronize
      Thread.sleep(1050);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    return rm;
  }

  private ReservationSubmissionRequest submitReservationTestHelper(
      ClientRMService clientService, long arrival, long deadline,
      long duration) {
    ReservationSubmissionResponse sResponse = null;
    GetNewReservationRequest newReservationRequest =
        GetNewReservationRequest.newInstance();
    ReservationId reservationID = null;
    try {
      reservationID = clientService.getNewReservation(newReservationRequest)
          .getReservationId();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    ReservationSubmissionRequest sRequest =
        ReservationSystemTestUtil.createSimpleReservationRequest(reservationID,
            4, arrival, deadline, duration);
    try {
      sResponse = clientService.submitReservation(sRequest);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(sResponse);
    assertNotNull(reservationID);
    System.out.println("Submit reservation response: " + reservationID);
    return sRequest;
  }

  @Test
  public void testCreateReservation() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // Submit the reservation again with the same request and make sure it
    // passes.
    try {
      clientService.submitReservation(sRequest);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Submit the reservation with the same reservation id but different
    // reservation definition, and ensure YarnException is thrown.
    arrival = clock.getTime();
    ReservationDefinition rDef = sRequest.getReservationDefinition();
    rDef.setArrival(arrival + duration);
    sRequest.setReservationDefinition(rDef);
    try {
      clientService.submitReservation(sRequest);
      fail("Reservation submission should fail if a duplicate "
          + "reservation id is used, but the reservation definition has been "
          + "updated.");
    } catch (Exception e) {
      assertTrue(e instanceof YarnException);
    }
  }

  @Test
  public void testUpdateReservation() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationDefinition rDef = sRequest.getReservationDefinition();
    ReservationRequest rr =
        rDef.getReservationRequests().getReservationResources().get(0);
    ReservationId reservationID = sRequest.getReservationId();
    rr.setNumContainers(5);
    arrival = clock.getTime();
    duration = 30000;
    deadline = (long) (arrival + 1.05 * duration);
    rr.setDuration(duration);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    ReservationUpdateRequest uRequest =
        ReservationUpdateRequest.newInstance(rDef, reservationID);
    ReservationUpdateResponse uResponse = null;
    try {
      uResponse = clientService.updateReservation(uRequest);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(uResponse);
    System.out.println("Update reservation response: " + uResponse);
  }

  @Test
  public void testListReservationsByReservationId() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationId reservationID = sRequest.getReservationId();
    ReservationListResponse response = null;
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
        -1, false);
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(1, response.getReservationAllocationState().size());
    assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    assertEquals(response.getReservationAllocationState().get(0)
        .getResourceAllocationRequests().size(), 0);
  }

  @Test
  public void testListReservationsByTimeInterval() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by a point in time within the reservation
    // range.
    arrival = clock.getTime();
    ReservationId reservationID = sRequest.getReservationId();
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", arrival + duration / 2,
        arrival + duration / 2, true);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(1, response.getReservationAllocationState().size());
    assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    // List reservations, search by time within reservation interval.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", 1, Long.MAX_VALUE, true);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(1, response.getReservationAllocationState().size());
    assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    // Verify that the full resource allocations exist.
    assertTrue(response.getReservationAllocationState().get(0)
        .getResourceAllocationRequests().size() > 0);

    // Verify that the full RDL is returned.
    ReservationRequests reservationRequests =
        response.getReservationAllocationState().get(0)
            .getReservationDefinition().getReservationRequests();
    assertEquals("R_ALL",
        reservationRequests.getInterpreter().toString());
    assertTrue(reservationRequests.getReservationResources().get(0)
        .getDuration() == duration);
  }

  @Test
  public void testListReservationsByInvalidTimeInterval() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by invalid end time == -1.
    ReservationListRequest request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 1, -1, true);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(1, response.getReservationAllocationState().size());
    assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), sRequest.getReservationId().getId());

    // List reservations, search by invalid end time < -1.
    request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 1, -10, true);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(1, response.getReservationAllocationState().size());
    assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), sRequest.getReservationId().getId());
  }

  @Test
  public void testListReservationsByTimeIntervalContainingNoReservations() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by very large start time.
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", Long.MAX_VALUE, -1, false);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    assertNotNull(response);
    assertThat(response.getReservationAllocationState()).isEmpty();

    duration = 30000;
    deadline = sRequest.getReservationDefinition().getDeadline();

    // List reservations, search by start time after the reservation
    // end time.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", deadline + duration,
        deadline + 2 * duration, false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    assertNotNull(response);
    assertThat(response.getReservationAllocationState()).isEmpty();

    arrival = clock.getTime();
    // List reservations, search by end time before the reservation start
    // time.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", 0, arrival - duration,
        false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    assertNotNull(response);
    assertThat(response.getReservationAllocationState()).isEmpty();

    // List reservations, search by very small end time.
    request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 0, 1, false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    assertNotNull(response);
    assertThat(response.getReservationAllocationState()).isEmpty();
  }

  @Test
  public void testReservationDelete() {
    resourceManager = setupResourceManager();
    ClientRMService clientService = resourceManager.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationId reservationID = sRequest.getReservationId();
    // Delete the reservation
    ReservationDeleteRequest dRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    ReservationDeleteResponse dResponse = null;
    try {
      dResponse = clientService.deleteReservation(dRequest);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(dResponse);
    System.out.println("Delete reservation response: " + dResponse);

    // List reservations, search by non-existent reservationID
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
        -1, false);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    assertNotNull(response);
    assertEquals(0, response.getReservationAllocationState().size());
  }

  @Test
  public void testGetNodeLabels() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, this.getRMContext()
                .getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();
    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y");
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY));

    NodeId node1 = NodeId.newInstance("host1", 1234);
    NodeId node2 = NodeId.newInstance("host2", 1234);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1, ImmutableSet.of("x"));
    map.put(node2, ImmutableSet.of("y"));
    labelsMgr.replaceLabelsOnNode(map);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    assertTrue(response.getNodeLabelList().containsAll(
        Arrays.asList(labelX, labelY)));

    // Get node labels mapping
    GetNodesToLabelsResponse response1 = client
        .getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Map<NodeId, Set<String>> nodeToLabels = response1.getNodeToLabels();
    assertTrue(nodeToLabels.keySet().containsAll(
        Arrays.asList(node1, node2)));
    assertTrue(nodeToLabels.get(node1)
        .containsAll(Arrays.asList(labelX.getName())));
    assertTrue(nodeToLabels.get(node2)
        .containsAll(Arrays.asList(labelY.getName())));
    // Below label "x" is not present in the response as exclusivity is true
    assertFalse(nodeToLabels.get(node1).containsAll(
        Arrays.asList(NodeLabel.newInstance("x"))));
  }

  @Test
  public void testGetLabelsToNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, this.getRMContext()
                .getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();

    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y", false);
    NodeLabel labelZ = NodeLabel.newInstance("z", false);
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY, labelZ));

    NodeId node1A = NodeId.newInstance("host1", 1234);
    NodeId node1B = NodeId.newInstance("host1", 5678);
    NodeId node2A = NodeId.newInstance("host2", 1234);
    NodeId node3A = NodeId.newInstance("host3", 1234);
    NodeId node3B = NodeId.newInstance("host3", 5678);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1A, ImmutableSet.of("x"));
    map.put(node1B, ImmutableSet.of("z"));
    map.put(node2A, ImmutableSet.of("y"));
    map.put(node3A, ImmutableSet.of("y"));
    map.put(node3B, ImmutableSet.of("z"));
    labelsMgr.replaceLabelsOnNode(map);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    assertTrue(response.getNodeLabelList().containsAll(
        Arrays.asList(labelX, labelY, labelZ)));

    // Get labels to nodes mapping
    GetLabelsToNodesResponse response1 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance());
    Map<String, Set<NodeId>> labelsToNodes = response1.getLabelsToNodes();
    assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX.getName(), labelY.getName(), labelZ.getName())));
    assertTrue(labelsToNodes.get(labelX.getName()).containsAll(
        Arrays.asList(node1A)));
    assertTrue(labelsToNodes.get(labelY.getName()).containsAll(
        Arrays.asList(node2A, node3A)));
    assertTrue(labelsToNodes.get(labelZ.getName()).containsAll(
        Arrays.asList(node1B, node3B)));

    // Get labels to nodes mapping for specific labels
    Set<String> setlabels = new HashSet<String>(Arrays.asList(new String[]{"x",
        "z"}));
    GetLabelsToNodesResponse response2 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance(setlabels));
    labelsToNodes = response2.getLabelsToNodes();
    assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX.getName(), labelZ.getName())));
    assertTrue(labelsToNodes.get(labelX.getName()).containsAll(
        Arrays.asList(node1A)));
    assertTrue(labelsToNodes.get(labelZ.getName()).containsAll(
        Arrays.asList(node1B, node3B)));
    assertThat(labelsToNodes.get(labelY.getName())).isNull();
  }

  @Test
  @Timeout(value = 120)
  public void testGetClusterNodeAttributes() throws IOException, YarnException {
    Configuration newConf = NodeAttributeTestUtils.getRandomDirConf(null);
    MockRM rm = new MockRM(newConf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
            this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      }
    };
    resourceManager = rm;
    rm.start();

    NodeAttributesManager mgr = rm.getRMContext().getNodeAttributesManager();
    NodeId host1 = NodeId.newInstance("host1", 0);
    NodeId host2 = NodeId.newInstance("host2", 0);
    NodeAttribute gpu = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
            NodeAttributeType.STRING, "nvida");
    NodeAttribute os = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "OS",
            NodeAttributeType.STRING, "windows64");
    NodeAttribute docker = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
            NodeAttributeType.STRING, "docker0");
    Map<String, Set<NodeAttribute>> nodes = new HashMap<>();
    nodes.put(host1.getHost(), ImmutableSet.of(gpu, os));
    nodes.put(host2.getHost(), ImmutableSet.of(docker));
    mgr.addNodeAttributes(nodes);
    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    GetClusterNodeAttributesRequest request =
        GetClusterNodeAttributesRequest.newInstance();
    GetClusterNodeAttributesResponse response =
        client.getClusterNodeAttributes(request);
    Set<NodeAttributeInfo> attributes = response.getNodeAttributes();
    assertEquals(3, attributes.size(), "Size not correct");
    assertTrue(attributes.contains(NodeAttributeInfo.newInstance(gpu)));
    assertTrue(attributes.contains(NodeAttributeInfo.newInstance(os)));
    assertTrue(attributes.contains(NodeAttributeInfo.newInstance(docker)));
  }

  @Test
  @Timeout(value = 120)
  public void testGetAttributesToNodes() throws IOException, YarnException {
    Configuration newConf = NodeAttributeTestUtils.getRandomDirConf(null);
    MockRM rm = new MockRM(newConf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
            this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      }
    };
    resourceManager = rm;
    rm.start();

    NodeAttributesManager mgr = rm.getRMContext().getNodeAttributesManager();
    String node1 = "host1";
    String node2 = "host2";
    NodeAttribute gpu =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
            NodeAttributeType.STRING, "nvidia");
    NodeAttribute os =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "OS",
            NodeAttributeType.STRING, "windows64");
    NodeAttribute docker =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
            NodeAttributeType.STRING, "docker0");
    NodeAttribute dist =
        NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "VERSION",
            NodeAttributeType.STRING, "3_0_2");
    Map<String, Set<NodeAttribute>> nodes = new HashMap<>();
    nodes.put(node1, ImmutableSet.of(gpu, os, dist));
    nodes.put(node2, ImmutableSet.of(docker, dist));
    mgr.addNodeAttributes(nodes);
    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    GetAttributesToNodesRequest request =
        GetAttributesToNodesRequest.newInstance();
    GetAttributesToNodesResponse response =
        client.getAttributesToNodes(request);
    Map<NodeAttributeKey, List<NodeToAttributeValue>> attrs =
        response.getAttributesToNodes();
    assertThat(response.getAttributesToNodes()).hasSize(4);
    assertThat(attrs.get(dist.getAttributeKey())).hasSize(2);
    assertThat(attrs.get(os.getAttributeKey())).hasSize(1);
    assertThat(attrs.get(gpu.getAttributeKey())).hasSize(1);
    assertTrue(findHostnameAndValInMapping(node1, "3_0_2",
        attrs.get(dist.getAttributeKey())));
    assertTrue(findHostnameAndValInMapping(node2, "3_0_2",
        attrs.get(dist.getAttributeKey())));
    assertTrue(findHostnameAndValInMapping(node2, "docker0",
        attrs.get(docker.getAttributeKey())));

    GetAttributesToNodesRequest request2 = GetAttributesToNodesRequest
        .newInstance(ImmutableSet.of(docker.getAttributeKey()));
    GetAttributesToNodesResponse response2 =
        client.getAttributesToNodes(request2);
    Map<NodeAttributeKey, List<NodeToAttributeValue>> attrs2 =
        response2.getAttributesToNodes();
    assertThat(attrs2).hasSize(1);
    assertTrue(findHostnameAndValInMapping(node2, "docker0",
        attrs2.get(docker.getAttributeKey())));

    GetAttributesToNodesRequest request3 =
        GetAttributesToNodesRequest.newInstance(
            ImmutableSet.of(docker.getAttributeKey(), os.getAttributeKey()));
    GetAttributesToNodesResponse response3 =
        client.getAttributesToNodes(request3);
    Map<NodeAttributeKey, List<NodeToAttributeValue>> attrs3 =
        response3.getAttributesToNodes();
    assertThat(attrs3).hasSize(2);
    assertTrue(findHostnameAndValInMapping(node1, "windows64",
        attrs3.get(os.getAttributeKey())));
    assertTrue(findHostnameAndValInMapping(node2, "docker0",
        attrs3.get(docker.getAttributeKey())));
  }

  private boolean findHostnameAndValInMapping(String hostname, String attrVal,
      List<NodeToAttributeValue> mappingVals) {
    for (NodeToAttributeValue value : mappingVals) {
      if (value.getHostname().equals(hostname)) {
        return attrVal.equals(value.getAttributeValue());
      }
    }
    return false;
  }

  @Test
  @Timeout(value = 120)
  public void testGetNodesToAttributes() throws IOException, YarnException {
    Configuration newConf = NodeAttributeTestUtils.getRandomDirConf(null);
    MockRM rm = new MockRM(newConf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
            this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      }
    };
    resourceManager = rm;
    rm.start();

    NodeAttributesManager mgr = rm.getRMContext().getNodeAttributesManager();
    String node1 = "host1";
    String node2 = "host2";
    NodeAttribute gpu = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
            NodeAttributeType.STRING, "nvida");
    NodeAttribute os = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "OS",
            NodeAttributeType.STRING, "windows64");
    NodeAttribute docker = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
            NodeAttributeType.STRING, "docker0");
    NodeAttribute dist = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "VERSION",
            NodeAttributeType.STRING, "3_0_2");
    Map<String, Set<NodeAttribute>> nodes = new HashMap<>();
    nodes.put(node1, ImmutableSet.of(gpu, os, dist));
    nodes.put(node2, ImmutableSet.of(docker, dist));
    mgr.addNodeAttributes(nodes);
    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Specify null for hostnames.
    GetNodesToAttributesRequest request1 =
        GetNodesToAttributesRequest.newInstance(null);
    GetNodesToAttributesResponse response1 =
        client.getNodesToAttributes(request1);
    Map<String, Set<NodeAttribute>> hostToAttrs =
        response1.getNodeToAttributes();
    assertEquals(2, hostToAttrs.size());

    assertTrue(hostToAttrs.get(node2).contains(dist));
    assertTrue(hostToAttrs.get(node2).contains(docker));
    assertTrue(hostToAttrs.get(node1).contains(dist));

    // Specify particular node
    GetNodesToAttributesRequest request2 =
        GetNodesToAttributesRequest.newInstance(ImmutableSet.of(node1));
    GetNodesToAttributesResponse response2 =
        client.getNodesToAttributes(request2);
    hostToAttrs = response2.getNodeToAttributes();
    assertEquals(1, response2.getNodeToAttributes().size());
    assertTrue(hostToAttrs.get(node1).contains(dist));

    // Test queury with empty set
    GetNodesToAttributesRequest request3 =
        GetNodesToAttributesRequest.newInstance(Collections.emptySet());
    GetNodesToAttributesResponse response3 =
        client.getNodesToAttributes(request3);
    hostToAttrs = response3.getNodeToAttributes();
    assertEquals(2, hostToAttrs.size());

    assertTrue(hostToAttrs.get(node2).contains(dist));
    assertTrue(hostToAttrs.get(node2).contains(docker));
    assertTrue(hostToAttrs.get(node1).contains(dist));

    // test invalid hostname
    GetNodesToAttributesRequest request4 =
        GetNodesToAttributesRequest.newInstance(ImmutableSet.of("invalid"));
    GetNodesToAttributesResponse response4 =
        client.getNodesToAttributes(request4);
    hostToAttrs = response4.getNodeToAttributes();
    assertEquals(0, hostToAttrs.size());
  }

  @Test
  @Timeout(value = 120)
  public void testUpdatePriorityAndKillAppWithZeroClusterResource()
      throws Exception {
    int maxPriority = 10;
    int appPriority = 5;
    conf = new YarnConfiguration();
    assumeFalse(conf.get(YarnConfiguration.RM_SCHEDULER).equals(FairScheduler.class.getName()),
        "FairScheduler does not support Application Priorities");
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        maxPriority);
    MockRM rm = new MockRM(conf);
    resourceManager = rm;
    rm.init(conf);
    rm.start();
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1024, rm)
        .withAppPriority(Priority.newInstance(appPriority))
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);
    ClientRMService rmService = rm.getClientRMService();
    testApplicationPriorityUpdation(rmService, app1, appPriority, appPriority);
    rm.killApp(app1.getApplicationId());
    rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
  }

  @Test
  @Timeout(value = 120)
  public void testUpdateApplicationPriorityRequest() throws Exception {
    int maxPriority = 10;
    int appPriority = 5;
    conf = new YarnConfiguration();
    assumeFalse(conf.get(YarnConfiguration.RM_SCHEDULER).equals(FairScheduler.class.getName()),
        "FairScheduler does not support Application Priorities");
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        maxPriority);
    MockRM rm = new MockRM(conf);
    resourceManager = rm;
    rm.init(conf);
    rm.start();
    rm.registerNode("host1:1234", 1024);
    // Start app1 with appPriority 5
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1024, rm)
        .withAppPriority(Priority.newInstance(appPriority))
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);

    assertEquals(appPriority, app1.getApplicationPriority().getPriority(),
        "Incorrect priority has been set to application");

    appPriority = 11;
    ClientRMService rmService = rm.getClientRMService();
    testApplicationPriorityUpdation(rmService, app1, appPriority, maxPriority);

    appPriority = 9;
    testApplicationPriorityUpdation(rmService, app1, appPriority, appPriority);

    rm.killApp(app1.getApplicationId());
    rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);


    // Update priority request for invalid application id.
    ApplicationId invalidAppId = ApplicationId.newInstance(123456789L, 3);
    UpdateApplicationPriorityRequest updateRequest =
        UpdateApplicationPriorityRequest.newInstance(invalidAppId,
            Priority.newInstance(appPriority));
    try {
      rmService.updateApplicationPriority(updateRequest);
      fail("ApplicationNotFoundException should be thrown "
          + "for invalid application id");
    } catch (ApplicationNotFoundException e) {
      // Expected
    }

    updateRequest =
        UpdateApplicationPriorityRequest.newInstance(app1.getApplicationId(),
            Priority.newInstance(11));
    assertEquals(appPriority, rmService.updateApplicationPriority(updateRequest)
        .getApplicationPriority().getPriority(),
        "Incorrect priority has been set to application");
  }

  private void testApplicationPriorityUpdation(ClientRMService rmService,
      RMApp app1, int tobeUpdatedPriority, int expected) throws YarnException,
      IOException {
    UpdateApplicationPriorityRequest updateRequest =
        UpdateApplicationPriorityRequest.newInstance(app1.getApplicationId(),
            Priority.newInstance(tobeUpdatedPriority));

    UpdateApplicationPriorityResponse updateApplicationPriority =
        rmService.updateApplicationPriority(updateRequest);

    assertEquals(expected, app1.getApplicationSubmissionContext().getPriority()
        .getPriority(), "Incorrect priority has been set to application");
    assertEquals(expected, updateApplicationPriority.getApplicationPriority().getPriority(),
        "Incorrect priority has been returned");
  }

  private File createExcludeFile(File testDir) throws IOException {
    File excludeFile = new File(testDir, "excludeFile");
    try (FileOutputStream out = new FileOutputStream(excludeFile)) {
      out.write("decommisssionedHost".getBytes(UTF_8));
    }
    return excludeFile;
  }

  @Test
  public void testRMStartWithDecommissionedNode() throws Exception {
    File testDir = GenericTestUtils.getRandomizedTestDir();
    assertTrue(testDir.mkdirs(),
        "Failed to create test directory: " + testDir.getAbsolutePath());
    try {
      File excludeFile = createExcludeFile(testDir);
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
          excludeFile.getAbsolutePath());
      MockRM rm = new MockRM(conf) {
        protected ClientRMService createClientRMService() {
          return new ClientRMService(this.rmContext, scheduler,
              this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
              this.getRMContext().getRMDelegationTokenSecretManager());
        };
      };
      resourceManager = rm;
      rm.start();

      rpc = YarnRPC.create(conf);
      InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
      LOG.info("Connecting to ResourceManager at " + rmAddress);
      client = (ApplicationClientProtocol) rpc.getProxy(
          ApplicationClientProtocol.class, rmAddress, conf);

      // Make call
      GetClusterNodesRequest request =
          GetClusterNodesRequest.newInstance(EnumSet.allOf(NodeState.class));
      List<NodeReport> nodeReports = client.getClusterNodes(request).getNodeReports();
      assertEquals(1, nodeReports.size());
    } finally {
      FileUtil.fullyDelete(testDir);
    }
  }

  @Test
  public void testGetResourceTypesInfoWhenResourceProfileDisabled()
      throws Exception {
    conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      }
    };
    resourceManager = rm;
    rm.start();

    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetAllResourceTypeInfoRequest request =
        GetAllResourceTypeInfoRequest.newInstance();
    GetAllResourceTypeInfoResponse response = client.getResourceTypeInfo(request);

    assertEquals(2, response.getResourceTypeInfo().size());

    // Check memory
    assertEquals(ResourceInformation.MEMORY_MB.getName(),
        response.getResourceTypeInfo().get(0).getName());
    assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        response.getResourceTypeInfo().get(0).getDefaultUnit());

    // Check vcores
    assertEquals(ResourceInformation.VCORES.getName(),
        response.getResourceTypeInfo().get(1).getName());
    assertEquals(ResourceInformation.VCORES.getUnits(),
        response.getResourceTypeInfo().get(1).getDefaultUnit());
  }

  @Test
  public void testGetApplicationsWithPerUserApps()
      throws IOException, YarnException {
    /*
     * Submit 3 applications alternately in two queues
     */
    // Basic setup
    ResourceScheduler scheduler = mockResourceScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(scheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    RMAppManager appManager = new RMAppManager(rmContext, scheduler, null,
        mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler())
        .thenReturn(new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });

    // Simulate Queue ACL manager which returns false always
    QueueACLsManager queueAclsManager = mock(QueueACLsManager.class);
    when(queueAclsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        anyList())).thenReturn(false);

    // Simulate app ACL manager which returns false always
    ApplicationACLsManager appAclsManager = mock(ApplicationACLsManager.class);
    when(appAclsManager.checkAccess(eq(UserGroupInformation.getCurrentUser()),
        any(ApplicationAccessType.class), any(String.class),
        any(ApplicationId.class))).thenReturn(false);
    ClientRMService rmService = new ClientRMService(rmContext, scheduler,
        appManager, appAclsManager, queueAclsManager, null);
    rmService.init(new Configuration());

    // Initialize appnames and queues
    String[] queues = {QUEUE_1, QUEUE_2};
    String[] appNames = {MockApps.newAppName(), MockApps.newAppName(),
        MockApps.newAppName()};
    ApplicationId[] appIds = {getApplicationId(101), getApplicationId(102),
        getApplicationId(103)};
    List<String> tags = Arrays.asList("Tag1", "Tag2", "Tag3");

    long[] submitTimeMillis = new long[3];
    // Submit applications
    for (int i = 0; i < appIds.length; i++) {
      ApplicationId appId = appIds[i];
      SubmitApplicationRequest submitRequest = mockSubmitAppRequest(appId,
          appNames[i], queues[i % queues.length],
          new HashSet<String>(tags.subList(0, i + 1)));
      rmService.submitApplication(submitRequest);
      submitTimeMillis[i] = System.currentTimeMillis();
    }

    // Test different cases of ClientRMService#getApplications()
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    assertEquals(6, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect total number of apps");

    rmService.setDisplayPerUserApps(true);
    assertEquals(0, rmService.getApplications(request).getApplicationList().size(),
        "Incorrect number of applications for user");
    rmService.setDisplayPerUserApps(false);
  }

  @Test
  public void testRegisterNMWithDiffUnits() throws Exception {
    ResourceUtils.resetResourceTypes();
    Configuration yarnConf = new YarnConfiguration();
    String resourceTypesFileName = "resource-types-4.xml";
    InputStream source =
        yarnConf.getClassLoader().getResourceAsStream(resourceTypesFileName);
    resourceTypesFile = new File(yarnConf.getClassLoader().
        getResource(".").getPath(), "resource-types.xml");
    FileUtils.copyInputStreamToFile(source, resourceTypesFile);
    ResourceUtils.getResourceTypes();

    yarnConf.setClass(
        CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class, ResourceCalculator.class);

    MockRM rm = new MockRM(yarnConf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();

    Resource resource = Resources.createResource(976562);
    resource.setResourceInformation("memory-mb",
        ResourceInformation.newInstance("memory-mb", "G", 976562));
    resource.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "T", 1));
    resource.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "M", 1));

    MockNM node = rm.registerNode("host1:1234", resource);
    node.nodeHeartbeat(true);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetClusterNodesRequest request =
        GetClusterNodesRequest.newInstance(EnumSet.of(NodeState.RUNNING));
    List<NodeReport> nodeReports =
        client.getClusterNodes(request).getNodeReports();
    assertEquals(1, nodeReports.size());
    assertNotSame(NodeState.UNHEALTHY, nodeReports.get(0).getNodeState(),
        "Node is expected to be healthy!");
    assertEquals(1, nodeReports.size());

    //Resource 'resource1' has been passed as 1T while registering NM.
    //1T should be converted to 1000G
    assertEquals("G", nodeReports.get(0).getCapability().
        getResourceInformation("resource1").getUnits());
    assertEquals(1000, nodeReports.get(0).getCapability().
        getResourceInformation("resource1").getValue());

    //Resource 'resource2' has been passed as 1M while registering NM
    //1M should be converted to 1000000000M
    assertEquals("m", nodeReports.get(0).getCapability().
        getResourceInformation("resource2").getUnits());
    assertEquals(1000000000, nodeReports.get(0).getCapability().
        getResourceInformation("resource2").getValue());

    //Resource 'memory-mb' has been passed as 976562G while registering NM
    //976562G should be converted to 976562Mi
    assertEquals("Mi", nodeReports.get(0).getCapability().
        getResourceInformation("memory-mb").getUnits());
    assertEquals(976562, nodeReports.get(0).getCapability().
        getResourceInformation("memory-mb").getValue());
  }

  @Test
  public void testGetClusterMetrics() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    resourceManager = rm;
    rm.start();

    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    clusterMetrics.incrDecommissioningNMs();
    repeat(2, clusterMetrics::incrDecommisionedNMs);
    repeat(3, clusterMetrics::incrNumActiveNodes);
    repeat(4, clusterMetrics::incrNumLostNMs);
    repeat(5, clusterMetrics::incrNumUnhealthyNMs);
    repeat(6, clusterMetrics::incrNumRebootedNMs);
    repeat(7, clusterMetrics::incrNumShutdownNMs);

    // Create a client.
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    client = (ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf);

    YarnClusterMetrics ymetrics = client.getClusterMetrics(
        GetClusterMetricsRequest.newInstance()).getClusterMetrics();

    assertEquals(0, ymetrics.getNumNodeManagers());
    assertEquals(1, ymetrics.getNumDecommissioningNodeManagers());
    assertEquals(2, ymetrics.getNumDecommissionedNodeManagers());
    assertEquals(3, ymetrics.getNumActiveNodeManagers());
    assertEquals(4, ymetrics.getNumLostNodeManagers());
    assertEquals(5, ymetrics.getNumUnhealthyNodeManagers());
    assertEquals(6, ymetrics.getNumRebootedNodeManagers());
    assertEquals(7, ymetrics.getNumShutdownNodeManagers());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (resourceTypesFile != null && resourceTypesFile.exists()) {
      resourceTypesFile.delete();
    }
    ClusterMetrics.destroy();
    DefaultMetricsSystem.shutdown();
    if (conf != null && client != null && rpc != null) {
      rpc.stopProxy(client, conf);
    }
    if (resourceManager != null) {
      resourceManager.close();
    }
  }

  private static void repeat(int n, Runnable r) {
    for (int i = 0; i < n; ++i) {
      r.run();
    }
  }
}
