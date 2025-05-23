/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.yarn.api.records.ContainerUpdateType.INCREASE_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils.RESOURCE_OUTSIDE_ALLOWED_RANGE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.impl.pb.UpdateContainerRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMServerUtils {

  @Test
  public void testValidateAndSplitUpdateResourceRequests() {
    List<UpdateContainerRequest> updateRequests = new ArrayList<>();
    int containerVersion = 10;
    int resource = 10;
    Resource maxAllocation = Resource.newInstance(resource, resource);

    UpdateContainerRequestPBImpl updateContainerRequestPBFail =
        new UpdateContainerRequestPBImpl();
    updateContainerRequestPBFail.setContainerVersion(containerVersion);
    updateContainerRequestPBFail
        .setCapability(Resource.newInstance(resource + 1, resource + 1));
    updateContainerRequestPBFail
        .setContainerId(Mockito.mock(ContainerId.class));

    ContainerId containerIdOk = Mockito.mock(ContainerId.class);
    Resource capabilityOk = Resource.newInstance(resource - 1, resource - 1);
    UpdateContainerRequestPBImpl updateContainerRequestPBOk =
        new UpdateContainerRequestPBImpl();
    updateContainerRequestPBOk.setContainerVersion(containerVersion);
    updateContainerRequestPBOk.setCapability(capabilityOk);
    updateContainerRequestPBOk.setContainerUpdateType(INCREASE_RESOURCE);
    updateContainerRequestPBOk.setContainerId(containerIdOk);

    updateRequests.add(updateContainerRequestPBOk);
    updateRequests.add(updateContainerRequestPBFail);

    Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
    RMContext rmContext = Mockito.mock(RMContext.class);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);

    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    Mockito.when(rmContext.getDispatcher()).thenReturn(dispatcher);

    RMContainer rmContainer = Mockito.mock(RMContainer.class);
    Mockito.when(scheduler.getRMContainer(Mockito.any()))
        .thenReturn(rmContainer);
    Container container = Mockito.mock(Container.class);
    Mockito.when(container.getVersion()).thenReturn(containerVersion);
    Mockito.when(rmContainer.getContainer()).thenReturn(container);
    Mockito.when(scheduler.getNormalizedResource(capabilityOk, maxAllocation))
        .thenReturn(capabilityOk);

    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(1, 0.5f, new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>(), updateRequests, null);

    List<UpdateContainerError> updateErrors = new ArrayList<>();
    ContainerUpdates containerUpdates =
        RMServerUtils.validateAndSplitUpdateResourceRequests(rmContext,
            allocateRequest, maxAllocation, updateErrors);
    assertEquals(1, updateErrors.size());
    assertEquals(resource + 1, updateErrors.get(0)
        .getUpdateContainerRequest().getCapability().getMemorySize());
    assertEquals(resource + 1, updateErrors.get(0)
        .getUpdateContainerRequest().getCapability().getVirtualCores());
    assertEquals(RESOURCE_OUTSIDE_ALLOWED_RANGE,
        updateErrors.get(0).getReason());

    assertEquals(1, containerUpdates.getIncreaseRequests().size());
    UpdateContainerRequest increaseRequest =
        containerUpdates.getIncreaseRequests().get(0);
    assertEquals(capabilityOk.getVirtualCores(),
        increaseRequest.getCapability().getVirtualCores());
    assertEquals(capabilityOk.getMemorySize(),
        increaseRequest.getCapability().getMemorySize());
    assertEquals(containerIdOk, increaseRequest.getContainerId());
  }

  @Test
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    assertTrue(result.size() != 0);
    assertThat(result.get(0)).isEqualTo(rmNode1);
    when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    assertTrue(result.size() != 0);
    assertThat(result.get(0)).isEqualTo(rmNode1);
    when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    assertTrue(result.size() != 0);
    assertThat(result.get(0)).isEqualTo(rmNode1);
    when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    assertTrue(result.size() != 0);
    assertThat(result.get(0)).isEqualTo(rmNode1);
  }

  @Test
  public void testGetApplicableNodeCountForAMLocality() throws Exception {
    List<NodeId> rack1Nodes = new ArrayList<>();
    for (int i = 0; i < 29; i++) {
      rack1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    NodeId node1 = NodeId.newInstance("node1", 1234);
    NodeId node2 = NodeId.newInstance("node2", 1234);
    rack1Nodes.add(node2);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, false);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    Mockito.when(scheduler.getNodeIds("/rack1")).thenReturn(rack1Nodes);
    Mockito.when(scheduler.getNodeIds("node1"))
        .thenReturn(Collections.singletonList(node1));
    Mockito.when(scheduler.getNodeIds("node2"))
        .thenReturn(Collections.singletonList(node2));
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest rackReq = createResourceRequest("/rack1", true, null);
    reqs.add(rackReq);
    assertEquals(30,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    assertEquals(30,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node1Req = createResourceRequest("node1", false, null);
    reqs.add(node1Req);
    assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node2Req = createResourceRequest("node2", false, null);
    reqs.add(node2Req);
    assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(2,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }

  @Test
  public void testGetApplicableNodeCountForAMLabels() throws Exception {
    Set<NodeId> noLabelNodes = new HashSet<>();
    for (int i = 0; i < 80; i++) {
      noLabelNodes.add(NodeId.newInstance("host" + i, 1234));
    }
    Set<NodeId> label1Nodes = new HashSet<>();
    for (int i = 80; i < 90; i++) {
      label1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    label1Nodes.add(NodeId.newInstance("host101", 0));
    label1Nodes.add(NodeId.newInstance("host102", 0));
    Map<String, Set<NodeId>> label1NodesMap = new HashMap<>();
    label1NodesMap.put("label1", label1Nodes);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    RMNodeLabelsManager labMan = Mockito.mock(RMNodeLabelsManager.class);
    Mockito.when(labMan.getNodesWithoutALabel()).thenReturn(noLabelNodes);
    Mockito.when(labMan.getLabelsToNodes(Collections.singleton("label1")))
        .thenReturn(label1NodesMap);
    Mockito.when(rmContext.getNodeLabelManager()).thenReturn(labMan);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setNodeLabelExpression("label1");
    assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }

  @Test
  public void testGetApplicableNodeCountForAMLocalityAndLabels()
      throws Exception {
    List<NodeId> rack1Nodes = new ArrayList<>();
    for (int i = 0; i < 29; i++) {
      rack1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    NodeId node1 = NodeId.newInstance("node1", 1234);
    NodeId node2 = NodeId.newInstance("node2", 1234);
    rack1Nodes.add(node2);
    Set<NodeId> noLabelNodes = new HashSet<>();
    for (int i = 0; i < 19; i++) {
      noLabelNodes.add(rack1Nodes.get(i));
    }
    noLabelNodes.add(node2);
    for (int i = 29; i < 89; i++) {
      noLabelNodes.add(NodeId.newInstance("host" + i, 1234));
    }
    Set<NodeId> label1Nodes = new HashSet<>();
    label1Nodes.add(node1);
    for (int i = 89; i < 93; i++) {
      label1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    for (int i = 19; i < 29; i++) {
      label1Nodes.add(rack1Nodes.get(i));
    }
    label1Nodes.add(NodeId.newInstance("host101", 0));
    label1Nodes.add(NodeId.newInstance("host102", 0));
    Map<String, Set<NodeId>> label1NodesMap = new HashMap<>();
    label1NodesMap.put("label1", label1Nodes);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    Mockito.when(scheduler.getNodeIds("/rack1")).thenReturn(rack1Nodes);
    Mockito.when(scheduler.getNodeIds("node1"))
        .thenReturn(Collections.singletonList(node1));
    Mockito.when(scheduler.getNodeIds("node2"))
        .thenReturn(Collections.singletonList(node2));
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    RMNodeLabelsManager labMan = Mockito.mock(RMNodeLabelsManager.class);
    Mockito.when(labMan.getNodesWithoutALabel()).thenReturn(noLabelNodes);
    Mockito.when(labMan.getLabelsToNodes(Collections.singleton("label1")))
        .thenReturn(label1NodesMap);
    Mockito.when(rmContext.getNodeLabelManager()).thenReturn(labMan);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest rackReq = createResourceRequest("/rack1", true, null);
    reqs.add(rackReq);
    assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node1Req = createResourceRequest("node1", false, null);
    reqs.add(node1Req);
    assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    assertEquals(0,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node2Req = createResourceRequest("node2", false, null);
    reqs.add(node2Req);
    assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    anyReq.setNodeLabelExpression("label1");
    rackReq.setNodeLabelExpression("label1");
    node1Req.setNodeLabelExpression("label1");
    node2Req.setNodeLabelExpression("label1");
    anyReq.setRelaxLocality(true);
    reqs = new ArrayList<>();
    reqs.add(anyReq);
    assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    rackReq.setRelaxLocality(true);
    reqs.add(rackReq);
    assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    node1Req.setRelaxLocality(false);
    reqs.add(node1Req);
    assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    node2Req.setRelaxLocality(false);
    reqs.add(node2Req);
    assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    assertEquals(0,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }
  @Test
  public void testConvertRmAppAttemptStateToYarnApplicationAttemptState() {
    assertEquals(
        YarnApplicationAttemptState.FAILED,
        RMServerUtils.convertRmAppAttemptStateToYarnApplicationAttemptState(
            RMAppAttemptState.FINAL_SAVING,
            RMAppAttemptState.FAILED
        )
    );
    assertEquals(
        YarnApplicationAttemptState.SCHEDULED,
        RMServerUtils.convertRmAppAttemptStateToYarnApplicationAttemptState(
            RMAppAttemptState.FINAL_SAVING,
            RMAppAttemptState.SCHEDULED
        )
    );
    assertEquals(
        YarnApplicationAttemptState.NEW,
        RMServerUtils.convertRmAppAttemptStateToYarnApplicationAttemptState(
            RMAppAttemptState.NEW,
            null
        )
    );
  }

  private ResourceRequest createResourceRequest(String resource,
      boolean relaxLocality, String nodeLabel) {
    return ResourceRequest.newInstance(Priority.newInstance(0),
        resource, Resource.newInstance(1, 1), 1, relaxLocality, nodeLabel);
  }
}
