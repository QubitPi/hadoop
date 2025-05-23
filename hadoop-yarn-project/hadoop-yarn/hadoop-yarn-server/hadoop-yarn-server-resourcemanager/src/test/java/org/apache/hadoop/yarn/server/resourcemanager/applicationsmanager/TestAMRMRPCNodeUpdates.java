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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAMRMRPCNodeUpdates {
  private MockRM rm;
  private ApplicationMasterService amService;

  @BeforeEach
  public void setUp() {
    this.rm = new MockRM() {
      @Override
      public void init(Configuration conf) {
        conf.set(
          CapacitySchedulerConfiguration.MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
          "1.0");
        super.init(conf);
      }
    };

    rm.start();
    amService = rm.getApplicationMasterService();
  }
  
  @AfterEach
  public void tearDown() {
    if (rm != null) {
      this.rm.stop();
    }
  }
  
  private void syncNodeHeartbeat(MockNM nm, boolean health) throws Exception {
    nm.nodeHeartbeat(health);
    rm.drainEvents();
  }
  
  private void syncNodeLost(MockNM nm) throws Exception {
    rm.sendNodeStarted(nm);
    rm.waitForState(nm.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(nm);
    rm.drainEvents();
  }

  private void syncNodeGracefulDecommission(
      MockNM nm, int timeout) throws Exception {
    rm.sendNodeGracefulDecommission(nm, timeout);
    rm.waitForState(nm.getNodeId(), NodeState.DECOMMISSIONING);
    rm.drainEvents();
  }

  private void syncNodeRecommissioning(MockNM nm) throws Exception {
    rm.sendNodeEvent(nm, RMNodeEventType.RECOMMISSION);
    rm.waitForState(nm.getNodeId(), NodeState.RUNNING);
    rm.drainEvents();
  }

  private AllocateResponse allocate(final ApplicationAttemptId attemptId,
      final AllocateRequest req) throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        rm.getRMContext().getRMApps().get(attemptId.getApplicationId())
          .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    return ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return amService.allocate(req);
      }
    });
  }

  @Test
  public void testAMRMDecommissioningNodes() throws Exception {
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10000);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 10000);
    rm.drainEvents();

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(2000, rm);

    // Trigger the scheduling so the AM gets 'launched' on nm1
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());

    // register AM returns no unusable node
    am1.registerAppAttempt();

    Integer decommissioningTimeout = 600;
    syncNodeGracefulDecommission(nm2, decommissioningTimeout);

    AllocateRequest allocateRequest1 =
        AllocateRequest.newInstance(0, 0F, null, null, null);
    AllocateResponse response1 =
        allocate(attempt1.getAppAttemptId(), allocateRequest1);
    List<NodeReport> updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    NodeReport nr = updatedNodes.iterator().next();
    assertEquals(
        decommissioningTimeout, nr.getDecommissioningTimeout());
    assertEquals(
        NodeUpdateType.NODE_DECOMMISSIONING, nr.getNodeUpdateType());
  }

  @Test
  public void testAMRMRecommissioningNodes() throws Exception {
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10000);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 10000);
    rm.drainEvents();

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(2000, rm);

    // Trigger the scheduling so the AM gets 'launched' on nm1
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());

    // register AM returns no unusable node
    am1.registerAppAttempt();

    // DECOMMISSION nm2
    Integer decommissioningTimeout = 600;
    syncNodeGracefulDecommission(nm2, decommissioningTimeout);

    AllocateRequest allocateRequest1 =
            AllocateRequest.newInstance(0, 0F, null, null, null);
    AllocateResponse response1 =
            allocate(attempt1.getAppAttemptId(), allocateRequest1);
    List<NodeReport> updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    NodeReport nr = updatedNodes.iterator().next();
    assertEquals(
            decommissioningTimeout, nr.getDecommissioningTimeout());
    assertEquals(
            NodeUpdateType.NODE_DECOMMISSIONING, nr.getNodeUpdateType());

    // Wait for nm2 to RECOMMISSION
    syncNodeRecommissioning(nm2);

    AllocateRequest allocateRequest2 = AllocateRequest
            .newInstance(response1.getResponseId(), 0F, null, null, null);
    AllocateResponse response2 =
            allocate(attempt1.getAppAttemptId(), allocateRequest2);
    List<NodeReport> updatedNodes2 = response2.getUpdatedNodes();
    assertEquals(1, updatedNodes2.size());
    NodeReport nr2 = updatedNodes2.iterator().next();
    assertEquals(
            NodeUpdateType.NODE_USABLE, nr2.getNodeUpdateType());
  }

  @Test
  public void testAMRMUnusableNodes() throws Exception {
    
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10000);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 10000);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 10000);
    MockNM nm4 = rm.registerNode("127.0.0.4:1234", 10000);
    rm.drainEvents();

    RMApp app1 = MockRMAppSubmitter.submitWithMemory(2000, rm);

    // Trigger the scheduling so the AM gets 'launched' on nm1
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    
    // register AM returns no unusable node
    am1.registerAppAttempt();

    // allocate request returns no updated node
    AllocateRequest allocateRequest1 =
        AllocateRequest.newInstance(0, 0F, null, null, null);
    AllocateResponse response1 =
        allocate(attempt1.getAppAttemptId(), allocateRequest1);
    List<NodeReport> updatedNodes = response1.getUpdatedNodes();
    assertEquals(0, updatedNodes.size());

    syncNodeHeartbeat(nm4, false);
    
    // allocate request returns updated node
    allocateRequest1 =
        AllocateRequest.newInstance(response1.getResponseId(), 0F, null, null,
          null);
    response1 = allocate(attempt1.getAppAttemptId(), allocateRequest1);
    updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    NodeReport nr = updatedNodes.iterator().next();
    assertEquals(nm4.getNodeId(), nr.getNodeId());
    assertEquals(NodeState.UNHEALTHY, nr.getNodeState());
    assertNull(nr.getDecommissioningTimeout());
    assertEquals(NodeUpdateType.NODE_UNUSABLE, nr.getNodeUpdateType());
    
    // resending the allocate request returns the same result
    response1 = allocate(attempt1.getAppAttemptId(), allocateRequest1);
    updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    assertEquals(nm4.getNodeId(), nr.getNodeId());
    assertEquals(NodeState.UNHEALTHY, nr.getNodeState());
    assertNull(nr.getDecommissioningTimeout());
    assertEquals(NodeUpdateType.NODE_UNUSABLE, nr.getNodeUpdateType());

    syncNodeLost(nm3);
    
    // subsequent allocate request returns delta
    allocateRequest1 =
        AllocateRequest.newInstance(response1.getResponseId(), 0F, null, null,
          null);
    response1 = allocate(attempt1.getAppAttemptId(), allocateRequest1);
    updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    assertEquals(nm3.getNodeId(), nr.getNodeId());
    assertEquals(NodeState.LOST, nr.getNodeState());
    assertNull(nr.getDecommissioningTimeout());
    assertEquals(NodeUpdateType.NODE_UNUSABLE, nr.getNodeUpdateType());
        
    // registering another AM gives it the complete failed list
    RMApp app2 = MockRMAppSubmitter.submitWithMemory(2000, rm);
    // Trigger nm2 heartbeat so that AM gets launched on it
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    
    // register AM returns all unusable nodes
    am2.registerAppAttempt();
    
    // allocate request returns no updated node
    AllocateRequest allocateRequest2 =
        AllocateRequest.newInstance(0, 0F, null, null, null);
    AllocateResponse response2 =
        allocate(attempt2.getAppAttemptId(), allocateRequest2);
    updatedNodes = response2.getUpdatedNodes();
    assertEquals(0, updatedNodes.size());
    
    syncNodeHeartbeat(nm4, true);
    
    // both AM's should get delta updated nodes
    allocateRequest1 =
        AllocateRequest.newInstance(response1.getResponseId(), 0F, null, null,
          null);
    response1 = allocate(attempt1.getAppAttemptId(), allocateRequest1);
    updatedNodes = response1.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    assertEquals(nm4.getNodeId(), nr.getNodeId());
    assertEquals(NodeState.RUNNING, nr.getNodeState());
    assertNull(nr.getDecommissioningTimeout());
    assertEquals(NodeUpdateType.NODE_USABLE, nr.getNodeUpdateType());
    
    allocateRequest2 =
        AllocateRequest.newInstance(response2.getResponseId(), 0F, null, null,
          null);
    response2 = allocate(attempt2.getAppAttemptId(), allocateRequest2);
    updatedNodes = response2.getUpdatedNodes();
    assertEquals(1, updatedNodes.size());
    nr = updatedNodes.iterator().next();
    assertEquals(nm4.getNodeId(), nr.getNodeId());
    assertEquals(NodeState.RUNNING, nr.getNodeState());
    assertNull(nr.getDecommissioningTimeout());
    assertEquals(NodeUpdateType.NODE_USABLE, nr.getNodeUpdateType());

    // subsequent allocate calls should return no updated nodes
    allocateRequest2 =
        AllocateRequest.newInstance(response2.getResponseId(), 0F, null, null,
          null);
    response2 = allocate(attempt2.getAppAttemptId(), allocateRequest2);
    updatedNodes = response2.getUpdatedNodes();
    assertEquals(0, updatedNodes.size());
    
    // how to do the above for LOST node
  
  }
}
