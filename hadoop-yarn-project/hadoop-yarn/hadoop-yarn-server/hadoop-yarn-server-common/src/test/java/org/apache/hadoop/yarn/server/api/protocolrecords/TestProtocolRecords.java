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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NMContainerStatusPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb
    .NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestProtocolRecords {

  @Test
  public void testResource() {
    final long mem = 123;
    final int vcores = 456;
    final Resource r = Resource.newInstance(mem, vcores);
    // should be a lightweight SimpleResource which is a private inner class
    // so just verify it's not the heavyweight pb impl.
    assertFalse(r instanceof ResourcePBImpl);
    assertEquals(mem, r.getMemorySize());
    assertEquals(vcores, r.getVirtualCores());

    ResourceProto proto = ProtoUtils.convertToProtoFormat(r);
    assertEquals(mem, proto.getMemory());
    assertEquals(vcores, proto.getVirtualCores());
    assertEquals(r, ProtoUtils.convertFromProtoFormat(proto));
  }

  @Test
  public void testNMContainerStatus() {
    ApplicationId appId = ApplicationId.newInstance(123456789, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    Resource resource = Resource.newInstance(1000, 200);

    NMContainerStatus report =
        NMContainerStatus.newInstance(containerId, 0,
          ContainerState.COMPLETE, resource, "diagnostics",
          ContainerExitStatus.ABORTED, Priority.newInstance(10), 1234);
    NMContainerStatus reportProto =
        new NMContainerStatusPBImpl(
          ((NMContainerStatusPBImpl) report).getProto());
    assertEquals("diagnostics", reportProto.getDiagnostics());
    assertEquals(resource, reportProto.getAllocatedResource());
    assertEquals(ContainerExitStatus.ABORTED,
      reportProto.getContainerExitStatus());
    assertEquals(ContainerState.COMPLETE,
      reportProto.getContainerState());
    assertEquals(containerId, reportProto.getContainerId());
    assertEquals(Priority.newInstance(10), reportProto.getPriority());
    assertEquals(1234, reportProto.getCreationTime());
  }

  @Test
  public void testRegisterNodeManagerRequest() {
    ApplicationId appId = ApplicationId.newInstance(123456789, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);

    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, 0,
          ContainerState.RUNNING, Resource.newInstance(1024, 1), "diagnostics",
          0, Priority.newInstance(10), 1234);
    List<NMContainerStatus> reports = Arrays.asList(containerReport);
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
          NodeId.newInstance("1.1.1.1", 1000), 8080,
            Resource.newInstance(1024, 1), "NM-version-id", reports,
            Arrays.asList(appId));
    RegisterNodeManagerRequest requestProto =
        new RegisterNodeManagerRequestPBImpl(
          ((RegisterNodeManagerRequestPBImpl) request).getProto());
    assertEquals(containerReport, requestProto
      .getNMContainerStatuses().get(0));
    assertEquals(8080, requestProto.getHttpPort());
    assertEquals("NM-version-id", requestProto.getNMVersion());
    assertEquals(NodeId.newInstance("1.1.1.1", 1000),
      requestProto.getNodeId());
    assertEquals(Resource.newInstance(1024, 1),
      requestProto.getResource());
    assertEquals(1, requestProto.getRunningApplications().size());
    assertEquals(appId, requestProto.getRunningApplications().get(0));
  }

  @Test
  public void testNodeHeartBeatResponse() throws IOException {
    NodeHeartbeatResponse record =
        Records.newRecord(NodeHeartbeatResponse.class);
    Map<ApplicationId, ByteBuffer> appCredentials =
        new HashMap<ApplicationId, ByteBuffer>();
    Credentials app1Cred = new Credentials();

    Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>();
    token1.setKind(new Text("kind1"));
    app1Cred.addToken(new Text("token1"), token1);
    Token<DelegationTokenIdentifier> token2 =
        new Token<DelegationTokenIdentifier>();
    token2.setKind(new Text("kind2"));
    app1Cred.addToken(new Text("token2"), token2);

    DataOutputBuffer dob = new DataOutputBuffer();
    app1Cred.writeTokenStorageToStream(dob);
    ByteBuffer byteBuffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    appCredentials.put(ApplicationId.newInstance(1234, 1), byteBuffer);
    record.setSystemCredentialsForApps(
        YarnServerBuilderUtils.convertToProtoFormat(appCredentials));

    NodeHeartbeatResponse proto =
        new NodeHeartbeatResponsePBImpl(
          ((NodeHeartbeatResponsePBImpl) record).getProto());
    assertEquals(appCredentials, YarnServerBuilderUtils
        .convertFromProtoFormat(proto.getSystemCredentialsForApps()));
  }

  @Test
  public void testNodeHeartBeatRequest() throws IOException {
    NodeHeartbeatRequest record =
        Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus nodeStatus =
        Records.newRecord(NodeStatus.class);
    OpportunisticContainersStatus opportunisticContainersStatus =
        Records.newRecord(OpportunisticContainersStatus.class);
    opportunisticContainersStatus.setEstimatedQueueWaitTime(123);
    opportunisticContainersStatus.setWaitQueueLength(321);
    nodeStatus.setOpportunisticContainersStatus(opportunisticContainersStatus);
    record.setNodeStatus(nodeStatus);

    Set<NodeAttribute> attributeSet =
        Sets.newHashSet(NodeAttribute.newInstance("attributeA",
                NodeAttributeType.STRING, "valueA"),
            NodeAttribute.newInstance("attributeB",
                NodeAttributeType.STRING, "valueB"));
    record.setNodeAttributes(attributeSet);

    NodeHeartbeatRequestPBImpl pb = new
        NodeHeartbeatRequestPBImpl(
        ((NodeHeartbeatRequestPBImpl) record).getProto());

    assertEquals(123,
        pb.getNodeStatus()
            .getOpportunisticContainersStatus().getEstimatedQueueWaitTime());
    assertEquals(321,
        pb.getNodeStatus().getOpportunisticContainersStatus()
            .getWaitQueueLength());
    assertEquals(2, pb.getNodeAttributes().size());
  }

  @Test
  public void testContainerStatus() {
    ContainerStatus status = Records.newRecord(ContainerStatus.class);
    List<String> ips = Arrays.asList("127.0.0.1", "139.5.25.2");
    status.setIPs(ips);
    status.setHost("locahost123");
    ContainerStatusPBImpl pb =
        new ContainerStatusPBImpl(((ContainerStatusPBImpl) status).getProto());
    assertEquals(ips, pb.getIPs());
    assertEquals("locahost123", pb.getHost());
    assertEquals(ExecutionType.GUARANTEED, pb.getExecutionType());
    status.setIPs(null);
    assertNull(status.getIPs());
  }
}
