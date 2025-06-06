/*
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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.federation.policies.BaseFederationPoliciesTest;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Simple test class for the {@link RejectAMRMProxyPolicy}.
 */
public class TestRejectAMRMProxyPolicy
    extends BaseFederationPoliciesTest {

  @BeforeEach
  public void setUp() throws Exception {
    setPolicy(new RejectAMRMProxyPolicy());
    // needed for base test to work
    setPolicyInfo(mock(WeightedPolicyInfo.class));

    for (int i = 1; i <= 2; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("sc" + i);
      SubClusterInfo sci = SubClusterInfo.newInstance(
          sc.toId(), "dns1:80", "dns1:81", "dns1:82", "dns1:83", SubClusterState.SC_RUNNING,
          System.currentTimeMillis(), "something");
      getActiveSubclusters().put(sc.toId(), sci);
    }

    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        mock(WeightedPolicyInfo.class), getActiveSubclusters());

  }

  @Test
  public void testSplitAllocateRequest() throws Exception {
    assertThrows(FederationPolicyException.class, () -> {
      // verify the request is broadcasted to all subclusters
      String[] hosts = new String[] {"host1", "host2" };
      List<ResourceRequest> resourceRequests = FederationPoliciesTestUtil
          .createResourceRequests(hosts, 2 * 1024, 2, 1, 3, null, false);

      Map<SubClusterId, List<ResourceRequest>> response =
          ((FederationAMRMProxyPolicy) getPolicy()).splitResourceRequests(
          resourceRequests, new HashSet<SubClusterId>());
    });
  }



}