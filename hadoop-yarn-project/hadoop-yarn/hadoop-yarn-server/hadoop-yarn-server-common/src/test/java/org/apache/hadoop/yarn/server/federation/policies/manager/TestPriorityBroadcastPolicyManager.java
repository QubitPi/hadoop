/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.manager;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.BroadcastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.router.PriorityRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Simple test of {@link PriorityBroadcastPolicyManager}.
 */
public class TestPriorityBroadcastPolicyManager extends BasePolicyManagerTest {

  private WeightedPolicyInfo policyInfo;

  @BeforeEach
  public void setup() {
    // configure a policy

    wfp = new PriorityBroadcastPolicyManager();
    wfp.setQueue("queue1");
    SubClusterId sc1 = SubClusterId.newInstance("sc1");
    SubClusterId sc2 = SubClusterId.newInstance("sc2");
    policyInfo = new WeightedPolicyInfo();

    Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
    routerWeights.put(new SubClusterIdInfo(sc1), 0.2f);
    routerWeights.put(new SubClusterIdInfo(sc2), 0.8f);
    policyInfo.setRouterPolicyWeights(routerWeights);

    ((PriorityBroadcastPolicyManager) wfp).setWeightedPolicyInfo(policyInfo);

    // set expected params that the base test class will use for tests
    expectedPolicyManager = PriorityBroadcastPolicyManager.class;
    expectedAMRMProxyPolicy = BroadcastAMRMProxyPolicy.class;
    expectedRouterPolicy = PriorityRouterPolicy.class;
  }

  @Test
  public void testPolicyInfoSetCorrectly() throws Exception {
    serializeAndDeserializePolicyManager(wfp, expectedPolicyManager,
        expectedAMRMProxyPolicy, expectedRouterPolicy);

    // check the policyInfo propagates through ser/der correctly
    assertEquals(
        ((PriorityBroadcastPolicyManager) wfp).getWeightedPolicyInfo(),
        policyInfo);
  }
}
