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

package org.apache.hadoop.yarn.server.federation.policies;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test class for {@link FederationPolicyInitializationContextValidator}.
 */
public class TestFederationPolicyInitializationContextValidator {

  private SubClusterPolicyConfiguration goodConfig;
  private SubClusterResolver goodSR;
  private FederationStateStoreFacade goodFacade;
  private SubClusterId goodHome;
  private FederationPolicyInitializationContext context;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    goodFacade = FederationPoliciesTestUtil.initFacade(conf);
    goodConfig = new MockPolicyManager().serializeConf();
    goodSR = FederationPoliciesTestUtil.initResolver();
    goodHome = SubClusterId.newInstance("homesubcluster");
    context = new FederationPolicyInitializationContext(goodConfig, goodSR,
        goodFacade, goodHome);
  }

  @Test
  public void correcInit() throws Exception {
    FederationPolicyInitializationContextValidator.validate(context,
        MockPolicyManager.class.getCanonicalName());
  }

  @Test
  public void nullContext() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      FederationPolicyInitializationContextValidator.validate(null,
          MockPolicyManager.class.getCanonicalName());
    });
  }

  @Test
  public void nullType() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      FederationPolicyInitializationContextValidator.validate(context, null);
    });
  }

  @Test
  public void wrongType() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      FederationPolicyInitializationContextValidator.validate(context,
          "WrongType");
    });
  }

  @Test
  public void nullConf() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      context.setSubClusterPolicyConfiguration(null);
      FederationPolicyInitializationContextValidator.validate(context,
          MockPolicyManager.class.getCanonicalName());
    });
  }

  @Test
  public void nullResolver() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      context.setFederationSubclusterResolver(null);
      FederationPolicyInitializationContextValidator.validate(context,
          MockPolicyManager.class.getCanonicalName());
    });
  }

  @Test
  public void nullFacade() throws Exception {
    assertThrows(FederationPolicyInitializationException.class, () -> {
      context.setFederationStateStoreFacade(null);
      FederationPolicyInitializationContextValidator.validate(context,
          MockPolicyManager.class.getCanonicalName());
    });
  }

  private class MockPolicyManager implements FederationPolicyManager {

    @Override
    public FederationAMRMProxyPolicy getAMRMPolicy(
        FederationPolicyInitializationContext policyContext,
        FederationAMRMProxyPolicy oldInstance)
        throws FederationPolicyInitializationException {
      return null;
    }

    @Override
    public FederationRouterPolicy getRouterPolicy(
        FederationPolicyInitializationContext policyContext,
        FederationRouterPolicy oldInstance)
        throws FederationPolicyInitializationException {
      return null;
    }

    @Override
    public SubClusterPolicyConfiguration serializeConf()
        throws FederationPolicyInitializationException {
      ByteBuffer buf = ByteBuffer.allocate(0);
      return SubClusterPolicyConfiguration.newInstance("queue1",
          this.getClass().getCanonicalName(), buf);
    }

    @Override
    public String getQueue() {
      return "default";
    }

    @Override
    public void setQueue(String queue) {

    }

    @Override
    public WeightedPolicyInfo getWeightedPolicyInfo() {
      return null;
    }

    @Override
    public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    }

    @Override
    public boolean isSupportWeightedPolicyInfo() {
      return false;
    }
  }

}
