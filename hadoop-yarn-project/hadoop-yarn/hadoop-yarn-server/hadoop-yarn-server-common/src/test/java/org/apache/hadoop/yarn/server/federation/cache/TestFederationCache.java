/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for FederationCache.
 */

public class TestFederationCache {

  public static Collection<Class[]> getParameters() {
    return Arrays.asList(new Class[][]{{FederationGuavaCache.class}, {FederationJCache.class},
        {FederationCaffeineCache.class}});
  }

  private final long clusterTs = System.currentTimeMillis();
  private final int numSubClusters = 3;
  private final int numApps = 5;
  private final int numQueues = 2;

  private Configuration conf;
  private FederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreTestUtil;
  private FederationStateStoreFacade facade;

  private void initTestFederationCache(Class cacheClassName)
      throws IOException, YarnException {
    conf = new Configuration();
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 1);
    conf.setClass(YarnConfiguration.FEDERATION_FACADE_CACHE_CLASS,
        cacheClassName, FederationCache.class);
    facade = FederationStateStoreFacade.getInstance(conf);
    setUp();
  }

  public void setUp() throws IOException, YarnException {
    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    facade.reinitialize(stateStore, conf);
    // hydrate the store
    stateStoreTestUtil = new FederationStateStoreTestUtil(stateStore);
    stateStoreTestUtil.registerSubClusters(numSubClusters);
    stateStoreTestUtil.addAppsHomeSC(clusterTs, numApps);
    stateStoreTestUtil.addPolicyConfigs(numQueues);
  }

  @AfterEach
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetSubCluster(Class cacheClassName)
      throws YarnException, IOException {
    initTestFederationCache(cacheClassName);
    for (int i = 0; i < numSubClusters; i++) {
      SubClusterId subClusterId =
          SubClusterId.newInstance(FederationStateStoreTestUtil.SC_PREFIX + i);
      SubClusterInfo expectedSubCluster = stateStoreTestUtil.querySubClusterInfo(subClusterId);
      SubClusterInfo cachedSubCluster = facade.getSubCluster(subClusterId);
      assertEquals(expectedSubCluster, cachedSubCluster);
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetPoliciesConfigurations(Class cacheClassName)
      throws YarnException, IOException {
    initTestFederationCache(cacheClassName);
    Map<String, SubClusterPolicyConfiguration> queuePolicies =
        facade.getPoliciesConfigurations();
    for (String queue : queuePolicies.keySet()) {
      SubClusterPolicyConfiguration expectedPC = stateStoreTestUtil.queryPolicyConfiguration(queue);
      SubClusterPolicyConfiguration cachedPC = queuePolicies.get(queue);
      assertEquals(expectedPC, cachedPC);
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetHomeSubClusterForApp(Class cacheClassName)
      throws YarnException, IOException {
    initTestFederationCache(cacheClassName);
    for (int i = 0; i < numApps; i++) {
      ApplicationId appId = ApplicationId.newInstance(clusterTs, i);
      SubClusterId expectedSC = stateStoreTestUtil.queryApplicationHomeSC(appId);
      SubClusterId cachedPC = facade.getApplicationHomeSubCluster(appId);
      assertEquals(expectedSC, cachedPC);
    }
  }
}
