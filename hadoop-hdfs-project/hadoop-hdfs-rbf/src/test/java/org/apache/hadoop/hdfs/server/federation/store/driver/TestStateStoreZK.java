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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test the ZooKeeper implementation of the State Store driver.
 */
public class TestStateStoreZK extends TestStateStoreDriverBase {

  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;
  private static String baseZNode;

  @BeforeAll
  public static void setupCluster() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    String connectString = curatorTestingServer.getConnectString();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();

    // Create the ZK State Store
    Configuration conf =
        getStateStoreConfiguration(StateStoreZooKeeperImpl.class);
    conf.set(RBFConfigKeys.FEDERATION_STORE_ZK_ADDRESS, connectString);
    // Disable auto-repair of connection
    conf.setLong(RBFConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS,
        TimeUnit.HOURS.toMillis(1));
    conf.setInt(RBFConfigKeys.FEDERATION_STORE_ZK_ASYNC_MAX_THREADS, 10);

    baseZNode = conf.get(RBFConfigKeys.FEDERATION_STORE_ZK_PARENT_PATH,
        RBFConfigKeys.FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT);
    getStateStore(conf);
  }

  @AfterAll
  public static void tearDownCluster() {
    curatorFramework.close();
    try {
      curatorTestingServer.stop();
    } catch (IOException e) {
    }
  }

  @BeforeEach
  public void startup() throws IOException {
    removeAll(getStateStoreDriver());
    StateStoreZooKeeperImpl stateStoreZooKeeper = (StateStoreZooKeeperImpl) getStateStoreDriver();
    stateStoreZooKeeper.setEnableConcurrent(false);
  }

  private <T extends BaseRecord> String generateFakeZNode(
      Class<T> recordClass) throws IOException {
    String nodeName = StateStoreUtils.getRecordName(recordClass);
    String primaryKey = "test";

    if (nodeName != null) {
      return baseZNode + "/" + nodeName + "/" + primaryKey;
    }
    return null;
  }

  private void testGetNullRecord(StateStoreDriver driver) throws Exception {
    testGetNullRecord(driver, MembershipState.class);
    testGetNullRecord(driver, MountTable.class);
    testGetNullRecord(driver, RouterState.class);
    testGetNullRecord(driver, DisabledNameservice.class);
  }

  private <T extends BaseRecord> void testGetNullRecord(
      StateStoreDriver driver, Class<T> recordClass) throws Exception {
    driver.removeAll(recordClass);

    String znode = generateFakeZNode(recordClass);
    assertNull(curatorFramework.checkExists().forPath(znode));

    curatorFramework.create().withMode(CreateMode.PERSISTENT)
        .withACL(null).forPath(znode, null);
    assertNotNull(curatorFramework.checkExists().forPath(znode));

    driver.get(recordClass);
    assertNull(curatorFramework.checkExists().forPath(znode));
  }

  @Test
  public void testAsyncPerformance() throws Exception {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    List<MountTable> insertList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      MountTable newRecord = generateFakeRecord(MountTable.class);
      insertList.add(newRecord);
    }
    // Insert Multiple on sync mode
    long startSyncPut = Time.now();
    stateStoreDriver.putAll(insertList, true, false);
    long endSyncPut = Time.now();
    // Removing 1000 records synchronously is painfully slow so test with only 5 records
    // Then remove the rest with removeAll()
    long startSyncRemove = Time.now();
    for (MountTable entry : insertList.subList(0, 5)) {
      stateStoreDriver.remove(entry);
    }
    long endSyncRemove = Time.now();
    stateStoreDriver.removeAll(MembershipState.class);

    stateStoreDriver.setEnableConcurrent(true);
    // Insert Multiple on async mode
    long startAsyncPut = Time.now();
    stateStoreDriver.putAll(insertList, true, false);
    long endAsyncPut = Time.now();
    long startAsyncRemove = Time.now();
    stateStoreDriver.removeMultiple(insertList.subList(0, 5));
    long endAsyncRemove = Time.now();
    assertTrue((endSyncPut - startSyncPut) > (endAsyncPut - startAsyncPut));
    assertTrue((endSyncRemove - startSyncRemove) > (endAsyncRemove - startAsyncRemove));
  }

  @Test
  public void testGetNullRecord() throws Exception {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    testGetNullRecord(stateStoreDriver);

    // test async mode
    stateStoreDriver.setEnableConcurrent(true);
    testGetNullRecord(stateStoreDriver);
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    testInsert(stateStoreDriver);
    // test async mode
    stateStoreDriver.setEnableConcurrent(true);
    testInsert(stateStoreDriver);
  }

  @Test
  public void testUpdate()
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    testPut(stateStoreDriver);

    // test async mode
    stateStoreDriver.setEnableConcurrent(true);
    testPut(stateStoreDriver);
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    testRemove(stateStoreDriver);

    // test async mode
    stateStoreDriver.setEnableConcurrent(true);
    testRemove(stateStoreDriver);
  }

  @Test
  public void testFetchErrors()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    StateStoreZooKeeperImpl stateStoreDriver = (StateStoreZooKeeperImpl) getStateStoreDriver();
    testFetchErrors(stateStoreDriver);

    // test async mode
    stateStoreDriver.setEnableConcurrent(true);
    testFetchErrors(stateStoreDriver);
  }

  @Test
  public void testCacheLoadMetrics() throws IOException {
    // inject value of CacheMountTableLoad as -1 initially, if tests get CacheMountTableLoadAvgTime
    // value as -1 ms, that would mean no other sample with value >= 0 would have been received and
    // hence this would be failure to assert that mount table avg load time is higher than -1
    getStateStoreService().getMetrics().setCacheLoading("MountTable", -1);
    long curMountTableLoadNum = getMountTableCacheLoadSamples(getStateStoreDriver());
    getStateStoreService().refreshCaches(true);
    getStateStoreService().refreshCaches(true);
    getStateStoreService().refreshCaches(true);
    testCacheLoadMetrics(getStateStoreDriver(), curMountTableLoadNum + 3, -1);
  }

}