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
package org.apache.hadoop.hdfs.server.datanode;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.function.Supplier;

/**
 * Test suite covering lifeline protocol handling in the DataNode.
 */
@Timeout(60)
public class TestDataNodeLifeline {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestDataNodeLifeline.class);

  static {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.TRACE);
  }

  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private DatanodeLifelineProtocolClientSideTranslatorPB lifelineNamenode;
  private DataNodeMetrics metrics;
  private DatanodeProtocolClientSideTranslatorPB namenode;
  private FSNamesystem namesystem;
  private DataNode dn;
  private BPServiceActor bpsa;
  private final BlockManagerFaultInjector injector = new BlockManagerFaultInjector() {
    @Override
    public void mockAnException() {
      throw new UnknownError("Unknown exception");
    }
  };

  @BeforeEach
  public void setup() throws Exception {
    // Configure cluster with lifeline RPC server enabled, and down-tune
    // heartbeat timings to try to force quick dead/stale DataNodes.
    conf = new HdfsConfiguration();
    conf.setInt(DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY, 2);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    conf.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.setInt(DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 6 * 1000);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    namesystem = cluster.getNameNode().getNamesystem();

    // Set up spies on RPC proxies so that we can inject failures.
    dn = cluster.getDataNodes().get(0);
    metrics = dn.getMetrics();
    assertNotNull(metrics);
    List<BPOfferService> allBpos = dn.getAllBpOs();
    assertNotNull(allBpos);
    assertEquals(1, allBpos.size());

    BPOfferService bpos = allBpos.get(0);
    List<BPServiceActor> allBpsa = bpos.getBPServiceActors();
    assertNotNull(allBpsa);
    assertEquals(1, allBpsa.size());

    bpsa = allBpsa.get(0);
    assertNotNull(bpsa);

    // Lifeline RPC proxy gets created on separate thread, so poll until found.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            if (bpsa.getLifelineNameNodeProxy() != null) {
              lifelineNamenode = spy(bpsa.getLifelineNameNodeProxy());
              bpsa.setLifelineNameNode(lifelineNamenode);
            }
            return lifelineNamenode != null;
          }
        }, 100, 10000);

    assertNotNull(bpsa.getNameNodeProxy());
    namenode = spy(bpsa.getNameNodeProxy());
    bpsa.setNameNode(namenode);
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
      GenericTestUtils.assertNoThreadsMatching(".*lifeline.*");
    }
  }

  @Test
  public void testSendLifelineIfHeartbeatBlocked() throws Exception {
    // Run the test for the duration of sending 10 lifeline RPC messages.
    int numLifelines = 10;
    CountDownLatch lifelinesSent = new CountDownLatch(numLifelines);

    // Intercept heartbeat to inject an artificial delay, until all expected
    // lifeline RPC messages have been sent.
    doAnswer(new LatchAwaitingAnswer<HeartbeatResponse>(lifelinesSent))
        .when(namenode).sendHeartbeat(
            any(DatanodeRegistration.class),
            any(StorageReport[].class),
            anyLong(),
            anyLong(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyBoolean(),
            any(SlowPeerReports.class),
            any(SlowDiskReports.class));

    // Intercept lifeline to trigger latch count-down on each call.
    doAnswer(new LatchCountingAnswer<Void>(lifelinesSent))
        .when(lifelineNamenode).sendLifeline(
            any(DatanodeRegistration.class),
            any(StorageReport[].class),
            anyLong(),
            anyLong(),
            anyInt(),
            anyInt(),
            anyInt(),
            any());

    // While waiting on the latch for the expected number of lifeline messages,
    // poll DataNode tracking information.  Thanks to the lifeline, we expect
    // that the DataNode always stays alive, and never goes stale or dead.
    while (!lifelinesSent.await(1, SECONDS)) {
      assertEquals(1, namesystem.getNumLiveDataNodes(),
          "Expect DataNode to be kept alive by lifeline.");
      assertEquals(0, namesystem.getNumDeadDataNodes(),
          "Expect DataNode not marked dead due to lifeline.");
      assertEquals(0, namesystem.getNumStaleDataNodes(),
          "Expect DataNode not marked stale due to lifeline.");
      // add a new volume on the next heartbeat
      cluster.getDataNodes().get(0).reconfigurePropertyImpl(
          DFS_DATANODE_DATA_DIR_KEY,
          cluster.getDataDirectory().concat("/data-new"));
    }

    // Verify that we did in fact call the lifeline RPC.
    verify(lifelineNamenode, atLeastOnce()).sendLifeline(
        any(DatanodeRegistration.class),
        any(StorageReport[].class),
        anyLong(),
        anyLong(),
        anyInt(),
        anyInt(),
        anyInt(),
        any());

    // Also verify lifeline call through metrics.  We expect at least
    // numLifelines, guaranteed by waiting on the latch.  There is a small
    // possibility of extra lifeline calls depending on timing, so we allow
    // slack in the assertion.
    assertTrue(getLongCounter("LifelinesNumOps",
            getMetrics(metrics.name())) >= numLifelines,
        "Expect metrics to count at least " + numLifelines + " calls.");
  }

  @Test
  public void testNoLifelineSentIfHeartbeatsOnTime() throws Exception {
    // Run the test for the duration of sending 10 heartbeat RPC messages.
    int numHeartbeats = 10;
    CountDownLatch heartbeatsSent = new CountDownLatch(numHeartbeats);

    // Intercept heartbeat to trigger latch count-down on each call.
    doAnswer(new LatchCountingAnswer<HeartbeatResponse>(heartbeatsSent))
        .when(namenode).sendHeartbeat(
            any(DatanodeRegistration.class),
            any(StorageReport[].class),
            anyLong(),
            anyLong(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyBoolean(),
            any(SlowPeerReports.class),
            any(SlowDiskReports.class));

    // While waiting on the latch for the expected number of heartbeat messages,
    // poll DataNode tracking information.  We expect that the DataNode always
    // stays alive, and never goes stale or dead.
    while (!heartbeatsSent.await(1, SECONDS)) {
      assertEquals(1, namesystem.getNumLiveDataNodes(),
          "Expect DataNode to be kept alive by lifeline.");
      assertEquals(0, namesystem.getNumDeadDataNodes(),
          "Expect DataNode not marked dead due to lifeline.");
      assertEquals(0, namesystem.getNumStaleDataNodes(),
          "Expect DataNode not marked stale due to lifeline.");
    }

    // Verify that we did not call the lifeline RPC.
    verify(lifelineNamenode, never()).sendLifeline(
        any(DatanodeRegistration.class),
        any(StorageReport[].class),
        anyLong(),
        anyLong(),
        anyInt(),
        anyInt(),
        anyInt(),
        any());

    // Also verify no lifeline calls through metrics.
    assertEquals(0, getLongCounter("LifelinesNumOps", getMetrics(metrics.name())),
        "Expect metrics to count no lifeline calls.");
  }

  @Test
  public void testLifelineForDeadNode() throws Exception {
    long initialCapacity = cluster.getNamesystem(0).getCapacityTotal();
    assertTrue(initialCapacity > 0);
    dn.setHeartbeatsDisabledForTests(true);
    cluster.setDataNodesDead();
    assertEquals(0, cluster.getNamesystem(0).getCapacityTotal(),
        "Capacity should be 0 after all DNs dead");
    bpsa.sendLifelineForTests();
    assertEquals(0, cluster.getNamesystem(0).getCapacityTotal(),
        "Lifeline should be ignored for dead node");
    // Wait for re-registration and heartbeat
    dn.setHeartbeatsDisabledForTests(false);
    final DatanodeDescriptor dnDesc = cluster.getNamesystem(0).getBlockManager()
        .getDatanodeManager().getDatanodes().iterator().next();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        return dnDesc.isAlive() && dnDesc.isHeartbeatedSinceRegistration();
      }
    }, 100, 5000);
    assertEquals(initialCapacity, cluster.getNamesystem(0).getCapacityTotal(),
        "Capacity should include only live capacity");
  }

  /**
   * Waits on a {@link CountDownLatch} before calling through to the method.
   */
  private final class LatchAwaitingAnswer<T> implements Answer<T> {
    private final CountDownLatch latch;

    public LatchAwaitingAnswer(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T answer(InvocationOnMock invocation)
        throws Throwable {
      LOG.info("Awaiting, remaining latch count is {}.", latch.getCount());
      latch.await();
      return (T)invocation.callRealMethod();
    }
  }

  /**
   * Counts on a {@link CountDownLatch} after each call through to the method.
   */
  private final class LatchCountingAnswer<T> implements Answer<T> {
    private final CountDownLatch latch;

    public LatchCountingAnswer(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T answer(InvocationOnMock invocation)
        throws Throwable {
      T result = (T)invocation.callRealMethod();
      latch.countDown();
      LOG.info("Countdown, remaining latch count is {}.", latch.getCount());
      return result;
    }
  }

  /**
   * Mock an exception in HeartbeatManager#updateHeartbeat and HeartbeatManager#updateLifeline
   * respectively, and trigger the heartbeat and lifeline in sequence. The capacityTotal obtained
   * before and after this operation should be the same.
   * @throws Exception
   */
  @Test
  public void testHeartbeatAndLifelineOnError() throws Exception {
    final Configuration config = new HdfsConfiguration();
    config.set(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, "0.0.0.0:0");

    try(MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(config).numDataNodes(1).build()) {
      cluster.waitActive();
      final FSNamesystem fsNamesystem = cluster.getNamesystem();

      // Get capacityTotal before triggering heartbeat and lifeline.
      DatanodeStatistics datanodeStatistics =
          fsNamesystem.getBlockManager().getDatanodeManager().getDatanodeStatistics();
      long capacityTotalBefore = datanodeStatistics.getCapacityTotal();

      // Mock an exception in HeartbeatManager#updateHeartbeat and HeartbeatManager#updateLifeline.
      BlockManagerFaultInjector.instance = injector;
      DataNode dataNode = cluster.getDataNodes().get(0);
      BlockPoolManager blockPoolManager = dataNode.getBlockPoolManager();
      for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
        if (bpos != null) {
          for (BPServiceActor actor : bpos.getBPServiceActors()) {
            try {
              actor.triggerHeartbeatForTests();
              actor.sendLifelineForTests();
            } catch (Throwable e) {
              assertTrue(e.getMessage().contains("Unknown exception"));
            }
          }
        }
      }

      // Get capacityTotal after triggering heartbeat and lifeline.
      long capacityTotalAfter = datanodeStatistics.getCapacityTotal();
      // The capacityTotal should be same.
      assertEquals(capacityTotalBefore, capacityTotalAfter);
    }
  }
}
