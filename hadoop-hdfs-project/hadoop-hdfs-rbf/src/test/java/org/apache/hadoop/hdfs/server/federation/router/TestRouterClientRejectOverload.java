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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.simulateSlowNamenode;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.simulateThrowExceptionRouterRpcServer;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToStandby;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToActive;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the Router overload control which rejects requests when the RPC client
 * is overloaded. This feature is managed by
 * {@link RBFConfigKeys#DFS_ROUTER_CLIENT_REJECT_OVERLOAD}.
 */
public class TestRouterClientRejectOverload {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterClientRejectOverload.class);

  private StateStoreDFSCluster cluster;

  @AfterEach
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean overloadControl, boolean ha)
      throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(ha, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);
    // Overload control
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD, overloadControl);

    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testWithoutOverloadControl() throws Exception {
    setupCluster(false, false);

    // Nobody should get overloaded
    testOverloaded(0);

    // Set subcluster 0 as slow
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 1);

    // Nobody should get overloaded, but it will be really slow
    testOverloaded(0);

    // No rejected requests expected
    for (RouterContext router : cluster.getRouters()) {
      FederationRPCMetrics rpcMetrics =
          router.getRouter().getRpcServer().getRPCMetrics();
      assertEquals(0, rpcMetrics.getProxyOpFailureClientOverloaded());
    }
  }

  @Test
  public void testOverloadControl() throws Exception {
    setupCluster(true, false);

    List<RouterContext> routers = cluster.getRouters();
    FederationRPCMetrics rpcMetrics0 =
        routers.get(0).getRouter().getRpcServer().getRPCMetrics();
    FederationRPCMetrics rpcMetrics1 =
        routers.get(1).getRouter().getRpcServer().getRPCMetrics();

    // Nobody should get overloaded
    testOverloaded(0);
    assertEquals(0, rpcMetrics0.getProxyOpFailureClientOverloaded());
    assertEquals(0, rpcMetrics1.getProxyOpFailureClientOverloaded());

    // Set subcluster 0 as slow
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 1);

    // The subcluster should be overloaded now and reject 4-5 requests
    testOverloaded(4, 6);
    assertTrue(rpcMetrics0.getProxyOpFailureClientOverloaded()
        + rpcMetrics1.getProxyOpFailureClientOverloaded() >= 4);

    // Client using HA with 2 Routers
    // A single Router gets overloaded, but 2 will handle it
    Configuration clientConf = cluster.getRouterClientConf();

    // Each Router should get a similar number of ops (>=8) out of 2*10
    long iniProxyOps0 = rpcMetrics0.getProxyOps();
    long iniProxyOps1 = rpcMetrics1.getProxyOps();
    testOverloaded(0, 0, new URI("hdfs://fed/"), clientConf, 10);
    long proxyOps0 = rpcMetrics0.getProxyOps() - iniProxyOps0;
    long proxyOps1 = rpcMetrics1.getProxyOps() - iniProxyOps1;
    assertEquals(2 * 10, proxyOps0 + proxyOps1);
    assertTrue(proxyOps0 >= 8, proxyOps0 + " operations: not distributed");
    assertTrue(proxyOps1 >= 8, proxyOps1 + " operations: not distributed");
  }

  private void testOverloaded(int expOverload) throws Exception {
    testOverloaded(expOverload, expOverload);
  }

  private void testOverloaded(int expOverloadMin, int expOverloadMax)
      throws Exception {
    RouterContext routerContext = cluster.getRandomRouter();
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    testOverloaded(expOverloadMin, expOverloadMax, address, conf, 10);
  }

  /**
   * Test if the Router gets overloaded by submitting requests in parallel.
   * We check how many requests got rejected at the end.
   * @param expOverloadMin Min number of requests expected as overloaded.
   * @param expOverloadMax Max number of requests expected as overloaded.
   * @param address Destination address.
   * @param conf Configuration of the client.
   * @param numOps Number of operations to submit.
   * @throws Exception If it cannot perform the test.
   */
  private void testOverloaded(int expOverloadMin, int expOverloadMax,
      final URI address, final Configuration conf, final int numOps)
          throws Exception {

    // Submit renewLease() ops which go to all subclusters
    final AtomicInteger overloadException = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(numOps);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numOps; i++) {
      // Stagger the operations a little (50ms)
      final int sleepTime = i * 50;
      Future<?> future = exec.submit(() -> {
        DFSClient routerClient = null;
        try {
          Thread.sleep(sleepTime);
          routerClient = new DFSClient(address, conf);
          String clientName = routerClient.getClientName();
          ClientProtocol routerProto = routerClient.getNamenode();
          routerProto.renewLease(clientName, null);
        } catch (RemoteException re) {
          IOException ioe = re.unwrapRemoteException();
          assertTrue(ioe instanceof StandbyException, "Wrong exception: " + ioe);
          assertExceptionContains("is overloaded", ioe);
          overloadException.incrementAndGet();
        } catch (IOException e) {
          fail("Unexpected exception: " + e);
        } catch (InterruptedException e) {
          fail("Cannot sleep: " + e);
        } finally {
          if (routerClient != null) {
            try {
              routerClient.close();
            } catch (IOException e) {
              LOG.error("Cannot close the client");
            }
          }
        }
      });
      futures.add(future);
    }
    // Wait until all the requests are done
    while (!futures.isEmpty()) {
      futures.remove(0).get();
    }
    exec.shutdown();

    int num = overloadException.get();
    if (expOverloadMin == expOverloadMax) {
      assertEquals(expOverloadMin, num);
    } else {
      assertTrue(num >= expOverloadMin, "Expected >=" + expOverloadMin + " but was " + num);
      assertTrue(num <= expOverloadMax, "Expected <=" + expOverloadMax + " but was " + num);
    }
  }

  @Test
  public void testConnectionNullException() throws Exception {
    setupCluster(false, false);

    // Choose 1st router
    RouterContext routerContext = cluster.getRouters().get(0);
    Router router = routerContext.getRouter();
    // This router will throw ConnectionNullException
    simulateThrowExceptionRouterRpcServer(router.getRpcServer());

    // Set dfs.client.failover.random.order false, to pick 1st router at first
    Configuration conf = cluster.getRouterClientConf();
    conf.setBoolean("dfs.client.failover.random.order", false);
    // Client to access Router Cluster
    DFSClient routerClient =
        new DFSClient(new URI("hdfs://fed"), conf);

    // Get router0 metrics
    FederationRPCMetrics rpcMetrics0 = cluster.getRouters().get(0)
        .getRouter().getRpcServer().getRPCMetrics();
    // Get router1 metrics
    FederationRPCMetrics rpcMetrics1 = cluster.getRouters().get(1)
        .getRouter().getRpcServer().getRPCMetrics();

    // Original failures
    long originalRouter0Failures = rpcMetrics0.getProxyOpFailureCommunicate();
    long originalRouter1Failures = rpcMetrics1.getProxyOpFailureCommunicate();

    // RPC call must be successful
    routerClient.getFileInfo("/");

    // Router 0 failures will increase
    assertEquals(originalRouter0Failures + 1,
        rpcMetrics0.getProxyOpFailureCommunicate());
    // Router 1 failures will not change
    assertEquals(originalRouter1Failures,
        rpcMetrics1.getProxyOpFailureCommunicate());
  }

  /**
   * When failover occurs, no namenodes are available within a short time.
   * Client will success after some retries.
   */
  @Test
  public void testNoNamenodesAvailable() throws Exception {
    setupCluster(false, true);

    transitionClusterNSToStandby(cluster);

    Configuration conf = cluster.getRouterClientConf();
    // Set dfs.client.failover.random.order false, to pick 1st router at first
    conf.setBoolean("dfs.client.failover.random.order", false);

    // Retries is 3 (see FailoverOnNetworkExceptionRetry#shouldRetry, will fail
    // when reties > max.attempts), so total access is 4.
    conf.setInt("dfs.client.retry.max.attempts", 2);
    DFSClient routerClient = new DFSClient(new URI("hdfs://fed"), conf);

    // Get router0 metrics
    FederationRPCMetrics rpcMetrics0 = cluster.getRouters().get(0)
        .getRouter().getRpcServer().getRPCMetrics();
    // Get router1 metrics
    FederationRPCMetrics rpcMetrics1 = cluster.getRouters().get(1)
        .getRouter().getRpcServer().getRPCMetrics();

    // GetFileInfo will throw Exception
    String exceptionMessage = "org.apache.hadoop.hdfs.server.federation."
        + "router.NoNamenodesAvailableException: No namenodes available "
        + "under nameservice ns0";
    RemoteException remoteException = assertThrows(RemoteException.class, () -> {

      // Original failures
      long originalRouter0Failures = rpcMetrics0.getProxyOpNoNamenodes();
      long originalRouter1Failures = rpcMetrics1.getProxyOpNoNamenodes();

      routerClient.getFileInfo("/");

      // Router 0 failures will increase
      assertEquals(originalRouter0Failures + 4,
          rpcMetrics0.getProxyOpNoNamenodes());
      // Router 1 failures do not change
      assertEquals(originalRouter1Failures,
          rpcMetrics1.getProxyOpNoNamenodes());

      // Make name services available
      transitionClusterNSToActive(cluster, 0);
      for (RouterContext routerContext : cluster.getRouters()) {
        // Manually trigger the heartbeat
        Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
            .getRouter().getNamenodeHeartbeatServices();
        for (NamenodeHeartbeatService service : heartbeatServices) {
          service.periodicInvoke();
        }
        // Update service cache
        routerContext.getRouter().getStateStore().refreshCaches(true);
      }

      originalRouter0Failures = rpcMetrics0.getProxyOpNoNamenodes();

      // RPC call must be successful
      routerClient.getFileInfo("/");
      // Router 0 failures do not change
      assertEquals(originalRouter0Failures, rpcMetrics0.getProxyOpNoNamenodes());
    });
    assertThat(remoteException.getMessage()).contains(exceptionMessage);
  }

  /**
   * When failover occurs, the router may record that the ns has no active namenode.
   * Only when the router updates the cache next time can the memory status be updated,
   * causing the router to report NoNamenodesAvailableException for a long time.
   */
  @Test
  public void testNoNamenodesAvailableLongTimeWhenNsFailover() throws Exception {
    setupCluster(false, true);
    transitionClusterNSToStandby(cluster);
    for (RouterContext routerContext : cluster.getRouters()) {
      // Manually trigger the heartbeat
      Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
              .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
      // Update service cache
      routerContext.getRouter().getStateStore().refreshCaches(true);
    }
    // Record the time after the router first updated the cache
    long firstLoadTime = Time.now();
    List<MiniRouterDFSCluster.NamenodeContext> namenodes = cluster.getNamenodes();

    // Make sure all namenodes are in standby state
    for (MiniRouterDFSCluster.NamenodeContext namenodeContext : namenodes) {
      assertEquals(STANDBY.ordinal(), namenodeContext.getNamenode().getNameNodeState());
    }

    Configuration conf = cluster.getRouterClientConf();
    // Set dfs.client.failover.random.order false, to pick 1st router at first
    conf.setBoolean("dfs.client.failover.random.order", false);

    DFSClient routerClient = new DFSClient(new URI("hdfs://fed"), conf);

    for (RouterContext routerContext : cluster.getRouters()) {
      // Get the second namenode in the router cache and make it active
      List<? extends FederationNamenodeContext> ns0 = routerContext.getRouter()
              .getNamenodeResolver()
              .getNamenodesForNameserviceId("ns0", false);

      String nsId = ns0.get(1).getNamenodeId();
      cluster.switchToActive("ns0", nsId);
      // Manually trigger the heartbeat, but the router does not manually load the cache
      Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
              .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
      assertEquals(ACTIVE.ordinal(),
              cluster.getNamenode("ns0", nsId).getNamenode().getNameNodeState());
    }

    // Get router0 metrics
    FederationRPCMetrics rpcMetrics0 = cluster.getRouters().get(0)
            .getRouter().getRpcServer().getRPCMetrics();
    // Original failures
    long originalRouter0Failures = rpcMetrics0.getProxyOpNoNamenodes();

    /*
     * At this time, the router has recorded 2 standby namenodes in memory,
     * and the first accessed namenode is indeed standby,
     * then an NoNamenodesAvailableException will be reported for the first access,
     * and the next access will be successful.
     */
    routerClient.getFileInfo("/");
    long successReadTime = Time.now();
    assertEquals(originalRouter0Failures + 1, rpcMetrics0.getProxyOpNoNamenodes());

    /*
     * access the active namenode without waiting for the router to update the cache,
     * even if there are 2 standby states recorded in the router memory.
     */
    assertTrue(successReadTime - firstLoadTime < cluster.getCacheFlushInterval());
  }


  @Test
  public void testAsyncCallerPoolMetrics() throws Exception {
    setupCluster(true, false);
    simulateSlowNamenode(cluster.getCluster().getNameNode(0), 2);
    final ObjectMapper objectMapper = new ObjectMapper();

    // Set only one router to make test easier
    cluster.getRouters().remove(1);
    FederationRPCMetrics metrics = cluster.getRouters().get(0).getRouter()
        .getRpcServer().getRPCMetrics();

    // No active connection initially
    Map<String, Integer> result = objectMapper
        .readValue(metrics.getAsyncCallerPool(), Map.class);
    assertEquals(0, result.get("active").intValue());
    assertEquals(0, result.get("total").intValue());
    assertEquals(4, result.get("max").intValue());

    ExecutorService exec = Executors.newSingleThreadExecutor();

    try {
      // Run a client request to create an active connection
      exec.submit(() -> {
        DFSClient routerClient = null;
        try {
          routerClient = new DFSClient(new URI("hdfs://fed"),
              cluster.getRouterClientConf());
          String clientName = routerClient.getClientName();
          ClientProtocol routerProto = routerClient.getNamenode();
          routerProto.renewLease(clientName, null);
        } catch (Exception e) {
          fail("Client request failed: " + e);
        } finally {
          if (routerClient != null) {
            try {
              routerClient.close();
            } catch (IOException e) {
              LOG.error("Cannot close the client");
            }
          }
        }
      });

      // Wait for client request to be active
      GenericTestUtils.waitFor(() -> {
        try {
          Map<String, Integer> newResult = objectMapper.readValue(
              metrics.getAsyncCallerPool(), Map.class);
          if (newResult.get("active") != 1) {
            return false;
          }
          if (newResult.get("max") != 4) {
            return false;
          }
          int total = newResult.get("total");
          // "total" is dynamic
          return total >= 1 && total <= 4;
        } catch (Exception e) {
          LOG.error("Not able to parse metrics result: " + e);
        }
        return false;
      }, 100, 2000);
    } finally {
      exec.shutdown();
    }
  }
}
