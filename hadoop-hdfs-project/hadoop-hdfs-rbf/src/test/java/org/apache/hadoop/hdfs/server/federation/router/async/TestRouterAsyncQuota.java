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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.ipc.CallerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRouterAsyncQuota {
  private static Configuration routerConf;
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;
  private static String ns0;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFs;
  private RouterRpcServer routerRpcServer;
  private AsyncQuota asyncQuota;

  private final String testfilePath = "/testdir/testAsyncQuota.file";

  @BeforeAll
  public static void setUpCluster() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 1, 2,
        DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);
    cluster.setNumDatanodesPerNameservice(3);
    cluster.setRacks(
        new String[] {"/rack1", "/rack2", "/rack3"});
    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    // Start routers with only an RPC service
    routerConf = new RouterConfigBuilder()
        .rpc()
        .quota(true)
        .build();

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 1);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY, 1);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_RESPONDER_COUNT_KEY, 1);
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    routerConf.setBoolean(DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY, true);
    cluster.addRouterOverrides(routerConf);
    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    cluster.waitActiveNamespaces();
    ns0 = cluster.getNameservices().get(0);
  }

  @AfterAll
  public static void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    router = cluster.getRandomRouter();
    routerFs = router.getFileSystem();
    routerRpcServer = router.getRouterRpcServer();
    routerRpcServer.initAsyncThreadPools(routerConf);
    RouterAsyncRpcClient asyncRpcClient = new RouterAsyncRpcClient(
        routerConf, router.getRouter(), routerRpcServer.getNamenodeResolver(),
        routerRpcServer.getRPCMonitor(),
        routerRpcServer.getRouterStateIdContext());
    RouterRpcServer spy = Mockito.spy(routerRpcServer);
    Mockito.when(spy.getRPCClient()).thenReturn(asyncRpcClient);
    asyncQuota = new AsyncQuota(router.getRouter(), spy);

    // Create mock locations
    MockResolver resolver = (MockResolver) router.getRouter().getSubclusterResolver();
    resolver.addLocation("/", ns0, "/");
    FsPermission permission = new FsPermission("705");
    routerFs.mkdirs(new Path("/testdir"), permission);
    FSDataOutputStream fsDataOutputStream = routerFs.create(
        new Path(testfilePath), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @AfterEach
  public void tearDown() throws IOException {
    // clear client context
    CallerContext.setCurrent(null);
    boolean delete = routerFs.delete(new Path("/testdir"));
    assertTrue(delete);
    if (routerFs != null) {
      routerFs.close();
    }
  }

  @Test
  public void testRouterAsyncGetQuotaUsage() throws Exception {
    asyncQuota.getQuotaUsage("/testdir");
    QuotaUsage quotaUsage = syncReturn(QuotaUsage.class);
    // 3-replication.
    Assertions.assertEquals(3 * 1024, quotaUsage.getSpaceConsumed());
    // We have one directory and one file.
    Assertions.assertEquals(2, quotaUsage.getFileAndDirectoryCount());
  }

  @Test
  public void testRouterAsyncSetQuotaUsage() throws Exception {
    asyncQuota.setQuota("/testdir", Long.MAX_VALUE, 8096, StorageType.DISK, false);
    syncReturn(void.class);
    asyncQuota.getQuotaUsage("/testdir");
    QuotaUsage quotaUsage = syncReturn(QuotaUsage.class);
    Assertions.assertEquals(8096, quotaUsage.getTypeQuota(StorageType.DISK));
  }
}
