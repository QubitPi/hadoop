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

package org.apache.hadoop.security.token.delegation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.ProtectACLCreateModeStatPathAndBytesable;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(300)
public class TestZKDelegationTokenSecretManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestZKDelegationTokenSecretManager.class);

  protected static final int TEST_RETRIES = 2;

  protected static final int RETRY_COUNT = 5;

  protected static final int RETRY_WAIT = 1000;

  protected static final long DAY_IN_SECS = 86400;

  protected TestingServer zkServer;

  @BeforeEach
  public void setup() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (zkServer != null) {
      zkServer.close();
    }
  }

  protected Configuration getSecretConf(String connectString) {
   Configuration conf = new Configuration();
   conf.setBoolean(DelegationTokenManager.ENABLE_ZK_KEY, true);
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_CONNECTION_STRING, connectString);
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH, "testPath");
   conf.set(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_AUTH_TYPE, "none");
   conf.setLong(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT, 100);
   conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.MAX_LIFETIME, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.RENEW_INTERVAL, DAY_IN_SECS);
   conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, DAY_IN_SECS);
   return conf;
  }

  @SuppressWarnings("unchecked")
  @Test
  @Order(1)
  public void testMultiNodeOperations() throws Exception {
      testMultiNodeOperationsImpl(false);
  }

  @Test
  @Order(2)
  public void testMultiNodeOperationsWithZeroRetry() throws Exception {
      testMultiNodeOperationsImpl(true);
  }

  public void testMultiNodeOperationsImpl(boolean setZeroRetry) throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
      DelegationTokenManager tm1, tm2 = null;
      String connectString = zkServer.getConnectString();
      Configuration conf = getSecretConf(connectString);
      if (setZeroRetry) {
          conf.setInt(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_NUM_RETRIES, 0);
      }
      tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.init();
      tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.init();

      Token<DelegationTokenIdentifier> token =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "foo");
      assertNotNull(token);
      tm2.verifyToken(token);
      tm2.renewToken(token, "foo");
      tm1.verifyToken(token);
      tm1.cancelToken(token, "foo");
      try {
        verifyTokenFail(tm2, token);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }

      token = (Token<DelegationTokenIdentifier>) tm2.createToken(
          UserGroupInformation.getCurrentUser(), "bar");
      assertNotNull(token);
      tm1.verifyToken(token);
      tm1.renewToken(token, "bar");
      tm2.verifyToken(token);
      tm2.cancelToken(token, "bar");
      try {
        verifyTokenFail(tm1, token);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }
      verifyDestroy(tm1, conf);
      verifyDestroy(tm2, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  @Order(5)
  public void testNodeUpAferAWhile() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
      String connectString = zkServer.getConnectString();
      Configuration conf = getSecretConf(connectString);
      DelegationTokenManager tm1 = new DelegationTokenManager(conf, new Text("bla"));
      tm1.init();
      Token<DelegationTokenIdentifier> token1 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "foo");
      assertNotNull(token1);
      Token<DelegationTokenIdentifier> token2 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "bar");
      assertNotNull(token2);
      Token<DelegationTokenIdentifier> token3 =
          (Token<DelegationTokenIdentifier>) tm1.createToken(
              UserGroupInformation.getCurrentUser(), "boo");
      assertNotNull(token3);

      tm1.verifyToken(token1);
      tm1.verifyToken(token2);
      tm1.verifyToken(token3);

      // Cancel one token
      tm1.cancelToken(token1, "foo");

      // Start second node after some time..
      Thread.sleep(1000);
      DelegationTokenManager tm2 = new DelegationTokenManager(conf, new Text("bla"));
      tm2.init();

      tm2.verifyToken(token2);
      tm2.verifyToken(token3);
      try {
        verifyTokenFail(tm2, token1);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }

      // Create a new token thru the new ZKDTSM
      Token<DelegationTokenIdentifier> token4 =
          (Token<DelegationTokenIdentifier>) tm2.createToken(
              UserGroupInformation.getCurrentUser(), "xyz");
      assertNotNull(token4);
      tm2.verifyToken(token4);
      tm1.verifyToken(token4);

      // Bring down tm2
      verifyDestroy(tm2, conf);

      // Start third node after some time..
      Thread.sleep(1000);
      DelegationTokenManager tm3 = new DelegationTokenManager(conf, new Text("bla"));
      tm3.init();

      tm3.verifyToken(token2);
      tm3.verifyToken(token3);
      tm3.verifyToken(token4);
      try {
        verifyTokenFail(tm3, token1);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        // Ignore
      }

      verifyDestroy(tm3, conf);
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  @Order(6)
  public void testMultiNodeCompeteForSeqNum() throws Exception {
    DelegationTokenManager tm1, tm2 = null;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    conf.setInt(
        ZKDelegationTokenSecretManager.ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE, 1000);
    tm1 = new DelegationTokenManager(conf, new Text("bla"));
    tm1.init();

    Token<DelegationTokenIdentifier> token1 =
        (Token<DelegationTokenIdentifier>) tm1.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    assertNotNull(token1);
    AbstractDelegationTokenIdentifier id1 =
        tm1.getDelegationTokenSecretManager().decodeTokenIdentifier(token1);
    assertEquals(1, id1.getSequenceNumber(), "Token seq should be the same");
    Token<DelegationTokenIdentifier> token2 =
        (Token<DelegationTokenIdentifier>) tm1.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    assertNotNull(token2);
    AbstractDelegationTokenIdentifier id2 =
        tm1.getDelegationTokenSecretManager().decodeTokenIdentifier(token2);
    assertEquals(2, id2.getSequenceNumber(), "Token seq should be the same");

    tm2 = new DelegationTokenManager(conf, new Text("bla"));
    tm2.init();

    Token<DelegationTokenIdentifier> token3 =
        (Token<DelegationTokenIdentifier>) tm2.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    assertNotNull(token3);
    AbstractDelegationTokenIdentifier id3 =
        tm2.getDelegationTokenSecretManager().decodeTokenIdentifier(token3);
    assertEquals(1001, id3.getSequenceNumber(), "Token seq should be the same");
    Token<DelegationTokenIdentifier> token4 =
        (Token<DelegationTokenIdentifier>) tm2.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    assertNotNull(token4);
    AbstractDelegationTokenIdentifier id4 =
        tm2.getDelegationTokenSecretManager().decodeTokenIdentifier(token4);
    assertEquals(1002, id4.getSequenceNumber(), "Token seq should be the same");

    verifyDestroy(tm1, conf);
    verifyDestroy(tm2, conf);
  }

  @SuppressWarnings("unchecked")
  @Test
  @Order(3)
  public void testRenewTokenSingleManager() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
      DelegationTokenManager tm1 = null;
      String connectString = zkServer.getConnectString();
      Configuration conf = getSecretConf(connectString);
      tm1 = new DelegationTokenManager(conf, new Text("foo"));
      tm1.init();

      Token<DelegationTokenIdentifier> token =
          (Token<DelegationTokenIdentifier>)
          tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
      assertNotNull(token);
      tm1.renewToken(token, "foo");
      tm1.verifyToken(token);
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  @Order(9)
  public void testCancelTokenSingleManager() throws Exception {
    for (int i = 0; i < TEST_RETRIES; i++) {
      DelegationTokenManager tm1 = null;
      String connectString = zkServer.getConnectString();
      Configuration conf = getSecretConf(connectString);
      tm1 = new DelegationTokenManager(conf, new Text("foo"));
      tm1.init();

      Token<DelegationTokenIdentifier> token =
          (Token<DelegationTokenIdentifier>)
          tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
      assertNotNull(token);
      tm1.cancelToken(token, "foo");
      try {
        verifyTokenFail(tm1, token);
        fail("Expected InvalidToken");
      } catch (SecretManager.InvalidToken it) {
        it.printStackTrace();
      }
      verifyDestroy(tm1, conf);
    }
  }

  @SuppressWarnings("rawtypes")
  protected void verifyDestroy(DelegationTokenManager tm, Configuration conf)
      throws Exception {
    tm.destroy();
    // wait for the pool to terminate
    long timeout =
        conf.getLong(
            ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT,
            ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT_DEFAULT);
    Thread.sleep(timeout * 3);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  @Order(7)
  public void testStopThreads() throws Exception {
    DelegationTokenManager tm1 = null;
    String connectString = zkServer.getConnectString();

    // let's make the update interval short and the shutdown interval
    // comparatively longer, so if the update thread runs after shutdown,
    // it will cause an error.
    final long updateIntervalSeconds = 1;
    final long shutdownTimeoutMillis = updateIntervalSeconds * 1000 * 5;
    Configuration conf = getSecretConf(connectString);
    conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, updateIntervalSeconds);
    conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, updateIntervalSeconds);
    conf.setLong(DelegationTokenManager.RENEW_INTERVAL, updateIntervalSeconds);

    conf.setLong(ZKDelegationTokenSecretManager.ZK_DTSM_ZK_SHUTDOWN_TIMEOUT, shutdownTimeoutMillis);
    tm1 = new DelegationTokenManager(conf, new Text("foo"));
    tm1.init();

    Token<DelegationTokenIdentifier> token =
      (Token<DelegationTokenIdentifier>)
    tm1.createToken(UserGroupInformation.getCurrentUser(), "foo");
    assertNotNull(token);
    tm1.destroy();
  }

  @Test
  public void testACLs() throws Exception {
    DelegationTokenManager tm1;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    String userPass = "myuser:mypass";
    final ACL digestACL = new ACL(ZooDefs.Perms.ALL, new Id("digest",
      DigestAuthenticationProvider.generateDigest(userPass)));
    ACLProvider digestAclProvider = new ACLProvider() {
      @Override
      public List<ACL> getAclForPath(String path) { return getDefaultAcl(); }

      @Override
      public List<ACL> getDefaultAcl() {
        List<ACL> ret = new ArrayList<ACL>();
        ret.add(digestACL);
        return ret;
      }
    };

    CuratorFramework curatorFramework =
      CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(retryPolicy)
        .aclProvider(digestAclProvider)
        .authorization("digest", userPass.getBytes(StandardCharsets.UTF_8))
        .build();
    curatorFramework.start();
    ZKDelegationTokenSecretManager.setCurator(curatorFramework);
    tm1 = new DelegationTokenManager(conf, new Text("bla"));
    tm1.init();

    // check ACL
    String workingPath = conf.get(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH);
    verifyACL(curatorFramework, "/" + workingPath, digestACL);

    tm1.destroy();
    ZKDelegationTokenSecretManager.setCurator(null);
    curatorFramework.close();
  }

  private void verifyACL(CuratorFramework curatorFramework,
      String path, ACL expectedACL) throws Exception {
    List<ACL> acls = curatorFramework.getACL().forPath(path);
    assertEquals(1, acls.size());
    assertEquals(expectedACL, acls.get(0));
  }

  // Since it is possible that there can be a delay for the cancel token message
  // initiated by one node to reach another node.. The second node can ofcourse
  // verify with ZK directly if the token that needs verification has been
  // cancelled but.. that would mean having to make an RPC call for every
  // verification request.
  // Thus, the eventual consistency tradef-off should be acceptable here...
  protected void verifyTokenFail(DelegationTokenManager tm,
      Token<DelegationTokenIdentifier> token) throws IOException,
      InterruptedException {
    verifyTokenFailWithRetry(tm, token, RETRY_COUNT);
  }

  private void verifyTokenFailWithRetry(DelegationTokenManager tm,
      Token<DelegationTokenIdentifier> token, int retryCount)
      throws IOException, InterruptedException {
    try {
      tm.verifyToken(token);
    } catch (SecretManager.InvalidToken er) {
      throw er;
    }
    if (retryCount > 0) {
      Thread.sleep(RETRY_WAIT);
      verifyTokenFailWithRetry(tm, token, retryCount - 1);
    }
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  @Order(8)
  public void testNodesLoadedAfterRestart() throws Exception {
    final String connectString = zkServer.getConnectString();
    final Configuration conf = getSecretConf(connectString);
    final int removeScan = 1;
    // Set the remove scan interval to remove expired tokens
    conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, removeScan);
    // Set the update interval to trigger background thread to run. The thread
    // is hard-coded to sleep at least 5 seconds.
    conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, 5);
    // Set token expire time to 5 seconds.
    conf.setLong(DelegationTokenManager.RENEW_INTERVAL, 5);

    DelegationTokenManager tm =
        new DelegationTokenManager(conf, new Text("bla"));
    tm.init();
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) tm
            .createToken(UserGroupInformation.getCurrentUser(), "good");
    assertNotNull(token);
    Token<DelegationTokenIdentifier> cancelled =
        (Token<DelegationTokenIdentifier>) tm
            .createToken(UserGroupInformation.getCurrentUser(), "cancelled");
    assertNotNull(cancelled);
    tm.verifyToken(token);
    tm.verifyToken(cancelled);

    // Cancel one token, verify it's gone
    tm.cancelToken(cancelled, "cancelled");
    final AbstractDelegationTokenSecretManager sm =
        tm.getDelegationTokenSecretManager();
    final ZKDelegationTokenSecretManager zksm =
        (ZKDelegationTokenSecretManager) sm;
    final AbstractDelegationTokenIdentifier idCancelled =
        sm.decodeTokenIdentifier(cancelled);
    LOG.info("Waiting for the cancelled token to be removed");

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        AbstractDelegationTokenSecretManager.DelegationTokenInformation dtinfo =
            zksm.getTokenInfo(idCancelled);
        return dtinfo == null;
      }
    }, 100, 5000);

    // Fake a restart which launches a new tm
    tm.destroy();
    tm = new DelegationTokenManager(conf, new Text("bla"));
    tm.init();
    final AbstractDelegationTokenSecretManager smNew =
        tm.getDelegationTokenSecretManager();
    final ZKDelegationTokenSecretManager zksmNew =
        (ZKDelegationTokenSecretManager) smNew;

    // The cancelled token should be gone, and not loaded.
    AbstractDelegationTokenIdentifier id =
        smNew.decodeTokenIdentifier(cancelled);
    AbstractDelegationTokenSecretManager.DelegationTokenInformation dtinfo =
        zksmNew.getTokenInfo(id);
    assertNull(dtinfo, "canceled dt should be gone!");

    // The good token should be loaded on startup, and removed after expiry.
    id = smNew.decodeTokenIdentifier(token);
    dtinfo = zksmNew.getTokenInfoFromMemory(id);
    assertNotNull(dtinfo, "good dt should be in memory!");

    // Wait for the good token to expire.
    Thread.sleep(5000);
    final ZKDelegationTokenSecretManager zksm1 = zksmNew;
    final AbstractDelegationTokenIdentifier id1 = id;
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("Waiting for the expired token to be removed...");
        return zksm1.getTokenInfo(id1) == null;
      }
    }, 1000, 5000);
  }

  @Test
  public void testCreatingParentContainersIfNeeded() throws Exception {

    String connectString = zkServer.getConnectString();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    Configuration conf = getSecretConf(connectString);
    CuratorFramework curatorFramework =
        CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(retryPolicy)
        .build();
    curatorFramework.start();
    ZKDelegationTokenSecretManager.setCurator(curatorFramework);
    DelegationTokenManager tm1 = new DelegationTokenManager(conf, new Text("foo"));

    // When the init method is called,
    // the ZKDelegationTokenSecretManager#startThread method will be called,
    // and the creatingParentContainersIfNeeded will be called to create the nameSpace.
    tm1.init();

    String workingPath = "/" + conf.get(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH,
        ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT) + "/ZKDTSMRoot";

    // Check if the created NameSpace exists.
    Stat stat = curatorFramework.checkExists().forPath(workingPath);
    assertNotNull(stat);

    tm1.destroy();
    curatorFramework.close();
  }

  @Test
  @Order(4)
  public void testCreateNameSpaceRepeatedly() throws Exception {

    String connectString = zkServer.getConnectString();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    Configuration conf = getSecretConf(connectString);
    CuratorFramework curatorFramework =
        CuratorFrameworkFactory.builder().
        connectString(connectString).
        retryPolicy(retryPolicy).
        build();
    curatorFramework.start();

    String workingPath = "/" + conf.get(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH,
        ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT) + "/ZKDTSMRoot-Test";
    CreateBuilder createBuilder = curatorFramework.create();
    ProtectACLCreateModeStatPathAndBytesable<String> createModeStat =
        createBuilder.creatingParentContainersIfNeeded();
    createModeStat.forPath(workingPath);

    // Check if the created NameSpace exists.
    Stat stat = curatorFramework.checkExists().forPath(workingPath);
    assertNotNull(stat);

    // Repeated creation will throw NodeExists exception
    LambdaTestUtils.intercept(KeeperException.class,
        "KeeperErrorCode = NodeExists for "+workingPath,
        () -> createModeStat.forPath(workingPath));
  }

  @Test
  public void testMultipleInit() throws Exception {

    String connectString = zkServer.getConnectString();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    Configuration conf = getSecretConf(connectString);
    CuratorFramework curatorFramework =
        CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .retryPolicy(retryPolicy)
            .build();
    curatorFramework.start();
    ZKDelegationTokenSecretManager.setCurator(curatorFramework);

    DelegationTokenManager tm1 = new DelegationTokenManager(conf, new Text("foo"));
    DelegationTokenManager tm2 = new DelegationTokenManager(conf, new Text("bar"));
    // When the init method is called,
    // the ZKDelegationTokenSecretManager#startThread method will be called,
    // and the creatingParentContainersIfNeeded will be called to create the nameSpace.
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    Callable<Boolean> tm1Callable = () -> {
      tm1.init();
      return true;
    };
    Callable<Boolean> tm2Callable = () -> {
      tm2.init();
      return true;
    };
    List<Future<Boolean>> futures = executorService.invokeAll(
        Arrays.asList(tm1Callable, tm2Callable));
    for(Future<Boolean> future : futures) {
      assertTrue(future.get());
    }
    executorService.shutdownNow();
    assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS));
    tm1.destroy();
    tm2.destroy();

    String workingPath = "/" + conf.get(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH,
        ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT) + "/ZKDTSMRoot";

    // Check if the created NameSpace exists.
    Stat stat = curatorFramework.checkExists().forPath(workingPath);
    assertNotNull(stat);

    curatorFramework.close();
  }
}
