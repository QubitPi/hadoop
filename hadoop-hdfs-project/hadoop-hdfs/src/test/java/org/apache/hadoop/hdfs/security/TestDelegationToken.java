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

package org.apache.hadoop.hdfs.security;


import static org.apache.hadoop.security.authentication.util.KerberosName.setRules;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDelegationToken {
  private MiniDFSCluster cluster;
  private DelegationTokenSecretManager dtSecretManager;
  private Configuration config;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDelegationToken.class);

  @BeforeEach
  public void setUp() throws Exception {
    config = new HdfsConfiguration();
    config.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
    config.setLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    config.set("hadoop.security.auth_to_local",
        "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");
    FileSystem.setDefaultUri(config, "hdfs://localhost:" + "0");
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(0).build();
    cluster.waitActive();
    dtSecretManager = NameNodeAdapter.getDtSecretManager(
        cluster.getNamesystem());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private Token<DelegationTokenIdentifier> generateDelegationToken(
      String owner, String renewer) {
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        owner), new Text(renewer), null);
    return new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);
  }

  @Test
  public void testDelegationTokenSecretManager() throws Exception {
    Token<DelegationTokenIdentifier> token = generateDelegationToken(
        "SomeUser", "JobTracker");
    // Fake renewer should not be able to renew
    try {
      dtSecretManager.renewToken(token, "FakeRenewer");
      fail("should have failed");
    } catch (AccessControlException ace) {
      // PASS
    }
    dtSecretManager.renewToken(token, "JobTracker");
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    byte[] tokenId = token.getIdentifier();
    identifier.readFields(new DataInputStream(
        new ByteArrayInputStream(tokenId)));
    assertTrue(null != dtSecretManager.retrievePassword(identifier));
    LOG.info("Sleep to expire the token");
    Thread.sleep(6000);
    //Token should be expired
    try {
      dtSecretManager.retrievePassword(identifier);
      //Should not come here
      fail("Token should have expired");
    } catch (InvalidToken e) {
      //Success
    }
    dtSecretManager.renewToken(token, "JobTracker");
    LOG.info("Sleep beyond the max lifetime");
    Thread.sleep(5000);
    try {
      dtSecretManager.renewToken(token, "JobTracker");
      fail("should have been expired");
    } catch (InvalidToken it) {
      // PASS
    }
  }

  @Test
  public void testCancelDelegationToken() throws Exception {
    Token<DelegationTokenIdentifier> token = generateDelegationToken(
        "SomeUser", "JobTracker");
    //Fake renewer should not be able to renew
    try {
      dtSecretManager.cancelToken(token, "FakeCanceller");
      fail("should have failed");
    } catch (AccessControlException ace) {
      // PASS
    }
    dtSecretManager.cancelToken(token, "JobTracker");
    try {
      dtSecretManager.renewToken(token, "JobTracker");
      fail("should have failed");
    } catch (InvalidToken it) {
      // PASS
    }
  }

  @Test
  public void testDelegationTokenMetrics() throws Exception {
    FSNamesystem namesystem = cluster.getNamesystem();
    // should start with no token
    assertEquals(0, namesystem.getCurrentTokensCount());

    // get token
    Token<DelegationTokenIdentifier> token = generateDelegationToken(
        "SomeUser", "JobTracker");
    assertEquals(1, namesystem.getCurrentTokensCount());

    // Renew token shouldn't change the count of tokens
    dtSecretManager.renewToken(token, "JobTracker");
    assertEquals(1, namesystem.getCurrentTokensCount());

    // Cancel token should remove the token from memory
    dtSecretManager.cancelToken(token, "JobTracker");
    assertEquals(0, namesystem.getCurrentTokensCount());
  }

  @Test
  public void testAddDelegationTokensDFSApi() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("JobTracker");
    DistributedFileSystem dfs = cluster.getFileSystem();
    Credentials creds = new Credentials();
    final Token<?> tokens[] = dfs.addDelegationTokens("JobTracker", creds);
    assertEquals(1, tokens.length);
    assertEquals(1, creds.numberOfTokens());
    checkTokenIdentifier(ugi, tokens[0]);

    final Token<?> tokens2[] = dfs.addDelegationTokens("JobTracker", creds);
    assertEquals(0, tokens2.length); // already have token
    assertEquals(1, creds.numberOfTokens());
  }

  @Test
  public void testDelegationTokenWebHdfsApi() throws Exception {
    GenericTestUtils.setLogLevel(NamenodeWebHdfsMethods.LOG, Level.TRACE);
    final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
        + config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    //get file system as JobTracker
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "JobTracker", new String[]{"user"});
    final WebHdfsFileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<WebHdfsFileSystem>() {
          @Override
          public WebHdfsFileSystem run() throws Exception {
            return (WebHdfsFileSystem) FileSystem.get(new URI(uri), config);
          }
        });

    { //test addDelegationTokens(..)
      Credentials creds = new Credentials();
      final Token<?> tokens[] = webhdfs.addDelegationTokens("JobTracker", creds);
      assertEquals(1, tokens.length);
      assertEquals(1, creds.numberOfTokens());
      assertSame(tokens[0], creds.getAllTokens().iterator().next());
      checkTokenIdentifier(ugi, tokens[0]);
      final Token<?> tokens2[] = webhdfs.addDelegationTokens("JobTracker", creds);
      assertEquals(0, tokens2.length);
    }
  }

  @Test
  public void testDelegationTokenWithDoAs() throws Exception {
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final Credentials creds = new Credentials();
    final Token<?> tokens[] = dfs.addDelegationTokens("JobTracker", creds);
    assertEquals(1, tokens.length);
    @SuppressWarnings("unchecked") final Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) tokens[0];
    final UserGroupInformation longUgi = UserGroupInformation
        .createRemoteUser("JobTracker/foo.com@FOO.COM");
    final UserGroupInformation shortUgi = UserGroupInformation
        .createRemoteUser("JobTracker");
    longUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        try {
          token.renew(config);
        } catch (Exception e) {
          fail("Could not renew delegation token for user " + longUgi);
        }
        return null;
      }
    });
    shortUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        token.renew(config);
        return null;
      }
    });
    longUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        try {
          token.cancel(config);
        } catch (Exception e) {
          fail("Could not cancel delegation token for user " + longUgi);
        }
        return null;
      }
    });
  }

  @Test
  public void testDelegationTokenUgi() throws Exception {
    final DistributedFileSystem dfs = cluster.getFileSystem();
    Token<?>[] tokens = dfs.addDelegationTokens("renewer", null);
    assertEquals(1, tokens.length);
    Token<?> token1 = tokens[0];
    DelegationTokenIdentifier ident =
        (DelegationTokenIdentifier) token1.decodeIdentifier();
    UserGroupInformation expectedUgi = ident.getUser();

    // get 2 new instances (clones) of the identifier, query their ugi
    // twice each, all ugi instances should be equivalent
    for (int i = 0; i < 2; i++) {
      DelegationTokenIdentifier identClone =
          (DelegationTokenIdentifier) token1.decodeIdentifier();
      assertEquals(ident, identClone);
      assertNotSame(ident, identClone);
      assertSame(expectedUgi, identClone.getUser());
      assertSame(expectedUgi, identClone.getUser());
    }

    // a new token must decode to a different ugi instance than the first token
    tokens = dfs.addDelegationTokens("renewer", null);
    assertEquals(1, tokens.length);
    Token<?> token2 = tokens[0];
    assertNotEquals(token1, token2);
    assertNotSame(expectedUgi, token2.decodeIdentifier().getUser());
  }

  /**
   * Test that the delegation token secret manager only runs when the
   * NN is out of safe mode. This is because the secret manager
   * has to log to the edit log, which should not be written in
   * safe mode. Regression test for HDFS-2579.
   */
  @Test
  public void testDTManagerInSafeMode() throws Exception {
    cluster.startDataNodes(config, 1, true, StartupOption.REGULAR, null);
    FileSystem fs = cluster.getFileSystem();
    for (int i = 0; i < 5; i++) {
      DFSTestUtil.createFile(fs, new Path("/test-" + i), 100, (short) 1, 1L);
    }
    cluster.getConfiguration(0).setInt(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY, 500);
    cluster.getConfiguration(0).setInt(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 30000);
    cluster.setWaitSafeMode(false);
    cluster.restartNameNode();
    NameNode nn = cluster.getNameNode();
    assertTrue(nn.isInSafeMode());
    DelegationTokenSecretManager sm =
        NameNodeAdapter.getDtSecretManager(nn.getNamesystem());
    assertFalse(sm.isRunning(), "Secret manager should not run in safe mode");

    NameNodeAdapter.leaveSafeMode(nn);
    assertTrue(sm.isRunning(), "Secret manager should start when safe mode is exited");

    LOG.info("========= entering safemode again");

    NameNodeAdapter.enterSafeMode(nn, false);
    assertFalse(sm.isRunning(), "Secret manager should stop again when safe mode " +
        "is manually entered");

    // Set the cluster to leave safemode quickly on its own.
    cluster.getConfiguration(0).setInt(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
    cluster.setWaitSafeMode(true);
    cluster.restartNameNode();
    nn = cluster.getNameNode();
    sm = NameNodeAdapter.getDtSecretManager(nn.getNamesystem());

    assertFalse(nn.isInSafeMode());
    assertTrue(sm.isRunning());
  }

  @SuppressWarnings("unchecked")
  private void checkTokenIdentifier(UserGroupInformation ugi, final Token<?> token)
      throws Exception {
    assertNotNull(token);
    // should be able to use token.decodeIdentifier() but webhdfs isn't
    // registered with the service loader for token decoding
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    byte[] tokenId = token.getIdentifier();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(tokenId));
    try {
      identifier.readFields(in);
    } finally {
      in.close();
    }
    assertNotNull(identifier);
    LOG.info("A valid token should have non-null password, and should be renewed successfully");
    assertTrue(null != dtSecretManager.retrievePassword(identifier));
    dtSecretManager.renewToken((Token<DelegationTokenIdentifier>) token, "JobTracker");
    ugi.doAs(
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            token.renew(config);
            token.cancel(config);
            return null;
          }
        });
  }

  @Test
  public void testDelegationTokenIdentifierToString() throws Exception {
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        "SomeUser"), new Text("JobTracker"), null);
    assertEquals("HDFS_DELEGATION_TOKEN token 0" +
            " for SomeUser with renewer JobTracker",
        dtId.toStringStable());
  }

  @Test
  public void testLogExpireTokensWhenChangeRules() throws IOException {
    setRules("RULE:[2:$1@$0](SomeUser.*)s/.*/SomeUser/");
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(
        new Text("SomeUser/HOST@EXAMPLE.COM"),
        new Text("SomeUser/HOST@EXAMPLE.COM"),
        new Text("SomeUser/HOST@EXAMPLE.COM"));
    Set<DelegationTokenIdentifier> expiredTokens = new HashSet();
    expiredTokens.add(dtId);
    setRules("RULE:[2:$1@$0](OtherUser.*)s/.*/OtherUser/");
    //rules was modified, causing the existing tokens
    //(May be loaded from other storage systems like zookeeper) to fail to match the kerberos rules,
    //return an exception that cannot be handled
    new AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>(10 * 1000, 10 * 1000,
        10 * 1000, 10 * 1000) {
      @Override
      public DelegationTokenIdentifier createIdentifier() {
        return null;
      }

      public void logExpireTokens(Collection<DelegationTokenIdentifier> expiredTokens)
          throws IOException {
        super.logExpireTokens(expiredTokens);
      }
    }.logExpireTokens(expiredTokens);
  }
}
