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

package org.apache.hadoop.ha;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.reset;

import java.util.Collections;
import java.util.UUID;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.ha.ActiveStandbyElector.State;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.AdditionalMatchers;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.slf4j.event.Level;

/**
 * Test for {@link ActiveStandbyElector} using real zookeeper.
 */
public class TestActiveStandbyElectorRealZK extends ClientBaseWithFixes {
  static final int NUM_ELECTORS = 2;
  
  static {
    GenericTestUtils.setLogLevel(ActiveStandbyElector.LOG, Level.TRACE);
  }
  
  static final String PARENT_DIR = "/" + UUID.randomUUID();

  ActiveStandbyElector[] electors = new ActiveStandbyElector[NUM_ELECTORS];
  private byte[][] appDatas = new byte[NUM_ELECTORS][];
  private ActiveStandbyElectorCallback[] cbs =
      new ActiveStandbyElectorCallback[NUM_ELECTORS];
  private ZooKeeperServer zkServer;

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    zkServer = getServer(serverFactory);

    for (int i = 0; i < NUM_ELECTORS; i++) {
      cbs[i] = mock(ActiveStandbyElectorCallback.class);
      appDatas[i] = Ints.toByteArray(i);
      electors[i] = new ActiveStandbyElector(hostPort, 5000, PARENT_DIR,
          Ids.OPEN_ACL_UNSAFE, Collections.<ZKAuthInfo> emptyList(), cbs[i],
          CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT, null);
    }
  }
  
  private void checkFatalsAndReset() throws Exception {
    for (int i = 0; i < NUM_ELECTORS; i++) {
      verify(cbs[i], never()).notifyFatalError(
          anyString());
      reset(cbs[i]);
    }
  }

  /**
   * the test creates 2 electors which try to become active using a real
   * zookeeper server. It verifies that 1 becomes active and 1 becomes standby.
   * Upon becoming active the leader quits election and the test verifies that
   * the standby now becomes active.
   */
  @Test
  @Timeout(value = 20)
  public void testActiveStandbyTransition() throws Exception {
    LOG.info("starting test with parentDir:" + PARENT_DIR);

    assertFalse(electors[0].parentZNodeExists());
    electors[0].ensureParentZNode();
    assertTrue(electors[0].parentZNodeExists());

    // First elector joins election, becomes active.
    electors[0].joinElection(appDatas[0]);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zkServer, PARENT_DIR, appDatas[0]);
    verify(cbs[0], timeout(1000)).becomeActive();
    checkFatalsAndReset();

    // Second elector joins election, becomes standby.
    electors[1].joinElection(appDatas[1]);
    verify(cbs[1], timeout(1000)).becomeStandby();
    checkFatalsAndReset();
    
    // First elector quits, second one should become active
    electors[0].quitElection(true);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zkServer, PARENT_DIR, appDatas[1]);
    verify(cbs[1], timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    // First one rejoins, becomes standby, second one stays active
    electors[0].joinElection(appDatas[0]);
    verify(cbs[0], timeout(1000)).becomeStandby();
    checkFatalsAndReset();
    
    // Second one expires, first one becomes active
    electors[1].preventSessionReestablishmentForTests();
    try {
      zkServer.closeSession(electors[1].getZKSessionIdForTests());
      
      ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
          zkServer, PARENT_DIR, appDatas[0]);
      verify(cbs[1], timeout(1000)).enterNeutralMode();
      verify(cbs[0], timeout(1000)).fenceOldActive(
          AdditionalMatchers.aryEq(appDatas[1]));
      verify(cbs[0], timeout(1000)).becomeActive();
    } finally {
      electors[1].allowSessionReestablishmentForTests();
    }
    
    // Second one eventually reconnects and becomes standby
    verify(cbs[1], timeout(5000)).becomeStandby();
    checkFatalsAndReset();
    
    // First one expires, second one should become active
    electors[0].preventSessionReestablishmentForTests();
    try {
      zkServer.closeSession(electors[0].getZKSessionIdForTests());
      
      ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
          zkServer, PARENT_DIR, appDatas[1]);
      verify(cbs[0], timeout(1000)).enterNeutralMode();
      verify(cbs[1], timeout(1000)).fenceOldActive(
          AdditionalMatchers.aryEq(appDatas[0]));
      verify(cbs[1], timeout(1000)).becomeActive();
    } finally {
      electors[0].allowSessionReestablishmentForTests();
    }
    
    checkFatalsAndReset();
  }
  
  @Test
  @Timeout(value = 15)
  public void testHandleSessionExpiration() throws Exception {
    ActiveStandbyElectorCallback cb = cbs[0];
    byte[] appData = appDatas[0];
    ActiveStandbyElector elector = electors[0];
    
    // Let the first elector become active
    elector.ensureParentZNode();
    elector.joinElection(appData);
    ZooKeeperServer zks = getServer(serverFactory);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appData);
    verify(cb, timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    LOG.info("========================== Expiring session");
    zks.closeSession(elector.getZKSessionIdForTests());

    // Should enter neutral mode when disconnected
    verify(cb, timeout(1000)).enterNeutralMode();

    // Should re-join the election and regain active
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appData);
    verify(cb, timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    LOG.info("========================== Quitting election");
    elector.quitElection(false);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    // Double check that we don't accidentally re-join the election
    // due to receiving the "expired" event.
    Thread.sleep(1000);
    verify(cb, never()).becomeActive();
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    checkFatalsAndReset();
  }
  
  @Test
  @Timeout(value = 15)
  public void testHandleSessionExpirationOfStandby() throws Exception {
    // Let elector 0 be active
    electors[0].ensureParentZNode();
    electors[0].joinElection(appDatas[0]);
    ZooKeeperServer zks = getServer(serverFactory);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appDatas[0]);
    verify(cbs[0], timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    // Let elector 1 be standby
    electors[1].joinElection(appDatas[1]);
    ActiveStandbyElectorTestUtil.waitForElectorState(null, electors[1],
        State.STANDBY);
    
    LOG.info("========================== Expiring standby's session");
    zks.closeSession(electors[1].getZKSessionIdForTests());

    // Should enter neutral mode when disconnected
    verify(cbs[1], timeout(1000)).enterNeutralMode();

    // Should re-join the election and go back to STANDBY
    ActiveStandbyElectorTestUtil.waitForElectorState(null, electors[1],
        State.STANDBY);
    checkFatalsAndReset();
    
    LOG.info("========================== Quitting election");
    electors[1].quitElection(false);

    // Double check that we don't accidentally re-join the election
    // by quitting elector 0 and ensuring elector 1 doesn't become active
    electors[0].quitElection(false);
    
    // due to receiving the "expired" event.
    Thread.sleep(1000);
    verify(cbs[1], never()).becomeActive();
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    checkFatalsAndReset();
  }

  @Test
  @Timeout(value = 15)
  public void testDontJoinElectionOnDisconnectAndReconnect() throws Exception {
    electors[0].ensureParentZNode();

    stopServer();
    ActiveStandbyElectorTestUtil.waitForElectorState(
        null, electors[0], State.NEUTRAL);
    startServer();
    waitForServerUp(hostPort, CONNECTION_TIMEOUT);
    // Have to sleep to allow time for the clients to reconnect.
    Thread.sleep(2000);
    verify(cbs[0], never()).becomeActive();
    verify(cbs[1], never()).becomeActive();
    checkFatalsAndReset();
  }

  /**
   * Test to verify that proper ZooKeeper ACLs can be updated on
   * ActiveStandbyElector's parent znode.
   */
  @Test
  @Timeout(value = 15)
  public void testSetZooKeeperACLsOnParentZnodeName()
      throws Exception {
    ActiveStandbyElectorCallback cb =
        mock(ActiveStandbyElectorCallback.class);
    ActiveStandbyElector elector =
        new ActiveStandbyElector(hostPort, 5000, PARENT_DIR,
            Ids.READ_ACL_UNSAFE, Collections.<ZKAuthInfo>emptyList(), cb,
            CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT, null);

    // Simulate the case by pre-creating znode 'parentZnodeName'. Then updates
    // znode's data so that data version will be increased to 1. Here znode's
    // aversion is 0.
    ZooKeeper otherClient = createClient();
    otherClient.create(PARENT_DIR, "sample1".getBytes(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    otherClient.setData(PARENT_DIR, "sample2".getBytes(), -1);
    otherClient.close();

    elector.ensureParentZNode();
  }
}
