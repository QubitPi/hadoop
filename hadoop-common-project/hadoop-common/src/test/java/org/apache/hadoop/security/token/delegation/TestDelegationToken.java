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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.MeanStatistic;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.Assertions;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class TestDelegationToken {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDelegationToken.class);
  private static final Text KIND = new Text("MY KIND");

  public static class TestDelegationTokenIdentifier 
  extends AbstractDelegationTokenIdentifier
  implements Writable {

    public TestDelegationTokenIdentifier() {
    }

    public TestDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
      super(owner, renewer, realUser);
    }

    @Override
    public Text getKind() {
      return KIND;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out); 
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }
  
  public static class TestDelegationTokenSecretManager 
  extends AbstractDelegationTokenSecretManager<TestDelegationTokenIdentifier> {

    public boolean isStoreNewMasterKeyCalled = false;
    public boolean isRemoveStoredMasterKeyCalled = false;
    public boolean isStoreNewTokenCalled = false;
    public boolean isRemoveStoredTokenCalled = false;
    public boolean isUpdateStoredTokenCalled = false;
    public TestDelegationTokenSecretManager(long delegationKeyUpdateInterval,
                         long delegationTokenMaxLifetime,
                         long delegationTokenRenewInterval,
                         long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
            delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    @Override
    public TestDelegationTokenIdentifier createIdentifier() {
      return new TestDelegationTokenIdentifier();
    }
    
    @Override
    protected byte[] createPassword(TestDelegationTokenIdentifier t) {
      return super.createPassword(t);
    }

    @Override
    protected void storeNewMasterKey(DelegationKey key) throws IOException {
      isStoreNewMasterKeyCalled = true;
      super.storeNewMasterKey(key);
    }

    @Override
    protected void removeStoredMasterKey(DelegationKey key) {
      isRemoveStoredMasterKeyCalled = true;
      Assertions.assertFalse(key.equals(allKeys.get(currentId)));
    }

    @Override
    protected void storeNewToken(TestDelegationTokenIdentifier ident,
        long renewDate) throws IOException {
      super.storeNewToken(ident, renewDate);
      isStoreNewTokenCalled = true;
    }

    @Override
    protected void removeStoredToken(TestDelegationTokenIdentifier ident)
        throws IOException {
      super.removeStoredToken(ident);
      isRemoveStoredTokenCalled = true;
    }

    @Override
    protected void updateStoredToken(TestDelegationTokenIdentifier ident,
        long renewDate) throws IOException {
      super.updateStoredToken(ident, renewDate);
      isUpdateStoredTokenCalled = true;
    }

    public byte[] createPassword(TestDelegationTokenIdentifier t, DelegationKey key) {
      return SecretManager.createPassword(t.getBytes(), key.getKey());
    }
    
    public Map<TestDelegationTokenIdentifier, DelegationTokenInformation> getAllTokens() {
      return currentTokens;
    }
    
    public DelegationKey getKey(TestDelegationTokenIdentifier id) {
      return allKeys.get(id.getMasterKeyId());
    }
  }

  public static class TestFailureDelegationTokenSecretManager
      extends TestDelegationTokenSecretManager {
    private boolean throwError = false;
    private long errorSleepMillis;

    public TestFailureDelegationTokenSecretManager(long errorSleepMillis) {
      super(24*60*60*1000, 10*1000, 1*1000, 60*60*1000);
      this.errorSleepMillis = errorSleepMillis;
    }

    public void setThrowError(boolean throwError) {
      this.throwError = throwError;
    }

    private void sleepAndThrow() throws IOException {
      try {
        Thread.sleep(errorSleepMillis);
        throw new IOException("Test exception");
      } catch (InterruptedException e) {
      }
    }

    @Override
    protected void storeNewToken(TestDelegationTokenIdentifier ident, long renewDate)
        throws IOException {
      if (throwError) {
        sleepAndThrow();
      }
      super.storeNewToken(ident, renewDate);
    }

    @Override
    protected void removeStoredToken(TestDelegationTokenIdentifier ident) throws IOException {
      if (throwError) {
        sleepAndThrow();
      }
      super.removeStoredToken(ident);
    }

    @Override
    protected void updateStoredToken(TestDelegationTokenIdentifier ident, long renewDate)
        throws IOException {
      if (throwError) {
        sleepAndThrow();
      }
      super.updateStoredToken(ident, renewDate);
    }
  }
  
  public static class TokenSelector extends 
  AbstractDelegationTokenSelector<TestDelegationTokenIdentifier>{

    protected TokenSelector() {
      super(KIND);
    }    
  }
  
  @Test
  public void testSerialization() throws Exception {
    TestDelegationTokenIdentifier origToken = new 
                        TestDelegationTokenIdentifier(new Text("alice"), 
                                          new Text("bob"), 
                                          new Text("colin"));
    TestDelegationTokenIdentifier newToken = new TestDelegationTokenIdentifier();
    origToken.setIssueDate(123);
    origToken.setMasterKeyId(321);
    origToken.setMaxDate(314);
    origToken.setSequenceNumber(12345);
    
    // clone origToken into newToken
    DataInputBuffer inBuf = new DataInputBuffer();
    DataOutputBuffer outBuf = new DataOutputBuffer();
    origToken.write(outBuf);
    inBuf.reset(outBuf.getData(), 0, outBuf.getLength());
    newToken.readFields(inBuf);
    
    // now test the fields
    assertEquals("alice", newToken.getUser().getUserName());
    assertEquals(new Text("bob"), newToken.getRenewer());
    assertEquals("colin", newToken.getUser().getRealUser().getUserName());
    assertEquals(123, newToken.getIssueDate());
    assertEquals(321, newToken.getMasterKeyId());
    assertEquals(314, newToken.getMaxDate());
    assertEquals(12345, newToken.getSequenceNumber());
    assertEquals(origToken, newToken);
  }
  
  private Token<TestDelegationTokenIdentifier> generateDelegationToken(
      TestDelegationTokenSecretManager dtSecretManager,
      String owner, String renewer) {
    TestDelegationTokenIdentifier dtId = 
      new TestDelegationTokenIdentifier(new Text(
        owner), new Text(renewer), null);
    return new Token<TestDelegationTokenIdentifier>(dtId, dtSecretManager);
  }
  
  private void shouldThrow(PrivilegedExceptionAction<Object> action,
                           Class<? extends Throwable> except) {
    try {
      action.run();
      Assertions.fail("action did not throw " + except);
    } catch (Throwable th) {
      LOG.info("Caught an exception: ", th);
      assertEquals(except, th.getClass(), "action threw wrong exception");
    }
  }

  @Test
  public void testGetUserNullOwner() {
    TestDelegationTokenIdentifier ident =
        new TestDelegationTokenIdentifier(null, null, null);
    UserGroupInformation ugi = ident.getUser();
    assertNull(ugi);
  }
  
  @Test
  public void testGetUserWithOwner() {
    TestDelegationTokenIdentifier ident =
        new TestDelegationTokenIdentifier(new Text("owner"), null, null);
    UserGroupInformation ugi = ident.getUser();
    assertNull(ugi.getRealUser());
    assertEquals("owner", ugi.getUserName());
    assertEquals(AuthenticationMethod.TOKEN, ugi.getAuthenticationMethod());
  }

  @Test
  public void testGetUserWithOwnerEqualsReal() {
    Text owner = new Text("owner");
    TestDelegationTokenIdentifier ident =
        new TestDelegationTokenIdentifier(owner, null, owner);
    UserGroupInformation ugi = ident.getUser();
    assertNull(ugi.getRealUser());
    assertEquals("owner", ugi.getUserName());
    assertEquals(AuthenticationMethod.TOKEN, ugi.getAuthenticationMethod());
  }

  @Test
  public void testGetUserWithOwnerAndReal() {
    Text owner = new Text("owner");
    Text realUser = new Text("realUser");
    TestDelegationTokenIdentifier ident =
        new TestDelegationTokenIdentifier(owner, null, realUser);
    UserGroupInformation ugi = ident.getUser();
    assertNotNull(ugi.getRealUser());
    assertNull(ugi.getRealUser().getRealUser());
    assertEquals("owner", ugi.getUserName());
    assertEquals("realUser", ugi.getRealUser().getUserName());
    assertEquals(AuthenticationMethod.PROXY,
                 ugi.getAuthenticationMethod());
    assertEquals(AuthenticationMethod.TOKEN,
                 ugi.getRealUser().getAuthenticationMethod());
  }

  @Test
  public void testDelegationTokenCount() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager =
        new TestDelegationTokenSecretManager(24*60*60*1000,
            3*1000, 1*1000, 3600000);
    try {
      dtSecretManager.startThreads();
      assertThat(dtSecretManager.getCurrentTokensSize()).isZero();
      final Token<TestDelegationTokenIdentifier> token1 =
          generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
      assertThat(dtSecretManager.getCurrentTokensSize()).isOne();
      final Token<TestDelegationTokenIdentifier> token2 =
          generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
      assertThat(dtSecretManager.getCurrentTokensSize()).isEqualTo(2);
      dtSecretManager.cancelToken(token1, "JobTracker");
      assertThat(dtSecretManager.getCurrentTokensSize()).isOne();
      dtSecretManager.cancelToken(token2, "JobTracker");
      assertThat(dtSecretManager.getCurrentTokensSize()).isZero();
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  public void testDelegationTokenSecretManager() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
          3*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      final Token<TestDelegationTokenIdentifier> token = 
        generateDelegationToken(
          dtSecretManager, "SomeUser", "JobTracker");
      Assertions.assertTrue(dtSecretManager.isStoreNewTokenCalled);
      // Fake renewer should not be able to renew
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "FakeRenewer");
          return null;
        }
      }, AccessControlException.class);
      long time = dtSecretManager.renewToken(token, "JobTracker");
      Assertions.assertTrue(dtSecretManager.isUpdateStoredTokenCalled);
      assertTrue(time > Time.now(), "renew time is in future");
      TestDelegationTokenIdentifier identifier = 
        new TestDelegationTokenIdentifier();
      byte[] tokenId = token.getIdentifier();
      identifier.readFields(new DataInputStream(
          new ByteArrayInputStream(tokenId)));
      Assertions.assertTrue(null != dtSecretManager.retrievePassword(identifier));
      LOG.info("Sleep to expire the token");
      Thread.sleep(2000);
      //Token should be expired
      try {
        dtSecretManager.retrievePassword(identifier);
        //Should not come here
        Assertions.fail("Token should have expired");
      } catch (InvalidToken e) {
        //Success
      }
      dtSecretManager.renewToken(token, "JobTracker");
      LOG.info("Sleep beyond the max lifetime");
      Thread.sleep(2000);
      
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "JobTracker");
          return null;
        }
      }, InvalidToken.class);
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test 
  public void testCancelDelegationToken() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      final Token<TestDelegationTokenIdentifier> token = 
        generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
      //Fake renewer should not be able to renew
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "FakeCanceller");
          return null;
        }
      }, AccessControlException.class);
      dtSecretManager.cancelToken(token, "JobTracker");
      Assertions.assertTrue(dtSecretManager.isRemoveStoredTokenCalled);
      shouldThrow(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          dtSecretManager.renewToken(token, "JobTracker");
          return null;
        }
      }, InvalidToken.class);
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  @Timeout(value = 10)
  public void testRollMasterKey() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(800,
        800,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      //generate a token and store the password
      Token<TestDelegationTokenIdentifier> token = generateDelegationToken(
          dtSecretManager, "SomeUser", "JobTracker");
      byte[] oldPasswd = token.getPassword();
      //store the length of the keys list
      int prevNumKeys = dtSecretManager.getAllKeys().length;
      
      dtSecretManager.rollMasterKey();
      Assertions.assertTrue(dtSecretManager.isStoreNewMasterKeyCalled);

      //after rolling, the length of the keys list must increase
      int currNumKeys = dtSecretManager.getAllKeys().length;
      assertThat(currNumKeys - prevNumKeys).isGreaterThanOrEqualTo(1);
      
      //after rolling, the token that was generated earlier must
      //still be valid (retrievePassword will fail if the token
      //is not valid)
      ByteArrayInputStream bi = 
        new ByteArrayInputStream(token.getIdentifier());
      TestDelegationTokenIdentifier identifier = 
        dtSecretManager.createIdentifier();
      identifier.readFields(new DataInputStream(bi));
      byte[] newPasswd = 
        dtSecretManager.retrievePassword(identifier);
      //compare the passwords
      Assertions.assertEquals(oldPasswd, newPasswd);
      // wait for keys to expire
      while(!dtSecretManager.isRemoveStoredMasterKeyCalled) {
        Thread.sleep(200);
      }
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDelegationTokenSelector() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    try {
      dtSecretManager.startThreads();
      AbstractDelegationTokenSelector ds = 
      new AbstractDelegationTokenSelector<TestDelegationTokenIdentifier>(KIND);
      
      //Creates a collection of tokens
      Token<TestDelegationTokenIdentifier> token1 = generateDelegationToken(
          dtSecretManager, "SomeUser1", "JobTracker");
      token1.setService(new Text("MY-SERVICE1"));
      
      Token<TestDelegationTokenIdentifier> token2 = generateDelegationToken(
          dtSecretManager, "SomeUser2", "JobTracker");
      token2.setService(new Text("MY-SERVICE2"));
      
      List<Token<TestDelegationTokenIdentifier>> tokens =
        new ArrayList<Token<TestDelegationTokenIdentifier>>();
      tokens.add(token1);
      tokens.add(token2);
      
      //try to select a token with a given service name (created earlier)
      Token<TestDelegationTokenIdentifier> t = 
        ds.selectToken(new Text("MY-SERVICE1"), tokens);
      Assertions.assertEquals(t, token1);
    } finally {
      dtSecretManager.stopThreads();
    }
  }
  
  @Test
  public void testParallelDelegationTokenCreation() throws Exception {
    final TestDelegationTokenSecretManager dtSecretManager = 
        new TestDelegationTokenSecretManager(2000, 24 * 60 * 60 * 1000, 
            7 * 24 * 60 * 60 * 1000, 2000);
    try {
      dtSecretManager.startThreads();
      int numThreads = 100;
      final int numTokensPerThread = 100;
      class tokenIssuerThread implements Runnable {

        @Override
        public void run() {
          for(int i =0;i <numTokensPerThread; i++) {
            generateDelegationToken(dtSecretManager, "auser", "arenewer");
            try {
              Thread.sleep(250); 
            } catch (Exception e) {
            }
          }
        }
      }
      Thread[] issuers = new Thread[numThreads];
      for (int i =0; i <numThreads; i++) {
        issuers[i] = new Daemon(new tokenIssuerThread());
        issuers[i].start();
      }
      for (int i =0; i <numThreads; i++) {
        issuers[i].join();
      }
      Map<TestDelegationTokenIdentifier, DelegationTokenInformation> tokenCache = dtSecretManager
          .getAllTokens();
      Assertions.assertEquals(numTokensPerThread*numThreads, tokenCache.size());
      Iterator<TestDelegationTokenIdentifier> iter = tokenCache.keySet().iterator();
      while (iter.hasNext()) {
        TestDelegationTokenIdentifier id = iter.next();
        DelegationTokenInformation info = tokenCache.get(id);
        Assertions.assertTrue(info != null);
        DelegationKey key = dtSecretManager.getKey(id);
        Assertions.assertTrue(key != null);
        byte[] storedPassword = dtSecretManager.retrievePassword(id);
        byte[] password = dtSecretManager.createPassword(id, key);
        Assertions.assertTrue(Arrays.equals(password, storedPassword));
        //verify by secret manager api
        dtSecretManager.verifyToken(id, password);
      }
    } finally {
      dtSecretManager.stopThreads();
    }
  }
  
  @Test 
  public void testDelegationTokenNullRenewer() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager = 
      new TestDelegationTokenSecretManager(24*60*60*1000,
        10*1000,1*1000,3600000);
    dtSecretManager.startThreads();
    TestDelegationTokenIdentifier dtId = new TestDelegationTokenIdentifier(new Text(
        "theuser"), null, null);
    Token<TestDelegationTokenIdentifier> token = new Token<TestDelegationTokenIdentifier>(
        dtId, dtSecretManager);
    Assertions.assertTrue(token != null);
    try {
      dtSecretManager.renewToken(token, "");
      Assertions.fail("Renewal must not succeed");
    } catch (IOException e) {
      //PASS
    }
  }

  private boolean testDelegationTokenIdentiferSerializationRoundTrip(Text owner,
      Text renewer, Text realUser) throws IOException {
    TestDelegationTokenIdentifier dtid = new TestDelegationTokenIdentifier(
        owner, renewer, realUser);
    DataOutputBuffer out = new DataOutputBuffer();
    dtid.writeImpl(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    try {
      TestDelegationTokenIdentifier dtid2 =
          new TestDelegationTokenIdentifier();
      dtid2.readFields(in);
      assertTrue(dtid.equals(dtid2));
      return true;
    } catch(IOException e){
      return false;
    }
  }
      
  @Test
  public void testSimpleDtidSerialization() throws IOException {
    assertTrue(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text("owner"), new Text("renewer"), new Text("realUser")));
    assertTrue(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text(""), new Text(""), new Text("")));
    assertTrue(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text(""), new Text("b"), new Text("")));
  }
  
  @Test
  public void testOverlongDtidSerialization() throws IOException {
    byte[] bigBuf = new byte[Text.DEFAULT_MAX_LEN + 1];
    for (int i = 0; i < bigBuf.length; i++) {
      bigBuf[i] = 0;
    }
    assertFalse(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text(bigBuf), new Text("renewer"), new Text("realUser")));
    assertFalse(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text("owner"), new Text(bigBuf), new Text("realUser")));
    assertFalse(testDelegationTokenIdentiferSerializationRoundTrip(
        new Text("owner"), new Text("renewer"), new Text(bigBuf)));
  }

  @Test
  public void testDelegationKeyEqualAndHash() {
    DelegationKey key1 = new DelegationKey(1111, 2222, "keyBytes".getBytes());
    DelegationKey key2 = new DelegationKey(1111, 2222, "keyBytes".getBytes());
    DelegationKey key3 = new DelegationKey(3333, 2222, "keyBytes".getBytes());
    Assertions.assertEquals(key1, key2);
    Assertions.assertFalse(key2.equals(key3));
  }

  @Test
  public void testEmptyToken() throws IOException {
    Token<?> token1 = new Token<TokenIdentifier>();

    Token<?> token2 = new Token<TokenIdentifier>(new byte[0], new byte[0],
        new Text(), new Text());
    assertEquals(token1, token2);
    assertEquals(token1.encodeToUrlString(), token2.encodeToUrlString());

    token2 = new Token<TokenIdentifier>(null, null, null, null);
    assertEquals(token1, token2);
    assertEquals(token1.encodeToUrlString(), token2.encodeToUrlString());
  }

  @Test
  public void testMultipleDelegationTokenSecretManagerMetrics() {
    TestDelegationTokenSecretManager dtSecretManager1 =
        new TestDelegationTokenSecretManager(0, 0, 0, 0);
    assertNotNull(dtSecretManager1.getMetrics());

    TestDelegationTokenSecretManager dtSecretManager2 =
        new TestDelegationTokenSecretManager(0, 0, 0, 0);
    assertNotNull(dtSecretManager2.getMetrics());

    DefaultMetricsSystem.instance().init("test");

    TestDelegationTokenSecretManager dtSecretManager3 =
        new TestDelegationTokenSecretManager(0, 0, 0, 0);
    assertNotNull(dtSecretManager3.getMetrics());
  }

  @Test
  public void testDelegationTokenSecretManagerMetrics() throws Exception {
    TestDelegationTokenSecretManager dtSecretManager =
        new TestDelegationTokenSecretManager(24*60*60*1000,
            10*1000, 1*1000, 60*60*1000);
    try {
      dtSecretManager.startThreads();

      final Token<TestDelegationTokenIdentifier> token = callAndValidateMetrics(
          dtSecretManager, dtSecretManager.getMetrics().getStoreToken(), "storeToken",
          () -> generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker"));

      callAndValidateMetrics(dtSecretManager, dtSecretManager.getMetrics().getUpdateToken(),
          "updateToken", () -> dtSecretManager.renewToken(token, "JobTracker"));

      callAndValidateMetrics(dtSecretManager, dtSecretManager.getMetrics().getRemoveToken(),
          "removeToken", () -> dtSecretManager.cancelToken(token, "JobTracker"));
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  @Test
  public void testDelegationTokenSecretManagerMetricsFailures() throws Exception {
    int errorSleepMillis = 200;
    TestFailureDelegationTokenSecretManager dtSecretManager =
        new TestFailureDelegationTokenSecretManager(errorSleepMillis);

    try {
      dtSecretManager.startThreads();

      final Token<TestDelegationTokenIdentifier> token =
          generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");

      dtSecretManager.setThrowError(true);

      callAndValidateFailureMetrics(dtSecretManager, "storeToken", false,
          errorSleepMillis,
          () -> generateDelegationToken(dtSecretManager, "SomeUser", "JobTracker"));

      callAndValidateFailureMetrics(dtSecretManager, "updateToken", true,
          errorSleepMillis, () -> dtSecretManager.renewToken(token, "JobTracker"));

      callAndValidateFailureMetrics(dtSecretManager, "removeToken", true,
          errorSleepMillis, () -> dtSecretManager.cancelToken(token, "JobTracker"));
    } finally {
      dtSecretManager.stopThreads();
    }
  }

  private <T> T callAndValidateMetrics(TestDelegationTokenSecretManager dtSecretManager,
      MutableRate metric, String statName,  Callable<T> callable)
      throws Exception {
    MeanStatistic stat = IOStatisticAssertions.lookupMeanStatistic(
        dtSecretManager.getMetrics().getIoStatistics(), statName + ".mean");
    long metricBefore = metric.lastStat().numSamples();
    long statBefore = stat.getSamples();
    T returnedObject = callable.call();
    assertEquals(metricBefore + 1, metric.lastStat().numSamples());
    assertEquals(statBefore + 1, stat.getSamples());
    return returnedObject;
  }

  private <T> void callAndValidateFailureMetrics(TestDelegationTokenSecretManager dtSecretManager,
      String statName, boolean expectError, int errorSleepMillis, Callable<T> callable)
      throws Exception {
    MutableCounterLong counter = dtSecretManager.getMetrics().getTokenFailure();
    MeanStatistic failureStat = IOStatisticAssertions.lookupMeanStatistic(
        dtSecretManager.getMetrics().getIoStatistics(), statName + ".failures.mean");
    long counterBefore = counter.value();
    long statBefore = failureStat.getSamples();
    if (expectError) {
      LambdaTestUtils.intercept(IOException.class, callable);
    } else {
      callable.call();
    }
    assertEquals(counterBefore + 1, counter.value());
    assertEquals(statBefore + 1, failureStat.getSamples());
    assertTrue(failureStat.getSum() >= errorSleepMillis);
  }
}
