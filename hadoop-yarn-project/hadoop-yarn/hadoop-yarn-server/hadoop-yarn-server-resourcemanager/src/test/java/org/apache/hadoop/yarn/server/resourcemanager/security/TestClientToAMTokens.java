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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMWithCustomAMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Timer;
import java.util.TimerTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestClientToAMTokens extends ParameterizedSchedulerTestBase {
  private YarnConfiguration conf;

  public void initTestClientToAMTokens(SchedulerType type) throws IOException {
    initParameterizedSchedulerTestBase(type);
    setup();
  }

  public void setup() {
    conf = getConf();
  }

  @TokenInfo(ClientToAMTokenSelector.class)
  @ProtocolInfo(protocolName =
      "org.apache.hadoop.yarn.server.resourcemanager.security$CustomProtocol",
      protocolVersion = 1)
  public interface CustomProtocol
      extends TestRpcServiceProtos.CustomProto.BlockingInterface {
  }

  private static class CustomSecurityInfo extends SecurityInfo {

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
      return new TokenInfo() {

        @Override
        public Class<? extends Annotation> annotationType() {
          return null;
        }

        @Override
        public Class<? extends TokenSelector<? extends TokenIdentifier>>
            value() {
          return ClientToAMTokenSelector.class;
        }
      };
    }

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
      return null;
    }
  };

  private static class CustomAM extends AbstractService implements
      CustomProtocol {

    private final ApplicationAttemptId appAttemptId;
    private final byte[] secretKey;
    private InetSocketAddress address;
    private boolean pinged = false;
    private ClientToAMTokenSecretManager secretMgr;
    
    public CustomAM(ApplicationAttemptId appId, byte[] secretKey) {
      super("CustomAM");
      this.appAttemptId = appId;
      this.secretKey = secretKey;
    }

    @Override
    public TestProtos.EmptyResponseProto ping(RpcController unused,
                TestProtos.EmptyRequestProto request) throws ServiceException {
      this.pinged = true;
      return TestProtos.EmptyResponseProto.newBuilder().build();
    }
    
    public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
      return secretMgr;
    }

    @Override
    protected void serviceStart() throws Exception {
      Configuration conf = getConfig();
      // Set RPC engine to protobuf RPC engine
      RPC.setProtocolEngine(conf, CustomProtocol.class,
          ProtobufRpcEngine2.class);
      UserGroupInformation.setConfiguration(conf);

      BlockingService service = TestRpcServiceProtos.CustomProto
          .newReflectiveBlockingService(this);

      Server server;
      try {
        secretMgr = new ClientToAMTokenSecretManager(
            this.appAttemptId, secretKey);
        server =
            new RPC.Builder(conf)
              .setProtocol(CustomProtocol.class)
              .setNumHandlers(1)
              .setSecretManager(secretMgr)
              .setInstance(service).build();
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }
      server.start();
      this.address = NetUtils.getConnectAddress(server);
      super.serviceStart();
    }

    public void setClientSecretKey(byte[] key) {
      secretMgr.setMasterKey(key);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testClientToAMTokens(SchedulerType type) throws Exception {
    initTestClientToAMTokens(type);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, CustomProtocol.class, ProtobufRpcEngine2.class);
    UserGroupInformation.setConfiguration(conf);

    ContainerManagementProtocol containerManager =
        mock(ContainerManagementProtocol.class);
    StartContainersResponse mockResponse = mock(StartContainersResponse.class);
    when(containerManager.startContainers((StartContainersRequest) any()))
      .thenReturn(mockResponse);

    MockRM rm = new MockRMWithCustomAMLauncher(conf, containerManager) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          getRMContext().getRMDelegationTokenSecretManager());
      };

      @Override
      protected void doSecureLogin() throws IOException {
      }
    };
    rm.start();

    // Submit an app
    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);

    // Set up a node.
    MockNM nm1 = rm.registerNode("localhost:1234", 3072);
    nm1.nodeHeartbeat(true);
    rm.drainEvents();
    
    nm1.nodeHeartbeat(true);
    rm.drainEvents();

    ApplicationAttemptId appAttempt = app.getCurrentAppAttempt().getAppAttemptId();
    final MockAM mockAM =
        new MockAM(rm.getRMContext(), rm.getApplicationMasterService(),
            app.getCurrentAppAttempt().getAppAttemptId());
    UserGroupInformation appUgi =
        UserGroupInformation.createRemoteUser(appAttempt.toString());
    RegisterApplicationMasterResponse response =
        appUgi.doAs(new PrivilegedAction<RegisterApplicationMasterResponse>() {

          @Override
          public RegisterApplicationMasterResponse run() {
            RegisterApplicationMasterResponse response = null;
            try {
              response = mockAM.registerAppAttempt();
            } catch (Exception e) {
              fail("Exception was not expected");
            }
            return response;
          }
        });

    // Get the app-report.
    GetApplicationReportRequest request =
        Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(app.getApplicationId());
    GetApplicationReportResponse reportResponse =
        rm.getClientRMService().getApplicationReport(request);
    ApplicationReport appReport = reportResponse.getApplicationReport();
    org.apache.hadoop.yarn.api.records.Token originalClientToAMToken =
        appReport.getClientToAMToken();

    // ClientToAMToken master key should have been received on register
    // application master response.
    assertNotNull(response.getClientToAMTokenMasterKey());
    assertTrue(response.getClientToAMTokenMasterKey().array().length > 0);
    
    // Start the AM with the correct shared-secret.
    ApplicationAttemptId appAttemptId =
        app.getAppAttempts().keySet().iterator().next();
    assertNotNull(appAttemptId);
    final CustomAM am =
        new CustomAM(appAttemptId, response.getClientToAMTokenMasterKey()
            .array());
    am.init(conf);
    am.start();

    // Now the real test!
    // Set up clients to be able to pick up correct tokens.
    SecurityUtil.setSecurityInfoProviders(new CustomSecurityInfo());

    // Verify denial for unauthenticated user
    try {
      CustomProtocol client = RPC.getProxy(CustomProtocol.class,
          1L, am.address, conf);
      client.ping(null, TestRpcBase.newEmptyRequest());
      fail("Access by unauthenticated user should fail!!");
    } catch (Exception e) {
      assertFalse(am.pinged);
    }

    Token<ClientToAMTokenIdentifier> token =
        ConverterUtils.convertFromYarn(originalClientToAMToken, am.address);

    // Verify denial for a malicious user with tampered ID
    verifyTokenWithTamperedID(conf, am, token);

    // Verify denial for a malicious user with tampered user-name
    verifyTokenWithTamperedUserName(conf, am, token);

    // Now for an authenticated user
    verifyValidToken(conf, am, token);
    
    // Verify for a new version token
    verifyNewVersionToken(conf, am, token, rm);

    am.stop();
    rm.stop();
  }

  private void verifyTokenWithTamperedID(final Configuration conf,
      final CustomAM am, Token<ClientToAMTokenIdentifier> token)
      throws IOException {
    // Malicious user, messes with appId
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("me");
    ClientToAMTokenIdentifier maliciousID =
        new ClientToAMTokenIdentifier(BuilderUtils.newApplicationAttemptId(
          BuilderUtils.newApplicationId(am.appAttemptId.getApplicationId()
            .getClusterTimestamp(), 42), 43), UserGroupInformation
          .getCurrentUser().getShortUserName());

    verifyTamperedToken(conf, am, token, ugi, maliciousID);
  }

  private void verifyTokenWithTamperedUserName(final Configuration conf,
      final CustomAM am, Token<ClientToAMTokenIdentifier> token)
      throws IOException {
    // Malicious user, messes with appId
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("me");
    ClientToAMTokenIdentifier maliciousID =
        new ClientToAMTokenIdentifier(am.appAttemptId, "evilOrc");

    verifyTamperedToken(conf, am, token, ugi, maliciousID);
  }

  private void verifyTamperedToken(final Configuration conf, final CustomAM am,
      Token<ClientToAMTokenIdentifier> token, UserGroupInformation ugi,
      ClientToAMTokenIdentifier maliciousID) {
    Token<ClientToAMTokenIdentifier> maliciousToken =
        new Token<ClientToAMTokenIdentifier>(maliciousID.getBytes(),
          token.getPassword(), token.getKind(),
          token.getService());
    ugi.addToken(maliciousToken);

    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>()  {
        @Override
        public Void run() throws Exception {
          try {
            CustomProtocol client = RPC.getProxy(CustomProtocol.class, 1L,
                  am.address, conf);
            client.ping(null, TestRpcBase.newEmptyRequest());
            fail("Connection initiation with illegally modified "
                + "tokens is expected to fail.");
            return null;
          } catch (ServiceException ex) {
            //fail("Cannot get a YARN remote exception as "
            //    + "it indicates RPC success");
            throw (Exception) ex.getCause();
          }
        }
      });
    } catch (Exception e) {
      assertEquals(RemoteException.class.getName(), e.getClass()
          .getName());
      e = ((RemoteException)e).unwrapRemoteException();
      assertEquals(SaslException.class
          .getCanonicalName(), e.getClass().getCanonicalName());
      assertTrue(e
          .getMessage()
          .contains("DIGEST-MD5: digest response format violation. " + "Mismatched response."));
      assertFalse(am.pinged);
    }
  }

  private void verifyNewVersionToken(final Configuration conf, final CustomAM am,
      Token<ClientToAMTokenIdentifier> token, MockRM rm) throws IOException,
      InterruptedException {
    UserGroupInformation ugi;
    ugi = UserGroupInformation.createRemoteUser("me");
    
    Token<ClientToAMTokenIdentifier> newToken = 
        new Token<ClientToAMTokenIdentifier>(
            new ClientToAMTokenIdentifierForTest(token.decodeIdentifier(), "message"),
            am.getClientToAMTokenSecretManager());
    newToken.setService(token.getService());
    
    ugi.addToken(newToken);

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        CustomProtocol client =
            RPC.getProxy(CustomProtocol.class, 1L, am.address, conf);
        client.ping(null, TestRpcBase.newEmptyRequest());
        assertTrue(am.pinged);
        return null;
      }
    });
  }
  
  private void verifyValidToken(final Configuration conf, final CustomAM am,
      Token<ClientToAMTokenIdentifier> token) throws IOException,
      InterruptedException {
    UserGroupInformation ugi;
    ugi = UserGroupInformation.createRemoteUser("me");
    ugi.addToken(token);

    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        CustomProtocol client = RPC.getProxy(CustomProtocol.class,
            1L, am.address, conf);
        client.ping(null, TestRpcBase.newEmptyRequest());
        assertTrue(am.pinged);
        return null;
      }
    });
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  @Timeout(20)
  public void testClientTokenRace(SchedulerType type) throws Exception {

    initTestClientToAMTokens(type);

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    ContainerManagementProtocol containerManager =
        mock(ContainerManagementProtocol.class);
    StartContainersResponse mockResponse = mock(StartContainersResponse.class);
    when(containerManager.startContainers((StartContainersRequest) any()))
      .thenReturn(mockResponse);

    MockRM rm = new MockRMWithCustomAMLauncher(conf, containerManager) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          getRMContext().getRMDelegationTokenSecretManager());
      };

      @Override
      protected void doSecureLogin() throws IOException {
      }
    };
    rm.start();

    // Submit an app
    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);

    // Set up a node.
    MockNM nm1 = rm.registerNode("localhost:1234", 3072);
    nm1.nodeHeartbeat(true);
    rm.drainEvents();

    nm1.nodeHeartbeat(true);
    rm.drainEvents();

    ApplicationAttemptId appAttempt = app.getCurrentAppAttempt().getAppAttemptId();
    final MockAM mockAM =
        new MockAM(rm.getRMContext(), rm.getApplicationMasterService(),
            app.getCurrentAppAttempt().getAppAttemptId());
    UserGroupInformation appUgi =
        UserGroupInformation.createRemoteUser(appAttempt.toString());
    RegisterApplicationMasterResponse response =
        appUgi.doAs(new PrivilegedAction<RegisterApplicationMasterResponse>() {

          @Override
          public RegisterApplicationMasterResponse run() {
            RegisterApplicationMasterResponse response = null;
            try {
              response = mockAM.registerAppAttempt();
            } catch (Exception e) {
              fail("Exception was not expected");
            }
            return response;
          }
        });

    // Get the app-report.
    GetApplicationReportRequest request =
        Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(app.getApplicationId());
    GetApplicationReportResponse reportResponse =
        rm.getClientRMService().getApplicationReport(request);
    ApplicationReport appReport = reportResponse.getApplicationReport();
    org.apache.hadoop.yarn.api.records.Token originalClientToAMToken =
        appReport.getClientToAMToken();

    // ClientToAMToken master key should have been received on register
    // application master response.
    final ByteBuffer clientMasterKey = response.getClientToAMTokenMasterKey();
    assertNotNull(clientMasterKey);
    assertTrue(clientMasterKey.array().length > 0);

    // Start the AM with the correct shared-secret.
    ApplicationAttemptId appAttemptId =
        app.getAppAttempts().keySet().iterator().next();
    assertNotNull(appAttemptId);
    final CustomAM am = new CustomAM(appAttemptId, null);
    am.init(conf);
    am.start();

    // Now the real test!
    // Set up clients to be able to pick up correct tokens.
    SecurityUtil.setSecurityInfoProviders(new CustomSecurityInfo());

    Token<ClientToAMTokenIdentifier> token =
        ConverterUtils.convertFromYarn(originalClientToAMToken, am.address);

    // Schedule the key to be set after a significant delay
    Timer timer = new Timer();
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        am.setClientSecretKey(clientMasterKey.array());
      }
    };
    timer.schedule(timerTask, 250);

    // connect should pause waiting for the master key to arrive
    verifyValidToken(conf, am, token);

    am.stop();
    rm.stop();
  }
}
