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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.retry.UnreliableInterface;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestNMProxy extends BaseContainerManagerTest {

  public TestNMProxy() throws UnsupportedFileSystemException {
    super();
  }

  int retryCount = 0;

  @BeforeEach
  public void setUp() throws Exception {
    containerManager.start();
  }

  @Override
  protected ContainerManagerImpl
      createContainerManager(DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        getNodeStatusUpdater(), metrics, dirsHandler) {

      @Override
      public StartContainersResponse startContainers(
          StartContainersRequest requests) throws YarnException, IOException {
        if (retryCount < 5) {
          retryCount++;
          if (isRetryPolicyRetryForEver()) {
            // Throw non network exception
            throw new IOException(
                new UnreliableInterface.UnreliableException());
          } else {
            throw new java.net.ConnectException("start container exception");
          }
        }
        return super.startContainers(requests);
      }

      private boolean isRetryPolicyRetryForEver() {
        return conf.getLong(
            YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, 1000) == -1;
      }

      @Override
      public StopContainersResponse stopContainers(
          StopContainersRequest requests) throws YarnException, IOException {
        if (retryCount < 5) {
          retryCount++;
          throw new java.net.ConnectException("stop container exception");
        }
        return super.stopContainers(requests);
      }

      @Override
      public GetContainerStatusesResponse getContainerStatuses(
          GetContainerStatusesRequest request) throws YarnException,
          IOException {
        if (retryCount < 5) {
          retryCount++;
          throw new java.net.ConnectException("get container status exception");
        }
        return super.getContainerStatuses(request);
      }
    };
  }

  @Test
  @Timeout(value = 20)
  public void testNMProxyRetry() throws Exception {
     conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, 10000);
     conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 100);
     StartContainersRequest allRequests =
         Records.newRecord(StartContainersRequest.class);

    ContainerManagementProtocol proxy = getNMProxy(conf);

    proxy.startContainers(allRequests);
    assertEquals(5, retryCount);

    retryCount = 0;
    proxy.stopContainers(Records.newRecord(StopContainersRequest.class));
    assertEquals(5, retryCount);

    retryCount = 0;
    proxy.getContainerStatuses(Records
      .newRecord(GetContainerStatusesRequest.class));
    assertEquals(5, retryCount);
  }

  @Test
  @Timeout(value = 20)
  public void testShouldNotRetryForeverForNonNetworkExceptionsOnNMConnections()
      throws Exception {
    assertThrows(IOException.class, ()->{
      conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, -1);
      StartContainersRequest allRequests =
          Records.newRecord(StartContainersRequest.class);

      ContainerManagementProtocol proxy = getNMProxy(conf);

      retryCount = 0;
      proxy.startContainers(allRequests);
    });
  }

  @Test
  @Timeout(value = 20)
  public void testNMProxyRPCRetry() throws Exception {
    conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, 1000);
    conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 100);
    StartContainersRequest allRequests =
        Records.newRecord(StartContainersRequest.class);
    Configuration newConf = new YarnConfiguration(conf);
    newConf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 100);

    newConf.setInt(CommonConfigurationKeysPublic.
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 100);
    // connect to some dummy address so that it can trigger
    // connection failure and RPC level retires.
    newConf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:1");
    ContainerManagementProtocol proxy = getNMProxy(newConf);
    LambdaTestUtils.intercept(SocketException.class,
        () -> proxy.startContainers(allRequests));
  }

  private ContainerManagementProtocol getNMProxy(Configuration conf) {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    org.apache.hadoop.yarn.api.records.Token nmToken =
        context.getNMTokenSecretManager().createNMToken(attemptId,
            context.getNodeId(), user);
    final InetSocketAddress address =
        conf.getSocketAddr(YarnConfiguration.NM_BIND_HOST,
            YarnConfiguration.NM_ADDRESS, YarnConfiguration.DEFAULT_NM_ADDRESS,
            YarnConfiguration.DEFAULT_NM_PORT);
    Token<NMTokenIdentifier> token =
        ConverterUtils.convertFromYarn(nmToken,
            SecurityUtil.buildTokenService(address));
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.addToken(token);
    return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class, ugi,
        YarnRPC.create(conf), address);
  }
}
