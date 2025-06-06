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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doNothing;

import org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.LocalRMInterface;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseContainerManagerTest {

  protected static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  protected static FileContext localFS;
  protected static File localDir;
  protected static File localLogDir;
  protected static File remoteLogDir;
  protected static File tmpDir;

  protected NodeManagerMetrics metrics = NodeManagerMetrics.create();

  public BaseContainerManagerTest() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
    localDir =
        new File("target", this.getClass().getSimpleName() + "-localDir")
            .getAbsoluteFile();
    localLogDir =
        new File("target", this.getClass().getSimpleName() + "-localLogDir")
            .getAbsoluteFile();
    remoteLogDir =
      new File("target", this.getClass().getSimpleName() + "-remoteLogDir")
          .getAbsoluteFile();
    tmpDir = new File("target", this.getClass().getSimpleName() + "-tmpDir");
  }

  protected static Logger LOG =
       LoggerFactory.getLogger(BaseContainerManagerTest.class);

  protected static final int HTTP_PORT = 5412;
  protected Configuration conf = new YarnConfiguration();
  protected Context context = new NMContext(new NMContainerTokenSecretManager(
    conf), new NMTokenSecretManagerInNM(), null,
      new ApplicationACLsManager(conf), new NMNullStateStoreService(), false,
      conf) {
    public int getHttpPort() {
      return HTTP_PORT;
    }
    @Override
    public ContainerExecutor getContainerExecutor() {
      return exec;
    }

    @Override
    public NMLogAggregationStatusTracker getNMLogAggregationStatusTracker() {
      NMLogAggregationStatusTracker mock = mock(
          NMLogAggregationStatusTracker.class);
      doNothing().when(mock).updateLogAggregationStatus(
          any(ApplicationId.class), any(LogAggregationStatus.class),
          anyLong(), anyString(), anyBoolean());
      return mock;
    }
  };
  protected ContainerExecutor exec;
  protected DeletionService delSrvc;
  protected String user = "nobody";
  protected NodeHealthCheckerService nodeHealthChecker;
  protected LocalDirsHandlerService dirsHandler;
  protected final long DUMMY_RM_IDENTIFIER = 1234;
  private NodeResourceMonitorImpl nodeResourceMonitor = mock(
      NodeResourceMonitorImpl.class);
  private NodeHealthCheckerService nodeHealthCheckerService;
  private NodeStatusUpdater nodeStatusUpdater;
  protected ContainerManagerImpl containerManager = null;

  public NodeStatusUpdater getNodeStatusUpdater() {
    return nodeStatusUpdater;
  }

  public void setNodeStatusUpdater(
      NodeStatusUpdater nodeStatusUpdater) {
    this.nodeStatusUpdater = nodeStatusUpdater;
  }

  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec = new DefaultContainerExecutor();
    exec.setConf(conf);
    return spy(exec);
  }

  @BeforeEach
  public void setup() throws IOException {
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localFS.delete(new Path(localLogDir.getAbsolutePath()), true);
    localFS.delete(new Path(remoteLogDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    localLogDir.mkdir();
    remoteLogDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:" + ServerSocketUtil.getPort(49162, 10);
    conf.set(YarnConfiguration.NM_ADDRESS, bindAddress);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "0.0.0.0:"
        + ServerSocketUtil.getPort(8040, 10));


    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    // Default delSrvc
    exec = createContainerExecutor();
    delSrvc = createDeletionService();
    delSrvc.init(conf);

    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    nodeHealthCheckerService = new NodeHealthCheckerService(dirsHandler);
    nodeStatusUpdater = new NodeStatusUpdaterImpl(
        context, new AsyncDispatcher(), nodeHealthCheckerService, metrics) {
      @Override
      protected ResourceTracker getRMClient() {
        return new LocalRMInterface();
      };

      @Override
      protected void stopRMProxy() {
        return;
      }

      @Override
      protected void startStatusUpdater() {
        return; // Don't start any updating thread.
      }

      @Override
      public long getRMIdentifier() {
        // There is no real RM registration, simulate and set RMIdentifier
        return DUMMY_RM_IDENTIFIER;
      }
    };

    containerManager = createContainerManager(delSrvc);
    ((NMContext)context).setContainerManager(containerManager);
    ((NMContext)context).setContainerExecutor(exec);
    ((NMContext)context).setNodeResourceMonitor(nodeResourceMonitor);
    nodeStatusUpdater.init(conf);
    containerManager.init(conf);
    nodeStatusUpdater.start();
    ((NMContext)context).setNodeStatusUpdater(nodeStatusUpdater);
    ((NMContext)context).setContainerStateTransitionListener(
        new NodeManager.DefaultContainerStateListener());
  }

  protected ContainerManagerImpl
      createContainerManager(DeletionService delSrvc) {
    
    return new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
      metrics, dirsHandler) {

      @Override
      protected void authorizeGetAndStopContainerRequest(
          ContainerId containerId, Container container, boolean stopRequest,
          NMTokenIdentifier identifier, String remoteUser)
          throws YarnException {
        // do nothing
      }
      @Override
      protected void authorizeUser(UserGroupInformation remoteUgi,
          NMTokenIdentifier nmTokenIdentifier) {
        // do nothing
      }
      @Override
      protected void authorizeStartAndResourceIncreaseRequest(
          NMTokenIdentifier nmTokenIdentifier,
          ContainerTokenIdentifier containerTokenIdentifier,
          boolean startRequest) throws YarnException {
        // do nothing
      }

      @Override
        protected void updateNMTokenIdentifier(
            NMTokenIdentifier nmTokenIdentifier) throws InvalidToken {
          // Do nothing
        }

      @Override
      public Map<String, ByteBuffer> getAuxServiceMetaData() {
        Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();
        serviceData.put("AuxService1",
            ByteBuffer.wrap("AuxServiceMetaData1".getBytes()));
        serviceData.put("AuxService2",
            ByteBuffer.wrap("AuxServiceMetaData2".getBytes()));
        return serviceData;
      }

      @Override
      protected NMTokenIdentifier selectNMTokenIdentifier(
          UserGroupInformation remoteUgi) {
        return new NMTokenIdentifier();
      }
    };
  }

  protected DeletionService createDeletionService() {
    return new DeletionService(exec) {
      @Override
      public void delete(DeletionTask deletionTask) {
        // Don't do any deletions.
        LOG.info("Psuedo delete: user - " + user
            + ", type - " + deletionTask.getDeletionTaskType());
      };
    };
  }

  @AfterEach
  public void tearDown() throws IOException, InterruptedException {
    if (containerManager != null) {
      containerManager.stop();
    }
    createContainerExecutor().deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(user)
        .setSubDir(new Path(localDir.getAbsolutePath()))
        .setBasedirs(new Path[] {})
        .build());
  }

  public static void waitForContainerState(
      ContainerManagementProtocol containerManager, ContainerId containerID,
      ContainerState finalState)
      throws InterruptedException, YarnException, IOException {
    waitForContainerState(containerManager, containerID,
        Arrays.asList(finalState), 20);
  }

  public static void waitForContainerState(
      ContainerManagementProtocol containerManager, ContainerId containerID,
      ContainerState finalState, int timeOutMax)
      throws InterruptedException, YarnException, IOException {
    waitForContainerState(containerManager, containerID,
        Arrays.asList(finalState), timeOutMax);
  }

  public static void waitForContainerState(
      ContainerManagementProtocol containerManager, ContainerId containerID,
      List<ContainerState> finalStates, int timeOutMax)
      throws InterruptedException, YarnException, IOException {
    List<ContainerId> list = new ArrayList<ContainerId>();
    list.add(containerID);
    GetContainerStatusesRequest request =
        GetContainerStatusesRequest.newInstance(list);
    ContainerStatus containerStatus = null;
    HashSet<ContainerState> fStates =
        new HashSet<>(finalStates);
    int timeoutSecs = 0;
    do {
      Thread.sleep(1000);
      containerStatus =
          containerManager.getContainerStatuses(request)
              .getContainerStatuses().get(0);
      LOG.info("Waiting for container to get into one of states " + fStates
          + ". Current state is " + containerStatus.getState());
      timeoutSecs += 1;
    } while (!fStates.contains(containerStatus.getState())
        && timeoutSecs < timeOutMax);
    LOG.info("Container state is " + containerStatus.getState());
    assertTrue(fStates.contains(containerStatus.getState()),
        "ContainerState is not correct (timedout)");
  }

  public static void waitForApplicationState(
      ContainerManagerImpl containerManager, ApplicationId appID,
      ApplicationState finalState)
      throws InterruptedException {
    // Wait for app-finish
    Application app =
        containerManager.getContext().getApplications().get(appID);
    int timeout = 0;
    while (!(app.getApplicationState().equals(finalState))
        && timeout++ < 15) {
      LOG.info("Waiting for app to reach " + finalState
          + ".. Current state is "
          + app.getApplicationState());
      Thread.sleep(1000);
    }
  
    assertTrue(app.getApplicationState().equals(finalState),
        "App is not in " + finalState + " yet!! Timedout!!");
  }

  public static void waitForNMContainerState(ContainerManagerImpl
      containerManager, ContainerId containerID,
          org.apache.hadoop.yarn.server.nodemanager.containermanager
              .container.ContainerState finalState)
                  throws InterruptedException, YarnException, IOException {
    waitForNMContainerState(containerManager, containerID, finalState, 20);
  }

  public static void waitForNMContainerState(ContainerManagerImpl
      containerManager, ContainerId containerID,
          org.apache.hadoop.yarn.server.nodemanager.containermanager
              .container.ContainerState finalState, int timeOutMax)
                  throws InterruptedException, YarnException, IOException {
    waitForNMContainerState(containerManager, containerID,
        Arrays.asList(finalState), timeOutMax);
  }

  public static void waitForNMContainerState(ContainerManagerImpl
      containerManager, ContainerId containerID,
          List<org.apache.hadoop.yarn.server.nodemanager.containermanager
          .container.ContainerState> finalStates, int timeOutMax)
              throws InterruptedException, YarnException, IOException {
    Container container = null;
    org.apache.hadoop.yarn.server.nodemanager
        .containermanager.container.ContainerState currentState = null;
    int timeoutSecs = 0;
    do {
      Thread.sleep(1000);
      container =
          containerManager.getContext().getContainers().get(containerID);
      if (container != null) {
        currentState = container.getContainerState();
      }
      if (currentState != null) {
        LOG.info("Waiting for NM container to get into one of the following " +
            "states: " + finalStates + ". Current state is " + currentState);
      }
      timeoutSecs += 1;
    } while (!finalStates.contains(currentState)
        && timeoutSecs < timeOutMax);
    LOG.info("Container state is " + currentState);
    assertTrue(finalStates.contains(currentState),
        "ContainerState is not correct (timedout)");
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager)
      throws IOException {
    return createContainerToken(cId, rmIdentifier, nodeId, user,
      containerTokenSecretManager, null);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext)
      throws IOException {
    Resource r = Resources.createResource(1024);
    return createContainerToken(cId, rmIdentifier, nodeId, user, r,
        containerTokenSecretManager, logAggregationContext);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext, ContainerType containerType)
      throws IOException {
    Resource r = Resources.createResource(1024);
    return createContainerToken(cId, rmIdentifier, nodeId, user, r,
        containerTokenSecretManager, logAggregationContext, containerType);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user, Resource resource,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext)
      throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, nodeId.toString(), user, resource,
          System.currentTimeMillis() + 100000L, 123, rmIdentifier,
          Priority.newInstance(0), 0, logAggregationContext, null);
    return BuilderUtils.newContainerToken(nodeId, containerTokenSecretManager
        .retrievePassword(containerTokenIdentifier),
            containerTokenIdentifier);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user, Resource resource,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext, ContainerType continerType)
      throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, nodeId.toString(), user, resource,
            System.currentTimeMillis() + 100000L, 123, rmIdentifier,
            Priority.newInstance(0), 0, logAggregationContext, null,
            continerType);
    return BuilderUtils.newContainerToken(nodeId,
        containerTokenSecretManager.retrievePassword(containerTokenIdentifier),
        containerTokenIdentifier);
  }

  public static Token createContainerToken(ContainerId cId, int version,
      long rmIdentifier, NodeId nodeId, String user, Resource resource,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext) throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, version, nodeId.toString(), user,
            resource, System.currentTimeMillis() + 100000L, 123, rmIdentifier,
            Priority.newInstance(0), 0, logAggregationContext, null,
            ContainerType.TASK, ExecutionType.GUARANTEED);
    return BuilderUtils.newContainerToken(nodeId,
        containerTokenSecretManager.retrievePassword(containerTokenIdentifier),
        containerTokenIdentifier);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user, Resource resource,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext, ExecutionType executionType)
      throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, 0, nodeId.toString(), user, resource,
            System.currentTimeMillis() + 100000L, 123, rmIdentifier,
            Priority.newInstance(0), 0, logAggregationContext, null,
            ContainerType.TASK, executionType);
    return BuilderUtils.newContainerToken(nodeId,
        containerTokenSecretManager.retrievePassword(containerTokenIdentifier),
        containerTokenIdentifier);
  }

  public static Token createContainerToken(ContainerId cId, int version,
      long rmIdentifier, NodeId nodeId, String user, Resource resource,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext, ExecutionType executionType)
      throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, version, nodeId.toString(), user,
            resource, System.currentTimeMillis() + 100000L, 123, rmIdentifier,
            Priority.newInstance(0), 0, logAggregationContext, null,
            ContainerType.TASK, executionType);
    return BuilderUtils.newContainerToken(nodeId,
        containerTokenSecretManager.retrievePassword(containerTokenIdentifier),
        containerTokenIdentifier);
  }

  public static ContainerId createContainerId(int id) {
    // Use default appId = 0
    return createContainerId(id, 0);
  }

  public static ContainerId createContainerId(int cId, int aId) {
    ApplicationId appId = ApplicationId.newInstance(0, aId);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId =
        ContainerId.newContainerId(appAttemptId, cId);
    return containerId;
  }
}
