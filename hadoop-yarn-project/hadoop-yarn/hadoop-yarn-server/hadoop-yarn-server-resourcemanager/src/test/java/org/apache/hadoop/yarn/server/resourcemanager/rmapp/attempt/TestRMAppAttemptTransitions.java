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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestRMAppAttemptTransitions {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRMAppAttemptTransitions.class);
  
  private static final String EMPTY_DIAGNOSTICS = "";
  private static final String FAILED_DIAGNOSTICS = "Attempt failed by user.";
  private static final String RM_WEBAPP_ADDR =
      WebAppUtils.getResolvedRMWebAppURLWithScheme(new Configuration());
  private static final String AHS_WEBAPP_ADDR =
      WebAppUtils.getHttpSchemePrefix(new Configuration()) +
      WebAppUtils.getAHSWebAppURLWithoutScheme(new Configuration());

  private boolean isSecurityEnabled;
  private RMContext rmContext;
  private RMContext spyRMContext;
  private YarnScheduler scheduler;
  private ResourceScheduler resourceScheduler;
  private ApplicationMasterService masterService;
  private ApplicationMasterLauncher applicationMasterLauncher;
  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMApplicationHistoryWriter writer;
  private SystemMetricsPublisher publisher;

  private RMStateStore store;

  private RMAppImpl application;
  private RMAppAttempt applicationAttempt;

  private Configuration conf = new Configuration();
  private AMRMTokenSecretManager amRMTokenManager =
      spy(new AMRMTokenSecretManager(conf, rmContext));
  private ClientToAMTokenSecretManagerInRM clientToAMTokenManager =
      spy(new ClientToAMTokenSecretManagerInRM());
  private NMTokenSecretManagerInRM nmTokenManager =
      spy(new NMTokenSecretManagerInRM(conf));
  private boolean transferStateFromPreviousAttempt = false;
  private EventHandler<RMNodeEvent> rmnodeEventHandler;

  private final class TestApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appID = event.getApplicationAttemptId();
      assertEquals(applicationAttempt.getAppAttemptId(), appID);
      try {
        applicationAttempt.handle(event);
      } catch (Throwable t) {
        LOG.error("Error in handling event type " + event.getType()
            + " for application " + appID, t);
      }
    }
  }

  // handle all the RM application events - same as in ResourceManager.java
  private final class TestApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {
    @Override
    public void handle(RMAppEvent event) {
      assertEquals(application.getApplicationId(), event.getApplicationId());
      if (event instanceof RMAppFailedAttemptEvent) {
        transferStateFromPreviousAttempt =
            ((RMAppFailedAttemptEvent) event)
              .getTransferStateFromPreviousAttempt();
      }
      try {
        application.handle(event);
      } catch (Throwable t) {
        LOG.error("Error in handling event type " + event.getType()
            + " for application " + application.getApplicationId(), t);
      }
    }
  }

  private final class TestSchedulerEventDispatcher implements
  EventHandler<SchedulerEvent> {
    @Override
    public void handle(SchedulerEvent event) {
      scheduler.handle(event);
    }
  }
  
  private final class TestAMLauncherEventDispatcher implements
  EventHandler<AMLauncherEvent> {
    @Override
    public void handle(AMLauncherEvent event) {
      applicationMasterLauncher.handle(event);
    }
  }

  private static int appId = 1;
  
  private ApplicationSubmissionContext submissionContext = null;
  private boolean unmanagedAM;

  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        { Boolean.FALSE },
        { Boolean.TRUE }
    });
  }

  private void initTestRMAppAttemptTransitions(boolean pIsSecurityEnabled)
      throws Exception {
    this.isSecurityEnabled = pIsSecurityEnabled;
    setUp();
  }

  @SuppressWarnings("deprecation")
  public void setUp() throws Exception {
    AuthenticationMethod authMethod = AuthenticationMethod.SIMPLE;
    if (isSecurityEnabled) {
      authMethod = AuthenticationMethod.KERBEROS;
    }
    SecurityUtil.setAuthenticationMethod(authMethod, conf);
    UserGroupInformation.setConfiguration(conf);
    InlineDispatcher rmDispatcher = new InlineDispatcher();
  
    ContainerAllocationExpirer containerAllocationExpirer =
        mock(ContainerAllocationExpirer.class);
    amLivelinessMonitor = mock(AMLivelinessMonitor.class);
    amFinishingMonitor = mock(AMLivelinessMonitor.class);
    writer = mock(RMApplicationHistoryWriter.class);
    MasterKeyData masterKeyData = amRMTokenManager.createNewMasterKey();
    when(amRMTokenManager.getMasterKey()).thenReturn(masterKeyData);
    rmContext =
        new RMContextImpl(rmDispatcher,
          containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
          null, amRMTokenManager,
          new RMContainerTokenSecretManager(conf),
          nmTokenManager,
          clientToAMTokenManager);
    
    store = mock(RMStateStore.class);
    ((RMContextImpl) rmContext).setStateStore(store);
    publisher = mock(SystemMetricsPublisher.class);
    rmContext.setSystemMetricsPublisher(publisher);
    rmContext.setRMApplicationHistoryWriter(writer);

    scheduler = mock(YarnScheduler.class);
    masterService = mock(ApplicationMasterService.class);
    applicationMasterLauncher = mock(ApplicationMasterLauncher.class);
    
    rmDispatcher.register(RMAppAttemptEventType.class,
        new TestApplicationAttemptEventDispatcher());
  
    rmDispatcher.register(RMAppEventType.class,
        new TestApplicationEventDispatcher());
    
    rmDispatcher.register(SchedulerEventType.class, 
        new TestSchedulerEventDispatcher());
    
    rmDispatcher.register(AMLauncherEventType.class, 
        new TestAMLauncherEventDispatcher());

    rmnodeEventHandler = mock(RMNodeImpl.class);
    rmDispatcher.register(RMNodeEventType.class, rmnodeEventHandler);

    rmDispatcher.init(conf);
    rmDispatcher.start();
    

    ApplicationId applicationId = MockApps.newAppID(appId++);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);

    resourceScheduler = mock(ResourceScheduler.class);

    ApplicationResourceUsageReport appResUsgRpt =
        mock(ApplicationResourceUsageReport.class);
    when(appResUsgRpt.getMemorySeconds()).thenReturn(0L);
    when(appResUsgRpt.getVcoreSeconds()).thenReturn(0L);
    when(resourceScheduler
        .getAppResourceUsageReport(any()))
     .thenReturn(appResUsgRpt);
    spyRMContext = spy(rmContext);
    Mockito.doReturn(resourceScheduler).when(spyRMContext).getScheduler();


    final String user = MockApps.newUserName();
    final String queue = MockApps.newQueue();
    submissionContext = mock(ApplicationSubmissionContext.class);
    when(submissionContext.getQueue()).thenReturn(queue);
    Resource resource = Resources.createResource(1536);
    ContainerLaunchContext amContainerSpec =
        BuilderUtils.newContainerLaunchContext(null, null,
            null, null, null, null);
    when(submissionContext.getAMContainerSpec()).thenReturn(amContainerSpec);
    when(submissionContext.getResource()).thenReturn(resource);

    unmanagedAM = false;
    
    application = mock(RMAppImpl.class);
    applicationAttempt =
        new RMAppAttemptImpl(applicationAttemptId, spyRMContext, scheduler,
            masterService, submissionContext, new Configuration(),
            Collections.singletonList(BuilderUtils.newResourceRequest(
                RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                submissionContext.getResource(), 1)), application) {
        @Override
        protected void onInvalidTranstion(
                RMAppAttemptEventType rmAppAttemptEventType,
                RMAppAttemptState state) {
            assertTrue(false, "RMAppAttemptImpl can't handle "
                + rmAppAttemptEventType + " at state " + state);
        }
    };

    when(application.getCurrentAppAttempt()).thenReturn(applicationAttempt);
    when(application.getApplicationId()).thenReturn(applicationId);
    spyRMContext.getRMApps().put(application.getApplicationId(), application);

    testAppAttemptNewState();
  }

  @AfterEach
  public void tearDown() throws Exception {
    ((AsyncDispatcher)this.spyRMContext.getDispatcher()).stop();
  }
  
  private String getProxyUrl(RMAppAttempt appAttempt) {
    String url = rmContext.getAppProxyUrl(conf,
        appAttempt.getAppAttemptId().getApplicationId());
    assertNotEquals("N/A", url);
    return url;
  }

  /**
   * {@link RMAppAttemptState#NEW}
   */
  private void testAppAttemptNewState() {
    assertEquals(RMAppAttemptState.NEW, 
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getDiagnostics().length());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    assertNotNull(applicationAttempt.getTrackingUrl());
    assertFalse("N/A".equals(applicationAttempt.getTrackingUrl()));
  }

  /**
   * {@link RMAppAttemptState#SUBMITTED}
   */
  private void testAppAttemptSubmittedState() {
    assertEquals(RMAppAttemptState.SUBMITTED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getDiagnostics().length());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    if (UserGroupInformation.isSecurityEnabled()) {
      verify(clientToAMTokenManager).createMasterKey(
          applicationAttempt.getAppAttemptId());
      // can't create ClientToken as at this time ClientTokenMasterKey has
      // not been registered in the SecretManager
      assertNull(applicationAttempt.createClientToken("some client"));
    }
    assertNull(applicationAttempt.createClientToken(null));
    // Check events
    verify(masterService).
        registerAppAttempt(applicationAttempt.getAppAttemptId());
    verify(scheduler).handle(any(AppAttemptAddedSchedulerEvent.class));
  }

  /**
   * {@link RMAppAttemptState#SUBMITTED} -> {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptSubmittedToFailedState(String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    
    // Check events
    verify(masterService).
        unregisterAttempt(applicationAttempt.getAppAttemptId());
    // ATTEMPT_FAILED should be notified to app if app attempt is submitted to
    // failed state.
    ArgumentMatcher<RMAppEvent> matcher =
        event -> event.getType() == RMAppEventType.ATTEMPT_FAILED;
    verify(application).handle(argThat(matcher));
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  /**
   * {@link RMAppAttemptState#KILLED}
   */
  private void testAppAttemptKilledState(Container amContainer, 
      String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.KILLED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAttemptFinalStateSaved();
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.KILLED);
  }
  
  /**
   * {@link RMAppAttemptState#LAUNCHED}
   */
  private void testAppAttemptRecoveredState() {
    assertEquals(RMAppAttemptState.LAUNCHED, 
        applicationAttempt.getAppAttemptState());
  }

  /**
   * {@link RMAppAttemptState#SCHEDULED}
   */
  @SuppressWarnings("unchecked")
  private void testAppAttemptScheduledState() {
    RMAppAttemptState expectedState;
    int expectedAllocateCount;
    if(unmanagedAM) {
      expectedState = RMAppAttemptState.LAUNCHED;
      expectedAllocateCount = 0;
    } else {
      expectedState = RMAppAttemptState.SCHEDULED;
      expectedAllocateCount = 1;
    }

    assertEquals(expectedState, applicationAttempt.getAppAttemptState());
    verify(scheduler, times(expectedAllocateCount)).allocate(
        any(ApplicationAttemptId.class), any(List.class), eq(null), any(List.class),
        any(List.class), any(List.class), any(ContainerUpdates.class));

    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertNull(applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    assertNull(applicationAttempt.getFinalApplicationStatus());
  }

  /**
   * {@link RMAppAttemptState#ALLOCATED}
   */
  @SuppressWarnings("unchecked")
  private void testAppAttemptAllocatedState(Container amContainer) {
    assertEquals(RMAppAttemptState.ALLOCATED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    // Check events
    verify(applicationMasterLauncher).handle(any(AMLauncherEvent.class));
    verify(scheduler, times(2)).allocate(any(ApplicationAttemptId.class),
        any(List.class), any(), any(List.class), any(), any(),
        any(ContainerUpdates.class));
    verify(nmTokenManager).clearNodeSetForAttempt(
      applicationAttempt.getAppAttemptId());
  }
  
  /**
   * {@link RMAppAttemptState#FAILED}
   */
  private void testAppAttemptFailedState(Container container, 
      String diagnostics) {
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    assertEquals(0,applicationAttempt.getJustFinishedContainers().size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(0.0, (double)applicationAttempt.getProgress(), 0.0001);
    assertEquals(0, application.getRanNodes().size());
    
    // Check events
    verify(application, times(1)).handle(any(RMAppFailedAttemptEvent.class));
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAttemptFinalStateSaved();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  private void testAppAttemptLaunchedState(Container container,
                                                RMAppAttemptState state) {
    assertEquals(state, applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    if (UserGroupInformation.isSecurityEnabled()) {
      // ClientTokenMasterKey has been registered in SecretManager, it's able to
      // create ClientToken now
      assertNotNull(applicationAttempt.createClientToken("some client"));
    }
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#RUNNING}
   */
  private void testAppAttemptRunningState(Container container,
      String host, int rpcPort, String trackingUrl, boolean unmanagedAM) {
    assertEquals(RMAppAttemptState.RUNNING, 
        applicationAttempt.getAppAttemptState());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(host, applicationAttempt.getHost());
    assertEquals(rpcPort, applicationAttempt.getRpcPort());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    if (unmanagedAM) {
      verifyUrl(trackingUrl, applicationAttempt.getTrackingUrl());
    } else {
      assertEquals(getProxyUrl(applicationAttempt),
          applicationAttempt.getTrackingUrl());
    }
    // TODO - need to add more checks relevant to this state
  }

  /**
   * {@link RMAppAttemptState#FINISHING}
   */
  private void testAppAttemptFinishingState(Container container,
      FinalApplicationStatus finalStatus,
      String trackingUrl,
      String diagnostics) {
    assertEquals(RMAppAttemptState.FINISHING,
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(getProxyUrl(applicationAttempt),
        applicationAttempt.getTrackingUrl());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(finalStatus, applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 0);
    verifyAttemptFinalStateSaved();
  }

  /**
   * {@link RMAppAttemptState#FINISHED}
   */
  private void testAppAttemptFinishedState(Container container,
      FinalApplicationStatus finalStatus, 
      String trackingUrl, 
      String diagnostics,
      int finishedContainerCount, boolean unmanagedAM) {
    assertEquals(RMAppAttemptState.FINISHED, 
        applicationAttempt.getAppAttemptState());
    assertEquals(diagnostics, applicationAttempt.getDiagnostics());
    verifyUrl(trackingUrl, applicationAttempt.getOriginalTrackingUrl());
    if (unmanagedAM) {
      verifyUrl(trackingUrl, applicationAttempt.getTrackingUrl()); 
    } else {
      assertEquals(getProxyUrl(applicationAttempt),
          applicationAttempt.getTrackingUrl());
    }
    verifyAttemptFinalStateSaved();
    assertEquals(finishedContainerCount, applicationAttempt
        .getJustFinishedContainers().size());
    assertEquals(0, getFinishedContainersSentToAM(applicationAttempt)
        .size());
    assertEquals(container, applicationAttempt.getMasterContainer());
    assertEquals(finalStatus, applicationAttempt.getFinalApplicationStatus());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FINISHED);
  }
  
  
  private void submitApplicationAttempt() {
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    applicationAttempt.handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.START));
    testAppAttemptSubmittedState();
  }

  private void scheduleApplicationAttempt() {
    submitApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_ADDED));
    
    if(unmanagedAM){
      assertEquals(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          applicationAttempt.getAppAttemptState());
      applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_NEW_SAVED));
    }
    
    testAppAttemptScheduledState();
  }

  @SuppressWarnings("unchecked")
  private Container allocateApplicationAttempt() {
    scheduleApplicationAttempt();
    
    // Mock the allocation of AM container 
    Container container = mock(Container.class);
    Resource resource = Resources.createResource(2048);
    when(container.getId()).thenReturn(
        BuilderUtils.newContainerId(applicationAttempt.getAppAttemptId(), 1));
    when(container.getResource()).thenReturn(resource);
    Allocation allocation = mock(Allocation.class);
    when(allocation.getContainers()).
        thenReturn(Collections.singletonList(container));
    when(scheduler.allocate(any(ApplicationAttemptId.class), any(List.class),
        any(), any(List.class), any(), any(),
        any(ContainerUpdates.class))).
    thenReturn(allocation);
    RMContainer rmContainer = mock(RMContainerImpl.class);
    when(scheduler.getRMContainer(container.getId())).
        thenReturn(rmContainer);
    when(container.getNodeId()).thenReturn(
        BuilderUtils.newNodeId("localhost", 0));
    
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.CONTAINER_ALLOCATED));
    
    assertEquals(RMAppAttemptState.ALLOCATED_SAVING, 
        applicationAttempt.getAppAttemptState());

    if (UserGroupInformation.isSecurityEnabled()) {
      // Before SAVED state, can't create ClientToken as at this time
      // ClientTokenMasterKey has not been registered in the SecretManager
      assertNull(applicationAttempt.createClientToken("some client"));
    }

    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_NEW_SAVED));

    if (UserGroupInformation.isSecurityEnabled()) {
      // Before SAVED state, can't create ClientToken as at this time
      // ClientTokenMasterKey has not been registered in the SecretManager
      assertNotNull(applicationAttempt.createClientToken("some client"));
    }

    testAppAttemptAllocatedState(container);
    
    return container;
  }
  
  private void launchApplicationAttempt(Container container) {
    launchApplicationAttempt(container, RMAppAttemptState.LAUNCHED);
  }

  private void launchApplicationAttempt(Container container,
                                        RMAppAttemptState state) {
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCHED));

    testAppAttemptLaunchedState(container, state);
  }

  private void runApplicationAttempt(Container container,
      String host, 
      int rpcPort, 
      String trackingUrl, boolean unmanagedAM) {
    applicationAttempt.handle(
        new RMAppAttemptRegistrationEvent(
            applicationAttempt.getAppAttemptId(),
            host, rpcPort, trackingUrl));
    
    testAppAttemptRunningState(container, host, rpcPort, trackingUrl, 
        unmanagedAM);
  }

  private void unregisterApplicationAttempt(Container container,
      FinalApplicationStatus finalStatus, String trackingUrl,
      String diagnostics) {
    applicationAttempt.handle(
        new RMAppAttemptUnregistrationEvent(
            applicationAttempt.getAppAttemptId(),
            trackingUrl, finalStatus, diagnostics));
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishingState(container, finalStatus,
        trackingUrl, diagnostics);
  }

  private void testUnmanagedAMSuccess(String url) {
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);
    // submit AM and check it goes to LAUNCHED state
    scheduleApplicationAttempt();
    testAppAttemptLaunchedState(null, RMAppAttemptState.LAUNCHED);
    verify(amLivelinessMonitor, times(1)).register(
        applicationAttempt.getAppAttemptId());

    // launch AM
    runApplicationAttempt(null, "host", 8042, url, true);

    // complete a container
    Container container = mock(Container.class);
    when(container.getNodeId()).thenReturn(NodeId.newInstance("host", 1234));
    application.handle(new RMAppRunningOnNodeEvent(application.getApplicationId(),
        container.getNodeId()));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), mock(ContainerStatus.class),
        container.getNodeId()));
    // complete AM
    String diagnostics = "Successful";
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
        applicationAttempt.getAppAttemptId(), url, finalStatus,
        diagnostics));
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishedState(null, finalStatus, url, diagnostics, 1,
        true);
    assertFalse(transferStateFromPreviousAttempt);
  }

  private void sendAttemptUpdateSavedEvent(RMAppAttempt applicationAttempt) {
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    applicationAttempt.handle(
      new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(), 
          RMAppAttemptEventType.ATTEMPT_UPDATE_SAVED));
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testUsageReport(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    // scheduler has info on running apps
    ApplicationAttemptId attemptId = applicationAttempt.getAppAttemptId();
    ApplicationResourceUsageReport appResUsgRpt =
            mock(ApplicationResourceUsageReport.class);
    when(appResUsgRpt.getMemorySeconds()).thenReturn(123456L);
    when(appResUsgRpt.getVcoreSeconds()).thenReturn(55544L);
    when(scheduler.getAppResourceUsageReport(any(ApplicationAttemptId.class)))
    .thenReturn(appResUsgRpt);

    // start and finish the attempt
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(attemptId,
        "", FinalApplicationStatus.SUCCEEDED, ""));

    // expect usage stats to come from the scheduler report
    ApplicationResourceUsageReport report = 
        applicationAttempt.getApplicationResourceUsageReport();
    assertEquals(123456L, report.getMemorySeconds());
    assertEquals(55544L, report.getVcoreSeconds());

    // finish app attempt and remove it from scheduler 
    when(appResUsgRpt.getMemorySeconds()).thenReturn(223456L);
    when(appResUsgRpt.getVcoreSeconds()).thenReturn(75544L);
    sendAttemptUpdateSavedEvent(applicationAttempt);
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        attemptId, 
        ContainerStatus.newInstance(
            amContainer.getId(), ContainerState.COMPLETE, "", 0), anyNodeId));

    when(scheduler.getSchedulerAppInfo(eq(attemptId))).thenReturn(null);

    report = applicationAttempt.getApplicationResourceUsageReport();
    assertEquals(223456, report.getMemorySeconds());
    assertEquals(75544, report.getVcoreSeconds());
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testUnmanagedAMUnexpectedRegistration(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);

    // submit AM and check it goes to SUBMITTED state
    submitApplicationAttempt();
    assertEquals(RMAppAttemptState.SUBMITTED,
        applicationAttempt.getAppAttemptState());

    // launch AM and verify attempt failed
    applicationAttempt.handle(new RMAppAttemptRegistrationEvent(
        applicationAttempt.getAppAttemptId(), "host", 8042, "oldtrackingurl"));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptSubmittedToFailedState(
        "Unmanaged AM must register after AM attempt reaches LAUNCHED state.");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testUnmanagedAMContainersCleanup(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    unmanagedAM = true;
    when(submissionContext.getUnmanagedAM()).thenReturn(true);
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    // submit AM and check it goes to SUBMITTED state
    submitApplicationAttempt();
    // launch AM and verify attempt failed
    applicationAttempt.handle(new RMAppAttemptRegistrationEvent(
      applicationAttempt.getAppAttemptId(), "host", 8042, "oldtrackingurl"));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertFalse(transferStateFromPreviousAttempt);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testNewToKilled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.NEW,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testNewToRecovered(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.RECOVER));
    testAppAttemptRecoveredState();
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testSubmittedToKilled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    submitApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testScheduledToKilled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    scheduleApplicationAttempt();
    applicationAttempt.handle(        
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.SCHEDULED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(null, EMPTY_DIAGNOSTICS);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAMCrashAtScheduled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    // This is to test sending CONTAINER_FINISHED event at SCHEDULED state.
    // Verify the state transition is correct.
    scheduleApplicationAttempt();
    ContainerStatus cs =
        SchedulerUtils.createAbnormalContainerStatus(
            BuilderUtils.newContainerId(
                applicationAttempt.getAppAttemptId(), 1),
            SchedulerUtils.LOST_CONTAINER);
    // send CONTAINER_FINISHED event at SCHEDULED state,
    // The state should be FINAL_SAVING with previous state SCHEDULED
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), cs, anyNodeId));
    // createApplicationAttemptState will return previous state (SCHEDULED),
    // if the current state is FINAL_SAVING.
    assertEquals(YarnApplicationAttemptState.SCHEDULED,
        applicationAttempt.createApplicationAttemptState());
    // send ATTEMPT_UPDATE_SAVED event,
    // verify the state is changed to state FAILED.
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAllocatedToKilled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptKilledState(amContainer, EMPTY_DIAGNOSTICS);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAllocatedToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    String diagnostics = "Launch Failed";
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCH_FAILED, diagnostics));
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptFailedState(amContainer, diagnostics);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  @Timeout(value = 10)
  public void testAllocatedToRunning(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    // Register attempt event arrives before launched attempt event
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    launchApplicationAttempt(amContainer, RMAppAttemptState.RUNNING);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  @Timeout(value = 10)
  public void testCreateAppAttemptReport(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    RMAppAttemptState[] attemptStates = RMAppAttemptState.values();
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.KILL));
    // ALL RMAppAttemptState TO BE CHECK
    RMAppAttempt attempt = spy(applicationAttempt);
    for (RMAppAttemptState rmAppAttemptState : attemptStates) {
      when(attempt.getState()).thenReturn(rmAppAttemptState);
      attempt.createApplicationAttemptReport();
    }
  }

  @Timeout(value = 10)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testLaunchedAtFinalSaving(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();

    // ALLOCATED->FINAL_SAVING
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.KILL));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
        applicationAttempt.getAppAttemptState());

    // verify for both launched and launch_failed transitions in final_saving
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.LAUNCHED));
    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCH_FAILED, "Launch Failed"));

    assertEquals(RMAppAttemptState.FINAL_SAVING,
        applicationAttempt.getAppAttemptState());

    testAppAttemptKilledState(amContainer, EMPTY_DIAGNOSTICS);

    // verify for both launched and launch_failed transitions in killed
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.LAUNCHED));
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCH_FAILED, "Launch Failed"));
    assertEquals(RMAppAttemptState.KILLED,
        applicationAttempt.getAppAttemptState());
  }

  @Timeout(value = 10)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAttemptAddedAtFinalSaving(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    submitApplicationAttempt();

    // SUBMITTED->FINAL_SAVING
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
                   .getAppAttemptId(), RMAppAttemptEventType.KILL));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
                   applicationAttempt.getAppAttemptState());

    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
                   .getAppAttemptId(), RMAppAttemptEventType.ATTEMPT_ADDED));

    assertEquals(RMAppAttemptState.FINAL_SAVING,
                   applicationAttempt.getAppAttemptState());
  }

  @Timeout(value = 10)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAttemptRegisteredAtFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);

    //send CONTAINER_FINISHED event
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0,
        amContainer.getResource()), anyNodeId));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
        applicationAttempt.getAppAttemptState());

    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());

    //send REGISTERED event
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.REGISTERED));

    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAttemptLaunchFailedAtFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    //send CONTAINER_FINISHED event
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0,
        amContainer.getResource()), anyNodeId));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
        applicationAttempt.getAppAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());

    //send LAUNCH_FAILED event
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.LAUNCH_FAILED));

    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAMCrashAtAllocated(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    String containerDiagMsg = "some error";
    int exitCode = 123;
    ContainerStatus cs =
        BuilderUtils.newContainerStatus(amContainer.getId(),
          ContainerState.COMPLETE, containerDiagMsg, exitCode,
          amContainer.getResource());
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), cs, anyNodeId));
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
    boolean shouldCheckURL = (applicationAttempt.getTrackingUrl() != null);
    verifyAMCrashAtAllocatedDiagnosticInfo(applicationAttempt.getDiagnostics(),
      exitCode, shouldCheckURL);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testRunningToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    String containerDiagMsg = "some error";
    int exitCode = 123;
    ContainerStatus cs = BuilderUtils.newContainerStatus(amContainer.getId(),
        ContainerState.COMPLETE, containerDiagMsg, exitCode,
            amContainer.getResource());
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        appAttemptId, cs, anyNodeId));

    // ignored ContainerFinished and Expire at FinalSaving if we were supposed
    // to Failed state.
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0,
            amContainer.getResource()), anyNodeId));
    applicationAttempt.handle(new RMAppAttemptEvent(
      applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, application.getRanNodes().size());
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testRunningToKilled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.KILL));

    // ignored ContainerFinished and Expire at FinalSaving if we were supposed
    // to Killed state.
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0,
            amContainer.getResource()), anyNodeId));
    applicationAttempt.handle(new RMAppAttemptEvent(
      applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState()); 
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.KILLED,
        applicationAttempt.getAppAttemptState());
    assertEquals(0, applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, application.getRanNodes().size());
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.KILLED);
  }

  @Timeout(value = 10)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testLaunchedExpire(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertTrue(applicationAttempt.getDiagnostics().contains("timed out"),
        "expire diagnostics missing");
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @SuppressWarnings("unchecked")
  @Timeout(value = 10)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testLaunchedFailWhileAHSEnabled(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Configuration myConf = new Configuration(conf);
    myConf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, true);
    ApplicationId applicationId = MockApps.newAppID(appId);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 2);
    RMAppAttempt  myApplicationAttempt =
        new RMAppAttemptImpl(applicationAttempt.getAppAttemptId(),
            spyRMContext, scheduler,masterService,
            submissionContext, myConf,
            Collections.singletonList(BuilderUtils.newResourceRequest(
                RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                submissionContext.getResource(), 1)), application);

    //submit, schedule and allocate app attempt
    myApplicationAttempt.handle(
        new RMAppAttemptEvent(myApplicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.START));
    myApplicationAttempt.handle(
        new RMAppAttemptEvent(
            myApplicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_ADDED));

    Container amContainer = mock(Container.class);
    Resource resource = Resources.createResource(2048);
    when(amContainer.getId()).thenReturn(
        BuilderUtils.newContainerId(myApplicationAttempt.getAppAttemptId(), 1));
    when(amContainer.getResource()).thenReturn(resource);
    Allocation allocation = mock(Allocation.class);
    when(allocation.getContainers()).
        thenReturn(Collections.singletonList(amContainer));
    when(scheduler.allocate(any(ApplicationAttemptId.class), any(List.class),
        any(), any(List.class), any(), any(),
        any(ContainerUpdates.class)))
        .thenReturn(allocation);
    RMContainer rmContainer = mock(RMContainerImpl.class);
    when(scheduler.getRMContainer(amContainer.getId())).thenReturn(rmContainer);

    myApplicationAttempt.handle(
        new RMAppAttemptEvent(myApplicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.CONTAINER_ALLOCATED));
    assertEquals(RMAppAttemptState.ALLOCATED_SAVING,
        myApplicationAttempt.getAppAttemptState());
    myApplicationAttempt.handle(
        new RMAppAttemptEvent(myApplicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.ATTEMPT_NEW_SAVED));

    // launch app attempt
    myApplicationAttempt.handle(
        new RMAppAttemptEvent(myApplicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCHED));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        myApplicationAttempt.createApplicationAttemptState());

    //fail container right after launched
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    myApplicationAttempt.handle(
        new RMAppAttemptContainerFinishedEvent(
            myApplicationAttempt.getAppAttemptId(),
            BuilderUtils.newContainerStatus(amContainer.getId(),
                ContainerState.COMPLETE, "", 0,
                amContainer.getResource()), anyNodeId));
    sendAttemptUpdateSavedEvent(myApplicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        myApplicationAttempt.getAppAttemptState());
    String rmAppPageUrl = pjoin(AHS_WEBAPP_ADDR, "applicationhistory", "app",
        myApplicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, myApplicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, myApplicationAttempt.getTrackingUrl());
  }

  @Timeout(value = 20)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testRunningExpire(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(new RMAppAttemptEvent(
        applicationAttempt.getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());
    assertTrue(applicationAttempt.getDiagnostics().contains("timed out"),
        "expire diagnostics missing");
    String rmAppPageUrl = pjoin(RM_WEBAPP_ADDR, "cluster", "app",
        applicationAttempt.getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testUnregisterToKilledFinishing(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.KILLED, "newtrackingurl",
        "Killed by user");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testTrackingUrlUnmanagedAM(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testUnmanagedAMSuccess("oldTrackingUrl");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testEmptyTrackingUrlUnmanagedAM(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testUnmanagedAMSuccess("");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testNullTrackingUrlUnmanagedAM(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testUnmanagedAMSuccess(null);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testManagedAMWithTrackingUrl(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testTrackingUrlManagedAM("theTrackingUrl");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testManagedAMWithEmptyTrackingUrl(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testTrackingUrlManagedAM("");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testManagedAMWithNullTrackingUrl(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    testTrackingUrlManagedAM(null);
  }

  private void testTrackingUrlManagedAM(String url) {
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, url, false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.SUCCEEDED, url, "Successful");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testUnregisterToSuccessfulFinishing(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    unregisterApplicationAttempt(amContainer,
        FinalApplicationStatus.SUCCEEDED, "mytrackingurl", "Successful");
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishingKill(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.FAILED;
    String trackingUrl = "newtrackingurl";
    String diagnostics = "Job failed";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(), 
            RMAppAttemptEventType.KILL));
    testAppAttemptFinishingState(amContainer, finalStatus, trackingUrl,
        diagnostics);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishingExpire(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    applicationAttempt.handle(
        new RMAppAttemptEvent(
            applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.EXPIRE));
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
        diagnostics, 0, false);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishingToFinishing(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    // container must be AM container to move from FINISHING to FINISHED
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(
        new RMAppAttemptContainerFinishedEvent(
            applicationAttempt.getAppAttemptId(),
            BuilderUtils.newContainerStatus(
                BuilderUtils.newContainerId(
                    applicationAttempt.getAppAttemptId(), 42),
                ContainerState.COMPLETE, "", 0,
                    amContainer.getResource()), anyNodeId));
    testAppAttemptFinishingState(amContainer, finalStatus, trackingUrl,
        diagnostics);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testSuccessfulFinishingToFinished(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    unregisterApplicationAttempt(amContainer, finalStatus, trackingUrl,
        diagnostics);
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(
        new RMAppAttemptContainerFinishedEvent(
            applicationAttempt.getAppAttemptId(),
            BuilderUtils.newContainerStatus(amContainer.getId(),
                ContainerState.COMPLETE, "", 0,
                    amContainer.getResource()), anyNodeId));
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
        diagnostics, 0, false);
  }

  // While attempt is at FINAL_SAVING, Contaienr_Finished event may come before
  // Attempt_Saved event, we stay on FINAL_SAVING on Container_Finished event
  // and then directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved
  // event
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinalSavingToFinishedWithContainerFinished(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successful";
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
      applicationAttempt.getAppAttemptId(), trackingUrl, finalStatus,
      diagnostics));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    // Container_finished event comes before Attempt_Saved event.
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
        amContainer.getId(), ContainerState.COMPLETE, "", 0,
            amContainer.getResource()), anyNodeId));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    // send attempt_saved
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
      diagnostics, 0, false);
  }

  // While attempt is at FINAL_SAVING, Expire event may come before
  // Attempt_Saved event, we stay on FINAL_SAVING on Expire event and then
  // directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved event.
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinalSavingToFinishedWithExpire(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
    String trackingUrl = "mytrackingurl";
    String diagnostics = "Successssseeeful";
    applicationAttempt.handle(new RMAppAttemptUnregistrationEvent(
      applicationAttempt.getAppAttemptId(), trackingUrl, finalStatus,
      diagnostics));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    // Expire event comes before Attempt_saved event.
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
      .getAppAttemptId(), RMAppAttemptEventType.EXPIRE));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
      applicationAttempt.getAppAttemptState());
    // send attempt_saved
    sendAttemptUpdateSavedEvent(applicationAttempt);
    testAppAttemptFinishedState(amContainer, finalStatus, trackingUrl,
      diagnostics, 0, false);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishedContainer(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);

    // Complete one container
    ContainerId containerId1 = BuilderUtils.newContainerId(applicationAttempt
        .getAppAttemptId(), 2);
    Container container1 = mock(Container.class);
    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    when(container1.getId()).thenReturn(
        containerId1);
    when(containerStatus1.getContainerId()).thenReturn(containerId1);
    when(container1.getNodeId()).thenReturn(NodeId.newInstance("host", 1234));

    application.handle(new RMAppRunningOnNodeEvent(application
        .getApplicationId(),
        container1.getNodeId()));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), containerStatus1,
        container1.getNodeId()));

    ArgumentCaptor<RMNodeFinishedContainersPulledByAMEvent> captor =
        ArgumentCaptor.forClass(RMNodeFinishedContainersPulledByAMEvent.class);

    // Verify justFinishedContainers
    assertEquals(1, applicationAttempt.getJustFinishedContainers()
        .size());
    assertEquals(container1.getId(), applicationAttempt
        .getJustFinishedContainers().get(0).getContainerId());
    assertEquals(0, getFinishedContainersSentToAM(applicationAttempt)
        .size());

    // Verify finishedContainersSentToAM gets container after pull
    List<ContainerStatus> containerStatuses = applicationAttempt
        .pullJustFinishedContainers();
    assertEquals(1, containerStatuses.size());
    Mockito.verify(rmnodeEventHandler, never()).handle(Mockito
        .any(RMNodeEvent.class));
    assertTrue(applicationAttempt.getJustFinishedContainers().isEmpty());
    assertEquals(1, getFinishedContainersSentToAM(applicationAttempt)
        .size());

    // Verify container is acked to NM via the RMNodeEvent after second pull
    containerStatuses = applicationAttempt.pullJustFinishedContainers();
    assertEquals(0, containerStatuses.size());
    Mockito.verify(rmnodeEventHandler).handle(captor.capture());
    assertEquals(container1.getId(), captor.getValue().getContainers()
        .get(0));
    assertTrue(applicationAttempt.getJustFinishedContainers().isEmpty());
    assertEquals(0, getFinishedContainersSentToAM(applicationAttempt)
        .size());

    // verify if no containers to acknowledge to NM then event should not be
    // triggered. Number of times event invoked is 1 i.e on second pull
    containerStatuses = applicationAttempt.pullJustFinishedContainers();
    assertEquals(0, containerStatuses.size());
    Mockito.verify(rmnodeEventHandler, times(1))
        .handle(Mockito.any(RMNodeEvent.class));
  }

  /**
   * Check a completed container that is not yet pulled by AM heartbeat,
   * is ACKed to NM for cleanup when the AM container exits.
   */
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishedContainerNotBeingPulledByAMHeartbeat(
      boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);

    application.handle(new RMAppRunningOnNodeEvent(application
        .getApplicationId(), amContainer.getNodeId()));

    // Complete a non-AM container
    ContainerId containerId1 = BuilderUtils.newContainerId(applicationAttempt
        .getAppAttemptId(), 2);
    Container container1 = mock(Container.class);
    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    when(container1.getId()).thenReturn(
        containerId1);
    when(containerStatus1.getContainerId()).thenReturn(containerId1);
    when(container1.getNodeId()).thenReturn(NodeId.newInstance("host", 1234));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), containerStatus1,
        container1.getNodeId()));

    // Verify justFinishedContainers
    ArgumentCaptor<RMNodeFinishedContainersPulledByAMEvent> captor =
        ArgumentCaptor.forClass(RMNodeFinishedContainersPulledByAMEvent.class);
    assertEquals(1, applicationAttempt.getJustFinishedContainers()
        .size());
    assertEquals(container1.getId(), applicationAttempt
        .getJustFinishedContainers().get(0).getContainerId());
    assertTrue(
        getFinishedContainersSentToAM(applicationAttempt).isEmpty());

    // finish AM container to emulate AM exit event
    containerStatus1 = mock(ContainerStatus.class);
    ContainerId amContainerId = amContainer.getId();
    when(containerStatus1.getContainerId()).thenReturn(amContainerId);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), containerStatus1,
        amContainer.getNodeId()));

    Mockito.verify(rmnodeEventHandler, times(2)).handle(captor.capture());
    List<RMNodeFinishedContainersPulledByAMEvent> containerPulledEvents =
        captor.getAllValues();
    // Verify AM container is acked to NM via the RMNodeEvent immediately
    assertEquals(amContainer.getId(),
        containerPulledEvents.get(0).getContainers().get(0));
    // Verify the non-AM container is acked to NM via the RMNodeEvent
    assertEquals(container1.getId(),
        containerPulledEvents.get(1).getContainers().get(0));
    assertTrue(applicationAttempt.getJustFinishedContainers().isEmpty(),
        "No container shall be added to justFinishedContainers" +
        " as soon as AM container exits");
    assertTrue(
        getFinishedContainersSentToAM(applicationAttempt).isEmpty());
  }

  /**
   * Check a completed container is ACKed to NM for cleanup after the AM
   * container has exited.
   */
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFinishedContainerAfterAMExit(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);

    // finish AM container to emulate AM exit event
    ContainerStatus containerStatus1 = mock(ContainerStatus.class);
    ContainerId amContainerId = amContainer.getId();
    when(containerStatus1.getContainerId()).thenReturn(amContainerId);
    application.handle(new RMAppRunningOnNodeEvent(application
        .getApplicationId(),
        amContainer.getNodeId()));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), containerStatus1,
        amContainer.getNodeId()));

    // Verify AM container is acked to NM via the RMNodeEvent immediately
    ArgumentCaptor<RMNodeFinishedContainersPulledByAMEvent> captor =
        ArgumentCaptor.forClass(RMNodeFinishedContainersPulledByAMEvent.class);
    Mockito.verify(rmnodeEventHandler).handle(captor.capture());
    assertEquals(amContainer.getId(),
        captor.getValue().getContainers().get(0));

    // Complete a non-AM container
    ContainerId containerId1 = BuilderUtils.newContainerId(applicationAttempt
        .getAppAttemptId(), 2);
    Container container1 = mock(Container.class);
    containerStatus1 = mock(ContainerStatus.class);
    when(container1.getId()).thenReturn(containerId1);
    when(containerStatus1.getContainerId()).thenReturn(containerId1);
    when(container1.getNodeId()).thenReturn(NodeId.newInstance("host", 1234));
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), containerStatus1,
        container1.getNodeId()));

    // Verify container is acked to NM via the RMNodeEvent immediately
    captor = ArgumentCaptor.forClass(
        RMNodeFinishedContainersPulledByAMEvent.class);
    Mockito.verify(rmnodeEventHandler, times(2)).handle(captor.capture());
    assertEquals(container1.getId(),
        captor.getAllValues().get(1).getContainers().get(0));
    assertTrue(applicationAttempt.getJustFinishedContainers().isEmpty(),
        "No container shall be added to justFinishedContainers" +
        " after AM container exited");
    assertTrue(getFinishedContainersSentToAM(applicationAttempt).isEmpty());
  }

  private static List<ContainerStatus> getFinishedContainersSentToAM(
      RMAppAttempt applicationAttempt) {
    List<ContainerStatus> containers = new ArrayList<ContainerStatus>();
    for (List<ContainerStatus> containerStatuses: applicationAttempt
        .getFinishedContainersSentToAMReference().values()) {
      containers.addAll(containerStatuses);
    }
    return containers;
  }

  // this is to test user can get client tokens only after the client token
  // master key is saved in the state store and also registered in
  // ClientTokenSecretManager
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testGetClientToken(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    assumeTrue(isSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();

    // before attempt is launched, can not get ClientToken
    Token<ClientToAMTokenIdentifier> token =
        applicationAttempt.createClientToken(null);
    assertNull(token);

    launchApplicationAttempt(amContainer);
    // after attempt is launched , can get ClientToken
    token = applicationAttempt.createClientToken(null);
    assertNull(token);
    token = applicationAttempt.createClientToken("clientuser");
    assertNotNull(token);

    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
      .getAppAttemptId(), RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    // after attempt is killed, can not get Client Token
    token = applicationAttempt.createClientToken(null);
    assertNull(token);
    token = applicationAttempt.createClientToken("clientuser");
    assertNull(token);
  }

  // this is to test master key is saved in the secret manager only after
  // attempt is launched and in secure-mode
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testApplicationAttemptMasterKey(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    ApplicationAttemptId appid = applicationAttempt.getAppAttemptId();
    boolean isMasterKeyExisted = clientToAMTokenManager.hasMasterKey(appid);

    if (isSecurityEnabled) {
      assertTrue(isMasterKeyExisted);
      assertNotNull(clientToAMTokenManager.getMasterKey(appid));
    } else {
      assertFalse(isMasterKeyExisted);
    }
    launchApplicationAttempt(amContainer);
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
      .getAppAttemptId(), RMAppAttemptEventType.KILL));
    assertEquals(YarnApplicationAttemptState.LAUNCHED,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    // after attempt is killed, can not get MasterKey
    isMasterKeyExisted = clientToAMTokenManager.hasMasterKey(appid);
    assertFalse(isMasterKeyExisted);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testFailedToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    // create a failed attempt.
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    when(application.getMaxAppAttempts()).thenReturn(2);
    when(application.getNumFailedAppAttempts()).thenReturn(1);

    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    ContainerStatus cs1 =
        ContainerStatus.newInstance(amContainer.getId(),
          ContainerState.COMPLETE, "some error", 123);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs1, anyNodeId));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    // should not kill containers when attempt fails.
    assertTrue(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);

    // failed attempt captured the container finished event.
    assertEquals(0, applicationAttempt.getJustFinishedContainers().size());
    ContainerStatus cs2 =
        ContainerStatus.newInstance(ContainerId.newContainerId(appAttemptId, 2),
          ContainerState.COMPLETE, "", 0);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs2, anyNodeId));
    assertEquals(1, applicationAttempt.getJustFinishedContainers().size());
    boolean found = false;
    for (ContainerStatus containerStatus:applicationAttempt
        .getJustFinishedContainers()) {
      if (cs2.getContainerId().equals(containerStatus.getContainerId())) {
        found = true;
      }
    }
    assertTrue(found);
  }

  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testContainerRemovedBeforeAllocate(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    scheduleApplicationAttempt();

    // Mock the allocation of AM container
    Container container = mock(Container.class);
    Resource resource = Resources.createResource(2048);
    when(container.getId()).thenReturn(
        BuilderUtils.newContainerId(applicationAttempt.getAppAttemptId(), 1));
    when(container.getResource()).thenReturn(resource);
    Allocation allocation = mock(Allocation.class);
    when(allocation.getContainers()).
        thenReturn(Collections.singletonList(container));
    when(scheduler.allocate(any(ApplicationAttemptId.class), any(List.class),
        any(), any(), any(), any(),
        any(ContainerUpdates.class))).
        thenReturn(allocation);

    //container removed, so return null
    when(scheduler.getRMContainer(container.getId())).
        thenReturn(null);

    applicationAttempt.handle(
        new RMAppAttemptEvent(applicationAttempt.getAppAttemptId(),
            RMAppAttemptEventType.CONTAINER_ALLOCATED));
    assertEquals(RMAppAttemptState.SCHEDULED,
        applicationAttempt.getAppAttemptState());
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testContainersCleanupForLastAttempt(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    // create a failed attempt.
    applicationAttempt =
        new RMAppAttemptImpl(applicationAttempt.getAppAttemptId(), spyRMContext,
          scheduler, masterService, submissionContext, new Configuration(),
            Collections.singletonList(BuilderUtils.newResourceRequest(
              RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
              submissionContext.getResource(), 1)), application);
    when(submissionContext.getKeepContainersAcrossApplicationAttempts())
      .thenReturn(true);
    when(submissionContext.getMaxAppAttempts()).thenReturn(1);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    ContainerStatus cs1 =
        ContainerStatus.newInstance(amContainer.getId(),
          ContainerState.COMPLETE, "some error", 123);
    ApplicationAttemptId appAttemptId = applicationAttempt.getAppAttemptId();
    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
      appAttemptId, cs1, anyNodeId));
    assertEquals(YarnApplicationAttemptState.RUNNING,
        applicationAttempt.createApplicationAttemptState());
    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
      applicationAttempt.getAppAttemptState());
    assertFalse(transferStateFromPreviousAttempt);
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }
  
  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testScheduleTransitionReplaceAMContainerRequestWithDefaults(
      boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    YarnScheduler mockScheduler = mock(YarnScheduler.class);
    when(mockScheduler.allocate(any(ApplicationAttemptId.class),
        any(List.class), any(List.class), any(List.class), any(List.class), any(List.class),
        any(ContainerUpdates.class)))
        .thenAnswer(new Answer<Allocation>() {

          @SuppressWarnings("rawtypes")
          @Override
          public Allocation answer(InvocationOnMock invocation)
              throws Throwable {
            ResourceRequest rr =
                (ResourceRequest) ((List) invocation.getArguments()[1]).get(0);
            
            // capacity shouldn't changed
            assertEquals(Resource.newInstance(3333, 1), rr.getCapability());
            assertEquals("label-expression", rr.getNodeLabelExpression());
            
            // priority, #container, relax-locality will be changed
            assertEquals(RMAppAttemptImpl.AM_CONTAINER_PRIORITY, rr.getPriority());
            assertEquals(1, rr.getNumContainers());
            assertEquals(ResourceRequest.ANY, rr.getResourceName());

            // just return an empty allocation
            List l = new ArrayList();
            Set s = new HashSet();
            return new Allocation(l, Resources.none(), s, s, l);
          }
        });
    
    // create an attempt.
    applicationAttempt =
        new RMAppAttemptImpl(applicationAttempt.getAppAttemptId(),
            spyRMContext, scheduler, masterService, submissionContext,
            new Configuration(), Collections.singletonList(
                ResourceRequest.newInstance(Priority.UNDEFINED, "host1",
                    Resource.newInstance(3333, 1), 3,
                false, "label-expression")), application);
    new RMAppAttemptImpl.ScheduleTransition().transition(
        (RMAppAttemptImpl) applicationAttempt, null);
  }

  @Timeout(value = 30)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testNewToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.FAIL, FAILED_DIAGNOSTICS));
    assertEquals(YarnApplicationAttemptState.NEW,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptFailedState(null, FAILED_DIAGNOSTICS);
    verifyTokenCount(applicationAttempt.getAppAttemptId(), 1);
  }

  @Timeout(value = 30)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testSubmittedToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    submitApplicationAttempt();
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.FAIL, FAILED_DIAGNOSTICS));
    assertEquals(YarnApplicationAttemptState.SUBMITTED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptFailedState(null, FAILED_DIAGNOSTICS);
  }

  @Timeout(value = 30)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testScheduledToFailed(boolean pIsSecurityEnabled) throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    scheduleApplicationAttempt();
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.FAIL, FAILED_DIAGNOSTICS));
    assertEquals(YarnApplicationAttemptState.SCHEDULED,
        applicationAttempt.createApplicationAttemptState());
    testAppAttemptFailedState(null, FAILED_DIAGNOSTICS);
  }

  @Timeout(value = 30)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testAllocatedToFailedUserTriggeredFailEvent(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    assertEquals(YarnApplicationAttemptState.ALLOCATED,
        applicationAttempt.createApplicationAttemptState());
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.FAIL, FAILED_DIAGNOSTICS));
    testAppAttemptFailedState(amContainer, FAILED_DIAGNOSTICS);
  }

  @Timeout(value = 30)
  @ParameterizedTest
  @MethodSource("getTestParameters")
  public void testRunningToFailedUserTriggeredFailEvent(boolean pIsSecurityEnabled)
      throws Exception {
    initTestRMAppAttemptTransitions(pIsSecurityEnabled);
    Container amContainer = allocateApplicationAttempt();
    launchApplicationAttempt(amContainer);
    runApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
    applicationAttempt.handle(new RMAppAttemptEvent(applicationAttempt
        .getAppAttemptId(), RMAppAttemptEventType.FAIL, FAILED_DIAGNOSTICS));
    assertEquals(RMAppAttemptState.FINAL_SAVING,
        applicationAttempt.getAppAttemptState());

    sendAttemptUpdateSavedEvent(applicationAttempt);
    assertEquals(RMAppAttemptState.FAILED,
        applicationAttempt.getAppAttemptState());

    NodeId anyNodeId = NodeId.newInstance("host", 1234);
    applicationAttempt.handle(new RMAppAttemptContainerFinishedEvent(
        applicationAttempt.getAppAttemptId(), BuilderUtils.newContainerStatus(
            amContainer.getId(), ContainerState.COMPLETE, "", 0,
            amContainer.getResource()), anyNodeId));

    assertEquals(1, applicationAttempt.getJustFinishedContainers().size());
    assertEquals(amContainer, applicationAttempt.getMasterContainer());
    assertEquals(0, application.getRanNodes().size());
    String rmAppPageUrl =
        pjoin(RM_WEBAPP_ADDR, "cluster", "app", applicationAttempt
            .getAppAttemptId().getApplicationId());
    assertEquals(rmAppPageUrl, applicationAttempt.getOriginalTrackingUrl());
    assertEquals(rmAppPageUrl, applicationAttempt.getTrackingUrl());
    verifyAMHostAndPortInvalidated();
    verifyApplicationAttemptFinished(RMAppAttemptState.FAILED);
  }

  private void verifyAMCrashAtAllocatedDiagnosticInfo(String diagnostics,
        int exitCode, boolean shouldCheckURL) {
    assertTrue(diagnostics.contains("logs"),
        "Diagnostic information does not point the logs to the users");
    assertTrue(diagnostics.contains(applicationAttempt.getAppAttemptId().toString()),
        "Diagnostic information does not contain application attempt id");
    assertTrue(diagnostics.contains("exitCode: " + exitCode),
        "Diagnostic information does not contain application exit code");
    if (shouldCheckURL) {
      assertTrue(diagnostics.contains(applicationAttempt.getTrackingUrl()),
          "Diagnostic information does not contain application proxy URL");
    }
  }

  private void verifyTokenCount(ApplicationAttemptId appAttemptId, int count) {
    verify(amRMTokenManager, times(count)).applicationMasterFinished(appAttemptId);
    if (UserGroupInformation.isSecurityEnabled()) {
      verify(clientToAMTokenManager, times(count)).unRegisterApplication(appAttemptId);
      if (count > 0) {
        assertNull(applicationAttempt.createClientToken("client"));
      }
    }
  }

  private void verifyUrl(String url1, String url2) {
    if (url1 == null || url1.trim().isEmpty()) {
      assertEquals("N/A", url2);
    } else {
      assertEquals(url1, url2);
    }
  }

  private void verifyAttemptFinalStateSaved() {
    verify(store, times(1)).updateApplicationAttemptState(
      any(ApplicationAttemptStateData.class));
  }

  private void verifyAMHostAndPortInvalidated() {
    assertEquals("N/A", applicationAttempt.getHost());
    assertEquals(-1, applicationAttempt.getRpcPort());
  }

  private void verifyApplicationAttemptFinished(RMAppAttemptState state) {
    ArgumentCaptor<RMAppAttemptState> finalState =
        ArgumentCaptor.forClass(RMAppAttemptState.class);
    verify(writer).applicationAttemptFinished(
        any(RMAppAttempt.class), finalState.capture());
    assertEquals(state, finalState.getValue());
    finalState =
        ArgumentCaptor.forClass(RMAppAttemptState.class);
    verify(publisher).appAttemptFinished(any(RMAppAttempt.class), finalState.capture(),
        any(RMApp.class), anyLong());
    assertEquals(state, finalState.getValue());
  }

}
