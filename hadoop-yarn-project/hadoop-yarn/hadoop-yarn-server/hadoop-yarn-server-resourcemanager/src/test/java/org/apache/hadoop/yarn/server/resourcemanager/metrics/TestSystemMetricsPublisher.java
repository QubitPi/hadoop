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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.recovery.MemoryTimelineStateStore;
import org.apache.hadoop.yarn.server.timeline.recovery.TimelineStateStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSystemMetricsPublisher {

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, 0}, {true, 1}});
  }

  private static ApplicationHistoryServer timelineServer;
  private static TimelineServiceV1Publisher metricsPublisher;
  private static TimelineStore store;

  private boolean rmTimelineServerV1PublisherBatchEnabled;
  private int rmTimelineServerV1PublisherInterval;

  private void initTestSystemMetricsPublisher(boolean pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    this.rmTimelineServerV1PublisherBatchEnabled = pRmTimelineServerV1PublisherBatchEnabled;
    this.rmTimelineServerV1PublisherInterval = pRmTimelineServerV1PublisherInterval;
    setup();
  }

  public void setup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED, true);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
        MemoryTimelineStore.class, TimelineStore.class);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STATE_STORE_CLASS,
        MemoryTimelineStateStore.class, TimelineStateStore.class);
    conf.setInt(
        YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
        2);
    conf.setBoolean(YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_BATCH_ENABLED,
        rmTimelineServerV1PublisherBatchEnabled);
    conf.setInt(YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL,
        rmTimelineServerV1PublisherInterval);

    timelineServer = new ApplicationHistoryServer();
    timelineServer.init(conf);
    timelineServer.start();
    store = timelineServer.getTimelineStore();

    metricsPublisher = new TimelineServiceV1Publisher();
    metricsPublisher.init(conf);
    metricsPublisher.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (metricsPublisher != null) {
      metricsPublisher.stop();
    }
    if (timelineServer != null) {
      timelineServer.stop();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  @Timeout(value = 10)
  @SuppressWarnings("checkstyle:methodlength")
  public void testPublishApplicationMetrics(boolean pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    initTestSystemMetricsPublisher(pRmTimelineServerV1PublisherBatchEnabled,
        pRmTimelineServerV1PublisherInterval);
    long stateUpdateTimeStamp = System.currentTimeMillis();
    for (int i = 1; i <= 2; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      RMApp app = createRMApp(appId);
      metricsPublisher.appCreated(app, app.getStartTime());
      metricsPublisher.appLaunched(app, app.getLaunchTime());
      if (i == 1) {
        when(app.getQueue()).thenReturn("new test queue");
        ApplicationSubmissionContext asc = mock(
            ApplicationSubmissionContext.class);
        when(asc.getUnmanagedAM()).thenReturn(false);
        when(asc.getPriority()).thenReturn(Priority.newInstance(1));
        when(asc.getNodeLabelExpression()).thenReturn("high-cpu");
        ContainerLaunchContext containerLaunchContext =
            mock(ContainerLaunchContext.class);
        when(containerLaunchContext.getCommands())
            .thenReturn(Collections.singletonList("java -Xmx1024m"));
        when(asc.getAMContainerSpec()).thenReturn(containerLaunchContext);
        when(app.getApplicationSubmissionContext()).thenReturn(asc);
        when(app.getApplicationPriority()).thenReturn(Priority.newInstance(1));
        metricsPublisher.appUpdated(app, 4L);
      } else {
        metricsPublisher.appUpdated(app, 4L);
      }
      metricsPublisher.appStateUpdated(app, YarnApplicationState.RUNNING,
          stateUpdateTimeStamp);
      metricsPublisher.appFinished(app, RMAppState.FINISHED,
          app.getFinishTime());
      if (i == 1) {
        metricsPublisher.appACLsUpdated(app, "uers1,user2", 4L);
      } else {
        // in case user doesn't specify the ACLs
        metricsPublisher.appACLsUpdated(app, null, 4L);
      }
      TimelineEntity entity = null;
      do {
        entity =
            store.getEntity(appId.toString(),
                ApplicationMetricsConstants.ENTITY_TYPE,
                EnumSet.allOf(Field.class));
        // ensure Five events are both published before leaving the loop
      } while (entity == null || entity.getEvents().size() < 6);
      // verify all the fields
      assertEquals(ApplicationMetricsConstants.ENTITY_TYPE,
          entity.getEntityType());
      assertEquals(app.getApplicationId().toString(), entity.getEntityId());
      assertEquals(
              app.getName(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.NAME_ENTITY_INFO));
      if (i != 1) {
        assertEquals(
            app.getQueue(),
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.QUEUE_ENTITY_INFO));
      }

      assertEquals(
          app.getApplicationSubmissionContext().getUnmanagedAM(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO));

      if (i != 1) {
        assertEquals(
            app.getApplicationSubmissionContext().getPriority().getPriority(),
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO));
      }

      assertEquals(app.getAmNodeLabelExpression(), entity.getOtherInfo()
          .get(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION));

      assertEquals(
          app.getApplicationSubmissionContext().getNodeLabelExpression(),
          entity.getOtherInfo()
              .get(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION));


      assertEquals(
              app.getUser(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.USER_ENTITY_INFO));

      assertEquals(
              app.getApplicationType(),
              entity.getOtherInfo().get(
                  ApplicationMetricsConstants.TYPE_ENTITY_INFO));
      assertEquals(app.getSubmitTime(),
          entity.getOtherInfo().get(
              ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO));
      assertTrue(verifyAppTags(app.getApplicationTags(),
          entity.getOtherInfo()));
      if (i == 1) {
        assertEquals("uers1,user2",
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO));

        assertEquals(
            app.getApplicationSubmissionContext().getAMContainerSpec()
                .getCommands(),
            entity.getOtherInfo()
                .get(ApplicationMetricsConstants.AM_CONTAINER_LAUNCH_COMMAND));
      } else {
        assertEquals(
            "",
            entity.getOtherInfo().get(
                ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO));
        assertEquals(
            app.getRMAppMetrics().getMemorySeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_MEM_METRICS).toString()));
        assertEquals(
            app.getRMAppMetrics().getVcoreSeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_CPU_METRICS).toString()));
        assertEquals(
            app.getRMAppMetrics().getPreemptedMemorySeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS)
                .toString()));
        assertEquals(
            app.getRMAppMetrics().getPreemptedVcoreSeconds(),
            Long.parseLong(entity.getOtherInfo()
                .get(ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS)
                .toString()));
      }
      assertEquals("context", entity.getOtherInfo()
          .get(ApplicationMetricsConstants.YARN_APP_CALLER_CONTEXT));
      boolean hasCreatedEvent = false;
      boolean hasLaunchedEvent = false;
      boolean hasUpdatedEvent = false;
      boolean hasFinishedEvent = false;
      boolean hasACLsUpdatedEvent = false;
      boolean hasStateUpdateEvent = false;
      for (TimelineEvent event : entity.getEvents()) {
        if (event.getEventType().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          hasCreatedEvent = true;
          assertEquals(app.getStartTime(), event.getTimestamp());
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE)) {
          hasLaunchedEvent = true;
          assertEquals(app.getLaunchTime(), event.getTimestamp());
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          hasFinishedEvent = true;
          assertEquals(app.getFinishTime(), event.getTimestamp());
          assertEquals(
              app.getDiagnostics().toString(),
              event.getEventInfo().get(
                  ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO));
          assertEquals(
              app.getFinalApplicationStatus().toString(),
              event.getEventInfo().get(
                  ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO));
          assertEquals(YarnApplicationState.FINISHED.toString(), event
              .getEventInfo().get(ApplicationMetricsConstants.STATE_EVENT_INFO));
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.UPDATED_EVENT_TYPE)) {
          hasUpdatedEvent = true;
          assertEquals(4L, event.getTimestamp());
          if (1 == i) {
            assertEquals(
                1,
                event.getEventInfo().get(
                    ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO));
            assertEquals(
                "new test queue",
                event.getEventInfo().get(
                    ApplicationMetricsConstants.QUEUE_ENTITY_INFO));
          }
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE)) {
          hasACLsUpdatedEvent = true;
          assertEquals(4L, event.getTimestamp());
        } else if (event.getEventType().equals(
              ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE)) {
          hasStateUpdateEvent = true;
          assertThat(event.getTimestamp()).isEqualTo(stateUpdateTimeStamp);
          assertEquals(YarnApplicationState.RUNNING.toString(), event
              .getEventInfo().get(
                   ApplicationMetricsConstants.STATE_EVENT_INFO));
        }
      }
      // Do assertTrue verification separately for easier debug
      assertTrue(hasCreatedEvent);
      assertTrue(hasLaunchedEvent);
      assertTrue(hasFinishedEvent);
      assertTrue(hasACLsUpdatedEvent);
      assertTrue(hasUpdatedEvent);
      assertTrue(hasStateUpdateEvent);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  @Timeout(value = 10)
  public void testPublishAppAttemptMetricsForUnmanagedAM(boolean
      pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    initTestSystemMetricsPublisher(pRmTimelineServerV1PublisherBatchEnabled,
        pRmTimelineServerV1PublisherInterval);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    RMAppAttempt appAttempt = createRMAppAttempt(appAttemptId,true);
    metricsPublisher.appAttemptRegistered(appAttempt, Integer.MAX_VALUE + 1L);
    RMApp app = mock(RMApp.class);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    metricsPublisher.appAttemptFinished(appAttempt, RMAppAttemptState.FINISHED, app,
        Integer.MAX_VALUE + 2L);
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
  }

  @ParameterizedTest
  @MethodSource("data")
  @Timeout(value = 10)
  public void testPublishAppAttemptMetrics(boolean pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    initTestSystemMetricsPublisher(pRmTimelineServerV1PublisherBatchEnabled,
        pRmTimelineServerV1PublisherInterval);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1);
    RMAppAttempt appAttempt = createRMAppAttempt(appAttemptId, false);
    metricsPublisher.appAttemptRegistered(appAttempt, Integer.MAX_VALUE + 1L);
    RMApp app = mock(RMApp.class);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    metricsPublisher.appAttemptFinished(appAttempt, RMAppAttemptState.FINISHED, app,
        Integer.MAX_VALUE + 2L);
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(appAttemptId.toString(),
              AppAttemptMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
    // verify all the fields
    assertEquals(AppAttemptMetricsConstants.ENTITY_TYPE,
        entity.getEntityType());
    assertEquals(appAttemptId.toString(), entity.getEntityId());
    assertEquals(
        appAttemptId.getApplicationId().toString(),
        entity.getPrimaryFilters()
            .get(AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER).iterator()
            .next());
    boolean hasRegisteredEvent = false;
    boolean hasFinishedEvent = false;
    for (TimelineEvent event : entity.getEvents()) {
      if (event.getEventType().equals(
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
        hasRegisteredEvent = true;
        assertEquals(appAttempt.getHost(),
            event.getEventInfo()
                .get(AppAttemptMetricsConstants.HOST_INFO));
        assertEquals(appAttempt.getRpcPort(),
                event.getEventInfo().get(
                    AppAttemptMetricsConstants.RPC_PORT_INFO));
        assertEquals(
            appAttempt.getMasterContainer().getId().toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.MASTER_CONTAINER_INFO));
      } else if (event.getEventType().equals(
          AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
        hasFinishedEvent = true;
        assertEquals(appAttempt.getDiagnostics(), event.getEventInfo()
            .get(AppAttemptMetricsConstants.DIAGNOSTICS_INFO));
        assertEquals(appAttempt.getTrackingUrl(), event.getEventInfo()
            .get(AppAttemptMetricsConstants.TRACKING_URL_INFO));
        assertEquals(
            appAttempt.getOriginalTrackingUrl(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO));
        assertEquals(
            FinalApplicationStatus.UNDEFINED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.FINAL_STATUS_INFO));
        assertEquals(
            YarnApplicationAttemptState.FINISHED.toString(),
            event.getEventInfo().get(
                AppAttemptMetricsConstants.STATE_INFO));
      }
    }
    assertTrue(hasRegisteredEvent && hasFinishedEvent);
  }

  @ParameterizedTest
  @MethodSource("data")
  @Timeout(value = 10)
  public void testPublishHostPortInfoOnContainerFinished(
      boolean pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    initTestSystemMetricsPublisher(pRmTimelineServerV1PublisherBatchEnabled,
        pRmTimelineServerV1PublisherInterval);
    ContainerId containerId =
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1);
    RMContainer container = createRMContainer(containerId);
    metricsPublisher.containerFinished(container, container.getFinishTime());
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(containerId.toString(),
              ContainerMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
    } while (entity == null || entity.getEvents().size() < 1);
    assertNotNull(entity.getOtherInfo());
    assertEquals(2, entity.getOtherInfo().size());
    assertNotNull(entity.getOtherInfo().get(
        ContainerMetricsConstants.ALLOCATED_HOST_INFO));
    assertNotNull(entity.getOtherInfo().get(
        ContainerMetricsConstants.ALLOCATED_PORT_INFO));
    assertEquals(
        container.getAllocatedNode().getHost(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_HOST_INFO));
    assertEquals(
        container.getAllocatedNode().getPort(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PORT_INFO));
  }

  @ParameterizedTest
  @MethodSource("data")
  @Timeout(value = 10)
  public void testPublishContainerMetrics(boolean pRmTimelineServerV1PublisherBatchEnabled,
      int pRmTimelineServerV1PublisherInterval) throws Exception {
    initTestSystemMetricsPublisher(pRmTimelineServerV1PublisherBatchEnabled,
        pRmTimelineServerV1PublisherInterval);
    ContainerId containerId =
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1);
    RMContainer container = createRMContainer(containerId);
    metricsPublisher.containerCreated(container, container.getCreationTime());
    metricsPublisher.containerFinished(container, container.getFinishTime());
    TimelineEntity entity = null;
    do {
      entity =
          store.getEntity(containerId.toString(),
              ContainerMetricsConstants.ENTITY_TYPE,
              EnumSet.allOf(Field.class));
      // ensure two events are both published before leaving the loop
    } while (entity == null || entity.getEvents().size() < 2);
    // verify all the fields
    assertEquals(ContainerMetricsConstants.ENTITY_TYPE,
        entity.getEntityType());
    assertEquals(containerId.toString(), entity.getEntityId());
    assertEquals(
        containerId.getApplicationAttemptId().toString(),
        entity.getPrimaryFilters()
            .get(ContainerMetricsConstants.PARENT_PRIMARIY_FILTER).iterator()
            .next());
    assertEquals(
        container.getAllocatedNode().getHost(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_HOST_INFO));
    assertEquals(
        container.getAllocatedNode().getPort(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PORT_INFO));
    assertEquals(container.getAllocatedResource().getMemorySize(),
        // KeyValueBasedTimelineStore could cast long to integer, need make sure
        // variables for compare have same type.
        ((Integer) entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_MEMORY_INFO))
            .longValue());
    assertEquals(
        container.getAllocatedResource().getVirtualCores(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_VCORE_INFO));
    assertEquals(
        container.getAllocatedPriority().getPriority(),
        entity.getOtherInfo().get(
            ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO));
    boolean hasCreatedEvent = false;
    boolean hasFinishedEvent = false;
    for (TimelineEvent event : entity.getEvents()) {
      if (event.getEventType().equals(
          ContainerMetricsConstants.CREATED_EVENT_TYPE)) {
        hasCreatedEvent = true;
        assertEquals(container.getCreationTime(), event.getTimestamp());
      } else if (event.getEventType().equals(
          ContainerMetricsConstants.FINISHED_EVENT_TYPE)) {
        hasFinishedEvent = true;
        assertEquals(container.getFinishTime(), event.getTimestamp());
        assertEquals(
            container.getDiagnosticsInfo(),
            event.getEventInfo().get(
                ContainerMetricsConstants.DIAGNOSTICS_INFO));
        assertEquals(
            container.getContainerExitStatus(),
            event.getEventInfo().get(
                ContainerMetricsConstants.EXIT_STATUS_INFO));
        assertEquals(container.getContainerState().toString(), event
            .getEventInfo().get(ContainerMetricsConstants.STATE_INFO));
      }
    }
    assertTrue(hasCreatedEvent && hasFinishedEvent);
  }

  private static RMApp createRMApp(ApplicationId appId) {
    RMApp app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getName()).thenReturn("test app");
    when(app.getApplicationType()).thenReturn("test app type");
    when(app.getUser()).thenReturn("test user");
    when(app.getQueue()).thenReturn("test queue");
    when(app.getSubmitTime()).thenReturn(Integer.MAX_VALUE + 1L);
    when(app.getStartTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(app.getLaunchTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(app.getFinishTime()).thenReturn(Integer.MAX_VALUE + 3L);
    when(app.getDiagnostics()).thenReturn(
        new StringBuilder("test diagnostics info"));
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(
        ApplicationAttemptId.newInstance(appId, 1));
    when(app.getCurrentAppAttempt()).thenReturn(appAttempt);
    when(app.getFinalApplicationStatus()).thenReturn(
        FinalApplicationStatus.UNDEFINED);
    Map<String, Long> resourceMap = new HashMap<>();
    resourceMap
        .put(ResourceInformation.MEMORY_MB.getName(), (long) Integer.MAX_VALUE);
    resourceMap.put(ResourceInformation.VCORES.getName(), Long.MAX_VALUE);
    Map<String, Long> preemptedMap = new HashMap<>();
    preemptedMap
        .put(ResourceInformation.MEMORY_MB.getName(), (long) Integer.MAX_VALUE);
    preemptedMap.put(ResourceInformation.VCORES.getName(), Long.MAX_VALUE);
    when(app.getRMAppMetrics())
        .thenReturn(new RMAppMetrics(null, 0, 0, resourceMap, preemptedMap,
            0));
    Set<String> appTags = new HashSet<String>();
    appTags.add("test");
    appTags.add("tags");
    when(app.getApplicationTags()).thenReturn(appTags);
    ApplicationSubmissionContext asc = mock(ApplicationSubmissionContext.class);
    when(asc.getUnmanagedAM()).thenReturn(false);
    when(asc.getPriority()).thenReturn(Priority.newInstance(10));
    when(asc.getNodeLabelExpression()).thenReturn("high-cpu");
    ContainerLaunchContext containerLaunchContext =
        mock(ContainerLaunchContext.class);
    when(containerLaunchContext.getCommands())
        .thenReturn(Collections.singletonList("java -Xmx1024m"));
    when(asc.getAMContainerSpec()).thenReturn(containerLaunchContext);
    when(app.getApplicationSubmissionContext()).thenReturn(asc);
    when(app.getAppNodeLabelExpression()).thenCallRealMethod();
    ResourceRequest amReq = mock(ResourceRequest.class);
    when(amReq.getNodeLabelExpression()).thenReturn("high-mem");
    when(app.getAMResourceRequests())
        .thenReturn(Collections.singletonList(amReq));
    when(app.getAmNodeLabelExpression()).thenCallRealMethod();
    when(app.getApplicationPriority()).thenReturn(Priority.newInstance(10));
    when(app.getCallerContext())
        .thenReturn(new CallerContext.Builder("context").build());
    when(app.getState()).thenReturn(RMAppState.SUBMITTED);
    return app;
  }

  private static RMAppAttempt createRMAppAttempt(
      ApplicationAttemptId appAttemptId, boolean unmanagedAMAttempt) {
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(appAttempt.getHost()).thenReturn("test host");
    when(appAttempt.getRpcPort()).thenReturn(-100);
    if (!unmanagedAMAttempt) {
      Container container = mock(Container.class);
      when(container.getId())
          .thenReturn(ContainerId.newContainerId(appAttemptId, 1));
      when(appAttempt.getMasterContainer()).thenReturn(container);
    }
    when(appAttempt.getDiagnostics()).thenReturn("test diagnostics info");
    when(appAttempt.getTrackingUrl()).thenReturn("test tracking url");
    when(appAttempt.getOriginalTrackingUrl()).thenReturn(
        "test original tracking url");
    return appAttempt;
  }

  private static RMContainer createRMContainer(ContainerId containerId) {
    RMContainer container = mock(RMContainer.class);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getAllocatedNode()).thenReturn(
        NodeId.newInstance("test host", -100));
    when(container.getAllocatedResource()).thenReturn(
        Resource.newInstance(-1, -1));
    when(container.getAllocatedPriority()).thenReturn(Priority.UNDEFINED);
    when(container.getCreationTime()).thenReturn(Integer.MAX_VALUE + 1L);
    when(container.getFinishTime()).thenReturn(Integer.MAX_VALUE + 2L);
    when(container.getDiagnosticsInfo()).thenReturn("test diagnostics info");
    when(container.getContainerExitStatus()).thenReturn(-1);
    when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
    Container mockContainer = mock(Container.class);
    when(container.getContainer()).thenReturn(mockContainer);
    when(mockContainer.getNodeHttpAddress())
      .thenReturn("http://localhost:1234");
    return container;
  }

  private static boolean verifyAppTags(Set<String> appTags,
      Map<String, Object> entityInfo) {
    if (!entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
      return false;
    }
    Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
    if (obj instanceof Collection<?>) {
      Collection<?> collection = (Collection<?>) obj;
      if (collection.size() != appTags.size()) {
        return false;
      }
      for (String appTag : appTags) {
        boolean match = false;
        for (Object o : collection) {
          if (o.toString().equals(appTag)) {
            match = true;
            break;
          }
        }
        if (!match) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
