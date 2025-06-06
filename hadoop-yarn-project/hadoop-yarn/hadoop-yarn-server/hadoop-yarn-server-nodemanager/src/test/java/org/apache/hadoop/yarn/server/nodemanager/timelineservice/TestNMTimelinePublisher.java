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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.impl.TimelineV2ClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerPauseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResumeEvent;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestNMTimelinePublisher {
  private static final String MEMORY_ID = "MEMORY";
  private static final String CPU_ID = "CPU";

  private NMTimelinePublisher publisher;
  private DummyTimelineClient timelineClient;
  private Configuration conf;
  private DrainDispatcher dispatcher;


  @BeforeEach
  public void setup() throws Exception {
    conf = new Configuration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setLong(YarnConfiguration.ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS,
        3000L);
    conf.setBoolean(YarnConfiguration.NM_PUBLISH_CONTAINER_EVENTS_ENABLED,
        true);
    timelineClient = new DummyTimelineClient(null);
    Context context = createMockContext();
    dispatcher = new DrainDispatcher();

    publisher = new NMTimelinePublisher(context) {
      public void createTimelineClient(ApplicationId appId) {
        if (!getAppToClientMap().containsKey(appId)) {
          timelineClient.init(getConfig());
          timelineClient.start();
          getAppToClientMap().put(appId, timelineClient);
        }
      }

      @Override protected AsyncDispatcher createDispatcher() {
        return dispatcher;
      }
    };
    publisher.init(conf);
    publisher.start();
  }

  private Context createMockContext() {
    Context context = mock(Context.class);
    when(context.getNodeId()).thenReturn(NodeId.newInstance("localhost", 0));

    ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>();
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);
    Container container = mock(Container.class);
    when(container.getContainerStartTime())
        .thenReturn(System.currentTimeMillis());
    containers.putIfAbsent(cId, container);
    when(context.getContainers()).thenReturn(containers);

    return context;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (publisher != null) {
      publisher.stop();
    }
    if (timelineClient != null) {
      timelineClient.stop();
    }
  }

  @Test public void testPublishContainerFinish() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 2);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);

    String diag = "test-diagnostics";
    int exitStatus = 0;
    ContainerStatus cStatus = mock(ContainerStatus.class);
    when(cStatus.getContainerId()).thenReturn(cId);
    when(cStatus.getDiagnostics()).thenReturn(diag);
    when(cStatus.getExitStatus()).thenReturn(exitStatus);
    long timeStamp = System.currentTimeMillis();

    ApplicationContainerFinishedEvent finishedEvent =
        new ApplicationContainerFinishedEvent(cStatus, timeStamp);

    publisher.createTimelineClient(appId);
    publisher.publishApplicationEvent(finishedEvent);
    publisher.stopTimelineClient(appId);
    dispatcher.await();

    ContainerEntity cEntity = new ContainerEntity();
    cEntity.setId(cId.toString());
    TimelineEntity[] lastPublishedEntities =
        timelineClient.getLastPublishedEntities();

    assertNotNull(lastPublishedEntities);
    assertEquals(1, lastPublishedEntities.length);
    TimelineEntity entity = lastPublishedEntities[0];
    assertTrue(cEntity.equals(entity));
    assertEquals(diag,
        entity.getInfo().get(ContainerMetricsConstants.DIAGNOSTICS_INFO));
    assertEquals(exitStatus,
        entity.getInfo().get(ContainerMetricsConstants.EXIT_STATUS_INFO));
    assertEquals(TimelineServiceHelper.invertLong(
        cId.getContainerId()), entity.getIdPrefix());
  }

  @Test
  public void testPublishContainerPausedEvent() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);

    ContainerEvent containerEvent =
        new ContainerPauseEvent(cId, "test pause");

    publisher.createTimelineClient(appId);
    publisher.publishContainerEvent(containerEvent);
    publisher.stopTimelineClient(appId);
    dispatcher.await();

    ContainerEntity cEntity = new ContainerEntity();
    cEntity.setId(cId.toString());
    TimelineEntity[] lastPublishedEntities =
        timelineClient.getLastPublishedEntities();

    assertNotNull(lastPublishedEntities);
    assertEquals(1, lastPublishedEntities.length);
    TimelineEntity entity = lastPublishedEntities[0];
    assertEquals(cEntity, entity);

    NavigableSet<TimelineEvent> events = entity.getEvents();
    assertEquals(1, events.size());
    assertEquals(ContainerMetricsConstants.PAUSED_EVENT_TYPE,
        events.iterator().next().getId());

    Map<String, Object> info = entity.getInfo();
    assertTrue(
        info.containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO));
    assertEquals("test pause",
        info.get(ContainerMetricsConstants.DIAGNOSTICS_INFO));
  }

  @Test
  public void testPublishContainerResumedEvent() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);

    ContainerEvent containerEvent =
        new ContainerResumeEvent(cId, "test resume");

    publisher.createTimelineClient(appId);
    publisher.publishContainerEvent(containerEvent);
    publisher.stopTimelineClient(appId);
    dispatcher.await();

    ContainerEntity cEntity = new ContainerEntity();
    cEntity.setId(cId.toString());
    TimelineEntity[] lastPublishedEntities =
        timelineClient.getLastPublishedEntities();

    assertNotNull(lastPublishedEntities);
    assertEquals(1, lastPublishedEntities.length);
    TimelineEntity entity = lastPublishedEntities[0];
    assertEquals(cEntity, entity);

    NavigableSet<TimelineEvent> events = entity.getEvents();
    assertEquals(1, events.size());
    assertEquals(ContainerMetricsConstants.RESUMED_EVENT_TYPE,
        events.iterator().next().getId());

    Map<String, Object> info = entity.getInfo();
    assertTrue(
        info.containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO));
    assertEquals("test resume",
        info.get(ContainerMetricsConstants.DIAGNOSTICS_INFO));
  }

  @Test
  public void testPublishContainerKilledEvent() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);

    ContainerEvent containerEvent =
        new ContainerKillEvent(cId, 1, "test kill");

    publisher.createTimelineClient(appId);
    publisher.publishContainerEvent(containerEvent);
    publisher.stopTimelineClient(appId);
    dispatcher.await();

    ContainerEntity cEntity = new ContainerEntity();
    cEntity.setId(cId.toString());
    TimelineEntity[] lastPublishedEntities =
        timelineClient.getLastPublishedEntities();

    assertNotNull(lastPublishedEntities);
    assertEquals(1, lastPublishedEntities.length);
    TimelineEntity entity = lastPublishedEntities[0];
    assertEquals(cEntity, entity);

    NavigableSet<TimelineEvent> events = entity.getEvents();
    assertEquals(1, events.size());
    assertEquals(ContainerMetricsConstants.KILLED_EVENT_TYPE,
        events.iterator().next().getId());

    Map<String, Object> info = entity.getInfo();
    assertTrue(
        info.containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO));
    assertEquals("test kill",
        info.get(ContainerMetricsConstants.DIAGNOSTICS_INFO));
    assertTrue(
        info.containsKey(ContainerMetricsConstants.EXIT_STATUS_INFO));
    assertEquals(1,
        info.get(ContainerMetricsConstants.EXIT_STATUS_INFO));
  }

  @Test public void testContainerResourceUsage() {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    publisher.createTimelineClient(appId);
    Container aContainer = mock(Container.class);
    when(aContainer.getContainerId()).thenReturn(ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId, 1), 0L));
    long idPrefix = TimelineServiceHelper.invertLong(
        aContainer.getContainerId().getContainerId());
    publisher.reportContainerResourceUsage(aContainer, 1024L, 8F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 8, idPrefix);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L, 0.8F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 1, idPrefix);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L, 0.49F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 0, idPrefix);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L,
        (float) ResourceCalculatorProcessTree.UNAVAILABLE);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L,
        ResourceCalculatorProcessTree.UNAVAILABLE, idPrefix);
  }

  private void verifyPublishedResourceUsageMetrics(DummyTimelineClient
      dummyTimelineClient, long memoryUsage, int cpuUsage, long idPrefix) {
    TimelineEntity[] entities = null;
    for (int i = 0; i < 10; i++) {
      entities = dummyTimelineClient.getLastPublishedEntities();
      if (entities != null) {
        break;
      }
      try {
        Thread.sleep(150L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    int numberOfResourceMetrics = 0;
    numberOfResourceMetrics +=
        (memoryUsage == ResourceCalculatorProcessTree.UNAVAILABLE) ? 0 : 1;
    numberOfResourceMetrics +=
        (cpuUsage == ResourceCalculatorProcessTree.UNAVAILABLE) ? 0 : 1;
    assertNotNull(entities, "entities are expected to be published");
    assertEquals(numberOfResourceMetrics, entities[0].getMetrics().size(),
        "Expected number of metrics notpublished");
    assertEquals(idPrefix, entities[0].getIdPrefix());
    Iterator<TimelineMetric> metrics = entities[0].getMetrics().iterator();
    while (metrics.hasNext()) {
      TimelineMetric metric = metrics.next();
      Iterator<Entry<Long, Number>> entrySet;
      switch (metric.getId()) {
      case CPU_ID:
        if (cpuUsage == ResourceCalculatorProcessTree.UNAVAILABLE) {
          fail("Not Expecting CPU Metric to be published");
        }
        entrySet = metric.getValues().entrySet().iterator();
        assertEquals(cpuUsage, entrySet.next().getValue(),
            "CPU usage metric not matching");
        break;
      case MEMORY_ID:
        if (memoryUsage == ResourceCalculatorProcessTree.UNAVAILABLE) {
          fail("Not Expecting Memory Metric to be published");
        }
        entrySet = metric.getValues().entrySet().iterator();
        assertEquals(memoryUsage, entrySet.next().getValue(),
            "Memory usage metric not matching");
        break;
      default:
        fail("Invalid Resource Usage metric");
        break;
      }
    }
  }

  protected static class DummyTimelineClient extends TimelineV2ClientImpl {
    public DummyTimelineClient(ApplicationId appId) {
      super(appId);
    }

    private TimelineEntity[] lastPublishedEntities;

    @Override public void putEntitiesAsync(TimelineEntity... entities)
        throws IOException, YarnException {
      this.lastPublishedEntities = entities;
    }

    @Override public void putEntities(TimelineEntity... entities)
        throws IOException, YarnException {
      this.lastPublishedEntities = entities;
    }

    public TimelineEntity[] getLastPublishedEntities() {
      return lastPublishedEntities;
    }

    public void reset() {
      lastPublishedEntities = null;
    }
  }
}
