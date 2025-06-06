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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getAutoCreatedQueueTemplateConfPrefix;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testing applications being retired from RM.
 *
 */

public class TestAppManager extends AppManagerTestBase{

  @RegisterExtension
  private UseCapacitySchedulerRule shouldUseCs = new UseCapacitySchedulerRule();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAppManager.class);
  private static RMAppEventType appEventType = RMAppEventType.KILL;

  private static String USER = "user_";
  private static String USER0 = USER + 0;
  private ResourceScheduler scheduler;

  private static final String USER_ID_PREFIX = "userid=";
  private static final String ROOT_PARENT_PATH =  PREFIX + "root.parent.";
  private static final QueuePath ROOT =  new QueuePath("root");
  private static final QueuePath DEFAULT =  new QueuePath("root.default");
  private static final QueuePath TEST =  new QueuePath("root.test");
  private static final QueuePath PARENT =  new QueuePath("root.parent");
  private static final QueuePath MANAGED_PARENT =  new QueuePath("root.managedparent");

  public synchronized RMAppEventType getAppEventType() {
    return appEventType;
  } 
  public synchronized void setAppEventType(RMAppEventType newType) {
    appEventType = newType;
  } 


  private static List<RMApp> newRMApps(int n, long time, RMAppState state) {
    List<RMApp> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(new MockRMApp(i, time, state));
    }
    return list;
  }

  public RMContext mockRMContext(int n, long time) {
    final List<RMApp> apps = newRMApps(n, time, RMAppState.FINISHED);
    final ConcurrentMap<ApplicationId, RMApp> map = Maps.newConcurrentMap();
    for (RMApp app : apps) {
      map.put(app.getApplicationId(), app);
    }
    Dispatcher rmDispatcher = new AsyncDispatcher();
    ContainerAllocationExpirer containerAllocationExpirer = new ContainerAllocationExpirer(
            rmDispatcher);
    AMLivelinessMonitor amLivelinessMonitor = new AMLivelinessMonitor(
            rmDispatcher);
    AMLivelinessMonitor amFinishingMonitor = new AMLivelinessMonitor(
            rmDispatcher);
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    RMContext context = new RMContextImpl(rmDispatcher,
            containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
            null, null, null, null, null) {
      @Override
      public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return map;
      }
    };
    ((RMContextImpl)context).setStateStore(mock(RMStateStore.class));
    metricsPublisher = mock(SystemMetricsPublisher.class);
    context.setSystemMetricsPublisher(metricsPublisher);
    context.setRMApplicationHistoryWriter(writer);
    ((RMContextImpl)context).setYarnConfiguration(new YarnConfiguration());
    return context;
  }

  public class TestAppManagerDispatcher implements
      EventHandler<RMAppManagerEvent> {


    public TestAppManagerDispatcher() {
    }

    @Override
    public void handle(RMAppManagerEvent event) {
       // do nothing
    }   
  }   

  public class TestDispatcher implements
      EventHandler<RMAppEvent> {

    public TestDispatcher() {
    }

    @Override
    public void handle(RMAppEvent event) {
      //RMApp rmApp = this.rmContext.getRMApps().get(appID);
      setAppEventType(event.getType());
      System.out.println("in handle routine " + getAppEventType().toString());
    }   
  }

  protected void addToCompletedApps(TestRMAppManager appMonitor, RMContext rmContext) {
    for (RMApp app : rmContext.getRMApps().values()) {
      if (app.getState() == RMAppState.FINISHED
          || app.getState() == RMAppState.KILLED
          || app.getState() == RMAppState.FAILED) {
        appMonitor.finishApplication(app.getApplicationId());
      }
    }
  }

  private RMContext rmContext;
  private SystemMetricsPublisher metricsPublisher;
  private TestRMAppManager appMonitor;
  private ApplicationSubmissionContext asContext;
  private ApplicationId appId;
  private QueueInfo mockDefaultQueueInfo;

  @SuppressWarnings("deprecation")
  @BeforeEach
  public void setUp() throws IOException {
    long now = System.currentTimeMillis();

    rmContext = mockRMContext(1, now - 10);
    rmContext
        .setRMTimelineCollectorManager(mock(RMTimelineCollectorManager.class));

    if (shouldUseCs.useCapacityScheduler()) {
      scheduler = mockResourceScheduler(CapacityScheduler.class);
    } else {
      scheduler = mockResourceScheduler();
    }

    ((RMContextImpl)rmContext).setScheduler(scheduler);

    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    ((RMContextImpl) rmContext).setYarnConfiguration(conf);
    ApplicationMasterService masterService =
        new ApplicationMasterService(rmContext, scheduler);
    appMonitor = new TestRMAppManager(rmContext,
        new ClientToAMTokenSecretManagerInRM(), scheduler, masterService,
        new ApplicationACLsManager(conf), conf);

    appId = MockApps.newAppID(1);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    asContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    asContext.setApplicationId(appId);
    asContext.setAMContainerSpec(mockContainerLaunchContext(recordFactory));
    asContext.setResource(mockResource());
    asContext.setPriority(Priority.newInstance(0));
    asContext.setQueue("default");
    mockDefaultQueueInfo = mock(QueueInfo.class);
    when(scheduler.getQueueInfo("default", false, false))
        .thenReturn(mockDefaultQueueInfo);

    setupDispatcher(rmContext, conf);
  }

  public static PlacementManager createMockPlacementManager(
      String userRegex, String placementQueue, String placementParentQueue
  ) throws YarnException {
    PlacementManager placementMgr = mock(PlacementManager.class);
    doAnswer(new Answer<ApplicationPlacementContext>() {

      @Override
      public ApplicationPlacementContext answer(InvocationOnMock invocation)
          throws Throwable {
        return new ApplicationPlacementContext(placementQueue, placementParentQueue);
      }

    }).when(placementMgr).placeApplication(
        any(ApplicationSubmissionContext.class),
        matches(userRegex),
        any(Boolean.class));

    return placementMgr;
  }

  private TestRMAppManager createAppManager(RMContext context, Configuration configuration) {
    ApplicationMasterService masterService = new ApplicationMasterService(context,
        context.getScheduler());

    return new TestRMAppManager(context,
        new ClientToAMTokenSecretManagerInRM(),
        context.getScheduler(), masterService,
        new ApplicationACLsManager(configuration), configuration);
  }

  @Test
  public void testQueueSubmitWithACLsEnabledWithQueueMapping()
      throws YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = new
        CapacitySchedulerConfiguration(conf, false);
    csConf.set(PREFIX + "root.queues", "default,test");

    csConf.setCapacity(DEFAULT, 50.0f);
    csConf.setMaximumCapacity(DEFAULT, 100.0f);

    csConf.setCapacity(TEST, 50.0f);
    csConf.setMaximumCapacity(TEST, 100.0f);

    csConf.set(PREFIX + "root.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.acl_administer_queue", " ");

    csConf.set(PREFIX + "root.default.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.default.acl_administer_queue", " ");

    csConf.set(PREFIX + "root.test.acl_submit_applications", "test");
    csConf.set(PREFIX + "root.test.acl_administer_queue", "test");

    MockRM newMockRM = new MockRM(csConf);
    RMContext newMockRMContext = newMockRM.getRMContext();
    newMockRMContext.setQueuePlacementManager(
        createMockPlacementManager("test", "root.test", null));
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    ApplicationSubmissionContext submission = createAppSubmissionContext(MockApps.newAppID(1));
    submission.setQueue("oldQueue");
    verifyAppSubmission(submission,
        newAppMonitor,
        newMockRMContext,
        "test",
        "root.test");

    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(2)),
        "test1");
  }

  @Test
  public void testQueueSubmitWithLeafQueueName()
      throws YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = new
        CapacitySchedulerConfiguration(conf, false);
    csConf.set(PREFIX + "root.queues", "default,test");

    csConf.setCapacity(DEFAULT, 50.0f);
    csConf.setMaximumCapacity(DEFAULT, 100.0f);

    csConf.setCapacity(TEST, 50.0f);
    csConf.setMaximumCapacity(TEST, 100.0f);

    MockRM newMockRM = new MockRM(csConf);
    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    ApplicationSubmissionContext submission = createAppSubmissionContext(MockApps.newAppID(1));
    submission.setQueue("test");
    verifyAppSubmission(submission,
        newAppMonitor,
        newMockRMContext,
        "test",
        "root.test");
  }

  @Test
  public void testQueueSubmitWithACLsEnabledWithQueueMappingForLegacyAutoCreatedQueue()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf, false);
    csConf.set(PREFIX + "root.queues", "default,managedparent");

    csConf.setCapacity(DEFAULT, 50.0f);
    csConf.setMaximumCapacity(DEFAULT, 100.0f);

    csConf.setCapacity(MANAGED_PARENT, 50.0f);
    csConf.setMaximumCapacity(MANAGED_PARENT, 100.0f);

    csConf.set(PREFIX + "root.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.acl_administer_queue", " ");

    csConf.set(PREFIX + "root.default.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.default.acl_administer_queue", " ");

    csConf.set(PREFIX + "root.managedparent.acl_administer_queue", "admin");
    csConf.set(PREFIX + "root.managedparent.acl_submit_applications", "user1");

    csConf.setAutoCreateChildQueueEnabled(MANAGED_PARENT, true);
    csConf.setAutoCreatedLeafQueueConfigCapacity(MANAGED_PARENT, 30f);
    csConf.setAutoCreatedLeafQueueConfigMaxCapacity(MANAGED_PARENT, 100f);

    MockRM newMockRM = new MockRM(csConf);
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    ManagedParentQueue managedParentQueue = new ManagedParentQueue(cs.getQueueContext(),
        "managedparent", cs.getQueue("root"), null);
    cs.getCapacitySchedulerQueueManager().addQueue("managedparent",
        managedParentQueue);

    RMContext newMockRMContext = newMockRM.getRMContext();
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1|user2", "user1", "root.managedparent"));
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    ApplicationSubmissionContext submission = createAppSubmissionContext(MockApps.newAppID(1));
    submission.setQueue("oldQueue");
    verifyAppSubmission(submission,
        newAppMonitor,
        newMockRMContext,
        "user1",
        "root.managedparent.user1");

    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(2)),
        "user2");
  }

  @Test
  public void testLegacyAutoCreatedQueuesWithACLTemplates()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf, false);

    QueuePath parentQueuePath = new QueuePath("root.parent");
    QueuePath user1QueuePath = new QueuePath("root.parent.user1");
    QueuePath user2QueuePath = new QueuePath("root.parent.user2");

    csConf.set(PREFIX + "root.queues", "parent");
    csConf.set(PREFIX + "root.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.acl_administer_queue", " ");

    csConf.setCapacity(parentQueuePath, 100.0f);
    csConf.set(PREFIX + "root.parent.acl_administer_queue", "user1,user4");
    csConf.set(PREFIX + "root.parent.acl_submit_applications", "user1,user4");

    csConf.setAutoCreateChildQueueEnabled(parentQueuePath, true);
    csConf.setAutoCreatedLeafQueueConfigCapacity(parentQueuePath, 50f);
    csConf.setAutoCreatedLeafQueueConfigMaxCapacity(parentQueuePath, 100f);
    String autoCreatedQueuePrefix =
            getAutoCreatedQueueTemplateConfPrefix(parentQueuePath);
    QueuePath autoCreatedQueuePath = new QueuePath(autoCreatedQueuePrefix);
    csConf.set(getQueuePrefix(autoCreatedQueuePath) + "acl_administer_queue", "user2,user4");
    csConf.set(getQueuePrefix(autoCreatedQueuePath) + "acl_submit_applications", "user2,user4");

    MockRM newMockRM = new MockRM(csConf);

    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user1 has permission on root.parent so a queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1", "user1", parentQueuePath.getFullPath()));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        user1QueuePath.getFullPath());

    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1|user2|user3|user4", "user2", parentQueuePath.getFullPath()));

    // user2 has permission (due to ACL templates)
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(2)),
        newAppMonitor,
        newMockRMContext,
        "user2",
        user2QueuePath.getFullPath());

    // user3 doesn't have permission
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(3)),
        "user3");

    // user4 has permission on root.parent
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(4)),
        newAppMonitor,
        newMockRMContext,
        "user4",
        user2QueuePath.getFullPath());

    // create the root.parent.user2 manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(user2QueuePath);
    AutoCreatedLeafQueue autoCreatedLeafQueue = (AutoCreatedLeafQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    ManagedParentQueue parentQueue = (ManagedParentQueue) cs.getQueue("parent");
    assertEquals(parentQueue, autoCreatedLeafQueue.getParent());
    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(5)),
        newAppMonitor,
        newMockRMContext,
        "user2",
        user2QueuePath.getFullPath());

    // user3 doesn't have permission for root.parent.user2 queue
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(6)),
        "user3");

    // user1 doesn't have permission for root.parent.user2 queue, but it has for root.parent
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(7)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        user2QueuePath.getFullPath());
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithSpecializedACLTemplatesAndDynamicParentQueue()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = createFlexibleAQCBaseACLConfiguration(conf);

    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "acl_administer_queue",
        "user3");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX
        + "acl_submit_applications", "user3");

    MockRM newMockRM = new MockRM(csConf);

    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user1 has permission on root.parent so a queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1", "user1", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        "root.parent.user1");

    // user2 doesn't have permission to create a dynamic leaf queue (parent only template)
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user2", "user2", "root.parent"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(2)),
        "user2");

    // user3 has permission on root.parent.user2.user3 due to ACL templates
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(3)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.parent.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.parent.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(4)),
        "user4");

    // create the root.parent.user2.user3 manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(new QueuePath("root.parent.user2.user3"));

    ParentQueue autoCreatedParentQueue = (ParentQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedParentQueue, "Auto Creation of Queue failed");
    ParentQueue parentQueue = (ParentQueue) cs.getQueue("parent");
    assertEquals(parentQueue, autoCreatedParentQueue.getParent());

    LeafQueue autoCreatedLeafQueue = (LeafQueue) cs.getQueue("user3");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedLeafQueue.getParent());

    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(5)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.parent.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.parent.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(6)),
        "user4");
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithMixedCommonLeafACLTemplatesAndDynamicParentQueue()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = createFlexibleAQCBaseACLConfiguration(conf);

    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "acl_administer_queue",
        "user3");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX
        + "acl_submit_applications", "user3");

    testFlexibleAQCDWithMixedTemplatesDynamicParentACLScenario(conf, csConf);
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithMixedCommonCommonACLTemplatesAndDynamicParentQueue()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = createFlexibleAQCBaseACLConfiguration(conf);

    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_administer_queue",
        "user3");
    csConf.set(ROOT_PARENT_PATH + "*." + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_submit_applications",
        "user3");

    testFlexibleAQCDWithMixedTemplatesDynamicParentACLScenario(conf, csConf);
  }

  private void testFlexibleAQCDWithMixedTemplatesDynamicParentACLScenario(
      YarnConfiguration conf, CapacitySchedulerConfiguration csConf)
      throws YarnException, IOException {
    MockRM newMockRM = new MockRM(csConf);

    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user1 has permission on root.parent so a queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1", "user1", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        "root.parent.user1");

    // user2 has permission on root.parent a dynamic leaf queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user2", "user2", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(2)),
        newAppMonitor,
        newMockRMContext,
        "user2",
        "root.parent.user2");

    // user3 has permission on root.parent.user2.user3 a dynamic parent and leaf queue
    // would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(3)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.parent.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.parent.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(4)),
        "user4");

    // create the root.parent.user2.user3 manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(new QueuePath("root.parent.user2.user3"));

    ParentQueue autoCreatedParentQueue = (ParentQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedParentQueue, "Auto Creation of Queue failed");
    ParentQueue parentQueue = (ParentQueue) cs.getQueue("parent");
    assertEquals(parentQueue, autoCreatedParentQueue.getParent());

    LeafQueue autoCreatedLeafQueue = (LeafQueue) cs.getQueue("user3");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedLeafQueue.getParent());

    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(5)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.parent.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.parent.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(6)),
        "user4");
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithACLTemplatesALeafOnly()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = createFlexibleAQCBaseACLConfiguration(conf);

    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    testFlexibleAQCLeafOnly(conf, csConf);
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithSpecialisedACLTemplatesALeafOnly()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();
    CapacitySchedulerConfiguration csConf = createFlexibleAQCBaseACLConfiguration(conf);

    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(ROOT_PARENT_PATH + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    testFlexibleAQCLeafOnly(conf, csConf);
  }

  private void testFlexibleAQCLeafOnly(
      YarnConfiguration conf,
      CapacitySchedulerConfiguration csConf)
      throws YarnException, IOException {
    MockRM newMockRM = new MockRM(csConf);
    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user1 has permission on root.parent so a queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1", "user1", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        "root.parent.user1");

    // user2 has permission on root.parent.user2 due to ACL templates
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user2", "user2", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(2)),
        newAppMonitor,
        newMockRMContext,
        "user2",
        "root.parent.user2");

    // user3 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(3)),
        "user3");

    // create the root.parent.user2 manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(new QueuePath("root.parent.user2"));

    ParentQueue autoCreatedParentQueue = (ParentQueue) cs.getQueue("parent");
    LeafQueue autoCreatedLeafQueue = (LeafQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedLeafQueue.getParent());

    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user2", "user2", "root.parent"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(4)),
        newAppMonitor,
        newMockRMContext,
        "user2",
        "root.parent.user2");

    // user3 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.parent"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(5)),
        "user3");
  }

  @Test
  public void testFlexibleAutoCreatedQueuesWithSpecializedACLTemplatesAndDynamicRootParentQueue()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf, false);
    csConf.set(PREFIX + "root.queues", "");
    csConf.set(PREFIX + "root.acl_submit_applications", "user1");
    csConf.set(PREFIX + "root.acl_administer_queue", "admin1");

    csConf.setAutoQueueCreationV2Enabled(ROOT, true);

    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    csConf.set(PREFIX + "root." + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(PREFIX + "root." + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "acl_administer_queue",
        "user3");
    csConf.set(PREFIX + "root." + "*." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX +
            "acl_submit_applications",
        "user3");

    MockRM newMockRM = new MockRM(csConf);

    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user1 has permission on root so a queue would be created
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user1", "user1", "root"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user1",
        "root.user1");

    // user2 doesn't have permission to create a dynamic leaf queue (parent only template)
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user2", "user2", "root"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(2)),
        "user2");

    // user3 has permission on root.user2.user3 due to ACL templates
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(3)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(4)),
        "user4");

    // create the root.user2.user3 manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(new QueuePath("root.user2.user3"));

    ParentQueue autoCreatedParentQueue = (ParentQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedParentQueue, "Auto Creation of Queue failed");
    ParentQueue parentQueue = (ParentQueue) cs.getQueue("root");
    assertEquals(parentQueue, autoCreatedParentQueue.getParent());

    LeafQueue autoCreatedLeafQueue = (LeafQueue) cs.getQueue("user3");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedLeafQueue.getParent());

    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "user3", "root.user2"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(5)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.user2.user3");

    // user4 doesn't have permission
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user4", "user4", "root.user2"));
    verifyAppSubmissionFailure(newAppMonitor,
        createAppSubmissionContext(MockApps.newAppID(6)),
        "user4");
  }

  @Test
  public void testFlexibleAutoCreatedQueuesMultiLevelDynamicParentACL()
      throws IOException, YarnException {
    YarnConfiguration conf = createYarnACLEnabledConfiguration();

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf, false);
    csConf.set(PREFIX + "root.queues", "");
    csConf.set(PREFIX + "root.acl_submit_applications", "user1");
    csConf.set(PREFIX + "root.acl_administer_queue", "admin1");

    csConf.setAutoQueueCreationV2Enabled(ROOT, true);

    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_administer_queue",
        "user2");
    csConf.set(PREFIX + "root." + AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + "acl_submit_applications",
        "user2");

    csConf.set(PREFIX + "root." + "user2.user3." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + "capacity",
        "1w");
    csConf.set(PREFIX + "root." + "user2.user3." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX +
            "acl_administer_queue",
        "user3");
    csConf.set(PREFIX + "root." + "user2.user3." + AUTO_QUEUE_LEAF_TEMPLATE_PREFIX +
            "acl_submit_applications",
        "user3");
    csConf.setMaximumAutoCreatedQueueDepth(4);

    MockRM newMockRM = new MockRM(csConf);

    RMContext newMockRMContext = newMockRM.getRMContext();
    TestRMAppManager newAppMonitor = createAppManager(newMockRMContext, conf);

    // user3 has permission on root.user2.user3.queue due to ACL templates
    newMockRMContext.setQueuePlacementManager(createMockPlacementManager(
        "user3", "queue", "root.user2.user3"));
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(1)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.user2.user3.queue");

    // create the root.user2.user3.queue manually
    CapacityScheduler cs =
        ((CapacityScheduler) newMockRM.getResourceScheduler());
    cs.getCapacitySchedulerQueueManager().createQueue(new QueuePath("root.user2.user3.queue"));

    ParentQueue autoCreatedParentQueue = (ParentQueue) cs.getQueue("user2");
    assertNotNull(autoCreatedParentQueue, "Auto Creation of Queue failed");
    ParentQueue parentQueue = (ParentQueue) cs.getQueue("root");
    assertEquals(parentQueue, autoCreatedParentQueue.getParent());

    ParentQueue autoCreatedParentQueue2 = (ParentQueue) cs.getQueue("user3");
    assertNotNull(autoCreatedParentQueue2, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedParentQueue2.getParent());

    LeafQueue autoCreatedLeafQueue = (LeafQueue) cs.getQueue("queue");
    assertNotNull(autoCreatedLeafQueue, "Auto Creation of Queue failed");
    assertEquals(autoCreatedParentQueue, autoCreatedParentQueue2.getParent());

    // reinitialize to load the ACLs for the queue
    cs.reinitialize(csConf, newMockRMContext);

    // template ACLs do work after reinitialize
    verifyAppSubmission(createAppSubmissionContext(MockApps.newAppID(2)),
        newAppMonitor,
        newMockRMContext,
        "user3",
        "root.user2.user3.queue");
  }

  private YarnConfiguration createYarnACLEnabledConfiguration() {
    YarnConfiguration conf = new YarnConfiguration(new Configuration(false));
    conf.set(YarnConfiguration.YARN_ACL_ENABLE, "true");
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    return conf;
  }

  private CapacitySchedulerConfiguration createFlexibleAQCBaseACLConfiguration(
      YarnConfiguration conf) {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf, false);
    csConf.set(PREFIX + "root.queues", "parent");
    csConf.set(PREFIX + "root.acl_submit_applications", " ");
    csConf.set(PREFIX + "root.acl_administer_queue", " ");

    csConf.setCapacity(PARENT, "1w");
    csConf.set(PREFIX + "root.parent.acl_administer_queue", "user1");
    csConf.set(PREFIX + "root.parent.acl_submit_applications", "user1");

    csConf.setAutoQueueCreationV2Enabled(PARENT, true);
    return csConf;
  }

  private static void verifyAppSubmissionFailure(TestRMAppManager appManager,
                                                 ApplicationSubmissionContext submission,
                                                 String user) {
    try {
      appManager.submitApplication(submission, user);
      fail(
          String.format("should fail since %s does not have permission to submit to queue", user));
    } catch (YarnException e) {
      assertTrue(e.getCause() instanceof AccessControlException);
    }
  }

  private static void verifyAppSubmission(ApplicationSubmissionContext submission,
                                          TestRMAppManager appManager,
                                          RMContext rmContext,
                                          String user,
                                          String expectedQueue) throws YarnException {
    appManager.submitApplication(submission, user);
    RMApp app = rmContext.getRMApps().get(submission.getApplicationId());
    assertNotNull(app, "app should not be null");
    assertEquals(expectedQueue, app.getQueue(),
        String.format("the queue should be placed on '%s' queue", expectedQueue));
  }

  private static ApplicationSubmissionContext createAppSubmissionContext(ApplicationId id) {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    ApplicationSubmissionContext appSubmission =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appSubmission.setApplicationId(id);
    appSubmission.setAMContainerSpec(mockContainerLaunchContext(recordFactory));
    appSubmission.setResource(mockResource());
    appSubmission.setPriority(Priority.newInstance(0));
    appSubmission.setQueue("default");
    return appSubmission;
  }

  @AfterEach
  public void tearDown() {
    setAppEventType(RMAppEventType.KILL);
    ((Service)rmContext.getDispatcher()).stop();
    UserGroupInformation.reset();
  }

  @Test
  public void testRMAppRetireNone() throws Exception {
    long now = System.currentTimeMillis();

    // Create such that none of the applications will retire since
    // haven't hit max #
    RMContext rmContext = mockRMContext(10, now - 10);
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 10);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext,conf);

    assertEquals(10, rmContext.getRMApps().size(),
        "Number of apps incorrect before checkAppTimeLimit");

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    assertEquals(10, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(10, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
    verify(rmContext.getStateStore(), never()).removeApplication(
      isA(RMApp.class));
  }

  @Test
  public void testQueueSubmitWithNoPermission() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(PREFIX + "root.acl_submit_applications", " ");
    conf.set(PREFIX + "root.acl_administer_queue", " ");

    conf.set(PREFIX + "root.default.acl_submit_applications", " ");
    conf.set(PREFIX + "root.default.acl_administer_queue", " ");
    conf.set(YarnConfiguration.YARN_ACL_ENABLE, "true");
    MockRM mockRM = new MockRM(conf);
    ClientRMService rmService = mockRM.getClientRMService();
    SubmitApplicationRequest req =
        Records.newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub =
        Records.newRecord(ApplicationSubmissionContext.class);
    sub.setApplicationId(appId);
    ResourceRequest resReg =
        ResourceRequest.newInstance(Priority.newInstance(0),
            ResourceRequest.ANY, Resource.newInstance(1024, 1), 1);
    sub.setAMContainerResourceRequests(Collections.singletonList(resReg));
    req.setApplicationSubmissionContext(sub);
    sub.setAMContainerSpec(mock(ContainerLaunchContext.class));
    try {
      rmService.submitApplication(req);
    } catch (Exception e) {
      e.printStackTrace();
      if (e instanceof YarnException) {
        assertTrue(e.getCause() instanceof AccessControlException);
      } else {
        fail("Yarn exception is expected : " + e.getMessage());
      }
    } finally {
      mockRM.close();
    }
  }

  @Test
  public void testRMAppRetireSome() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS, 3); 
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 3);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, conf);

    assertEquals(10, rmContext
        .getRMApps().size(), "Number of apps incorrect before");

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    assertEquals(3, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(3, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
    verify(rmContext.getStateStore(), times(7)).removeApplication(
      isA(RMApp.class));
  }

  @Test
  public void testRMAppRetireSomeDifferentStates() throws Exception {
    long now = System.currentTimeMillis();

    // these parameters don't matter, override applications below
    RMContext rmContext = mockRMContext(10, now - 20000);
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS, 2);
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);

    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, conf);

    // clear out applications map
    rmContext.getRMApps().clear();
    assertEquals(0, rmContext.getRMApps().size(), "map isn't empty");

    // 6 applications are in final state, 4 are not in final state.
    // / set with various finished states
    RMApp app = new MockRMApp(0, now - 20000, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(1, now - 200000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(2, now - 30000, RMAppState.FINISHED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(3, now - 20000, RMAppState.RUNNING);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(4, now - 20000, RMAppState.NEW);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    // make sure it doesn't expire these since still running
    app = new MockRMApp(5, now - 10001, RMAppState.KILLED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(6, now - 30000, RMAppState.ACCEPTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(7, now - 20000, RMAppState.SUBMITTED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(8, now - 10001, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);
    app = new MockRMApp(9, now - 20000, RMAppState.FAILED);
    rmContext.getRMApps().put(app.getApplicationId(), app);

    assertEquals(10, rmContext
        .getRMApps().size(), "Number of apps incorrect before");

    // add them to completed apps list
    addToCompletedApps(appMonitor, rmContext);

    // shouldn't  have to many apps
    appMonitor.checkAppNumCompletedLimit();
    assertEquals(6, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(2, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
    // 6 applications in final state, 4 of them are removed
    verify(rmContext.getStateStore(), times(4)).removeApplication(
      isA(RMApp.class));
  }

  @Test
  public void testRMAppRetireNullApp() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, new Configuration());

    assertEquals(10, rmContext
        .getRMApps().size(), "Number of apps incorrect before");

    appMonitor.finishApplication(null);

    assertEquals(0, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
  }

  @Test
  public void testRMAppRetireZeroSetting() throws Exception {
    long now = System.currentTimeMillis();

    RMContext rmContext = mockRMContext(10, now - 20000);
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS, 0);
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 0);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, conf);
    assertEquals(10, rmContext
        .getRMApps().size(), "Number of apps incorrect before");

    addToCompletedApps(appMonitor, rmContext);
    assertEquals(10, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect");

    appMonitor.checkAppNumCompletedLimit();

    assertEquals(0, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(0, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
    verify(rmContext.getStateStore(), times(10)).removeApplication(
      isA(RMApp.class));
  }

  @Test
  public void testStateStoreAppLimitLessThanMemoryAppLimit() {
    long now = System.currentTimeMillis();
    final int allApps = 10;
    RMContext rmContext = mockRMContext(allApps, now - 20000);
    Configuration conf = new YarnConfiguration();
    int maxAppsInMemory = 8;
    int maxAppsInStateStore = 4;
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, maxAppsInMemory);
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS,
      maxAppsInStateStore);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, conf);

    addToCompletedApps(appMonitor, rmContext);
    assertEquals(allApps, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect");
    appMonitor.checkAppNumCompletedLimit();

    assertEquals(maxAppsInMemory, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(maxAppsInMemory, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");

    int numRemoveAppsFromStateStore = 10 - maxAppsInStateStore;
    verify(rmContext.getStateStore(), times(numRemoveAppsFromStateStore))
      .removeApplication(isA(RMApp.class));
    assertEquals(maxAppsInStateStore,
      appMonitor.getNumberOfCompletedAppsInStateStore());
  }

  @Test
  public void testStateStoreAppLimitGreaterThanMemoryAppLimit() {
    long now = System.currentTimeMillis();
    final int allApps = 10;
    RMContext rmContext = mockRMContext(allApps, now - 20000);
    Configuration conf = new YarnConfiguration();
    int maxAppsInMemory = 8;
    conf.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, maxAppsInMemory);
    // greater than maxCompletedAppsInMemory, reset to RM_MAX_COMPLETED_APPLICATIONS.
    conf.setInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS, 1000);
    TestRMAppManager appMonitor = new TestRMAppManager(rmContext, conf);

    addToCompletedApps(appMonitor, rmContext);
    assertEquals(allApps, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect");
    appMonitor.checkAppNumCompletedLimit();

    int numRemoveApps = allApps - maxAppsInMemory;
    assertEquals(maxAppsInMemory, rmContext.getRMApps().size(),
        "Number of apps incorrect after # completed check");
    assertEquals(maxAppsInMemory, appMonitor.getCompletedAppsListSize(),
        "Number of completed apps incorrect after check");
    verify(rmContext.getStateStore(), times(numRemoveApps)).removeApplication(
      isA(RMApp.class));
    assertEquals(maxAppsInMemory,
      appMonitor.getNumberOfCompletedAppsInStateStore());
  }

  protected void setupDispatcher(RMContext rmContext, Configuration conf) {
    TestDispatcher testDispatcher = new TestDispatcher();
    TestAppManagerDispatcher testAppManagerDispatcher = 
        new TestAppManagerDispatcher();
    rmContext.getDispatcher().register(RMAppEventType.class, testDispatcher);
    rmContext.getDispatcher().register(RMAppManagerEventType.class, testAppManagerDispatcher);
    ((Service)rmContext.getDispatcher()).init(conf);
    ((Service)rmContext.getDispatcher()).start();
    assertEquals(RMAppEventType.KILL, appEventType, "app event type is wrong before");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testRMAppSubmitAMContainerResourceRequests() throws Exception {
    asContext.setResource(Resources.createResource(1024));
    asContext.setAMContainerResourceRequest(
        ResourceRequest.newInstance(Priority.newInstance(0),
            ResourceRequest.ANY, Resources.createResource(1024), 1, true));
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(0),
        ResourceRequest.ANY, Resources.createResource(1025), 1, false));
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(0),
        "/rack", Resources.createResource(1025), 1, false));
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(0),
        "/rack/node", Resources.createResource(1025), 1, true));
    asContext.setAMContainerResourceRequests(cloneResourceRequests(reqs));
    // getAMContainerResourceRequest uses the first entry of
    // getAMContainerResourceRequests
    assertEquals(reqs.get(0), asContext.getAMContainerResourceRequest());
    assertEquals(reqs, asContext.getAMContainerResourceRequests());
    RMApp app = testRMAppSubmit();
    for (ResourceRequest req : reqs) {
      req.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    }

    // setAMContainerResourceRequests has priority over
    // setAMContainerResourceRequest and setResource
    assertEquals(reqs, app.getAMResourceRequests());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testRMAppSubmitAMContainerResourceRequest() throws Exception {
    asContext.setResource(Resources.createResource(1024));
    asContext.setAMContainerResourceRequests(null);
    ResourceRequest req =
        ResourceRequest.newInstance(Priority.newInstance(0),
            ResourceRequest.ANY, Resources.createResource(1025), 1, true);
    req.setNodeLabelExpression(RMNodeLabelsManager.NO_LABEL);
    asContext.setAMContainerResourceRequest(ResourceRequest.clone(req));
    // getAMContainerResourceRequests uses a singleton list of
    // getAMContainerResourceRequest
    assertEquals(req, asContext.getAMContainerResourceRequest());
    assertEquals(req, asContext.getAMContainerResourceRequests().get(0));
    assertEquals(1, asContext.getAMContainerResourceRequests().size());
    RMApp app = testRMAppSubmit();
    // setAMContainerResourceRequest has priority over setResource
    assertEquals(Collections.singletonList(req),
        app.getAMResourceRequests());
  }

  @Test
  public void testRMAppSubmitAMContainerWithNoLabelByRMDefaultAMNodeLabel() throws Exception {
    List<ResourceRequest> reqs = new ArrayList<>();
    ResourceRequest anyReq = ResourceRequest.newInstance(
        Priority.newInstance(1),
        ResourceRequest.ANY, Resources.createResource(1024), 1, false, null,
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
    reqs.add(anyReq);
    asContext.setAMContainerResourceRequests(cloneResourceRequests(reqs));
    asContext.setNodeLabelExpression("fixed");

    Configuration conf = new Configuration(false);
    String defaultAMNodeLabel = "core";
    conf.set(YarnConfiguration.AM_DEFAULT_NODE_LABEL, defaultAMNodeLabel);

    when(mockDefaultQueueInfo.getAccessibleNodeLabels()).thenReturn(
        new HashSet<String>() {{ add("core"); }});

    TestRMAppManager newAppMonitor = createAppManager(rmContext, conf);
    newAppMonitor.submitApplication(asContext, "test");

    RMApp app = rmContext.getRMApps().get(appId);
    waitUntilEventProcessed();
    assertEquals(defaultAMNodeLabel,
        app.getAMResourceRequests().get(0).getNodeLabelExpression());
  }

  @Test
  public void testRMAppSubmitResource() throws Exception {
    asContext.setResource(Resources.createResource(1024));
    asContext.setAMContainerResourceRequests(null);
    RMApp app = testRMAppSubmit();

    // setResource
    assertEquals(Collections.singletonList(
        ResourceRequest.newInstance(RMAppAttemptImpl.AM_CONTAINER_PRIORITY,
        ResourceRequest.ANY, Resources.createResource(1024), 1, true,
            "")),
        app.getAMResourceRequests());
  }

  @Test
  public void testRMAppSubmitNoResourceRequests() throws Exception {
    asContext.setResource(null);
    asContext.setAMContainerResourceRequests(null);
    try {
      testRMAppSubmit();
      fail("Should have failed due to no ResourceRequest");
    } catch (InvalidResourceRequestException e) {
      assertEquals(
          "Invalid resource request, no resources requested",
          e.getMessage());
    }
  }

  @Test
  public void testRMAppSubmitAMContainerResourceRequestsDisagree()
      throws Exception {
    asContext.setResource(null);
    List<ResourceRequest> reqs = new ArrayList<>();
    when(mockDefaultQueueInfo.getAccessibleNodeLabels()).thenReturn
        (new HashSet<String>() {{ add("label1"); add(""); }});
    ResourceRequest anyReq = ResourceRequest.newInstance(
        Priority.newInstance(1),
        ResourceRequest.ANY, Resources.createResource(1024), 1, false, "label1",
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
    reqs.add(anyReq);
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(2),
        "/rack", Resources.createResource(1025), 2, false, "",
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC)));
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(3),
        "/rack/node", Resources.createResource(1026), 3, true, "",
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC)));
    asContext.setAMContainerResourceRequests(cloneResourceRequests(reqs));
    RMApp app = testRMAppSubmit();
    // It should force the requests to all agree on these points
    for (ResourceRequest req : reqs) {
      req.setCapability(anyReq.getCapability());
      req.setExecutionTypeRequest(
          ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
      req.setNumContainers(1);
      req.setPriority(Priority.newInstance(0));
    }
    assertEquals(reqs, app.getAMResourceRequests());
  }

  @Test
  public void testRMAppSubmitAMContainerResourceRequestsNoAny()
      throws Exception {
    asContext.setResource(null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(1),
        "/rack", Resources.createResource(1025), 1, false));
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(1),
        "/rack/node", Resources.createResource(1025), 1, true));
    asContext.setAMContainerResourceRequests(cloneResourceRequests(reqs));
    // getAMContainerResourceRequest uses the first entry of
    // getAMContainerResourceRequests
    assertEquals(reqs, asContext.getAMContainerResourceRequests());
    try {
      testRMAppSubmit();
      fail("Should have failed due to missing ANY ResourceRequest");
    } catch (InvalidResourceRequestException e) {
      assertEquals(
          "Invalid resource request, no resource request specified with *",
          e.getMessage());
    }
  }

  @Test
  public void testRMAppSubmitAMContainerResourceRequestsTwoManyAny()
      throws Exception {
    asContext.setResource(null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(1),
        ResourceRequest.ANY, Resources.createResource(1025), 1, false));
    reqs.add(ResourceRequest.newInstance(Priority.newInstance(1),
        ResourceRequest.ANY, Resources.createResource(1025), 1, false));
    asContext.setAMContainerResourceRequests(cloneResourceRequests(reqs));
    // getAMContainerResourceRequest uses the first entry of
    // getAMContainerResourceRequests
    assertEquals(reqs, asContext.getAMContainerResourceRequests());
    try {
      testRMAppSubmit();
      fail("Should have failed due to too many ANY ResourceRequests");
    } catch (InvalidResourceRequestException e) {
      assertEquals(
          "Invalid resource request, only one resource request with * is " +
              "allowed", e.getMessage());
    }
  }

  private RMApp testRMAppSubmit() throws Exception {
    appMonitor.submitApplication(asContext, "test");
    return waitUntilEventProcessed();
  }

  private RMApp waitUntilEventProcessed() throws InterruptedException {
    RMApp app = rmContext.getRMApps().get(appId);
    assertNotNull(app, "app is null");
    assertEquals(appId, app.getApplicationId(), "app id doesn't match");
    assertEquals(RMAppState.NEW, app.getState(), "app state doesn't match");
    // wait for event to be processed
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) &&
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    assertEquals(RMAppEventType.START, getAppEventType(),
        "app event type sent is wrong");
    return app;
  }

  @Test
  public void testRMAppSubmitWithInvalidTokens() throws Exception {
    // Setup invalid security tokens
    DataOutputBuffer dob = new DataOutputBuffer();
    ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    asContext.getAMContainerSpec().setTokens(securityTokens);
    try {
      appMonitor.submitApplication(asContext, "test");
      fail("Application submission should fail because" +
          " Tokens are invalid.");
    } catch (YarnException e) {
      // Exception is expected
      assertTrue(e.getMessage().contains("java.io.EOFException"),
          "The thrown exception is not java.io.EOFException");
    }
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) &&
        timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    assertEquals(RMAppEventType.APP_REJECTED, getAppEventType(),
        "app event type sent is wrong");
    asContext.getAMContainerSpec().setTokens(null);
  }

  @Test
  @Timeout(value = 30)
  public void testRMAppSubmitMaxAppAttempts() throws Exception {
    int[] globalMaxAppAttempts = new int[] { 10, 1 };
    int[] rmAmMaxAttempts = new int[] { 8, 1 };
    int[][] individualMaxAppAttempts = new int[][]{
        new int[]{ 9, 10, 11, 0 },
        new int[]{ 1, 10, 0, -1 }};
    int[][] expectedNums = new int[][]{
        new int[]{ 9, 10, 10, 8 },
        new int[]{ 1, 1, 1, 1 }};
    for (int i = 0; i < globalMaxAppAttempts.length; ++i) {
      for (int j = 0; j < individualMaxAppAttempts.length; ++j) {
        scheduler = mockResourceScheduler();
        Configuration conf = new Configuration();
        conf.setInt(YarnConfiguration.GLOBAL_RM_AM_MAX_ATTEMPTS,
            globalMaxAppAttempts[i]);
        conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, rmAmMaxAttempts[i]);
        ApplicationMasterService masterService =
            new ApplicationMasterService(rmContext, scheduler);
        TestRMAppManager appMonitor = new TestRMAppManager(rmContext,
            new ClientToAMTokenSecretManagerInRM(), scheduler, masterService,
            new ApplicationACLsManager(conf), conf);

        ApplicationId appID = MockApps.newAppID(i * 4 + j + 1);
        asContext.setApplicationId(appID);
        if (individualMaxAppAttempts[i][j] != 0) {
          asContext.setMaxAppAttempts(individualMaxAppAttempts[i][j]);
        }
        appMonitor.submitApplication(asContext, "test");
        RMApp app = rmContext.getRMApps().get(appID);
        assertEquals(expectedNums[i][j], app.getMaxAppAttempts(),
            "max application attempts doesn't match");

        // wait for event to be processed
        int timeoutSecs = 0;
        while ((getAppEventType() == RMAppEventType.KILL) &&
            timeoutSecs++ < 20) {
          Thread.sleep(1000);
        }
        setAppEventType(RMAppEventType.KILL);
      }
    }
  }

  @Test
  @Timeout(value = 30)
  public void testRMAppSubmitDuplicateApplicationId() throws Exception {
    ApplicationId appId = MockApps.newAppID(0);
    asContext.setApplicationId(appId);
    RMApp appOrig = rmContext.getRMApps().get(appId);
    assertTrue("testApp1" != appOrig.getName(), "app name matches "
        + "but shouldn't");

    // our testApp1 should be rejected and original app with same id should be left in place
    try {
      appMonitor.submitApplication(asContext, "test");
      fail("Exception is expected when applicationId is duplicate.");
    } catch (YarnException e) {
      assertTrue(e.getMessage().contains("Cannot add a duplicate!"),
          "The thrown exception is not the expectd one.");
    }

    // make sure original app didn't get removed
    RMApp app = rmContext.getRMApps().get(appId);
    assertNotNull(app, "app is null");
    assertEquals(appId, app.getApplicationId(), "app id doesn't match");
    assertEquals(RMAppState.FINISHED, app.getState(), "app state doesn't match");
  }

  @SuppressWarnings("deprecation")
  @Test
  @Timeout(value = 30)
  public void testRMAppSubmitInvalidResourceRequest() throws Exception {
    asContext.setResource(Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 1));

    // submit an app
    try {
      appMonitor.submitApplication(asContext, "test");
      fail("Application submission should fail because resource" +
          " request is invalid.");
    } catch (YarnException e) {
      // Exception is expected
      // TODO Change this to assert the expected exception type - post YARN-142
      // sub-task related to specialized exceptions.
      assertTrue(e.getMessage().contains("Invalid resource request"),
          "The thrown exception is not" +
          " InvalidResourceRequestException");
    }
  }

  @Test
  @Timeout(value = 30)
  public void testEscapeApplicationSummary() {
    RMApp app = mock(RMAppImpl.class);
    ApplicationSubmissionContext asc = mock(ApplicationSubmissionContext.class);
    when(asc.getNodeLabelExpression()).thenReturn("test");
    when(app.getApplicationSubmissionContext()).thenReturn(asc);
    when(app.getApplicationId()).thenReturn(
        ApplicationId.newInstance(100L, 1));
    when(app.getName()).thenReturn("Multiline\n\n\r\rAppName");
    when(app.getUser()).thenReturn("Multiline\n\n\r\rUserName");
    when(app.getQueue()).thenReturn("Multiline\n\n\r\rQueueName");
    when(app.getState()).thenReturn(RMAppState.RUNNING);
    when(app.getApplicationType()).thenReturn("MAPREDUCE");
    when(app.getSubmitTime()).thenReturn(1000L);
    when(app.getLaunchTime()).thenReturn(2000L);
    when(app.getApplicationTags()).thenReturn(Sets.newHashSet("tag2", "tag1"));

    RMAppAttempt mockRMAppAttempt = mock(RMAppAttempt.class);
    Container mockContainer = mock(Container.class);
    NodeId mockNodeId = mock(NodeId.class);
    String host = "127.0.0.1";

    when(mockNodeId.getHost()).thenReturn(host);
    when(mockContainer.getNodeId()).thenReturn(mockNodeId);
    when(mockRMAppAttempt.getMasterContainer()).thenReturn(mockContainer);
    when(app.getCurrentAppAttempt()).thenReturn(mockRMAppAttempt);

    Map<String, Long> resourceSecondsMap = new HashMap<>();
    resourceSecondsMap.put(ResourceInformation.MEMORY_MB.getName(), 16384L);
    resourceSecondsMap.put(ResourceInformation.VCORES.getName(), 64L);
    RMAppMetrics metrics =
        new RMAppMetrics(Resource.newInstance(1234, 56),
            10, 1, resourceSecondsMap, new HashMap<>(), 1234);
    when(app.getRMAppMetrics()).thenReturn(metrics);
    when(app.getDiagnostics()).thenReturn(new StringBuilder(
        "Multiline\n\n\r\rDiagnostics=Diagn,ostic"));

    RMAppManager.ApplicationSummary.SummaryBuilder summary =
        new RMAppManager.ApplicationSummary().createAppSummary(app);
    String msg = summary.toString();
    LOG.info("summary: " + msg);
    assertFalse(msg.contains("\n"));
    assertFalse(msg.contains("\r"));

    String escaped = "\\n\\n\\r\\r";
    assertTrue(msg.contains("Multiline" + escaped +"AppName"));
    assertTrue(msg.contains("Multiline" + escaped +"UserName"));
    assertTrue(msg.contains("Multiline" + escaped +"QueueName"));
    assertTrue(msg.contains("appMasterHost=" + host));
    assertTrue(msg.contains("submitTime=1000"));
    assertTrue(msg.contains("launchTime=2000"));
    assertTrue(msg.contains("memorySeconds=16384"));
    assertTrue(msg.contains("vcoreSeconds=64"));
    assertTrue(msg.contains("preemptedAMContainers=1"));
    assertTrue(msg.contains("preemptedNonAMContainers=10"));
    assertTrue(msg.contains("preemptedResources=<memory:1234\\, vCores:56>"));
    assertTrue(msg.contains("applicationType=MAPREDUCE"));
    assertTrue(msg.contains("applicationTags=tag1\\,tag2"));
    assertTrue(msg.contains("applicationNodeLabel=test"));
    assertTrue(msg.contains("diagnostics=Multiline" + escaped
        + "Diagnostics\\=Diagn\\,ostic"));
    assertTrue(msg.contains("totalAllocatedContainers=1234"));
  }

  @Test
  public void testRMAppSubmitWithQueueChanged() throws Exception {
    // Setup a PlacementManager returns a new queue
    PlacementManager placementMgr = mock(PlacementManager.class);
    doAnswer(new Answer<ApplicationPlacementContext>() {

      @Override
      public ApplicationPlacementContext answer(InvocationOnMock invocation)
          throws Throwable {
        return new ApplicationPlacementContext("newQueue");
      }

    }).when(placementMgr).placeApplication(
        any(ApplicationSubmissionContext.class),
        any(String.class),
        any(Boolean.class));
    rmContext.setQueuePlacementManager(placementMgr);

    asContext.setQueue("oldQueue");
    appMonitor.submitApplication(asContext, "test");

    RMApp app = rmContext.getRMApps().get(appId);
    RMAppEvent event = new RMAppEvent(appId, RMAppEventType.START);
    rmContext.getRMApps().get(appId).handle(event);
    event = new RMAppEvent(appId, RMAppEventType.APP_NEW_SAVED);
    rmContext.getRMApps().get(appId).handle(event);

    assertNotNull(app, "app is null");
    assertEquals("newQueue", asContext.getQueue());

    // wait for event to be processed
    int timeoutSecs = 0;
    while ((getAppEventType() == RMAppEventType.KILL) && timeoutSecs++ < 20) {
      Thread.sleep(1000);
    }
    assertEquals(RMAppEventType.START, getAppEventType(),
        "app event type sent is wrong");
  }

  private static ResourceScheduler mockResourceScheduler() {
    return mockResourceScheduler(ResourceScheduler.class);
  }

  private static <T extends ResourceScheduler> ResourceScheduler
      mockResourceScheduler(Class<T> schedulerClass) {
    ResourceScheduler scheduler = mock(schedulerClass);
    when(scheduler.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    when(scheduler.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(scheduler.getMaximumResourceCapability(any(String.class))).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));

    when(scheduler.getMaximumResourceCapability(anyString())).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));

    ResourceCalculator rs = mock(ResourceCalculator.class);
    when(scheduler.getResourceCalculator()).thenReturn(rs);

    when(scheduler.getNormalizedResource(any(), any()))
        .thenAnswer(new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            return (Resource) invocationOnMock.getArguments()[0];
          }
        });

    return scheduler;
  }

  private static ContainerLaunchContext mockContainerLaunchContext(
      RecordFactory recordFactory) {
    ContainerLaunchContext amContainer = recordFactory.newRecordInstance(
        ContainerLaunchContext.class);
    amContainer.setApplicationACLs(new HashMap<ApplicationAccessType, String>());
    return amContainer;
  }

  private static Resource mockResource() {
    return Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
  }

  private static List<ResourceRequest> cloneResourceRequests(
      List<ResourceRequest> reqs) {
    List<ResourceRequest> cloneReqs = new ArrayList<>();
    for (ResourceRequest req : reqs) {
      cloneReqs.add(ResourceRequest.clone(req));
    }
    return cloneReqs;
  }

  @Test
  public void testGetUserNameForPlacementTagBasedPlacementDisabled()
      throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String userIdTag = USER_ID_PREFIX + userNameFromAppTag;
    setApplicationTags("tag1", userIdTag, "tag2");
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }

  /**
   * Test for the case when the application tag based placement is enabled and
   * the submitting user 'user1' is whitelisted and the user from the
   * application tag has access to queue.
   * Expected behaviour: the placement is done for user from the tag 'user2'
   */
  @Test
  public void testGetUserNameForPlacementTagBasedPlacementEnabled()
          throws YarnException {
    String user = "user1";
    String usernameFromAppTag = "user2";
    String expectedQueue = "user1Queue";
    String expectedUser = usernameFromAppTag;
    String userIdTag = USER_ID_PREFIX + usernameFromAppTag;
    setApplicationTags("tag1", userIdTag, "tag2");
    enableApplicationTagPlacement(true, user);
    verifyPlacementUsername(expectedQueue, user, usernameFromAppTag,
            expectedUser);
  }

  /**
   * Test for the case when the application tag based placement is enabled.
   * And submitting user 'user1' is whitelisted and  there are multiple valid
   * username tags passed
   * Expected behaviour: the placement is done for the first valid username
   * from the tag 'user2'
   */
  @Test
  public void testGetUserNameForPlacementTagBasedPlacementMultipleUserIds()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String expectedUser = userNameFromAppTag;
    String userIdTag = USER_ID_PREFIX + expectedUser;
    String userIdTag2 = USER_ID_PREFIX + "user3";
    setApplicationTags("tag1", userIdTag, "tag2", userIdTag2);
    enableApplicationTagPlacement(true, user);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag,
            expectedUser);
  }

  /**
   * Test for the case when the application tag based placement is enabled.
   * And no username is set in the application tag
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementTagBasedPlacementNoUserId()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = null;
    setApplicationTags("tag1", "tag2");
    enableApplicationTagPlacement(true, user);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }

  /**
   * Test for the case when the application tag based placement is enabled but
   * the user from the application tag 'user2' does not have access to the
   * queue.
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementUserWithoutAccessToQueue()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String userIdTag = USER_ID_PREFIX + userNameFromAppTag;
    setApplicationTags("tag1", userIdTag, "tag2");
    enableApplicationTagPlacement(false, user);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }

  /**
   * Test for the case when the application tag based placement is enabled but
   * the submitting user 'user1' is not whitelisted and there is a valid
   * username tag passed.
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementNotWhitelistedUser()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String userIdTag = USER_ID_PREFIX + userNameFromAppTag;
    setApplicationTags("tag1", userIdTag, "tag2");
    enableApplicationTagPlacement(true, "someUser");
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }

  /**
   * Test for the case when the application tag based placement is enabled but
   * there is no whitelisted user.
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementEmptyWhiteList()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String userIdTag = USER_ID_PREFIX + userNameFromAppTag;
    setApplicationTags("tag1", userIdTag, "tag2");
    enableApplicationTagPlacement(false);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }


  /**
   * Test for the case when the application tag based placement is enabled and
   * there is one wrongly qualified user
   * 'userid=' and a valid user 'userid=user2' passed
   * with application tag.
   * Expected behaviour: the placement is done for the first valid username
   * from the tag 'user2'
   */
  @Test
  public void testGetUserNameForPlacementWronglyQualifiedFirstUserNameInTag()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "user2";
    String expectedUser = userNameFromAppTag;
    String userIdTag = USER_ID_PREFIX + userNameFromAppTag;
    String wrongUserIdTag = USER_ID_PREFIX;
    setApplicationTags("tag1", wrongUserIdTag, userIdTag, "tag2");
    enableApplicationTagPlacement(true, user);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag,
            expectedUser);
  }

  /**
   * Test for the case when the application tag based placement is enabled and
   * there is only one wrongly qualified user 'userid=' passed
   * with application tag.
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementWronglyQualifiedUserNameInTag()
          throws YarnException {
    String user = "user1";
    String expectedQueue = "user1Queue";
    String userNameFromAppTag = "";
    String wrongUserIdTag = USER_ID_PREFIX;
    setApplicationTags("tag1", wrongUserIdTag, "tag2");
    enableApplicationTagPlacement(true, user);
    verifyPlacementUsername(expectedQueue, user, userNameFromAppTag, user);
  }

  /**
   * Test for the case when the application tag based placement is enabled.
   * And there is no placement rule defined for the user from the application tag
   * Expected behaviour: the placement is done for the submitting user 'user1'
   */
  @Test
  public void testGetUserNameForPlacementNoRuleDefined()
          throws YarnException {
    String user = "user1";
    String expectedUser = user;
    String userNameFromAppTag = "user2";
    String wrongUserIdTag = USER_ID_PREFIX + userNameFromAppTag;
    setApplicationTags("tag1", wrongUserIdTag, "tag2");
    enableApplicationTagPlacement(true, user);
    PlacementManager placementMgr = mock(PlacementManager.class);
    when(placementMgr.placeApplication(asContext, userNameFromAppTag))
            .thenReturn(null);
    String userNameForPlacement = appMonitor
            .getUserNameForPlacement(user, asContext, placementMgr);
    assertEquals(expectedUser, userNameForPlacement);
  }

  @Test
  @UseMockCapacityScheduler
  public void testCheckAccessFullPathWithCapacityScheduler()
      throws YarnException {
    // make sure we only combine "parent + queue" if CS is selected
    testCheckAccess("root.users", "hadoop");
  }

  @Test
  @UseMockCapacityScheduler
  public void testCheckAccessLeafQueueOnlyWithCapacityScheduler()
      throws YarnException {
    // make sure we that NPE is avoided if there's no parent defined
    testCheckAccess(null, "hadoop");
  }

  private void testCheckAccess(String parent, String queue)
      throws YarnException {
    enableApplicationTagPlacement(true, "hadoop");
    String userIdTag = USER_ID_PREFIX + "hadoop";
    setApplicationTags("tag1", userIdTag, "tag2");
    PlacementManager placementMgr = mock(PlacementManager.class);
    ApplicationPlacementContext appContext;
    String expectedQueue;
    if (parent == null) {
      appContext = new ApplicationPlacementContext(queue);
      expectedQueue = queue;
    } else {
      appContext = new ApplicationPlacementContext(queue, parent);
      expectedQueue = parent + "." + queue;
    }

    when(placementMgr.placeApplication(asContext, "hadoop"))
            .thenReturn(appContext);
    appMonitor.getUserNameForPlacement("hadoop", asContext, placementMgr);

    ArgumentCaptor<String> queueNameCaptor =
        ArgumentCaptor.forClass(String.class);
    verify(scheduler).checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), queueNameCaptor.capture());

    assertEquals(expectedQueue, queueNameCaptor.getValue(),
        "Expected access check for queue");
  }

  private void enableApplicationTagPlacement(boolean userHasAccessToQueue,
                                             String... whiteListedUsers) {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration
            .APPLICATION_TAG_BASED_PLACEMENT_ENABLED, true);
    conf.setStrings(YarnConfiguration
            .APPLICATION_TAG_BASED_PLACEMENT_USER_WHITELIST, whiteListedUsers);
    ((RMContextImpl) rmContext).setYarnConfiguration(conf);
    when(scheduler.checkAccess(any(UserGroupInformation.class),
            eq(QueueACL.SUBMIT_APPLICATIONS), any(String.class)))
            .thenReturn(userHasAccessToQueue);
    ApplicationMasterService masterService =
            new ApplicationMasterService(rmContext, scheduler);
    appMonitor = new TestRMAppManager(rmContext,
            new ClientToAMTokenSecretManagerInRM(),
            scheduler, masterService,
            new ApplicationACLsManager(conf), conf);
  }

  private void verifyPlacementUsername(final String queue,
          final String submittingUser, final String userNameFRomAppTag,
          final String expectedUser)
          throws YarnException {
    PlacementManager placementMgr = mock(PlacementManager.class);
    ApplicationPlacementContext appContext
            = new ApplicationPlacementContext(queue);
    when(placementMgr.placeApplication(asContext, userNameFRomAppTag))
            .thenReturn(appContext);
    String userNameForPlacement = appMonitor
            .getUserNameForPlacement(submittingUser, asContext, placementMgr);
    assertEquals(expectedUser, userNameForPlacement);
  }

  private void setApplicationTags(String... tags) {
    Set<String> applicationTags = new TreeSet<>();
    Collections.addAll(applicationTags, tags);
    asContext.setApplicationTags(applicationTags);
  }

  private class UseCapacitySchedulerRule implements BeforeEachCallback {
    private boolean useCapacityScheduler;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      Method testMethod = context.getRequiredTestMethod();
      useCapacityScheduler = testMethod.getAnnotation(UseMockCapacityScheduler.class) != null;
    }

    public boolean useCapacityScheduler() {
      return useCapacityScheduler;
    }
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface UseMockCapacityScheduler {
    // mark test cases with this which require
    // the scheduler type to be CapacityScheduler
  }
}
