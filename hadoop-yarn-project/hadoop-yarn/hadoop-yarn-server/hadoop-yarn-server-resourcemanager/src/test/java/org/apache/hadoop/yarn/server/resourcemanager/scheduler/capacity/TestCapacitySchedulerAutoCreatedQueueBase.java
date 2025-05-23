/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.security.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.TestGroupsCaching;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCapacitySchedulerAutoCreatedQueueBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestCapacitySchedulerAutoCreatedQueueBase.class);
  public static final int GB = 1024;
  public static final ContainerUpdates NULL_UPDATE_REQUESTS =
      new ContainerUpdates();

  public static final String DEFAULT_PATH = CapacitySchedulerConfiguration.ROOT + ".default";
  public static final String A_PATH = CapacitySchedulerConfiguration.ROOT + ".a";
  public static final String B_PATH = CapacitySchedulerConfiguration.ROOT + ".b";
  public static final String C_PATH = CapacitySchedulerConfiguration.ROOT + ".c";
  public static final String D_PATH = CapacitySchedulerConfiguration.ROOT + ".d";
  public static final String E_PATH = CapacitySchedulerConfiguration.ROOT + ".e";

  public static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  public static final QueuePath DEFAULT = new QueuePath(DEFAULT_PATH);
  public static final QueuePath A = new QueuePath(A_PATH);
  public static final QueuePath B = new QueuePath(B_PATH);
  public static final QueuePath C = new QueuePath(C_PATH);
  public static final QueuePath D = new QueuePath(D_PATH);
  public static final QueuePath E = new QueuePath(E_PATH);
  public static final String ESUBGROUP1_PATH =
      CapacitySchedulerConfiguration.ROOT + ".esubgroup1";
  public static final String FGROUP_PATH =
      CapacitySchedulerConfiguration.ROOT + ".fgroup";
  public static final String A1_PATH = A_PATH + ".a1";
  public static final String A2_PATH = A_PATH + ".a2";
  public static final String B1_PATH = B_PATH + ".b1";
  public static final String B2_PATH = B_PATH + ".b2";
  public static final String B3_PATH = B_PATH + ".b3";
  public static final String B4_PATH = B_PATH + ".b4subgroup1";
  public static final String ESUBGROUP1_A_PATH = ESUBGROUP1_PATH + ".e";
  public static final String FGROUP_F_PATH = FGROUP_PATH + ".f";

  public static final QueuePath A1 = new QueuePath(A1_PATH);
  public static final QueuePath A2 = new QueuePath(A2_PATH);
  public static final QueuePath B1 = new QueuePath(B1_PATH);
  public static final QueuePath B2 = new QueuePath(B2_PATH);
  public static final QueuePath B3 = new QueuePath(B3_PATH);
  public static final QueuePath B4 = new QueuePath(B4_PATH);
  public static final QueuePath E_GROUP = new QueuePath(ESUBGROUP1_PATH);
  public static final QueuePath F_GROUP = new QueuePath(FGROUP_PATH);
  public static final QueuePath E_SG = new QueuePath(ESUBGROUP1_A_PATH);
  public static final QueuePath F_SG = new QueuePath(FGROUP_F_PATH);

  public static final float A_CAPACITY = 20f;
  public static final float B_CAPACITY = 20f;
  public static final float C_CAPACITY = 20f;
  public static final float D_CAPACITY = 20f;
  public static final float ESUBGROUP1_CAPACITY = 10f;
  public static final float FGROUP_CAPACITY = 10f;

  public static final float A1_CAPACITY = 30;
  public static final float A2_CAPACITY = 70;
  public static final float B1_CAPACITY = 60f;
  public static final float B2_CAPACITY = 20f;
  public static final float B3_CAPACITY = 10f;
  public static final float B4_CAPACITY = 10f;
  public static final int NODE_MEMORY = 16;

  public static final int NODE1_VCORES = 16;
  public static final int NODE2_VCORES = 32;
  public static final int NODE3_VCORES = 48;

  public static final String TEST_GROUP = "testusergroup";
  public static final String TEST_GROUPUSER = "testuser";
  public static final String TEST_GROUP1 = "testusergroup1";
  public static final String TEST_GROUPUSER1 = "testuser1";
  public static final String TEST_GROUP2 = "testusergroup2";
  public static final String TEST_GROUPUSER2 = "testuser2";
  public static final String USER = "user_";
  public static final String USER0 = USER + 0;
  public static final String USER1 = USER + 1;
  public static final String USER2 = USER + 2;
  public static final String USER3 = USER + 3;
  public static final String PARENT_QUEUE = "c";
  public static final QueuePath PARENT_QUEUE_PATH = new QueuePath(PARENT_QUEUE);

  public static final Set<String> accessibleNodeLabelsOnC = new HashSet<>();

  public static final String NODEL_LABEL_GPU = "GPU";
  public static final String NODEL_LABEL_SSD = "SSD";

  public static final float NODE_LABEL_GPU_TEMPLATE_CAPACITY = 30.0f;
  public static final float NODEL_LABEL_SSD_TEMPLATE_CAPACITY = 40.0f;
  public static final ImmutableSet<String> RESOURCE_TYPES = ImmutableSet.of("memory", "vcores");

  protected MockRM mockRM = null;
  protected MockNM nm1 = null;
  protected MockNM nm2 = null;
  protected MockNM nm3 = null;
  protected CapacityScheduler cs;
  protected SpyDispatcher dispatcher;
  private static EventHandler<Event> rmAppEventEventHandler;

  public static class SpyDispatcher extends AsyncDispatcher {

    public static BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    public static class SpyRMAppEventHandler implements EventHandler<Event> {
      public void handle(Event event) {
        eventQueue.add(event);
      }
    }

    @Override
    protected void dispatch(Event event) {
      eventQueue.add(event);
    }

    @Override
    public EventHandler<Event> getEventHandler() {
      return rmAppEventEventHandler;
    }

    void spyOnNextEvent(Event expectedEvent, long timeout)
        throws InterruptedException {

      Event event = eventQueue.poll(timeout, TimeUnit.MILLISECONDS);
      assertEquals(expectedEvent.getType(), event.getType());
      assertEquals(expectedEvent.getClass(), event.getClass());
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    QueueMetrics.clearQueueMetrics();
    CapacitySchedulerConfiguration conf = setupSchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    setupQueueMappings(conf, PARENT_QUEUE, true, new int[] { 0, 1, 2, 3 });

    dispatcher = new SpyDispatcher();
    rmAppEventEventHandler = new SpyDispatcher.SpyRMAppEventHandler();
    dispatcher.register(RMAppEventType.class, rmAppEventEventHandler);

    RMNodeLabelsManager mgr = setupNodeLabelManager(conf);

    mockRM = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    mockRM.start();
    cs.start();

    setupNodes(mockRM);
  }

  protected void setupNodes(MockRM newMockRM) throws Exception {
    NodeLabel ssdLabel = Records.newRecord(NodeLabel.class);
    ssdLabel.setName(NODEL_LABEL_SSD);
    ssdLabel.setExclusivity(true);

    nm1 = // label = SSD
        new MockNM("h1:1234",
            Resource.newInstance(NODE_MEMORY * GB, NODE1_VCORES),
            newMockRM.getResourceTrackerService(),
            YarnVersionInfo.getVersion(),
            new HashSet<NodeLabel>() {{ add(ssdLabel); }});

    nm1.registerNode();

    NodeLabel gpuLabel = Records.newRecord(NodeLabel.class);
    gpuLabel.setName(NODEL_LABEL_GPU);
    gpuLabel.setExclusivity(true);

    //Label = GPU
    nm2 = new MockNM("h2:1234",
        Resource.newInstance(NODE_MEMORY * GB, NODE2_VCORES),
        newMockRM.getResourceTrackerService(),
        YarnVersionInfo.getVersion(),
        new HashSet<NodeLabel>() {{ add(gpuLabel); }});
    nm2.registerNode();

    nm3 = // label = ""
        new MockNM("h3:1234", NODE_MEMORY * GB, NODE3_VCORES, newMockRM
            .getResourceTrackerService
                ());
    nm3.registerNode();
  }

  public static CapacitySchedulerConfiguration setupQueueMappings(
      CapacitySchedulerConfiguration conf, String parentQueue, boolean
      overrideWithQueueMappings, int[] userIds) {

    List<String> queuePlacementRules = new ArrayList<>();
    queuePlacementRules.add(YarnConfiguration.USER_GROUP_PLACEMENT_RULE);
    conf.setQueuePlacementRules(queuePlacementRules);

    List<QueueMapping> existingMappings = conf.getQueueMappings();

    //set queue mapping
    List<QueueMapping> queueMappings = new ArrayList<>();
    for (int i = 0; i < userIds.length; i++) {
      //Set C as parent queue name for auto queue creation
      QueueMapping userQueueMapping = QueueMappingBuilder.create()
                                          .type(QueueMapping.MappingType.USER)
                                          .source(USER + userIds[i])
                                          .queue(
                                              getQueueMapping(parentQueue,
                                                  USER + userIds[i]))
                                          .build();
      queueMappings.add(userQueueMapping);
    }

    existingMappings.addAll(queueMappings);
    conf.setQueueMappings(existingMappings);
    //override with queue mappings
    conf.setOverrideWithQueueMappings(overrideWithQueueMappings);
    return conf;
  }

  public static CapacitySchedulerConfiguration setupGroupQueueMappings
      (String parentQueue, CapacitySchedulerConfiguration conf, String
          leafQueueName) {

    List<QueueMapping> existingMappings = conf.getQueueMappings();

    //set queue mapping
    List<QueueMapping> queueMappings = new ArrayList<>();

    //setup group mapping
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestGroupsCaching.FakeunPrivilegedGroupMapping.class, ShellBasedUnixGroupsMapping.class);
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
        TEST_GROUPUSER +"=" + TEST_GROUP + ";" + TEST_GROUPUSER1 +"="
            + TEST_GROUP1 + ";" + TEST_GROUPUSER2 + "=" + TEST_GROUP2 + ";invalid_user=invalid_group");
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf);

    QueueMapping userQueueMapping = QueueMappingBuilder.create()
                                        .type(QueueMapping.MappingType.GROUP)
                                        .source(TEST_GROUP)
                                        .queue(
                                            getQueueMapping(parentQueue,
                                                leafQueueName))
                                        .build();

    QueueMapping userQueueMapping1 = QueueMappingBuilder.create()
        .type(QueueMapping.MappingType.GROUP)
        .source(TEST_GROUP1)
        .queue(
            getQueueMapping(parentQueue,
                leafQueueName))
        .build();

    QueueMapping userQueueMapping2 = QueueMappingBuilder.create()
        .type(QueueMapping.MappingType.GROUP)
        .source(TEST_GROUP2)
        .queue(
            getQueueMapping(parentQueue,
                leafQueueName))
        .build();

    queueMappings.add(userQueueMapping);
    queueMappings.add(userQueueMapping1);
    queueMappings.add(userQueueMapping2);
    existingMappings.addAll(queueMappings);
    conf.setQueueMappings(existingMappings);
    return conf;
  }

  /**
   * @param conf, to be modified
   * @return, CS configuration which has C
   * as an auto creation enabled parent queue
   *  <p>
   * root
   * /     \      \       \
   * a        b      c    d
   * / \    /  |  \
   * a1  a2 b1  b2  b3
   */

  public static CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    //setup new queues with one of them auto enabled
    // Define top-level queues
    // Set childQueue for root
    conf.setQueues(ROOT,
        new String[] {"a", "b", "c", "d", "esubgroup1", "esubgroup2", "fgroup",
            "a1group", "ggroup", "g"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);
    conf.setCapacity(C, C_CAPACITY);
    conf.setCapacity(D, D_CAPACITY);
    conf.setCapacity(E_GROUP, ESUBGROUP1_CAPACITY);
    conf.setCapacity(F_GROUP, FGROUP_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[] { "b1", "b2", "b3", "b4subgroup1" });
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);
    conf.setCapacity(B4, B4_CAPACITY);
    conf.setUserLimitFactor(B4, 100.0f);

    conf.setQueues(E_GROUP, new String[] {"e"});
    conf.setCapacity(E_SG, 100f);
    conf.setUserLimitFactor(E_SG, 100.0f);
    conf.setQueues(F_GROUP, new String[] {"f"});
    conf.setCapacity(F_SG, 100f);
    conf.setUserLimitFactor(F_SG, 100.0f);

    conf.setUserLimitFactor(C, 1.0f);
    conf.setAutoCreateChildQueueEnabled(C, true);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueConfigCapacity(C, 50.0f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(C, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(C, 100);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(C, 3.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(C, 3.0f);
    conf.setAutoCreatedLeafQueueConfigMaximumAllocation(C,
        "memory-mb=10240,vcores=6");

    conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(C, NODEL_LABEL_GPU,
        NODE_LABEL_GPU_TEMPLATE_CAPACITY);
    conf.setAutoCreatedLeafQueueTemplateMaxCapacity(C, NODEL_LABEL_GPU, 100.0f);
    conf.setAutoCreatedLeafQueueTemplateCapacityByLabel(C, NODEL_LABEL_SSD,
        NODEL_LABEL_SSD_TEMPLATE_CAPACITY);
    conf.setAutoCreatedLeafQueueTemplateMaxCapacity(C, NODEL_LABEL_SSD,
        100.0f);

    conf.setDefaultNodeLabelExpression(C, NODEL_LABEL_GPU);
    conf.setAutoCreatedLeafQueueConfigDefaultNodeLabelExpression
        (C, NODEL_LABEL_SSD);


    LOG.info("Setup " + D + " as an auto leaf creation enabled parent queue");

    conf.setUserLimitFactor(D, 1.0f);
    conf.setAutoCreateChildQueueEnabled(D, true);
    conf.setUserLimit(D, 100);
    conf.setUserLimitFactor(D, 3.0f);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueConfigCapacity(D, 10.0f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(D, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(D, 3);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(D, 100);

    conf.set(CapacitySchedulerConfiguration.PREFIX + C + DOT
            + CapacitySchedulerConfiguration
            .AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
            + DOT + CapacitySchedulerConfiguration.ORDERING_POLICY,
        FAIR_APP_ORDERING_POLICY);

    accessibleNodeLabelsOnC.add(NODEL_LABEL_GPU);
    accessibleNodeLabelsOnC.add(NODEL_LABEL_SSD);
    accessibleNodeLabelsOnC.add(NO_LABEL);

    conf.setAccessibleNodeLabels(C, accessibleNodeLabelsOnC);
    conf.setAccessibleNodeLabels(ROOT, accessibleNodeLabelsOnC);
    conf.setCapacityByLabel(ROOT, NODEL_LABEL_GPU, 100f);
    conf.setCapacityByLabel(ROOT, NODEL_LABEL_SSD, 100f);

    conf.setAccessibleNodeLabels(C, accessibleNodeLabelsOnC);
    conf.setCapacityByLabel(C, NODEL_LABEL_GPU, 100f);
    conf.setCapacityByLabel(C, NODEL_LABEL_SSD, 100f);

    LOG.info("Setup " + D + " as an auto leaf creation enabled parent queue");

    return conf;
  }

  public static CapacitySchedulerConfiguration
      setupQueueConfigurationForSingleAutoCreatedLeafQueue(
      CapacitySchedulerConfiguration conf) {

    //setup new queues with one of them auto enabled
    // Define top-level queues
    // Set childQueue for root
    conf.setQueues(ROOT,
        new String[] {"c"});
    conf.setCapacity(C, 100f);

    conf.setUserLimitFactor(C, 1.0f);
    conf.setAutoCreateChildQueueEnabled(C, true);

    //Setup leaf queue template configs
    conf.setAutoCreatedLeafQueueConfigCapacity(C, 100f);
    conf.setAutoCreatedLeafQueueConfigMaxCapacity(C, 100.0f);
    conf.setAutoCreatedLeafQueueConfigUserLimit(C, 100);
    conf.setAutoCreatedLeafQueueConfigUserLimitFactor(C, 3.0f);

    return conf;
  }

  public static void setupQueueConfigurationForSingleFlexibleAutoCreatedLeafQueue(
          CapacitySchedulerConfiguration conf) {

    //setup new queues with one of them auto enabled
    // Define top-level queues
    // Set childQueue for root
    conf.setQueues(ROOT,
            new String[] {"c"});
    conf.setCapacity(C, 100f);

    conf.setUserLimitFactor(C, 1.0f);
    conf.setAutoQueueCreationV2Enabled(C, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  protected void validateCapacities(AutoCreatedLeafQueue autoCreatedLeafQueue,
      float capacity, float absCapacity, float maxCapacity,
      float absMaxCapacity) {
    assertEquals(capacity, autoCreatedLeafQueue.getCapacity(), EPSILON);
    assertEquals(absCapacity, autoCreatedLeafQueue.getAbsoluteCapacity(),
        EPSILON);
    assertEquals(maxCapacity, autoCreatedLeafQueue.getMaximumCapacity(),
        EPSILON);
    assertEquals(absMaxCapacity,
        autoCreatedLeafQueue.getAbsoluteMaximumCapacity(), EPSILON);
  }

  protected void cleanupQueue(String queueName) throws YarnException {
    AutoCreatedLeafQueue queue = (AutoCreatedLeafQueue) cs.getQueue(queueName);
    if (queue != null) {
      setEntitlement(queue, new QueueEntitlement(0.0f, 0.0f));
      ((ManagedParentQueue) queue.getParent()).removeChildQueue(
          queue.getQueuePath());
      cs.getCapacitySchedulerQueueManager().removeQueue(queue.getQueuePath());
    }
  }

  protected ApplicationId submitApp(MockRM rm, CSQueue parentQueue,
      String leafQueueName, String user, int expectedNumAppsInParentQueue,
      int expectedNumAppsInLeafQueue) throws Exception {

    CapacityScheduler capacityScheduler =
        (CapacityScheduler) rm.getResourceScheduler();
    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-auto-queue-activation")
            .withUser(user)
            .withAcls(null)
            .withQueue(leafQueueName)
            .withUnmanagedAM(false)
            .build();
    RMApp rmApp = MockRMAppSubmitter.submit(rm, data);

    // check preconditions
    List<ApplicationAttemptId> appsInParentQueue =
        capacityScheduler.getAppsInQueue(parentQueue.getQueuePath());
    assertEquals(expectedNumAppsInParentQueue, appsInParentQueue.size());

    List<ApplicationAttemptId> appsInLeafQueue =
        capacityScheduler.getAppsInQueue(leafQueueName);
    assertEquals(expectedNumAppsInLeafQueue, appsInLeafQueue.size());

    return rmApp.getApplicationId();
  }

  protected List<QueueMapping> setupQueueMapping(
      CapacityScheduler newCS, String user, String parentQueue, String queue) {
    List<QueueMapping> queueMappings = new ArrayList<>();
    queueMappings.add(QueueMappingBuilder.create()
                          .type(QueueMapping.MappingType.USER)
                          .source(user)
                          .queue(getQueueMapping(parentQueue, queue))
                          .build());
    newCS.getConfiguration().setQueueMappings(queueMappings);
    return queueMappings;
  }

  protected CapacitySchedulerConfiguration setupSchedulerConfiguration() {
    Configuration schedConf = new Configuration();
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.minimum-allocation", 1);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.maximum-allocation", 8);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.minimum-allocation", 1024);
    schedConf.setInt(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.maximum-allocation", 16384);


    return new CapacitySchedulerConfiguration(schedConf);
  }

  protected void setSchedulerMinMaxAllocation(CapacitySchedulerConfiguration conf) {
    unsetMinMaxAllocation(conf);

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 8);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 1024);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 18384);

  }

  private void unsetMinMaxAllocation(CapacitySchedulerConfiguration conf) {
    conf.unset(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.minimum-allocation");
    conf.unset(YarnConfiguration.RESOURCE_TYPES
        + ".vcores.maximum-allocation");
    conf.unset(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.minimum-allocation");
    conf.unset(YarnConfiguration.RESOURCE_TYPES
        + ".memory-mb.maximum-allocation");
  }

  protected MockRM setupSchedulerInstance() throws Exception {

    if (mockRM != null) {
      mockRM.stop();
    }

    CapacitySchedulerConfiguration conf = setupSchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    setupQueueMappings(conf, PARENT_QUEUE, true, new int[] {0, 1, 2, 3});

    RMNodeLabelsManager mgr = setupNodeLabelManager(conf);
    MockRM newMockRM = new MockRM(conf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    newMockRM.start();
    ((CapacityScheduler) newMockRM.getResourceScheduler()).start();
    setupNodes(newMockRM);
    return newMockRM;
  }

  static String getQueueMapping(String parentQueue, String leafQueue) {
    return parentQueue + DOT + leafQueue;
  }

  protected RMNodeLabelsManager setupNodeLabelManager(
      CapacitySchedulerConfiguration conf) throws IOException {
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of(NODEL_LABEL_SSD, NODEL_LABEL_GPU));
    mgr.addLabelsToNode(ImmutableMap
        .of(NodeId.newInstance("h1", 0),
            TestUtils.toSet(NODEL_LABEL_SSD)));
    mgr.addLabelsToNode(ImmutableMap
        .of(NodeId.newInstance("h2", 0),
            TestUtils.toSet(NODEL_LABEL_GPU)));
    return mgr;
  }

  protected ApplicationAttemptId submitApp(CapacityScheduler newCS, String user,
      String queue, String parentQueue) {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, queue, user,
        new ApplicationPlacementContext(queue, parentQueue));
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(
        appAttemptId, false);
    newCS.handle(addAppEvent);
    newCS.handle(addAttemptEvent);
    return appAttemptId;
  }

  protected RMApp submitApp(String user, String queue, String nodeLabel)
      throws Exception {
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, mockRM)
            .withAppName("test-auto-queue-creation" + RandomUtils.nextInt(0, 100))
            .withUser(user)
            .withAcls(null)
            .withQueue(queue)
            .withAmLabel(nodeLabel)
            .build();
    RMApp app = MockRMAppSubmitter.submit(mockRM, data);
    assertEquals(app.getAmNodeLabelExpression(), nodeLabel);
    // check preconditions
    List<ApplicationAttemptId> appsInC = cs.getAppsInQueue(PARENT_QUEUE);
    assertEquals(1, appsInC.size());
    assertNotNull(cs.getQueue(queue));

    return app;
  }

  void setEntitlement(AutoCreatedLeafQueue queue,
      QueueEntitlement entitlement) {
    queue.setCapacity(entitlement.getCapacity());
    queue.setAbsoluteCapacity(
        queue.getParent().getAbsoluteCapacity() * entitlement.getCapacity());
    // note: we currently set maxCapacity to capacity
    // this might be revised later
    queue.setMaxCapacity(entitlement.getMaxCapacity());
  }

  protected void validateUserAndAppLimits(
      AutoCreatedLeafQueue autoCreatedLeafQueue, int maxApps,
      int maxAppsPerUser) {
    assertEquals(maxApps, autoCreatedLeafQueue.getMaxApplications());
    assertEquals(maxAppsPerUser,
        autoCreatedLeafQueue.getMaxApplicationsPerUser());
  }

  protected void validateContainerLimits(
      AutoCreatedLeafQueue autoCreatedLeafQueue, int vCoreLimit,
      long memorySize) {
    assertEquals(vCoreLimit,
        autoCreatedLeafQueue.getMaximumAllocation().getVirtualCores());
    assertEquals(memorySize,
        autoCreatedLeafQueue.getMaximumAllocation().getMemorySize());
  }

  protected void validateInitialQueueEntitlement(CSQueue parentQueue,
      String leafQueueName,
      Map<String, Float> expectedTotalChildQueueAbsCapacityByLabel,
      Set<String> nodeLabels)
      throws SchedulerDynamicEditException, InterruptedException {
    validateInitialQueueEntitlement(mockRM, cs, parentQueue, leafQueueName,
        expectedTotalChildQueueAbsCapacityByLabel, nodeLabels);
  }

  protected void validateInitialQueueEntitlement(ResourceManager rm,
      CSQueue parentQueue, String leafQueueName,
      Map<String, Float> expectedTotalChildQueueAbsCapacityByLabel,
      Set<String> nodeLabels)
      throws SchedulerDynamicEditException, InterruptedException {
    validateInitialQueueEntitlement(rm,
        (CapacityScheduler) rm.getResourceScheduler(), parentQueue,
        leafQueueName, expectedTotalChildQueueAbsCapacityByLabel, nodeLabels);
  }

  protected void validateInitialQueueEntitlement(ResourceManager rm,
      CapacityScheduler capacityScheduler, CSQueue parentQueue,
      String leafQueueName,
      Map<String, Float> expectedTotalChildQueueAbsCapacityByLabel,
      Set<String> nodeLabels)
      throws SchedulerDynamicEditException, InterruptedException {
    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    AutoCreatedLeafQueue leafQueue =
        (AutoCreatedLeafQueue) capacityScheduler.getQueue(leafQueueName);

    Map<String, QueueEntitlement> expectedEntitlements = new HashMap<>();
    QueueCapacities cap = autoCreateEnabledParentQueue.getLeafQueueTemplate()
        .getQueueCapacities();

    for (String label : nodeLabels) {
      validateCapacitiesByLabel(autoCreateEnabledParentQueue, leafQueue, label);
      assertEquals(true, policy.isActive(leafQueue, label));

      assertEquals(expectedTotalChildQueueAbsCapacityByLabel.get(label),
          policy.getAbsoluteActivatedChildQueueCapacity(label), EPSILON);

      QueueEntitlement expectedEntitlement = new QueueEntitlement(
          cap.getCapacity(label), cap.getMaximumCapacity(label));

      expectedEntitlements.put(label, expectedEntitlement);

      validateEffectiveMinResource(rm, capacityScheduler, leafQueue, label,
          expectedEntitlements);
    }
  }

  protected void validateCapacitiesByLabel(ManagedParentQueue
      autoCreateEnabledParentQueue, AutoCreatedLeafQueue leafQueue, String
      label) throws InterruptedException {
    assertEquals(autoCreateEnabledParentQueue.getLeafQueueTemplate()
            .getQueueCapacities().getCapacity(label),
        leafQueue.getQueueCapacities()
            .getCapacity(label), EPSILON);
    assertEquals(autoCreateEnabledParentQueue.getLeafQueueTemplate()
            .getQueueCapacities().getMaximumCapacity(label),
        leafQueue.getQueueCapacities()
            .getMaximumCapacity(label), EPSILON);
  }

  protected void validateEffectiveMinResource(ResourceManager rm,
      CapacityScheduler cs, CSQueue leafQueue, String label,
      Map<String, QueueEntitlement> expectedQueueEntitlements) {
    ManagedParentQueue parentQueue = (ManagedParentQueue) leafQueue.getParent();

    Resource resourceByLabel = rm.getRMContext().getNodeLabelManager()
        .getResourceByLabel(label, cs.getClusterResource());
    Resource effMinCapacity = Resources.multiply(resourceByLabel,
        expectedQueueEntitlements.get(label).getCapacity()
            * parentQueue.getQueueCapacities().getAbsoluteCapacity(label));
    assertEquals(effMinCapacity, Resources.multiply(resourceByLabel,
        leafQueue.getQueueCapacities().getAbsoluteCapacity(label)));

    if (expectedQueueEntitlements.get(label).getCapacity() > EPSILON) {
      if (leafQueue.getCapacityConfigType().equals(ABSOLUTE_RESOURCE)) {
        QueuePath templatePrefix = QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix(
            parentQueue.getQueuePathObject());
        Resource resourceTemplate = parentQueue.getLeafQueueTemplate().getLeafQueueConfigs()
            .getMinimumResourceRequirement(label, templatePrefix, RESOURCE_TYPES);
        assertEquals(resourceTemplate, leafQueue.getEffectiveCapacity(label));
      } else {
        assertEquals(effMinCapacity, leafQueue.getEffectiveCapacity(label));
      }
    } else {
      assertEquals(Resource.newInstance(0, 0),
          leafQueue.getEffectiveCapacity(label));
    }

    if (leafQueue.getQueueCapacities().getAbsoluteCapacity(label) > 0) {
      assertTrue(Resources.greaterThan(cs.getResourceCalculator(),
          cs.getClusterResource(), effMinCapacity, Resources.none()));
    } else {
      assertTrue(Resources.equals(effMinCapacity, Resources.none()));
    }
  }

  protected void validateActivatedQueueEntitlement(CSQueue parentQueue,
      String leafQueueName, Map<String, Float>
      expectedTotalChildQueueAbsCapacity,
      List<QueueManagementChange> queueManagementChanges, Set<String>
      expectedNodeLabels)
      throws SchedulerDynamicEditException {
    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    QueueCapacities cap = autoCreateEnabledParentQueue.getLeafQueueTemplate()
        .getQueueCapacities();

    AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue)
        cs.getQueue(leafQueueName);

    Map<String, QueueEntitlement> expectedEntitlements = new HashMap<>();

    for (String label : expectedNodeLabels) {
      //validate leaf queue state
      assertEquals(true, policy.isActive(leafQueue, label));

      QueueEntitlement expectedEntitlement = new QueueEntitlement(
          cap.getCapacity(label), cap.getMaximumCapacity(label));

      //validate parent queue state
      assertEquals(expectedTotalChildQueueAbsCapacity.get(label),
          policy.getAbsoluteActivatedChildQueueCapacity(label), EPSILON);

      expectedEntitlements.put(label, expectedEntitlement);
    }

    //validate capacity
    validateQueueEntitlements(leafQueueName, expectedEntitlements,
        queueManagementChanges, expectedNodeLabels);
  }

  protected void validateDeactivatedQueueEntitlement(CSQueue parentQueue,
      String leafQueueName, Map<String, Float>
      expectedTotalChildQueueAbsCapacity,
      List<QueueManagementChange>
          queueManagementChanges)
      throws SchedulerDynamicEditException {
    QueueEntitlement expectedEntitlement =
        new QueueEntitlement(0.0f, 1.0f);

    ManagedParentQueue autoCreateEnabledParentQueue =
        (ManagedParentQueue) parentQueue;

    AutoCreatedLeafQueue leafQueue =
        (AutoCreatedLeafQueue) cs.getQueue(leafQueueName);

    GuaranteedOrZeroCapacityOverTimePolicy policy =
        (GuaranteedOrZeroCapacityOverTimePolicy) autoCreateEnabledParentQueue
            .getAutoCreatedQueueManagementPolicy();

    Map<String, QueueEntitlement> expectedEntitlements = new HashMap<>();

    for (String label : accessibleNodeLabelsOnC) {
      //validate parent queue state
      LOG.info("Validating label " + label);
      assertEquals(expectedTotalChildQueueAbsCapacity.get(label), policy
          .getAbsoluteActivatedChildQueueCapacity(label), EPSILON);

      //validate leaf queue state
      assertEquals(false, policy.isActive(leafQueue, label));
      expectedEntitlements.put(label, expectedEntitlement);
    }

    //validate capacity
    validateQueueEntitlements(leafQueueName, expectedEntitlements,
        queueManagementChanges, accessibleNodeLabelsOnC);
  }

  void validateQueueEntitlements(String leafQueueName,
      Map<String, QueueEntitlement> expectedEntitlements,
      List<QueueManagementChange>
          queueEntitlementChanges, Set<String> expectedNodeLabels) {
    AutoCreatedLeafQueue leafQueue = (AutoCreatedLeafQueue) cs.getQueue(
        leafQueueName);
    validateQueueEntitlementChanges(leafQueue, expectedEntitlements,
        queueEntitlementChanges, expectedNodeLabels);
  }

  private void validateQueueEntitlementChanges(AutoCreatedLeafQueue leafQueue,
      Map<String, QueueEntitlement> expectedQueueEntitlements,
      final List<QueueManagementChange> queueEntitlementChanges, Set<String>
      expectedNodeLabels) {
    boolean found = false;

    for (QueueManagementChange entitlementChange : queueEntitlementChanges) {
      if (leafQueue.getQueuePath().equals(
          entitlementChange.getQueue().getQueuePath())) {

        AutoCreatedLeafQueueConfig updatedQueueTemplate =
            entitlementChange.getUpdatedQueueTemplate();

        for (String label : expectedNodeLabels) {
          QueueEntitlement newEntitlement = new QueueEntitlement(
              updatedQueueTemplate.getQueueCapacities().getCapacity(label),
              updatedQueueTemplate.getQueueCapacities().getMaximumCapacity
                  (label));
          assertEquals(expectedQueueEntitlements.get(label), newEntitlement);
          validateEffectiveMinResource(mockRM, cs, leafQueue, label,
              expectedQueueEntitlements);
        }
        found = true;
        break;
      }
    }
    if (!found) {
      fail(
          "Could not find the specified leaf queue in entitlement changes : "
              + leafQueue.getQueuePath());
    }
  }

  protected Map<String, Float> populateExpectedAbsCapacityByLabelForParentQueue
      (int numLeafQueues) {
    Map<String, Float> expectedChildQueueAbsCapacity = new HashMap<>();
    expectedChildQueueAbsCapacity.put(NODEL_LABEL_GPU,
        NODE_LABEL_GPU_TEMPLATE_CAPACITY/100 * numLeafQueues);
    expectedChildQueueAbsCapacity.put(NODEL_LABEL_SSD,
        NODEL_LABEL_SSD_TEMPLATE_CAPACITY/100 * numLeafQueues);
    expectedChildQueueAbsCapacity.put(NO_LABEL, 0.1f * numLeafQueues);
    return expectedChildQueueAbsCapacity;
  }
}
