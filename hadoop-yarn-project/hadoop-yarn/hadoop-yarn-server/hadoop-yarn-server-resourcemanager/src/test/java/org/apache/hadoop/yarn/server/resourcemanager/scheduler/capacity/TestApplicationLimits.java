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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.setQueueHandler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestApplicationLimits {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TestApplicationLimits.class);
  final static int GB = 1024;

  LeafQueue queue;
  CSQueue root;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  RMContext rmContext = null;
  private CapacitySchedulerContext csContext;

  
  @BeforeEach
  public void setUp() throws IOException {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration();
    setupQueueConfiguration(csConf);
    
    rmContext = TestUtils.getMockRMContext();
    Resource clusterResource = Resources.createResource(10 * 16 * GB, 10 * 32);

    csContext = createCSContext(csConf, resourceCalculator,
        Resources.createResource(GB, 1), Resources.createResource(16*GB, 32),
        clusterResource);
    when(csContext.getRMContext()).thenReturn(rmContext);
    CapacitySchedulerQueueContext queueContext = new CapacitySchedulerQueueContext(csContext);
    setQueueHandler(csContext);

    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    CSQueueStore queues = new CSQueueStore();
    root = CapacitySchedulerQueueManager
        .parseQueue(queueContext, csConf, null, "root",
            queues, queues,
            TestUtils.spyHook);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    queue = spy(new LeafQueue(queueContext, A, root, null));
    QueueResourceQuotas queueResourceQuotas = ((LeafQueue) queues.get(A))
        .getQueueResourceQuotas();
    doReturn(queueResourceQuotas).when(queue).getQueueResourceQuotas();

    // Stub out ACL checks
    doReturn(true).
        when(queue).hasAccess(any(QueueACL.class), 
                              any(UserGroupInformation.class));
    
    // Some default values
    doReturn(100).when(queue).getMaxApplications();
    doReturn(25).when(queue).getMaxApplicationsPerUser();
  }

  private static final String A = "a";
  private static final String B = "b";
  private static final  String C = "c";
  private static final  String D = "d";
  private static final  String AA1 = "a1";
  private static final  String AA2 = "a2";
  private static final  String AA3 = "a3";
  private static final QueuePath ROOT_QUEUE_PATH =
      new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final QueuePath A_QUEUE_PATH = ROOT_QUEUE_PATH.createNewLeaf(A);
  private static final QueuePath B_QUEUE_PATH = ROOT_QUEUE_PATH.createNewLeaf(B);
  private static final QueuePath C_QUEUE_PATH = ROOT_QUEUE_PATH.createNewLeaf(C);
  private static final QueuePath D_QUEUE_PATH = ROOT_QUEUE_PATH.createNewLeaf(D);
  private static final QueuePath AA1_QUEUE_PATH = A_QUEUE_PATH.createNewLeaf(AA1);
  private static final QueuePath AA2_QUEUE_PATH = A_QUEUE_PATH.createNewLeaf(AA2);
  private static final QueuePath AA3_QUEUE_PATH = A_QUEUE_PATH.createNewLeaf(AA3);
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(ROOT_QUEUE_PATH, new String[] {A, B});

    conf.setCapacity(A_QUEUE_PATH, 10);
    conf.setCapacity(B_QUEUE_PATH, 90);
    
    conf.setUserLimit(A_QUEUE_PATH, 50);
    conf.setUserLimitFactor(A_QUEUE_PATH, 5.0f);
    
    LOG.info("Setup top-level queues a and b");
  }

  private FiCaSchedulerApp getMockApplication(int appId, String user,
    Resource amResource) {
    FiCaSchedulerApp application = mock(FiCaSchedulerApp.class);
    ApplicationAttemptId applicationAttemptId =
        TestUtils.getMockApplicationAttemptId(appId, 0);
    doReturn(applicationAttemptId.getApplicationId()).
        when(application).getApplicationId();
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    doReturn(amResource).when(application).getAMResource();
    doReturn(Priority.newInstance(0)).when(application).getPriority();
    doReturn(CommonNodeLabelsManager.NO_LABEL).when(application)
        .getAppAMNodePartitionName();
    doReturn(amResource).when(application).getAMResource(
        CommonNodeLabelsManager.NO_LABEL);
    when(application.compareInputOrderTo(any(FiCaSchedulerApp.class))).thenCallRealMethod();
    when(application.isRunnable()).thenReturn(true);
    return application;
  }
  
  @Test
  public void testAMResourceLimit() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    // This uses the default 10% of cluster value for the max am resources
    // which are allowed, at 80GB = 8GB for AM's at the queue level.  The user
    // am limit is 4G initially (based on the queue absolute capacity)
    // when there is only 1 user, and drops to 2G (the userlimit) when there
    // is a second user
    Resource clusterResource = Resource.newInstance(80 * GB, 40);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    queue = (LeafQueue) root.getChildQueues().stream().filter(
        child -> child.getQueueName().equals(A))
        .findFirst().orElseThrow(NoSuchElementException::new);
    queue.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    ActiveUsersManager activeUsersManager = mock(ActiveUsersManager.class);
    when(queue.getAbstractUsersManager()).thenReturn(activeUsersManager);

    assertEquals(Resource.newInstance(8 * GB, 1),
        queue.calculateAndGetAMResourceLimit());
    assertEquals(Resource.newInstance(4 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // Two apps for user_0, both start
    int APPLICATION_ID = 0;
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    when(activeUsersManager.getNumActiveUsers()).thenReturn(1);

    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // AMLimits unchanged
    assertEquals(Resource.newInstance(8 * GB, 1), queue.getAMResourceLimit());
    assertEquals(Resource.newInstance(4 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // One app for user_1, starts
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_1, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_2, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    
    when(activeUsersManager.getNumActiveUsers()).thenReturn(2);
    
    // Now userAMResourceLimit drops to the queue configured 50% as there is
    // another user active
    assertEquals(Resource.newInstance(8 * GB, 1), queue.getAMResourceLimit());
    assertEquals(Resource.newInstance(2 * GB, 1),
      queue.getUserAMResourceLimit());
    
    // Second user_1 app cannot start
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_1, 
      Resource.newInstance(2 * GB, 1));
    queue.submitApplicationAttempt(app_3, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_1));

    // Now finish app so another should be activated
    queue.finishApplicationAttempt(app_2, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    
  }
  
  @Test
  public void testLimitsComputation() throws Exception {
    final float epsilon = 1e-5f;

    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);

    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = 
      Resources.createResource(100 * 16 * GB, 100 * 16);

    CapacitySchedulerContext context = createCSContext(csConf, resourceCalculator,
        Resources.createResource(GB, 1), Resources.createResource(16*GB, 16),
        clusterResource);
    CapacitySchedulerQueueManager queueManager = context.getCapacitySchedulerQueueManager();
    CapacitySchedulerQueueContext queueContext = new CapacitySchedulerQueueContext(context);

    CSQueueStore queues = new CSQueueStore();
    CSQueue root = 
        CapacitySchedulerQueueManager.parseQueue(queueContext, csConf, null,
            "root", queues, queues, TestUtils.spyHook);
    queueManager.setRootQueue(root);
    root.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    LeafQueue queue = (LeafQueue)queues.get(A);
    
    LOG.info("Queue 'A' -" +
    		" aMResourceLimit=" + queue.getAMResourceLimit() + 
    		" UserAMResourceLimit=" + 
    		queue.getUserAMResourceLimit());
    
    Resource amResourceLimit = Resource.newInstance(160 * GB, 1);
    assertThat(queue.calculateAndGetAMResourceLimit()).
        isEqualTo(amResourceLimit);
    assertThat(queue.getUserAMResourceLimit()).isEqualTo(
        Resource.newInstance(80*GB, 1));
    
    // Assert in metrics
    assertThat(queue.getMetrics().getAMResourceLimitMB()).isEqualTo(
        amResourceLimit.getMemorySize());
    assertThat(queue.getMetrics().getAMResourceLimitVCores()).isEqualTo(
        amResourceLimit.getVirtualCores());

    assertEquals((int)(clusterResource.getMemorySize() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB());
    
    // Add some nodes to the cluster & test new limits
    clusterResource = Resources.createResource(120 * 16 * GB);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    assertThat(queue.calculateAndGetAMResourceLimit()).isEqualTo(
        Resource.newInstance(192 * GB, 1));
    assertThat(queue.getUserAMResourceLimit()).isEqualTo(
        Resource.newInstance(96*GB, 1));
    
    assertEquals((int)(clusterResource.getMemorySize() * queue.getAbsoluteCapacity()),
        queue.getMetrics().getAvailableMB());

    // should return -1 if per queue setting not set
    assertEquals(
        (int)CapacitySchedulerConfiguration.UNDEFINED, 
        csConf.getMaximumApplicationsPerQueue(queue.getQueuePathObject()));
    int expectedMaxApps =  
        (int)
        (CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS * 
        queue.getAbsoluteCapacity());
    assertEquals(expectedMaxApps, queue.getMaxApplications());

    int expectedMaxAppsPerUser = Math.min(expectedMaxApps,
        (int)(expectedMaxApps * (queue.getUserLimit()/100.0f) *
        queue.getUserLimitFactor()));
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());

    // should default to global setting if per queue setting not set
    assertEquals(CapacitySchedulerConfiguration.DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT,
        csConf.getMaximumApplicationMasterResourcePerQueuePercent(
            queue.getQueuePathObject()), epsilon);

    // Change the per-queue max AM resources percentage.
    csConf.setFloat(PREFIX + queue.getQueuePath()
        + ".maximum-am-resource-percent", 0.5f);
    queueContext.reinitialize();
    // Re-create queues to get new configs.
    queues = new CSQueueStore();
    root = CapacitySchedulerQueueManager.parseQueue(
        queueContext, csConf, null, "root",
        queues, queues, TestUtils.spyHook);
    clusterResource = Resources.createResource(100 * 16 * GB);
    queueManager.setRootQueue(root);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    queue = (LeafQueue)queues.get(A);

    assertEquals(0.5f,
        csConf.getMaximumApplicationMasterResourcePerQueuePercent(
          queue.getQueuePathObject()), epsilon);

    assertThat(queue.calculateAndGetAMResourceLimit()).isEqualTo(
        Resource.newInstance(800 * GB, 1));
    assertThat(queue.getUserAMResourceLimit()).isEqualTo(
        Resource.newInstance(400*GB, 1));

    // Change the per-queue max applications.
    csConf.setInt(PREFIX + queue.getQueuePath() + ".maximum-applications",
        9999);
    queueContext.reinitialize();
    // Re-create queues to get new configs.
    queues = new CSQueueStore();
    root = CapacitySchedulerQueueManager.parseQueue(
        queueContext, csConf, null, "root",
        queues, queues, TestUtils.spyHook);
    root.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));

    queue = (LeafQueue)queues.get(A);
    assertEquals(9999, (int)csConf.getMaximumApplicationsPerQueue(
        queue.getQueuePathObject()));
    assertEquals(9999, queue.getMaxApplications());

    expectedMaxAppsPerUser = Math.min(9999, (int)(9999 *
        (queue.getUserLimit()/100.0f) * queue.getUserLimitFactor()));
    assertEquals(expectedMaxAppsPerUser, queue.getMaxApplicationsPerUser());
  }
  
  @Test
  public void testActiveApplicationLimits() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    final String user_2 = "user_2";

    assertEquals(Resource.newInstance(16 * GB, 1),
        queue.calculateAndGetAMResourceLimit());
    assertEquals(Resource.newInstance(8 * GB, 1),
      queue.getUserAMResourceLimit());
    
    int APPLICATION_ID = 0;
    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit third application, should remain pending due to user amlimit
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_2, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Finish one application, app_2 should be activated
    queue.finishApplicationAttempt(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit another one for user_0
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_3, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Submit first app for user_1
    FiCaSchedulerApp app_4 = getMockApplication(APPLICATION_ID++, user_1,
      Resources.createResource(8 * GB, 0));
    queue.submitApplicationAttempt(app_4, user_1);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));

    // Submit first app for user_2, should block due to queue amlimit
    FiCaSchedulerApp app_5 = getMockApplication(APPLICATION_ID++, user_2,
      Resources.createResource(8 * GB, 0));
    queue.submitApplicationAttempt(app_5, user_2);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_2));

    // Now finish one app of user_1 so app_5 should be activated
    queue.finishApplicationAttempt(app_4, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(0, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
    assertEquals(1, queue.getNumActiveApplications(user_2));
    assertEquals(0, queue.getNumPendingApplications(user_2));
    
  }
  
  @Test
  public void testActiveLimitsWithKilledApps() throws Exception {
    final String user_0 = "user_0";

    int APPLICATION_ID = 0;

    // Submit first application
    FiCaSchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_0, user_0);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_0));

    // Submit second application
    FiCaSchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_1, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_1));

    // Submit third application, should remain pending
    FiCaSchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_2, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getPendingApplications().contains(app_2));

    // Submit fourth application, should remain pending
    FiCaSchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0,
      Resources.createResource(4 * GB, 0));
    queue.submitApplicationAttempt(app_3, user_0);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(2, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getPendingApplications().contains(app_3));

    // Kill 3rd pending application
    queue.finishApplicationAttempt(app_2, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getPendingApplications().contains(app_2));
    assertFalse(queue.getApplications().contains(app_2));

    // Finish 1st application, app_3 should become active
    queue.finishApplicationAttempt(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertTrue(queue.getApplications().contains(app_3));
    assertFalse(queue.getPendingApplications().contains(app_3));
    assertFalse(queue.getApplications().contains(app_0));

    // Finish 2nd application
    queue.finishApplicationAttempt(app_1, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_1));

    // Finish 4th application
    queue.finishApplicationAttempt(app_3, A);
    assertEquals(0, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(0, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    assertFalse(queue.getApplications().contains(app_3));
  }

  @Test
  public void testHeadroom() throws Exception {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    csConf.setUserLimit(A_QUEUE_PATH, 25);
    setupQueueConfiguration(csConf);

    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(100 * 16 * GB);

    CapacitySchedulerContext context = createCSContext(csConf, resourceCalculator,
        Resources.createResource(GB), Resources.createResource(16*GB), clusterResource);
    CapacitySchedulerQueueManager queueManager = context.getCapacitySchedulerQueueManager();
    CapacitySchedulerQueueContext queueContext = new CapacitySchedulerQueueContext(context);

    CSQueueStore queues = new CSQueueStore();
    CSQueue rootQueue = CapacitySchedulerQueueManager.parseQueue(queueContext,
        csConf, null, "root", queues, queues, TestUtils.spyHook);
    queueManager.setRootQueue(rootQueue);
    rootQueue.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    ResourceUsage queueCapacities = rootQueue.getQueueResourceUsage();
    when(context.getClusterResourceUsage())
        .thenReturn(queueCapacities);

    // Manipulate queue 'a'
    LeafQueue queue = TestLeafQueue.stubLeafQueue((LeafQueue)queues.get(A));
    queue.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, rack_0, 0, 16*GB);

    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    RecordFactory recordFactory = 
        RecordFactoryProvider.getRecordFactory(null);
    RMContext rmContext = TestUtils.getMockRMContext();
    RMContext spyRMContext = spy(rmContext);
    
    ConcurrentMap<ApplicationId, RMApp> spyApps = 
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    ResourceRequest amResourceRequest = mock(ResourceRequest.class);
    Resource amResource = Resources.createResource(0, 0);
    when(amResourceRequest.getCapability()).thenReturn(amResource);
    when(rmApp.getAMResourceRequests()).thenReturn(
        Collections.singletonList(amResourceRequest));
    doReturn(rmApp)
        .when(spyApps).get(ArgumentMatchers.<ApplicationId>any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getRMAppAttempt(any()))
        .thenReturn(rmAppAttempt);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    doReturn(rmApp)
        .when(spyApps).get(ArgumentMatchers.<ApplicationId>any());
    doReturn(true).when(spyApps)
        .containsKey(ArgumentMatchers.<ApplicationId>any());

    Priority priority_1 = TestUtils.createMockPriority(1);

    // Submit first application with some resource-requests from user_0, 
    // and check headroom
    final ApplicationAttemptId appAttemptId_0_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0_0 = new FiCaSchedulerApp(
      appAttemptId_0_0, user_0, queue, 
            queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_0, user_0);

    List<ResourceRequest> app_0_0_requests = new ArrayList<ResourceRequest>();
    app_0_0_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_0_0.updateResourceRequests(app_0_0_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);
    Resource expectedHeadroom = Resources.createResource(5*16*GB, 1);
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());

    // Submit second application from user_0, check headroom
    final ApplicationAttemptId appAttemptId_0_1 = 
        TestUtils.getMockApplicationAttemptId(1, 0); 
    FiCaSchedulerApp app_0_1 = new FiCaSchedulerApp(
      appAttemptId_0_1, user_0, queue, 
            queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_0_1, user_0);
    
    List<ResourceRequest> app_0_1_requests = new ArrayList<ResourceRequest>();
    app_0_1_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_0_1.updateResourceRequests(app_0_1_requests);

    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());// no change
    
    // Submit first application from user_1, check  for new headroom
    final ApplicationAttemptId appAttemptId_1_0 = 
        TestUtils.getMockApplicationAttemptId(2, 0); 
    FiCaSchedulerApp app_1_0 = new FiCaSchedulerApp(
      appAttemptId_1_0, user_1, queue, 
            queue.getAbstractUsersManager(), spyRMContext);
    queue.submitApplicationAttempt(app_1_0, user_1);

    List<ResourceRequest> app_1_0_requests = new ArrayList<ResourceRequest>();
    app_1_0_requests.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 2,
            true, priority_1, recordFactory));
    app_1_0.updateResourceRequests(app_1_0_requests);
    
    // Schedule to compute 
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(10*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());
    assertEquals(expectedHeadroom, app_1_0.getHeadroom());

    // Now reduce cluster size and check for the smaller headroom
    clusterResource = Resources.createResource(90*16*GB);
    rootQueue.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Any change is cluster resource needs to enforce user-limit recomputation.
    // In existing code, LeafQueue#updateClusterResource handled this. However
    // here that method was not used.
    queue.getUsersManager().userLimitNeedsRecompute();
    queue.assignContainers(clusterResource, node_0, new ResourceLimits(
        clusterResource), SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY); // Schedule to compute
    expectedHeadroom = Resources.createResource(9*16*GB / 2, 1); // changes
    assertEquals(expectedHeadroom, app_0_0.getHeadroom());
    assertEquals(expectedHeadroom, app_0_1.getHeadroom());
    assertEquals(expectedHeadroom, app_1_0.getHeadroom());
  }

  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    // Define top-level
    conf.setQueues(ROOT_QUEUE_PATH,
        new String[]{"a", "b", "c", "d"});
    conf.setCapacityByLabel(ROOT_QUEUE_PATH, "x", 100);
    conf.setCapacityByLabel(ROOT_QUEUE_PATH, "y", 100);
    conf.setCapacityByLabel(ROOT_QUEUE_PATH, "z", 100);

    conf.setInt(CapacitySchedulerConfiguration.QUEUE_GLOBAL_MAX_APPLICATION,
        20);
    conf.setInt("yarn.scheduler.capacity.root.a.a1.maximum-applications", 1);
    conf.setFloat("yarn.scheduler.capacity.root.d.user-limit-factor", 0.1f);
    conf.setInt("yarn.scheduler.capacity.maximum-applications", 4);

    conf.setQueues(A_QUEUE_PATH, new String[]{"a1", "a2", "a3"});
    conf.setCapacity(A_QUEUE_PATH, 50);
    conf.setCapacity(B_QUEUE_PATH, 50);
    conf.setCapacity(C_QUEUE_PATH, 0);
    conf.setCapacity(D_QUEUE_PATH, 0);
    conf.setCapacity(AA1_QUEUE_PATH, 50);
    conf.setCapacity(AA2_QUEUE_PATH, 50);
    conf.setCapacity(AA3_QUEUE_PATH, 0);

    conf.setCapacityByLabel(A_QUEUE_PATH, "y", 25);
    conf.setCapacityByLabel(B_QUEUE_PATH, "y", 50);
    conf.setCapacityByLabel(C_QUEUE_PATH, "y", 25);
    conf.setCapacityByLabel(D_QUEUE_PATH, "y", 0);

    conf.setCapacityByLabel(A_QUEUE_PATH, "x", 50);
    conf.setCapacityByLabel(B_QUEUE_PATH, "x", 50);

    conf.setCapacityByLabel(A_QUEUE_PATH, "z", 50);
    conf.setCapacityByLabel(B_QUEUE_PATH, "z", 50);

    conf.setCapacityByLabel(AA1_QUEUE_PATH, "x", 100);
    conf.setCapacityByLabel(AA2_QUEUE_PATH, "x", 0);

    conf.setCapacityByLabel(AA1_QUEUE_PATH, "y", 25);
    conf.setCapacityByLabel(AA2_QUEUE_PATH, "y", 75);

    conf.setCapacityByLabel(AA2_QUEUE_PATH, "z", 75);
    conf.setCapacityByLabel(AA3_QUEUE_PATH, "z", 25);
    return conf;
  }

  private Set<String> toSet(String... elements) {
    Set<String> set = Sets.newHashSet(elements);
    return set;
  }

  @Test
  @Timeout(value = 120)
  public void testApplicationLimitSubmit() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of("x", "y", "z"));

    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 4096);
    MockNM nm2 = rm.registerNode("h2:1234", 4096);
    MockNM nm3 = rm.registerNode("h3:1234", 4096);

    // Submit application to queue c where the default partition capacity is
    // zero
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("c")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app1.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(RMAppState.ACCEPTED, app1.getState());
    rm.killApp(app1.getApplicationId());

    RMApp app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app2.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(RMAppState.ACCEPTED, app2.getState());

    // Check second application is rejected and based on queue level max
    // application app is rejected
    RMApp app3 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("a1")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app3.getApplicationId(), RMAppState.FAILED);
    assertEquals(RMAppState.FAILED, app3.getState());
    assertEquals(
        "org.apache.hadoop.security.AccessControlException: "
            + "Queue root.a.a1 already has 1 applications, cannot accept "
            + "submission of application: " + app3.getApplicationId(),
        app3.getDiagnostics().toString());

    // based on per user max app settings, app should be rejected instantly
    RMApp app13 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user")
            .withAcls(null)
            .withQueue("d")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app13.getApplicationId(), RMAppState.FAILED);
    assertEquals(RMAppState.FAILED, app13.getState());
    assertEquals(
        "org.apache.hadoop.security.AccessControlException: Queue"
            + " root.d already has 0 applications from user user cannot"
            + " accept submission of application: " + app13.getApplicationId(),
        app13.getDiagnostics().toString());

    RMApp app11 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user2")
            .withAcls(null)
            .withQueue("a2")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app11.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(RMAppState.ACCEPTED, app11.getState());
    RMApp app12 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user2")
            .withAcls(null)
            .withQueue("a2")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app12.getApplicationId(), RMAppState.ACCEPTED);
    assertEquals(RMAppState.ACCEPTED, app12.getState());
    // based on system max limit application is rejected
    RMApp app14 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user2")
            .withAcls(null)
            .withQueue("a2")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app14.getApplicationId(), RMAppState.ACCEPTED);
    RMApp app15 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("app")
            .withUser("user2")
            .withAcls(null)
            .withQueue("a2")
            .withWaitForAppAcceptedState(false)
            .build());
    rm.drainEvents();
    rm.waitForState(app15.getApplicationId(), RMAppState.FAILED);
    assertEquals(RMAppState.FAILED, app15.getState());
    assertEquals(
        "Maximum system application limit reached,cannot"
            + " accept submission of application: " + app15.getApplicationId(),
        app15.getDiagnostics().toString());

    rm.killApp(app2.getApplicationId());
    rm.killApp(app13.getApplicationId());
    rm.killApp(app14.getApplicationId());
    rm.stop();
  }

  // Test that max AM limit is correct in the case where one resource is
  // depleted but the other is not. Use DominantResourceCalculator.
  @Test
  public void testAMResourceLimitWithDRCAndFullParent() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    csConf.setFloat(CapacitySchedulerConfiguration.
        MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, 0.3f);

    // Total cluster resources.
    Resource clusterResource = Resources.createResource(100 * GB, 1000);

    CapacitySchedulerQueueContext queueContext = new CapacitySchedulerQueueContext(
        createCSContext(csConf, new DominantResourceCalculator(), Resources.createResource(GB),
            Resources.createResource(16*GB), clusterResource));

    // Set up queue hierarchy.
    CSQueueStore queues = new CSQueueStore();
    CSQueue rootQueue = CapacitySchedulerQueueManager.parseQueue(queueContext,
        csConf, null, "root", queues, queues, TestUtils.spyHook);
    rootQueue.updateClusterResource(clusterResource,
        new ResourceLimits(clusterResource));

    // Queue "queueA" has a 30% capacity guarantee. The max pct of "queueA" that
    // can be used for AMs is 30%. So, 30% of <memory: 100GB, vCores: 1000> is
    // <memory: 30GB, vCores: 30>, which is the guaranteed capacity of "queueA".
    // 30% of that (rounded to the nearest 1GB) is <memory: 9GB, vCores: 9>. The
    // max AM queue limit should never be less than that for any resource.
    LeafQueue queueA = TestLeafQueue.stubLeafQueue((LeafQueue)queues.get(A));
    queueA.setCapacity(30.0f);
    queueA.setUserLimitFactor(10f);
    queueA.setMaxAMResourcePerQueuePercent(0.3f);
    // Make sure "queueA" knows the total cluster resource.
    queueA.updateClusterResource(clusterResource, new ResourceLimits(
        clusterResource));
    // Get "queueA"'s guaranteed capacity (<memory: 30GB, vCores: 300>).
    Resource capacity =
        Resources.multiply(clusterResource, (queueA.getCapacity()/100));
    // Limit is the actual resources available to "queueA". The following
    // simulates the case where a second queue ("queueB") has "borrowed" almost
    // all of "queueA"'s resources because "queueB" has a max capacity of 100%
    // and has gone well over its guaranteed capacity. In this case, "queueB"
    // has used 99GB of memory and used 505 vCores. This is to make vCores
    // dominant in the calculations for the available resources.
    when(queueA.getEffectiveCapacity(any())).thenReturn(capacity);
    Resource limit = Resource.newInstance(1024, 495);
    ResourceLimits currentResourceLimits =
        new ResourceLimits(limit, Resources.none());
    queueA.updateClusterResource(clusterResource, currentResourceLimits);
    Resource expectedAmLimit = Resources.multiply(capacity,
        queueA.getMaxAMResourcePerQueuePercent());
    Resource amLimit = queueA.calculateAndGetAMResourceLimit();
    assertTrue(amLimit.getMemorySize() >= expectedAmLimit.getMemorySize(),
        "AM memory limit is less than expected: Expected: " +
        expectedAmLimit.getMemorySize() + "; Computed: "
        + amLimit.getMemorySize());
    assertTrue(amLimit.getVirtualCores() >= expectedAmLimit.getVirtualCores(),
        "AM vCore limit is less than expected: Expected: " +
        expectedAmLimit.getVirtualCores() + "; Computed: "
        + amLimit.getVirtualCores());
  }

  private CapacitySchedulerContext createCSContext(CapacitySchedulerConfiguration csConf,
      ResourceCalculator rc, Resource minResource, Resource maxResource, Resource clusterResource) {
    YarnConfiguration conf = new YarnConfiguration();

    CapacitySchedulerContext context = mock(CapacitySchedulerContext.class);
    when(context.getConfiguration()).thenReturn(csConf);
    when(context.getConf()).thenReturn(conf);
    when(context.getMinimumResourceCapability()).
        thenReturn(minResource);
    when(context.getMaximumResourceCapability()).
        thenReturn(maxResource);
    when(context.getResourceCalculator()).
        thenReturn(rc);
    CapacitySchedulerQueueManager queueManager = new CapacitySchedulerQueueManager(conf,
        rmContext.getNodeLabelManager(), null);
    when(context.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(context.getCapacitySchedulerQueueManager()).thenReturn(queueManager);

    when(context.getRMContext()).thenReturn(rmContext);
    when(context.getPreemptionManager()).thenReturn(new PreemptionManager());
    setQueueHandler(context);

    // Total cluster resources.
    when(context.getClusterResource()).thenReturn(clusterResource);

    return context;
  }

}
