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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.TestWorkPreservingRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.Times;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for application life time monitor feature test.
 */
public class TestApplicationLifetimeMonitor {
  private final long maxLifetime = 30L;
  private static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private static final String CQ1 = "child1";
  private static final QueuePath CQ1_QUEUE_PATH = ROOT.createNewLeaf(CQ1);
  private static final QueuePath DEFAULT_QUEUE_PATH = ROOT.createNewLeaf("default");

  private YarnConfiguration conf;

  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{CapacityScheduler.class});
    params.add(new Object[]{FairScheduler.class});
    return params;
  }

  private Class scheduler;

  private void initTestApplicationLifetimeMonitor(Class schedulerParameter)
      throws IOException {
    scheduler = schedulerParameter;
    setup();
  }

  public void setup() throws IOException {
    if (scheduler.equals(CapacityScheduler.class)) {
      // Since there is limited lifetime monitoring support in fair scheduler
      // it does not need queue setup
      long defaultLifetime = 15L;
      Configuration capacitySchedulerConfiguration =
          setUpCSQueue(maxLifetime, defaultLifetime);
      conf = new YarnConfiguration(capacitySchedulerConfiguration);
    } else {
      conf = new YarnConfiguration();
    }
    // Always run for CS, since other scheduler do not support this.
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        scheduler, ResourceScheduler.class);
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    UserGroupInformation.setConfiguration(conf);
    conf.setLong(YarnConfiguration.RM_APPLICATION_MONITOR_INTERVAL_MS,
        3000L);
  }

  @Timeout(value = 60)
  @ParameterizedTest
  @MethodSource("data")
  public void testApplicationLifetimeMonitor(Class schedulerParameter)
      throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    MockRM rm = null;
    try {
      rm = new MockRM(conf);
      rm.start();

      Priority appPriority = Priority.newInstance(0);
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * 1024);

      Map<ApplicationTimeoutType, Long> timeouts =
          new HashMap<ApplicationTimeoutType, Long>();
      timeouts.put(ApplicationTimeoutType.LIFETIME, 10L);
      RMApp app1 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(timeouts)
              .build());

      // 20L seconds
      timeouts.put(ApplicationTimeoutType.LIFETIME, 20L);
      RMApp app2 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(timeouts)
              .build());

      // user not set lifetime, so queue max lifetime will be considered.
      RMApp app3 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(Collections.emptyMap())
              .build());

      // asc lifetime exceeds queue max lifetime
      timeouts.put(ApplicationTimeoutType.LIFETIME, 40L);
      RMApp app4 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(timeouts)
              .build());

      nm1.nodeHeartbeat(true);
      // Send launch Event
      MockAM am1 =
          rm.sendAMLaunched(app1.getCurrentAppAttempt().getAppAttemptId());
      am1.registerAppAttempt();
      rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
      assertTrue((System.currentTimeMillis() - app1.getSubmitTime()) > 10000,
          "Application killed before lifetime value");

      Map<ApplicationTimeoutType, String> updateTimeout =
          new HashMap<ApplicationTimeoutType, String>();
      long newLifetime = 40L;
      // update 30L seconds more to timeout which is greater than queue max
      // lifetime
      String formatISO8601 =
          Times.formatISO8601(System.currentTimeMillis() + newLifetime * 1000);
      updateTimeout.put(ApplicationTimeoutType.LIFETIME, formatISO8601);
      UpdateApplicationTimeoutsRequest request =
          UpdateApplicationTimeoutsRequest.newInstance(app2.getApplicationId(),
              updateTimeout);

      Map<ApplicationTimeoutType, Long> applicationTimeouts =
          app2.getApplicationTimeouts();
      // has old timeout time
      long beforeUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      // update app2 lifetime to new time i.e now + timeout
      rm.getRMContext().getClientRMService().updateApplicationTimeouts(request);

      applicationTimeouts =
          app2.getApplicationTimeouts();
      long afterUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      assertTrue(afterUpdate > beforeUpdate,
          "Application lifetime value not updated");

      // verify for application report.
      RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);
      GetApplicationReportRequest appRequest =
          recordFactory.newRecordInstance(GetApplicationReportRequest.class);
      appRequest.setApplicationId(app2.getApplicationId());
      Map<ApplicationTimeoutType, ApplicationTimeout> appTimeouts = rm
          .getRMContext().getClientRMService().getApplicationReport(appRequest)
          .getApplicationReport().getApplicationTimeouts();
      assertTrue(!appTimeouts.isEmpty(), "Application Timeout are empty.");
      ApplicationTimeout timeout =
          appTimeouts.get(ApplicationTimeoutType.LIFETIME);
      assertTrue(timeout.getRemainingTime() > 0,
          "Application remaining time is incorrect");

      rm.waitForState(app2.getApplicationId(), RMAppState.KILLED);
      // verify for app killed with updated lifetime
      assertTrue(app2.getFinishTime() > afterUpdate,
          "Application killed before lifetime value");

      if (scheduler.equals(CapacityScheduler.class)) {
        // Supported only on capacity scheduler
        rm.waitForState(app3.getApplicationId(), RMAppState.KILLED);

        // app4 submitted exceeding queue max lifetime,
        // so killed after queue max lifetime.
        rm.waitForState(app4.getApplicationId(), RMAppState.KILLED);
        long totalTimeRun = app4.getFinishTime() - app4.getSubmitTime();
        assertTrue(totalTimeRun > (maxLifetime * 1000),
            "Application killed before lifetime value");
        assertTrue(totalTimeRun < ((maxLifetime + 10L) * 1000),
            "Application killed before lifetime value " + totalTimeRun);
      }
    } finally {
      stopRM(rm);
    }
  }

  @Timeout(value = 180)
  @ParameterizedTest
  @MethodSource("data")
  public void testApplicationLifetimeOnRMRestart(Class schedulerParameter) throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());

    MockRM rm1 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true);

    long appLifetime = 30L;
    Map<ApplicationTimeoutType, Long> timeouts =
        new HashMap<ApplicationTimeoutType, Long>();
    timeouts.put(ApplicationTimeoutType.LIFETIME, appLifetime);
    RMApp app1 = MockRMAppSubmitter.submit(rm1,
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
            .withAppPriority(Priority.newInstance(0))
            .withApplicationTimeouts(timeouts)
            .build());
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // Re-start RM
    MockRM rm2 = new MockRM(conf, memStore);

    // make sure app has been unregistered with old RM else both will trigger
    // Expire event
    rm1.getRMContext().getRMAppLifetimeMonitor().unregisterApp(
        app1.getApplicationId(), ApplicationTimeoutType.LIFETIME);

    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // recover app
    RMApp recoveredApp1 =
        rm2.getRMContext().getRMApps().get(app1.getApplicationId());

    NMContainerStatus amContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    NMContainerStatus runningContainer = TestRMRestart.createNMContainerStatus(
        am1.getApplicationAttemptId(), 2, ContainerState.RUNNING);

    nm1.registerNode(Arrays.asList(amContainer, runningContainer), null);

    // Wait for RM to settle down on recovering containers;
    TestWorkPreservingRMRestart.waitForNumContainersToRecover(2, rm2,
        am1.getApplicationAttemptId());
    Set<ContainerId> launchedContainers =
        ((RMNodeImpl) rm2.getRMContext().getRMNodes().get(nm1.getNodeId()))
            .getLaunchedContainers();
    assertTrue(launchedContainers.contains(amContainer.getContainerId()));
    assertTrue(launchedContainers.contains(runningContainer.getContainerId()));

    // check RMContainers are re-recreated and the container state is correct.
    rm2.waitForState(nm1, amContainer.getContainerId(),
        RMContainerState.RUNNING);
    rm2.waitForState(nm1, runningContainer.getContainerId(),
        RMContainerState.RUNNING);

    // re register attempt to rm2
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.ACCEPTED);
    am1.setAMRMProtocol(rm2.getApplicationMasterService(), rm2.getRMContext());
    am1.registerAppAttempt();
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.RUNNING);

    // wait for app life time and application to be in killed state.
    rm2.waitForState(recoveredApp1.getApplicationId(), RMAppState.KILLED);
    assertTrue(recoveredApp1.getFinishTime() > (recoveredApp1.getSubmitTime()
        + appLifetime * 1000), "Application killed before lifetime value");
  }

  @Timeout(value = 60)
  @ParameterizedTest
  @MethodSource("data")
  public void testUpdateApplicationTimeoutForStateStoreUpdateFail(Class schedulerParameter)
      throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    MockRM rm1 = null;
    try {
      MemoryRMStateStore memStore = new MemoryRMStateStore() {
        private int count = 0;

        @Override
        public synchronized void updateApplicationStateInternal(
            ApplicationId appId, ApplicationStateData appState)
            throws Exception {
          // fail only 1 time.
          if (count++ == 0) {
            throw new Exception("State-store update failed");
          }
          super.updateApplicationStateInternal(appId, appState);
        }
      };
      memStore.init(conf);
      rm1 = new MockRM(conf, memStore);
      rm1.start();
      MockNM nm1 =
          new MockNM("127.0.0.1:1234", 8192, rm1.getResourceTrackerService());
      nm1.registerNode();
      nm1.nodeHeartbeat(true);

      long appLifetime = 30L;
      Map<ApplicationTimeoutType, Long> timeouts =
          new HashMap<ApplicationTimeoutType, Long>();
      timeouts.put(ApplicationTimeoutType.LIFETIME, appLifetime);
      RMApp app1 = MockRMAppSubmitter.submit(rm1,
          MockRMAppSubmissionData.Builder.createWithMemory(200, rm1)
              .withAppPriority(Priority.newInstance(0))
              .withApplicationTimeouts(timeouts)
              .build());

      Map<ApplicationTimeoutType, String> updateTimeout =
          new HashMap<ApplicationTimeoutType, String>();
      long newLifetime = 10L;
      // update 10L seconds more to timeout i.e 30L seconds overall
      updateTimeout.put(ApplicationTimeoutType.LIFETIME,
          Times.formatISO8601(System.currentTimeMillis() + newLifetime * 1000));
      UpdateApplicationTimeoutsRequest request =
          UpdateApplicationTimeoutsRequest.newInstance(app1.getApplicationId(),
              updateTimeout);

      Map<ApplicationTimeoutType, Long> applicationTimeouts =
          app1.getApplicationTimeouts();
      // has old timeout time
      long beforeUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      try {
        // update app2 lifetime to new time i.e now + timeout
        rm1.getRMContext().getClientRMService()
            .updateApplicationTimeouts(request);
        fail("Update application should fail.");
      } catch (YarnException e) {
        // expected
        assertTrue(e.getMessage().contains(app1.getApplicationId().toString()),
            "State-store exception does not containe appId");
      }

      applicationTimeouts = app1.getApplicationTimeouts();
      // has old timeout time
      long afterUpdate =
          applicationTimeouts.get(ApplicationTimeoutType.LIFETIME);

      assertEquals(beforeUpdate, afterUpdate,
          "Application timeout is updated");
      rm1.waitForState(app1.getApplicationId(), RMAppState.KILLED);
      // verify for app killed with updated lifetime
      assertTrue(app1.getFinishTime() > afterUpdate,
          "Application killed before lifetime value");
    } finally {
      stopRM(rm1);
    }
  }

  @Timeout(value = 120)
  @ParameterizedTest
  @MethodSource("data")
  public void testInheritAppLifetimeFromParentQueue(Class schedulerParameter) throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    YarnConfiguration yarnConf = conf;
    long maxRootLifetime = 20L;
    long defaultRootLifetime = 10L;
    if (scheduler.equals(CapacityScheduler.class)) {
      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration();
      csConf.setQueues(ROOT, new String[] {CQ1});
      csConf.setCapacity(CQ1_QUEUE_PATH, 100);
      csConf.setMaximumLifetimePerQueue(ROOT, maxRootLifetime);
      csConf.setDefaultLifetimePerQueue(ROOT, defaultRootLifetime);
      yarnConf = new YarnConfiguration(csConf);
    }

    MockRM rm = null;
    try {
      rm = new MockRM(yarnConf);
      rm.start();

      Priority appPriority = Priority.newInstance(0);
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * 1024);

      // user not set lifetime, so queue max lifetime will be considered.
      RMApp app1 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(Collections.emptyMap())
              .withQueue(CQ1)
              .build());

      nm1.nodeHeartbeat(true);

      if (scheduler.equals(CapacityScheduler.class)) {
        // Supported only on capacity scheduler
        CapacityScheduler csched =
            (CapacityScheduler) rm.getResourceScheduler();

        rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
        long totalTimeRun = app1.getFinishTime() - app1.getSubmitTime();
        // Child queue should have inherited parent max and default lifetimes.
        assertEquals(maxRootLifetime,
            csched.getQueue(CQ1).getMaximumApplicationLifetime(),
            "Child queue max lifetime should have overridden"
            + " parent value");
        assertEquals(defaultRootLifetime,
            csched.getQueue(CQ1).getDefaultApplicationLifetime(),
            "Child queue default lifetime should have"
            + "  overridden parent value");
        // app1 (run in the 'child1' queue) should have run longer than the
        // default lifetime but less than the max lifetime.
        assertTrue(totalTimeRun > (defaultRootLifetime * 1000),
            "Application killed before default lifetime value");
        assertTrue(totalTimeRun < (maxRootLifetime * 1000),
            "Application killed after max lifetime value " + totalTimeRun);
      }
    } finally {
      stopRM(rm);
    }
  }

  @Timeout(value = 120)
  @ParameterizedTest
  @MethodSource("data")
  public void testOverrideParentQueueMaxAppLifetime(Class schedulerParameter) throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    YarnConfiguration yarnConf = conf;
    long maxRootLifetime = 20L;
    long maxChildLifetime = 40L;
    long defaultRootLifetime = 10L;
    if (scheduler.equals(CapacityScheduler.class)) {
      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration();
      csConf.setQueues(ROOT, new String[] {CQ1});
      csConf.setCapacity(CQ1_QUEUE_PATH, 100);
      csConf.setMaximumLifetimePerQueue(ROOT, maxRootLifetime);
      csConf.setMaximumLifetimePerQueue(CQ1_QUEUE_PATH, maxChildLifetime);
      csConf.setDefaultLifetimePerQueue(ROOT, defaultRootLifetime);
      csConf.setDefaultLifetimePerQueue(CQ1_QUEUE_PATH, maxChildLifetime);
      yarnConf = new YarnConfiguration(csConf);
    }

    MockRM rm = null;
    try {
      rm = new MockRM(yarnConf);
      rm.start();

      Priority appPriority = Priority.newInstance(0);
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * 1024);

      // user not set lifetime, so queue max lifetime will be considered.
      RMApp app1 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(Collections.emptyMap())
              .withQueue(CQ1)
              .build());

      nm1.nodeHeartbeat(true);

      if (scheduler.equals(CapacityScheduler.class)) {
        // Supported only on capacity scheduler
        CapacityScheduler csched =
            (CapacityScheduler) rm.getResourceScheduler();

        rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
        long totalTimeRun = app1.getFinishTime() - app1.getSubmitTime();
        // Child queue's max lifetime can override parent's and be larger.
        assertTrue((maxRootLifetime < maxChildLifetime) &&
            (totalTimeRun > (maxChildLifetime * 1000)),
            "Application killed before default lifetime value");
        assertEquals(maxRootLifetime, csched.getRootQueue().getMaximumApplicationLifetime(),
            "Root queue max lifetime property set incorrectly");
        assertEquals(maxChildLifetime, csched.getQueue(CQ1).getMaximumApplicationLifetime(),
            "Child queue max lifetime should have overridden"
            + " parent value");
      }
    } finally {
      stopRM(rm);
    }
  }

  @Timeout(value = 120)
  @ParameterizedTest
  @MethodSource("data")
  public void testOverrideParentQueueDefaultAppLifetime(
      Class schedulerParameter) throws Exception {
    initTestApplicationLifetimeMonitor(schedulerParameter);
    YarnConfiguration yarnConf = conf;
    long maxRootLifetime = -1L;
    long maxChildLifetime = -1L;
    long defaultChildLifetime = 10L;
    if (scheduler.equals(CapacityScheduler.class)) {
      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration();
      csConf.setQueues(ROOT, new String[] {CQ1});
      csConf.setCapacity(CQ1_QUEUE_PATH, 100);
      csConf.setMaximumLifetimePerQueue(ROOT, maxRootLifetime);
      csConf.setMaximumLifetimePerQueue(CQ1_QUEUE_PATH, maxChildLifetime);
      csConf.setDefaultLifetimePerQueue(CQ1_QUEUE_PATH, defaultChildLifetime);
      yarnConf = new YarnConfiguration(csConf);
    }

    MockRM rm = null;
    try {
      rm = new MockRM(yarnConf);
      rm.start();

      Priority appPriority = Priority.newInstance(0);
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * 1024);

      // user not set lifetime, so queue max lifetime will be considered.
      RMApp app1 = MockRMAppSubmitter.submit(rm,
          MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
              .withAppPriority(appPriority)
              .withApplicationTimeouts(Collections.emptyMap())
              .withQueue(CQ1)
              .build());

      nm1.nodeHeartbeat(true);

      if (scheduler.equals(CapacityScheduler.class)) {
        // Supported only on capacity scheduler
        CapacityScheduler csched =
            (CapacityScheduler) rm.getResourceScheduler();

        rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
        long totalTimeRun = app1.getFinishTime() - app1.getSubmitTime();
        // app1 (run in 'child1' queue) should have overridden the parent's
        // default lifetime.
        assertTrue(totalTimeRun > (defaultChildLifetime * 1000),
            "Application killed before default lifetime value");
        // Root and child queue's max lifetime should be -1.
        assertEquals(maxRootLifetime, csched.getRootQueue().getMaximumApplicationLifetime(),
            "Root queue max lifetime property set incorrectly");
        assertEquals(maxChildLifetime, csched.getQueue(CQ1).getMaximumApplicationLifetime(),
            "Child queue max lifetime property set incorrectly");
        // 'child1' queue's default lifetime should have overridden parent's.
        assertEquals(defaultChildLifetime, csched.getQueue(CQ1).getDefaultApplicationLifetime(),
            "Child queue default lifetime should have"
            + " overridden parent value");
      }
    } finally {
      stopRM(rm);
    }
  }

  private CapacitySchedulerConfiguration setUpCSQueue(long maxLifetime,
      long defaultLifetime) {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.setQueues(ROOT,
        new String[] {"default"});
    csConf.setCapacity(DEFAULT_QUEUE_PATH, 100);
    csConf.setMaximumLifetimePerQueue(DEFAULT_QUEUE_PATH, maxLifetime);
    csConf.setDefaultLifetimePerQueue(DEFAULT_QUEUE_PATH, defaultLifetime);

    return csConf;
  }

  private void stopRM(MockRM rm) {
    if (rm != null) {
      rm.stop();
    }
  }
}
