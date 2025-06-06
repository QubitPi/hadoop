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
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class TestCapacitySchedulerPlanFollower extends
    TestSchedulerPlanFollowerBase {

  private RMContext rmContext;
  private RMContext spyRMContext;
  private CapacitySchedulerContext csContext;
  private CapacityScheduler cs;

  @BeforeEach
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    scheduler = cs;

    rmContext = TestUtils.getMockRMContext();
    spyRMContext = spy(rmContext);

    ConcurrentMap<ApplicationId, RMApp> spyApps =
        spy(new ConcurrentHashMap<ApplicationId, RMApp>());
    RMApp rmApp = mock(RMApp.class);
    RMAppAttempt rmAppAttempt = mock(RMAppAttempt.class);
    when(rmApp.getRMAppAttempt(any()))
        .thenReturn(rmAppAttempt);
    when(rmApp.getCurrentAppAttempt()).thenReturn(rmAppAttempt);
    Mockito.doReturn(rmApp)
        .when(spyApps).get(ArgumentMatchers.<ApplicationId>any());
    Mockito.doReturn(true)
        .when(spyApps).containsKey(ArgumentMatchers.<ApplicationId>any());
    when(spyRMContext.getRMApps()).thenReturn(spyApps);
    when(spyRMContext.getScheduler()).thenReturn(scheduler);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(csConf);

    cs.setConf(csConf);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(minAlloc);
    when(csContext.getMaximumResourceCapability()).thenReturn(maxAlloc);
    when(csContext.getClusterResource()).thenReturn(
        Resources.createResource(100 * 16 * GB, 100 * 32));
    when(scheduler.getClusterResource()).thenReturn(
        Resources.createResource(125 * GB, 125));
    when(csContext.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(csConf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    cs.setRMContext(spyRMContext);
    cs.init(csConf);
    cs.start();

    setupPlanFollower();
  }

  private void setupPlanFollower() throws Exception {
    mClock = mock(Clock.class);
    mAgent = mock(ReservationAgent.class);

    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();
    QueuePath reservationQueuePath =
        ReservationSystemTestUtil.getFullReservationQueuePath();
    CapacitySchedulerConfiguration csConf = cs.getConfiguration();
    csConf.setReservationWindow(reservationQueuePath, 20L);
    csConf.setMaximumCapacity(reservationQueuePath, 40);
    csConf.setAverageCapacity(reservationQueuePath, 20);
    policy.init(reservationQ, csConf);
  }

  @Test
  public void testWithMoveOnExpiry() throws PlanningException,
      InterruptedException, AccessControlException {
    // invoke plan follower test with move
    testPlanFollower(true);
  }

  @Test
  public void testWithKillOnExpiry() throws PlanningException,
      InterruptedException, AccessControlException {
    // invoke plan follower test with kill
    testPlanFollower(false);
  }

  @Override
  protected void verifyCapacity(Queue defQ) {
    CSQueue csQueue = (CSQueue) defQ;
    assertTrue(csQueue.getCapacity() > 0.9);
  }

  @Override
  protected void checkDefaultQueueBeforePlanFollowerRun(){
    Queue defQ = getDefaultQueue();
    assertEquals(0, getNumberOfApplications(defQ));
    assertNotNull(defQ);
  }

  @Override
  protected Queue getDefaultQueue() {
    return cs.getQueue("dedicated" + ReservationConstants.DEFAULT_QUEUE_SUFFIX);
  }

  @Override
  protected int getNumberOfApplications(Queue queue) {
    CSQueue csQueue = (CSQueue) queue;
    int numberOfApplications = csQueue.getNumApplications();
    return numberOfApplications;
  }

  @Override
  protected CapacitySchedulerPlanFollower createPlanFollower() {
    CapacitySchedulerPlanFollower planFollower =
        new CapacitySchedulerPlanFollower();
    planFollower.init(mClock, scheduler, Collections.singletonList(plan));
    return planFollower;
  }

  @Override
  protected void assertReservationQueueExists(ReservationId r) {
    CSQueue q = cs.getQueue(r.toString());
    assertNotNull(q);
  }

  @Override
  protected void assertReservationQueueExists(ReservationId r2,
      double expectedCapacity, double expectedMaxCapacity) {
    CSQueue q = cs.getQueue(r2.toString());
    assertNotNull(q);
    assertEquals(expectedCapacity, q.getCapacity(), 0.01);
    assertEquals(expectedMaxCapacity, q.getMaximumCapacity(), 1.0);
  }

  @Override
  protected void assertReservationQueueDoesNotExist(ReservationId r2) {
    CSQueue q2 = cs.getQueue(r2.toString());
    assertNull(q2);
  }

  public static ApplicationACLsManager mockAppACLsManager() {
    Configuration conf = new Configuration();
    return new ApplicationACLsManager(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scheduler != null) {
      cs.stop();
    }
  }

  protected Queue getReservationQueue(String reservationId) {
    return cs.getQueue(reservationId);
  }
}
