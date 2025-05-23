/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSMaxRunningAppsEnforcer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;

@SuppressWarnings({ "rawtypes" })
public class TestReservationSystem extends
    ParameterizedSchedulerTestBase {

  private final static String ALLOC_FILE = new File(
      FairSchedulerTestBase.TEST_DIR, TestReservationSystem.class.getName()
          + ".xml").getAbsolutePath();
  private AbstractYarnScheduler scheduler;
  private AbstractReservationSystem reservationSystem;
  private RMContext rmContext;
  private Configuration conf;
  private RMContext mockRMContext;

  public void initTestReservationSystem(SchedulerType type) throws IOException {
    initParameterizedSchedulerTestBase(type);
    setUp();
  }

  public void setUp() throws IOException {
    scheduler = initializeScheduler();
    rmContext = getRMContext();
    reservationSystem = configureReservationSystem();
    reservationSystem.setRMContext(rmContext);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @AfterEach
  public void tearDown() {
    conf = null;
    reservationSystem = null;
    rmContext = null;
    scheduler = null;
    clearRMContext();
    QueueMetrics.clearQueueMetrics();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testInitialize(SchedulerType type) throws IOException {
    initTestReservationSystem(type);
    try {
      reservationSystem.reinitialize(scheduler.getConfig(), rmContext);
    } catch (YarnException e) {
      fail(e.getMessage());
    }
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getReservationQueueName());
    } else {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getFullReservationQueueName());
    }

  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testReinitialize(SchedulerType type) throws IOException {
    initTestReservationSystem(type);
    conf = scheduler.getConfig();
    try {
      reservationSystem.reinitialize(conf, rmContext);
    } catch (YarnException e) {
      fail(e.getMessage());
    }
    if (getSchedulerType().equals(SchedulerType.CAPACITY)) {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getReservationQueueName());
    } else {
      ReservationSystemTestUtil.validateReservationQueue(reservationSystem,
          ReservationSystemTestUtil.getFullReservationQueueName());
    }

    // Dynamically add a plan
    String newQ = "reservation";
    assertNull(reservationSystem.getPlan(newQ));
    updateSchedulerConf(conf, newQ);
    try {
      scheduler.reinitialize(conf, rmContext);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    try {
      reservationSystem.reinitialize(conf, rmContext);
    } catch (YarnException e) {
      fail(e.getMessage());
    }
    ReservationSystemTestUtil.validateReservationQueue(
        reservationSystem,
        "root." + newQ);
  }

  @SuppressWarnings("rawtypes")
  public AbstractYarnScheduler initializeScheduler() throws IOException {
    switch (getSchedulerType()) {
    case CAPACITY:
      return initializeCapacityScheduler();
    case FAIR:
      return initializeFairScheduler();
    }
    return null;
  }

  public AbstractReservationSystem configureReservationSystem() {
    switch (getSchedulerType()) {
    case CAPACITY:
      return new CapacityReservationSystem();
    case FAIR:
      return new FairReservationSystem();
    }
    return null;
  }

  public void updateSchedulerConf(Configuration conf, String newQ)
      throws IOException {
    switch (getSchedulerType()) {
    case CAPACITY:
      ReservationSystemTestUtil.updateQueueConfiguration(
          (CapacitySchedulerConfiguration) conf, newQ);
    case FAIR:
      ReservationSystemTestUtil.updateFSAllocationFile(ALLOC_FILE);
    }
  }

  public RMContext getRMContext() {
    return mockRMContext;
  }

  public void clearRMContext() {
    mockRMContext = null;
  }

  private CapacityScheduler initializeCapacityScheduler() {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);

    CapacityScheduler cs = spy(new CapacityScheduler());
    cs.setConf(conf);
    CSMaxRunningAppsEnforcer enforcer =
        mock(CSMaxRunningAppsEnforcer.class);
    cs.setMaxRunningAppsEnforcer(enforcer);

    mockRMContext = ReservationSystemTestUtil.createRMContext(conf);

    cs.setRMContext(mockRMContext);
    try {
      cs.serviceInit(conf);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    ReservationSystemTestUtil.initializeRMContext(10, cs, mockRMContext);
    return cs;
  }

  private Configuration createFSConfiguration() {
    FairSchedulerTestBase testHelper = new FairSchedulerTestBase();
    Configuration conf = testHelper.createConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    return conf;
  }

  private FairScheduler initializeFairScheduler() throws IOException {
    Configuration conf = createFSConfiguration();
    ReservationSystemTestUtil.setupFSAllocationFile(ALLOC_FILE);

    // Setup
    mockRMContext = ReservationSystemTestUtil.createRMContext(conf);
    return ReservationSystemTestUtil
        .setupFairScheduler(mockRMContext, conf, 10);
  }
}
