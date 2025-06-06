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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.slf4j.event.Level;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test UAM handling in RM.
 */
public class TestWorkPreservingUnmanagedAM
    extends ParameterizedSchedulerTestBase {

  private YarnConfiguration conf;

  public void initTestWorkPreservingUnmanagedAM(SchedulerType type) throws IOException {
    initParameterizedSchedulerTestBase(type);
    setup();
  }

  public void setup() {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  /**
   * Test UAM work preserving restart. When the keepContainersAcrossAttempt flag
   * is on, we allow UAM to directly register again and move on without getting
   * the applicationAlreadyRegistered exception.
   */
  protected void testUAMRestart(boolean keepContainers) throws Exception {
    // start RM
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm.registerNode();
    Set<NodeId> tokenCacheClientSide = new HashSet();

    // create app and launch the UAM
    boolean unamanged = true;
    int maxAttempts = 1;
    boolean waitForAccepted = true;
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
        .withAppName("")
        .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
        .withAcls(null)
        .withUnmanagedAM(unamanged)
        .withQueue(null)
        .withMaxAppAttempts(maxAttempts)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(waitForAccepted)
        .withKeepContainers(keepContainers)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);

    MockAM am = MockRM.launchUAM(app, rm, nm);

    // Register for the first time
    am.registerAppAttempt();

    // Allocate two containers to UAM
    int numContainers = 3;
    AllocateResponse allocateResponse =
        am.allocate("127.0.0.1", 1000, numContainers, new ArrayList<ContainerId>());
    allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    List<Container> conts = allocateResponse.getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      allocateResponse =
          am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
      conts.addAll(allocateResponse.getAllocatedContainers());
      Thread.sleep(100);
    }
    checkNMTokenForContainer(tokenCacheClientSide, conts);

    // Release one container
    List<ContainerId> releaseList =
        Collections.singletonList(conts.get(0).getId());
    List<ContainerStatus> finishedConts =
        am.allocate(new ArrayList<ResourceRequest>(), releaseList)
            .getCompletedContainersStatuses();
    while (finishedConts.size() < releaseList.size()) {
      nm.nodeHeartbeat(true);
      finishedConts
          .addAll(am
              .allocate(new ArrayList<ResourceRequest>(),
                  new ArrayList<ContainerId>())
              .getCompletedContainersStatuses());
      Thread.sleep(100);
    }

    // Register for the second time
    RegisterApplicationMasterResponse response = null;
    try {
      response = am.registerAppAttempt(false);
      // When AM restart, it means nmToken in client side should be missing
      tokenCacheClientSide.clear();
      response.getNMTokensFromPreviousAttempts()
          .forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    } catch (InvalidApplicationMasterRequestException e) {
      assertEquals(false, keepContainers);
      return;
    }
    assertEquals(true, keepContainers, "RM should not allow second register"
        + " for UAM without keep container flag ");

    // Expecting the two running containers previously
    assertEquals(2, response.getContainersFromPreviousAttempts().size());
    assertEquals(1, response.getNMTokensFromPreviousAttempts().size());

    // Allocate one more containers to UAM, just to be safe
    numContainers = 1;
    am.allocate("127.0.0.1", 1000, numContainers, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    allocateResponse = am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
    allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    conts = allocateResponse.getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      allocateResponse =
          am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
      conts.addAll(allocateResponse.getAllocatedContainers());
      Thread.sleep(100);
    }
    checkNMTokenForContainer(tokenCacheClientSide, conts);

    rm.stop();
  }

  protected void testUAMRestartWithoutTransferContainer(boolean keepContainers) throws Exception {
    // start RM
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm.registerNode();
    Set<NodeId> tokenCacheClientSide = new HashSet();

    // create app and launch the UAM
    boolean unamanged = true;
    int maxAttempts = 1;
    boolean waitForAccepted = true;
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
            .withAppName("")
            .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
            .withAcls(null)
            .withUnmanagedAM(unamanged)
            .withQueue(null)
            .withMaxAppAttempts(maxAttempts)
            .withCredentials(null)
            .withAppType(null)
            .withWaitForAppAcceptedState(waitForAccepted)
            .withKeepContainers(keepContainers)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);

    MockAM am = MockRM.launchUAM(app, rm, nm);

    // Register for the first time
    am.registerAppAttempt();

    // Allocate two containers to UAM
    int numContainers = 3;
    AllocateResponse allocateResponse =
        am.allocate("127.0.0.1", 1000, numContainers, new ArrayList<ContainerId>());
    allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    List<Container> conts = allocateResponse.getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      allocateResponse =
          am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
      conts.addAll(allocateResponse.getAllocatedContainers());
      Thread.sleep(100);
    }
    checkNMTokenForContainer(tokenCacheClientSide, conts);

    // Release all containers, then there are no transfer containfer app attempt
    List<ContainerId> releaseList = new ArrayList();
    releaseList.add(conts.get(0).getId());
    releaseList.add(conts.get(1).getId());
    releaseList.add(conts.get(2).getId());
    List<ContainerStatus> finishedConts =
        am.allocate(new ArrayList<ResourceRequest>(), releaseList)
            .getCompletedContainersStatuses();
    while (finishedConts.size() < releaseList.size()) {
      nm.nodeHeartbeat(true);
      finishedConts
          .addAll(am
              .allocate(new ArrayList<ResourceRequest>(),
                  new ArrayList<ContainerId>())
              .getCompletedContainersStatuses());
      Thread.sleep(100);
    }

    // Register for the second time
    RegisterApplicationMasterResponse response = null;
    try {
      response = am.registerAppAttempt(false);
      // When AM restart, it means nmToken in client side should be missing
      tokenCacheClientSide.clear();
      response.getNMTokensFromPreviousAttempts()
          .forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    } catch (InvalidApplicationMasterRequestException e) {
      assertEquals(false, keepContainers);
      return;
    }
    assertEquals(true, keepContainers, "RM should not allow second register"
        + " for UAM without keep container flag ");

    // Expecting the zero running containers previously
    assertEquals(0, response.getContainersFromPreviousAttempts().size());
    assertEquals(0, response.getNMTokensFromPreviousAttempts().size());

    // Allocate one more containers to UAM, just to be safe
    numContainers = 1;
    am.allocate("127.0.0.1", 1000, numContainers, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    allocateResponse = am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
    allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
    conts = allocateResponse.getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      allocateResponse =
          am.allocate(new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
      allocateResponse.getNMTokens().forEach(token -> tokenCacheClientSide.add(token.getNodeId()));
      conts.addAll(allocateResponse.getAllocatedContainers());
      Thread.sleep(100);
    }
    checkNMTokenForContainer(tokenCacheClientSide, conts);

    rm.stop();
  }

  @Timeout(value = 600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUAMRestartKeepContainers(SchedulerType type) throws Exception {
    initTestWorkPreservingUnmanagedAM(type);
    testUAMRestart(true);
  }

  @Timeout(value = 600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUAMRestartNoKeepContainers(SchedulerType type) throws Exception {
    initTestWorkPreservingUnmanagedAM(type);
    testUAMRestart(false);
  }

  @Timeout(value = 600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUAMRestartKeepContainersWithoutTransferContainer(
      SchedulerType type) throws Exception {
    initTestWorkPreservingUnmanagedAM(type);
    testUAMRestartWithoutTransferContainer(true);
  }

  @Timeout(value = 600)
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testUAMRestartNoKeepContainersWithoutTransferContainer(
      SchedulerType type) throws Exception {
    initTestWorkPreservingUnmanagedAM(type);
    testUAMRestartWithoutTransferContainer(false);
  }

  private void checkNMTokenForContainer(Set<NodeId> cacheToken, List<Container> containers) {
    for (Container container : containers) {
      assertTrue(cacheToken.contains(container.getNodeId()));
    }
  }
}
