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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyService.RequestInterceptorChainWrapper;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAMRMProxyService extends BaseAMRMProxyTest {

  private static final Logger LOG =
       LoggerFactory.getLogger(TestAMRMProxyService.class);

  private static MockResourceManagerFacade mockRM;

  /**
   * Test if the pipeline is created properly.
   *
   * @throws Exception There was an error registerApplicationMaster.
   */
  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RequestInterceptor root =
        super.getAMRMProxyService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      switch (index) {
      case 0:
      case 1:
      case 2:
        assertEquals(PassThroughRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        assertEquals(MockRequestInterceptor.class.getName(), root.getClass().getName());
        break;
      }

      root = root.getNextInterceptor();
      index++;
    }

    assertEquals(Integer.toString(4), Integer.toString(index),
        "The number of interceptors in chain does not match");

  }

  /**
   * Tests registration of a single application master.
   * 
   * @throws Exception There was an error registerApplicationMaster.
   */
  @Test
  public void testRegisterOneApplicationMaster() throws Exception {
    // The testAppId identifier is used as host name and the mock resource
    // manager return it as the queue name. Assert that we received the queue
    // name
    int testAppId = 1;
    RegisterApplicationMasterResponse response1 = registerApplicationMaster(testAppId);
    assertNotNull(response1);
    assertEquals(Integer.toString(testAppId), response1.getQueue());
  }

  /**
   * Tests the case when interceptor pipeline initialization fails.
   *
   * @throws IOException There was an error registerApplicationMaster.
   */
  @Test
  public void testInterceptorInitFailure() throws IOException {
    Configuration conf = this.getConf();
    // Override with a bad interceptor configuration
    conf.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        "class.that.does.not.exist");

    // Reinitialize instance with the new config
    createAndStartAMRMProxyService(conf);
    int testAppId = 1;
    try {
      registerApplicationMaster(testAppId);
      fail("Should not reach here. Expecting an exception thrown");
    } catch (Exception e) {
      Map<ApplicationId, RequestInterceptorChainWrapper> pipelines =
          getAMRMProxyService().getPipelines();
      ApplicationId id = getApplicationId(testAppId);
      assertNull(pipelines.get(id),
          "The interceptor pipeline should be removed if initialization fails");
    }
  }

  /**
   * Tests the registration of multiple application master serially one at a
   * time.
   * 
   * @throws Exception There was an error registerApplicationMaster.
   */
  @Test
  public void testRegisterMultipleApplicationMasters() throws Exception {
    for (int testAppId = 0; testAppId < 3; testAppId++) {
      RegisterApplicationMasterResponse response = registerApplicationMaster(testAppId);
      assertNotNull(response);
      assertEquals(Integer.toString(testAppId), response.getQueue());
    }
  }

  /**
   * Tests the registration of multiple application masters using multiple
   * threads in parallel.
   *
   */
  @Test
  public void testRegisterMultipleApplicationMastersInParallel() {
    int numberOfRequests = 5;
    ArrayList<String> testContexts =
        CreateTestRequestIdentifiers(numberOfRequests);
    super.registerApplicationMastersInParallel(testContexts);
  }

  private ArrayList<String> CreateTestRequestIdentifiers(
      int numberOfRequests) {
    ArrayList<String> testContexts = new ArrayList<>();
    LOG.info("Creating " + numberOfRequests + " contexts for testing");
    for (int ep = 0; ep < numberOfRequests; ep++) {
      testContexts.add("test-endpoint-" + ep);
      LOG.info("Created test context: " + testContexts.get(ep));
    }
    return testContexts;
  }

  @Test
  public void testFinishOneApplicationMasterWithSuccess() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId,
            FinalApplicationStatus.SUCCEEDED);

    assertNotNull(finishResponse);
    assertTrue(finishResponse.getIsUnregistered());
  }

  @Test
  public void testFinishOneApplicationMasterWithFailure() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId, FinalApplicationStatus.FAILED);

    assertNotNull(finishResponse);

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
      fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishInvalidApplicationMaster() {
    try {
      // Try to finish an application master that was not registered.
      finishApplicationMaster(4, FinalApplicationStatus.SUCCEEDED);
      fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishMultipleApplicationMasters() throws Exception {
    int numberOfRequests = 3;
    for (int index = 0; index < numberOfRequests; index++) {
      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(index);
      assertNotNull(registerResponse);
      assertEquals(Integer.toString(index),
          registerResponse.getQueue());
    }

    // Finish in reverse sequence
    for (int index = numberOfRequests - 1; index >= 0; index--) {
      FinishApplicationMasterResponse finishResponse =
          finishApplicationMaster(index, FinalApplicationStatus.SUCCEEDED);

      assertNotNull(finishResponse);
      assertTrue(finishResponse.getIsUnregistered());

      // Assert that the application has been removed from the collection
      assertEquals(this.getAMRMProxyService().getPipelines().size(), index);
    }

    try {
      // Try to finish an application master that is already finished.
      finishApplicationMaster(1, FinalApplicationStatus.SUCCEEDED);
      fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }

    try {
      // Try to finish an application master that was not registered.
      finishApplicationMaster(4, FinalApplicationStatus.SUCCEEDED);
      fail("The request to finish application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("Finish registration failed as expected because it was not registered");
    }
  }

  @Test
  public void testFinishMultipleApplicationMastersInParallel()
      throws Exception {
    int numberOfRequests = 5;
    ArrayList<String> testContexts = new ArrayList<>();
    LOG.info("Creating {} contexts for testing", numberOfRequests);
    for (int i = 0; i < numberOfRequests; i++) {
      testContexts.add("test-endpoint-" + i);
      LOG.info("Created test context: " + testContexts.get(i));

      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(i);
      assertNotNull(registerResponse);
      assertEquals(Integer.toString(i), registerResponse.getQueue());
    }

    finishApplicationMastersInParallel(testContexts);
  }

  @Test
  public void testAllocateRequestWithNullValues() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId),
        registerResponse.getQueue());

    AllocateResponse allocateResponse = allocate(testAppId);
    assertNotNull(allocateResponse);

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId,
            FinalApplicationStatus.SUCCEEDED);

    assertNotNull(finishResponse);
    assertTrue(finishResponse.getIsUnregistered());
  }

  @Test
  public void testAllocateRequestWithoutRegistering() {

    try {
      // Try to allocate an application master without registering.
      allocate(1);
      fail("The request to allocate application master should have failed");
    } catch (Throwable ex) {
      // This is expected. So nothing required here.
      LOG.info("AllocateRequest failed as expected because AM was not registered");
    }
  }

  @Test
  public void testAllocateWithOneResourceRequest() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    getContainersAndAssert(testAppId, 1);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateWithMultipleResourceRequest() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    getContainersAndAssert(testAppId, 10);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateAndReleaseContainers() throws Exception {
    int testAppId = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId);
    assertNotNull(registerResponse);
    List<Container> containers = getContainersAndAssert(testAppId, 10);
    releaseContainersAndAssert(testAppId, containers);
    finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
  }

  @Test
  public void testAllocateAndReleaseContainersForMultipleAM()
      throws Exception {
    int numberOfApps = 5;
    for (int testAppId = 0; testAppId < numberOfApps; testAppId++) {
      RegisterApplicationMasterResponse registerResponse =
          registerApplicationMaster(testAppId);
      assertNotNull(registerResponse);
      List<Container> containers = getContainersAndAssert(testAppId, 10);
      releaseContainersAndAssert(testAppId, containers);
    }
    for (int testAppId = 0; testAppId < numberOfApps; testAppId++) {
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
    }
  }

  @Test
  public void testAllocateAndReleaseContainersForMultipleAMInParallel()
      throws Exception {
    int numberOfApps = 6;
    ArrayList<Integer> tempAppIds = new ArrayList<>();
    for (int i = 0; i < numberOfApps; i++) {
      tempAppIds.add(i);
    }

    List<Integer> responses =
        runInParallel(tempAppIds, testAppId -> {
          try {
            RegisterApplicationMasterResponse registerResponse =
                registerApplicationMaster(testAppId);
            assertNotNull(registerResponse, "response is null");
            List<Container> containers =
                getContainersAndAssert(testAppId, 10);
            releaseContainersAndAssert(testAppId, containers);

            LOG.info("Successfully registered application master with appId: {}", testAppId);
          } catch (Throwable ex) {
            LOG.error("Failed to register application master with appId: {}", testAppId, ex);
            testAppId = null;
          }

          return testAppId;
        });

    assertEquals(tempAppIds.size(), responses.size(),
        "Number of responses received does not match with request");

    for (Integer testAppId : responses) {
      assertNotNull(testAppId);
      finishApplicationMaster(testAppId, FinalApplicationStatus.SUCCEEDED);
    }
  }

  @Test
  public void testMultipleAttemptsSameNode() throws Exception {

    String user = "hadoop";
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId applicationAttemptId;

    // First Attempt

    RegisterApplicationMasterResponse response1 =
        registerApplicationMaster(appId.getId());
    assertNotNull(response1);

    AllocateResponse allocateResponse = allocate(appId.getId());
    assertNotNull(allocateResponse);

    // Second Attempt

    applicationAttemptId = ApplicationAttemptId.newInstance(appId, 2);
    getAMRMProxyService().initializePipeline(applicationAttemptId, user,
        new Token<>(), null, null, false, null);

    RequestInterceptorChainWrapper chain2 =
        getAMRMProxyService().getPipelines().get(appId);
    assertEquals(applicationAttemptId, chain2.getApplicationAttemptId());

    allocateResponse = allocate(appId.getId());
    assertNotNull(allocateResponse);
  }

  private List<Container> getContainersAndAssert(int appId,
      int numberOfResourceRequests) throws Exception {
    AllocateRequest allocateRequest =
        Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(1);

    List<Container> containers =
        new ArrayList<>(numberOfResourceRequests);
    List<ResourceRequest> askList = new ArrayList<>(numberOfResourceRequests);
    for (int testAppId = 0; testAppId < numberOfResourceRequests; testAppId++) {
      askList.add(createResourceRequest("test-node-" + testAppId, 6000, 2, testAppId % 5, 1));
    }

    allocateRequest.setAskList(askList);

    AllocateResponse allocateResponse = allocate(appId, allocateRequest);
    assertNotNull(allocateResponse, "allocate() returned null response");
    assertNull(allocateResponse.getAMRMToken(),
        "new AMRMToken from RM should have been nulled by AMRMProxyService");

    containers.addAll(allocateResponse.getAllocatedContainers());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containers.size() < askList.size() && numHeartbeat++ < 10) {
      allocateResponse =
          allocate(appId, Records.newRecord(AllocateRequest.class));
      assertNotNull(allocateResponse, "allocate() returned null response");
      assertNull(allocateResponse.getAMRMToken(),
          "new AMRMToken from RM should have been nulled by AMRMProxyService");

      containers.addAll(allocateResponse.getAllocatedContainers());

      LOG.info("Number of allocated containers in this request: {}.",
          allocateResponse.getAllocatedContainers().size());
      LOG.info("Total number of allocated containers: {}.", containers.size());
      Thread.sleep(10);
    }

    // We broadcast the request, the number of containers we received will be
    // higher than we ask
    assertTrue(askList.size() <= containers.size(),
        "The asklist count is not same as response");
    return containers;
  }

  private void releaseContainersAndAssert(int appId,
      List<Container> containers) throws Exception {
    assertTrue(containers.size() > 0);
    AllocateRequest allocateRequest =
        Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(1);

    List<ContainerId> relList = new ArrayList<>(containers.size());
    for (Container container : containers) {
      relList.add(container.getId());
    }

    allocateRequest.setReleaseList(relList);

    AllocateResponse allocateResponse = allocate(appId, allocateRequest);
    assertNotNull(allocateResponse);
    assertNull(allocateResponse.getAMRMToken(),
        "new AMRMToken from RM should have been nulled by AMRMProxyService");

    // We need to make sure all the resource managers received the
    // release list. The containers sent by the mock resource managers will be
    // aggregated and returned back to us, and we can assert if all the release
    // lists reached the sub-clusters
    List<ContainerId> containersForReleasedContainerIds = new ArrayList<>();
    List<ContainerId> newlyFinished = getCompletedContainerIds(
        allocateResponse.getCompletedContainersStatuses());
    containersForReleasedContainerIds.addAll(newlyFinished);

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size()
        && numHeartbeat++ < 10) {
      allocateResponse =
          allocate(appId, Records.newRecord(AllocateRequest.class));
      assertNotNull(allocateResponse);
      assertNull(allocateResponse.getAMRMToken(),
          "new AMRMToken from RM should have been nulled by AMRMProxyService");

      newlyFinished = getCompletedContainerIds(
          allocateResponse.getCompletedContainersStatuses());
      containersForReleasedContainerIds.addAll(newlyFinished);

      LOG.info("Number of containers received in this request: {}.",
          allocateResponse.getAllocatedContainers().size());
      LOG.info("Total number of containers received: {}.",
          containersForReleasedContainerIds.size());
      Thread.sleep(10);
    }

    assertEquals(relList.size(), containersForReleasedContainerIds.size());
  }

  /**
   * Test AMRMProxy restart with recovery.
   */
  @Test
  public void testRecovery() throws Exception {

    Configuration conf = createConfiguration();
    // Use the MockRequestInterceptorAcrossRestart instead for the chain
    conf.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        MockRequestInterceptorAcrossRestart.class.getName());

    mockRM = new MockResourceManagerFacade(new YarnConfiguration(conf), 0);

    createAndStartAMRMProxyService(conf);

    int testAppId1 = 1;
    RegisterApplicationMasterResponse registerResponse =
        registerApplicationMaster(testAppId1);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId1),
        registerResponse.getQueue());

    int testAppId2 = 2;
    registerResponse = registerApplicationMaster(testAppId2);
    assertNotNull(registerResponse);
    assertEquals(Integer.toString(testAppId2),
        registerResponse.getQueue());

    AllocateResponse allocateResponse = allocate(testAppId2);
    assertNotNull(allocateResponse);

    // At the time of kill, app1 just registerAM, app2 already did one allocate.
    // Both application should be recovered
    createAndStartAMRMProxyService(conf);
    assertEquals(2, getAMRMProxyService().getPipelines().size());

    allocateResponse = allocate(testAppId1);
    assertNotNull(allocateResponse);

    FinishApplicationMasterResponse finishResponse =
        finishApplicationMaster(testAppId1, FinalApplicationStatus.SUCCEEDED);
    assertNotNull(finishResponse);
    assertTrue(finishResponse.getIsUnregistered());

    allocateResponse = allocate(testAppId2);
    assertNotNull(allocateResponse);

    finishResponse =
        finishApplicationMaster(testAppId2, FinalApplicationStatus.SUCCEEDED);

    assertNotNull(finishResponse);
    assertTrue(finishResponse.getIsUnregistered());

    int testAppId3 = 3;
    try {
      // Try to finish an application master that is not registered.
      finishApplicationMaster(testAppId3, FinalApplicationStatus.SUCCEEDED);
      fail("The Mock RM should complain about not knowing the third app");
    } catch (Throwable ex) {
    }

    mockRM = null;
  }

  /**
   * Test AMRMProxy restart with application recovery failure.
   */
  @Test
  public void testAppRecoveryFailure() throws YarnException, Exception {
    Configuration conf = createConfiguration();
    // Use the MockRequestInterceptorAcrossRestart instead for the chain
    conf.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        BadRequestInterceptorAcrossRestart.class.getName());

    mockRM = new MockResourceManagerFacade(new YarnConfiguration(conf), 0);

    createAndStartAMRMProxyService(conf);

    // Create an app entry in NMSS
    registerApplicationMaster(1);

    RecoveredAMRMProxyState state =
        getNMContext().getNMStateStore().loadAMRMProxyState();
    assertEquals(1, state.getAppContexts().size());

    // AMRMProxy restarts and recover
    createAndStartAMRMProxyService(conf);

    state = getNMContext().getNMStateStore().loadAMRMProxyState();
    // The app that failed to recover should have been removed from NMSS
    assertEquals(0, state.getAppContexts().size());
  }

  @Test
  public void testCheckIfAppExistsInStateStore()
      throws IOException {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    Configuration conf = createConfiguration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);

    createAndStartAMRMProxyService(conf);

    assertFalse(getAMRMProxyService().checkIfAppExistsInStateStore(appId));

    Configuration distConf = createConfiguration();
    conf.setBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED, true);

    createAndStartAMRMProxyService(distConf);

    assertTrue(getAMRMProxyService().checkIfAppExistsInStateStore(appId));
  }

  /**
   * A mock interceptor implementation that uses the same mockRM instance across
   * restart.
   */
  public static class MockRequestInterceptorAcrossRestart
      extends AbstractRequestInterceptor {

    public MockRequestInterceptorAcrossRestart() {
    }

    @Override
    public void init(AMRMProxyApplicationContext appContext) {
      super.init(appContext);
      if (mockRM == null) {
        throw new RuntimeException("mockRM not initialized yet");
      }
    }

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request)
        throws YarnException, IOException {
      return mockRM.registerApplicationMaster(request);
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request)
        throws YarnException, IOException {
      return mockRM.finishApplicationMaster(request);
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      return mockRM.allocate(request);
    }
  }

  /**
   * A mock interceptor implementation that throws when recovering.
   */
  public static class BadRequestInterceptorAcrossRestart
      extends MockRequestInterceptorAcrossRestart {

    @Override
    public void recover(Map<String, byte[]> recoveredDataMap) {
      throw new RuntimeException("Kaboom");
    }
  }

}
