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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueuePlacementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueuePlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestApplicationACLs extends ParameterizedSchedulerTestBase {

  private static final String APP_OWNER = "owner";
  private static final String FRIEND = "friend";
  private static final String ENEMY = "enemy";
  private static final String QUEUE_ADMIN_USER = "queue-admin-user";
  private static final String SUPER_USER = "superUser";
  private static final String FRIENDLY_GROUP = "friendly-group";
  private static final String SUPER_GROUP = "superGroup";
  private static final String UNAVAILABLE = "N/A";

  private static final Logger LOG =
      LoggerFactory.getLogger(TestApplicationACLs.class);

  private MockRM resourceManager;
  private Configuration conf;
  private YarnRPC rpc;
  private InetSocketAddress rmAddress;
  private ApplicationClientProtocol rmClient;
  private RecordFactory recordFactory;
  private boolean isQueueUser;

  public void initTestApplicationACLs(SchedulerType type)
      throws IOException, InterruptedException {
    initParameterizedSchedulerTestBase(type);
    setup();
  }

  public void setup() throws InterruptedException, IOException {
    conf = getConf();
    rpc = YarnRPC.create(conf);
    rmAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
    RMStateStoreFactory.getStore(conf);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    AccessControlList adminACL = new AccessControlList("");
    adminACL.addGroup(SUPER_GROUP);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACL.getAclString());
    recordFactory = RecordFactoryProvider
        .getRecordFactory(conf);
    isQueueUser = false;

    resourceManager = new MockRM(conf) {

      @Override
      protected QueueACLsManager createQueueACLsManager(
          ResourceScheduler scheduler,
          Configuration conf) {
        QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
        when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
            any(QueueACL.class), any(RMApp.class), any(String.class),
            any())).thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            return isQueueUser;
          }
        });
        return mockQueueACLsManager;
      }

      protected ClientRMService createClientRMService() {
        return new ClientRMService(getRMContext(), this.scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, null);
      };
    };
    new Thread() {
      public void run() {
        UserGroupInformation.createUserForTesting(ENEMY, new String[] {});
        UserGroupInformation.createUserForTesting(FRIEND,
            new String[] { FRIENDLY_GROUP });
        UserGroupInformation.createUserForTesting(SUPER_USER,
            new String[] { SUPER_GROUP });
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 60) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1500);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      // RM could have failed.
      throw new IOException(
          "ResourceManager failed to start. Final state is "
              + resourceManager.getServiceState());
    }

    UserGroupInformation owner = UserGroupInformation
        .createRemoteUser(APP_OWNER);
    rmClient = owner.doAs(new PrivilegedExceptionAction<ApplicationClientProtocol>() {
      @Override
      public ApplicationClientProtocol run() throws Exception {
        return (ApplicationClientProtocol) rpc.getProxy(ApplicationClientProtocol.class,
            rmAddress, conf);
      }
    });
  }

  @AfterEach
  public void tearDown() {
    if(resourceManager != null) {
      resourceManager.stop();
    }
  }

  @Override
  protected void configureFairScheduler(YarnConfiguration configuration) {
    final String testDir = new File(System.getProperty("test.build.data",
        "/tmp")).getAbsolutePath();
    final String allocFile = new File(testDir, "test-queues.xml")
        .getAbsolutePath();

    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("root")
            .subQueue(new AllocationFileQueue.Builder("default").build())
            .build())
        .queuePlacementPolicy(new AllocationFileQueuePlacementPolicy()
            .addRule(new AllocationFileQueuePlacementRule(
                AllocationFileQueuePlacementRule.RuleName.SPECIFIED)
                .create(false))
            .addRule(new AllocationFileQueuePlacementRule(
                AllocationFileQueuePlacementRule.RuleName.REJECT)))
        .writeToFile(allocFile);

    configuration.set(FairSchedulerConfiguration.ALLOCATION_FILE, allocFile);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testApplicationACLs(SchedulerType type) throws Exception {

    initTestApplicationACLs(type);

    verifyOwnerAccess();

    verifySuperUserAccess();

    verifyFriendAccess();

    verifyEnemyAccess();

    verifyAdministerQueueUserAccess();

    verifyInvalidQueueWithAcl();
  }

  @SuppressWarnings("deprecation")
  private ApplicationId submitAppAndGetAppId(AccessControlList viewACL,
      AccessControlList modifyACL) throws Exception {
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext context = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    ApplicationId applicationId = rmClient.getNewApplication(
        recordFactory.newRecordInstance(GetNewApplicationRequest.class))
        .getApplicationId();
    context.setApplicationId(applicationId);

    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, viewACL.getAclString());
    acls.put(ApplicationAccessType.MODIFY_APP, modifyACL.getAclString());

    ContainerLaunchContext amContainer = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    Resource resource = Resources.createResource(1024);
    context.setResource(resource);
    amContainer.setApplicationACLs(acls);
    if (conf.get(YarnConfiguration.RM_SCHEDULER)
        .equals(FairScheduler.class.getName())) {
      context.setQueue("root.default");
    }
    context.setAMContainerSpec(amContainer);
    submitRequest.setApplicationSubmissionContext(context);
    rmClient.submitApplication(submitRequest);
    resourceManager.waitForState(applicationId, RMAppState.ACCEPTED);
    return applicationId;
  }

  private ApplicationClientProtocol getRMClientForUser(String user)
      throws IOException, InterruptedException {
    UserGroupInformation userUGI = UserGroupInformation
        .createRemoteUser(user);
    ApplicationClientProtocol userClient = userUGI
        .doAs(new PrivilegedExceptionAction<ApplicationClientProtocol>() {
          @Override
          public ApplicationClientProtocol run() throws Exception {
            return (ApplicationClientProtocol) rpc.getProxy(ApplicationClientProtocol.class,
                rmAddress, conf);
          }
        });
    return userClient;
  }

  private void verifyOwnerAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    // View as owner
    rmClient.getApplicationReport(appReportRequest);

    // List apps as owner
    assertEquals(1, rmClient.getApplications(
        recordFactory.newRecordInstance(GetApplicationsRequest.class))
        .getApplicationList().size(), "App view by owner should list the apps!!");

    // Kill app as owner
    rmClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifySuperUserAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ApplicationClientProtocol superUserClient = getRMClientForUser(SUPER_USER);

    // View as the superUser
    superUserClient.getApplicationReport(appReportRequest);

    // List apps as superUser
    assertEquals(2,
        superUserClient.getApplications(
        recordFactory.newRecordInstance(GetApplicationsRequest.class))
        .getApplicationList().size(), "App view by super-user should list the apps!!");

    // Kill app as the superUser
    superUserClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifyFriendAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ApplicationClientProtocol friendClient = getRMClientForUser(FRIEND);

    // View as the friend
    friendClient.getApplicationReport(appReportRequest);

    // List apps as friend
    assertEquals(3, friendClient.getApplications(
        recordFactory.newRecordInstance(GetApplicationsRequest.class))
        .getApplicationList().size(), "App view by a friend should list the apps!!");

    // Kill app as the friend
    friendClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifyEnemyAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ApplicationClientProtocol enemyRmClient = getRMClientForUser(ENEMY);

    // View as the enemy
    ApplicationReport appReport = enemyRmClient.getApplicationReport(
        appReportRequest).getApplicationReport();
    verifyEnemyAppReport(appReport);

    // List apps as enemy
    List<ApplicationReport> appReports = enemyRmClient
        .getApplications(recordFactory
            .newRecordInstance(GetApplicationsRequest.class))
        .getApplicationList();
    assertEquals(4, appReports.size(),
        "App view by enemy should list the apps!!");
    for (ApplicationReport report : appReports) {
      verifyEnemyAppReport(report);
    }

    // Kill app as the enemy
    try {
      enemyRmClient.forceKillApplication(finishAppRequest);
      fail("App killing by the enemy should fail!!");
    } catch (YarnException e) {
      LOG.info("Got exception while killing app as the enemy", e);
      assertTrue(e.getMessage().contains(
          "User enemy cannot perform operation MODIFY_APP on " + applicationId));
    }

    rmClient.forceKillApplication(finishAppRequest);
  }

  private void verifyEnemyAppReport(ApplicationReport appReport) {
    assertEquals(UNAVAILABLE, appReport.getHost(), "Enemy should not see app host!");
    assertEquals(-1, appReport.getRpcPort(), "Enemy should not see app rpc port!");
    assertEquals(null, appReport.getClientToAMToken(),
        "Enemy should not see app client token!");
    assertEquals(UNAVAILABLE, appReport.getDiagnostics(),
        "Enemy should not see app diagnostics!");
    assertEquals(UNAVAILABLE, appReport.getTrackingUrl(),
        "Enemy should not see app tracking url!");
    assertEquals(UNAVAILABLE, appReport.getOriginalTrackingUrl(),
        "Enemy should not see app original tracking url!");
    ApplicationResourceUsageReport usageReport =
        appReport.getApplicationResourceUsageReport();
    assertEquals(-1, usageReport.getNumUsedContainers(),
        "Enemy should not see app used containers");
    assertEquals(-1, usageReport.getNumReservedContainers(),
        "Enemy should not see app reserved containers");
    assertEquals(-1, usageReport.getUsedResources().getMemorySize(),
        "Enemy should not see app used resources");
    assertEquals(-1, usageReport.getReservedResources().getMemorySize(),
        "Enemy should not see app reserved resources");
    assertEquals(-1, usageReport.getNeededResources().getMemorySize(),
        "Enemy should not see app needed resources");
  }

  private void verifyInvalidQueueWithAcl() throws Exception {
    isQueueUser = true;
    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext context =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    ApplicationId applicationId = rmClient
        .getNewApplication(
            recordFactory.newRecordInstance(GetNewApplicationRequest.class))
        .getApplicationId();
    context.setApplicationId(applicationId);
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    ContainerLaunchContext amContainer =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    Resource resource = Resources.createResource(1024);
    context.setResource(resource);
    amContainer.setApplicationACLs(acls);
    context.setQueue("InvalidQueue");
    context.setAMContainerSpec(amContainer);
    submitRequest.setApplicationSubmissionContext(context);
    rmClient.submitApplication(submitRequest);
    resourceManager.waitForState(applicationId, RMAppState.FAILED);
    final GetApplicationReportRequest appReportRequest =
        recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    GetApplicationReportResponse applicationReport =
        rmClient.getApplicationReport(appReportRequest);
    ApplicationReport appReport = applicationReport.getApplicationReport();
    if (conf.get(YarnConfiguration.RM_SCHEDULER)
        .equals(FairScheduler.class.getName())) {
      assertTrue(appReport.getDiagnostics()
          .contains("user owner application rejected by placement rules."));
    } else {
      assertTrue(appReport.getDiagnostics()
          .contains("submitted by user owner to unknown queue: InvalidQueue"));
    }
  }

  private void verifyAdministerQueueUserAccess() throws Exception {
    isQueueUser = true;
    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ApplicationClientProtocol administerQueueUserRmClient =
        getRMClientForUser(QUEUE_ADMIN_USER);

    // View as the administerQueueUserRmClient
    administerQueueUserRmClient.getApplicationReport(appReportRequest);

    // List apps as administerQueueUserRmClient
    assertEquals(5, administerQueueUserRmClient.getApplications(
        recordFactory.newRecordInstance(GetApplicationsRequest.class))
        .getApplicationList().size(), "App view by queue-admin-user should list the apps!!");

    // Kill app as the administerQueueUserRmClient
    administerQueueUserRmClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }
}
