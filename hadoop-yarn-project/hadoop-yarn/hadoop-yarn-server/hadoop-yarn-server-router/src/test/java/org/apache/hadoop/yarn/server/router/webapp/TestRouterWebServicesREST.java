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

package org.apache.hadoop.yarn.server.router.webapp;

import static javax.servlet.http.HttpServletResponse.SC_ACCEPTED;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.ADD_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_APPATTEMPTS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_APPATTEMPTS_APPATTEMPTID_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_PRIORITY;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_QUEUE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_APPID_STATE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_NEW_APPLICATION;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_TIMEOUT;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APPS_TIMEOUTS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APP_ID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.APP_STATISTICS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.GET_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.GET_NODE_TO_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.INFO;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.LABEL_MAPPINGS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.METRICS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODE_RESOURCE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID_GETLABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.NODES_NODEID_REPLACE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.REMOVE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.REPLACE_NODE_TO_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_DELETE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_NEW;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_SUBMIT;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RESERVATION_UPDATE;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER_ACTIVITIES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER_APP_ACTIVITIES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.SCHEDULER_LOGS;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.STATES;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.TIME;
import static org.apache.hadoop.yarn.server.router.webapp.HTTPMethods.POST;
import static org.apache.hadoop.yarn.server.router.webapp.HTTPMethods.PUT;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.getNMWebAppURLWithoutScheme;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.getRMWebAppURLWithScheme;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.getRouterWebAppURLWithScheme;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewApplication;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewReservation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

/**
 * This test validate E2E the correctness of the RouterWebServices. It starts
 * Router, RM and NM in 3 different processes to avoid servlet conflicts. Each
 * test creates a REST call to Router and validate that the operation complete
 * successfully.
 */
@NotThreadSafe
public class TestRouterWebServicesREST {

  /** The number of concurrent submissions for multi-thread test. */
  private static final int NUM_THREADS_TESTS = 100;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterWebServicesREST.class);

  private static String userName = "test";

  private static JavaProcess rm;
  private static JavaProcess nm;
  private static JavaProcess router;

  private static String rmAddress;
  private static String routerAddress;
  private static String nmAddress;

  private static Configuration conf;

  /**
   * Wait until the webservice is up and running.
   */
  public static void waitWebAppRunning(
      final String address, final String path) {
    try {
      final Client clientToRouter = ClientBuilder.newClient();
      final WebTarget toRouter = clientToRouter
          .target(address)
          .path(path);
      GenericTestUtils.waitFor(() -> {
        try {
          Response response = toRouter.request(APPLICATION_JSON).get(Response.class);
          if (response.getStatus() == SC_OK) {
            // process is up and running
            return true;
          }
        } catch (Exception e) {
          // process is not up and running
        }
        return false;
      }, 1000, 20 * 1000);
    } catch (Exception e) {
      fail("Web app not running");
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new YarnConfiguration();

    File baseDir = GenericTestUtils.getTestDir("processes");
    baseDir.mkdirs();
    String baseName = TestRouterWebServicesREST.class.getSimpleName();

    File rmOutput = new File(baseDir, baseName + "-rm.log");
    rmOutput.createNewFile();
    List<String> addClasspath = new LinkedList<>();
    addClasspath.add("../hadoop-yarn-server-timelineservice/target/classes");
    rm = new JavaProcess(ResourceManager.class, addClasspath, rmOutput);
    rmAddress = getRMWebAppURLWithScheme(conf);
    waitWebAppRunning(rmAddress, RM_WEB_SERVICE_PATH);

    File routerOutput = new File(baseDir, baseName + "-router.log");
    routerOutput.createNewFile();
    router = new JavaProcess(Router.class, routerOutput);
    routerAddress = getRouterWebAppURLWithScheme(conf);
    waitWebAppRunning(routerAddress, RM_WEB_SERVICE_PATH);

    File nmOutput = new File(baseDir, baseName + "-nm.log");
    nmOutput.createNewFile();
    nm = new JavaProcess(NodeManager.class, nmOutput);
    nmAddress = "http://" + getNMWebAppURLWithoutScheme(conf);
    waitWebAppRunning(nmAddress, "/ws/v1/node");
  }

  @AfterAll
  public static void stop() throws Exception {
    if (nm != null) {
      nm.stop();
    }
    if (router != null) {
      router.stop();
    }
    if (rm != null) {
      rm.stop();
    }
  }

  /**
   * Performs 2 GET calls one to RM and the one to Router. In positive case, it
   * returns the 2 answers in a list.
   */
  private static <T> List<T> performGetCalls(final String path,
      final Class<T> returnType, final String queryName,
      final String queryValue) throws IOException, InterruptedException {
    Client clientToRouter = ClientBuilder.newClient();
    WebTarget toRouter = clientToRouter.target(routerAddress).path(path);

    Client clientToRM = ClientBuilder.newClient();
    WebTarget toRM = clientToRM.target(rmAddress).path(path);

    final Builder toRouterBuilder;
    final Builder toRMBuilder;

    if (queryValue != null && queryName != null) {
      toRouterBuilder = toRouter.
          queryParam(queryName, queryValue).
          resolveTemplate("appid", queryValue).
          request(APPLICATION_XML);
      toRMBuilder = toRM.
          queryParam(queryName, queryValue).
          resolveTemplate("appid", queryValue).
          request(APPLICATION_XML);
    } else {
      toRouterBuilder = toRouter.request(APPLICATION_XML);
      toRMBuilder = toRM.request(APPLICATION_XML);
    }

    return UserGroupInformation.createRemoteUser(userName)
        .doAs((PrivilegedExceptionAction<List<T>>) () -> {
          Response response = toRouterBuilder.get(Response.class);
          Response response2 = toRMBuilder.get(Response.class);
          assertEquals(SC_OK, response.getStatus());
          assertEquals(SC_OK, response2.getStatus());
          List<T> responses = new ArrayList<>();
          responses.add(response.readEntity(returnType));
          responses.add(response2.readEntity(returnType));
          return responses;
        });
  }

  /**
   * Performs a POST/PUT/DELETE call to Router and returns the ClientResponse.
   */
  private static Response performCall(final String webAddress,
      final String queryKey, final String queryValue, final Object context,
      final HTTPMethods method) throws IOException, InterruptedException {

    return UserGroupInformation.createRemoteUser(userName)
        .doAs((PrivilegedExceptionAction<Response>) () -> {
          Client clientToRouter = ClientBuilder.newClient();
          WebTarget toRouter = clientToRouter
              .target(routerAddress)
              .path(webAddress);

          WebTarget toRouterWR = toRouter;
          if (queryKey != null && queryValue != null) {
            toRouterWR = toRouterWR.queryParam(queryKey, queryValue);
          }

          Invocation.Builder builder = toRouterWR.request(APPLICATION_XML);

          Response response = null;

          switch (method) {
          case DELETE:
            response = builder.delete(Response.class);
            break;
          case POST:
            response = builder.post(Entity.entity(context, APPLICATION_XML));
            break;
          case PUT:
            response = builder.put(Entity.entity(context, APPLICATION_XML));
            break;
          default:
            break;
          }

          return response;
        });
  }

  /**
   * This test validates the correctness of {@link RMWebServiceProtocol#get()}
   * inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testInfoXML() throws Exception {

    List<ClusterInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH, ClusterInfo.class, null, null);

    ClusterInfo routerResponse = responses.get(0);
    ClusterInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getRMVersion(),
        routerResponse.getRMVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterInfo()} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testClusterInfoXML() throws Exception {

    List<ClusterInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + INFO, ClusterInfo.class, null, null);

    ClusterInfo routerResponse = responses.get(0);
    ClusterInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getRMVersion(),
        routerResponse.getRMVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterMetricsInfo()} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testMetricsInfoXML() throws Exception {

    List<ClusterMetricsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + METRICS, ClusterMetricsInfo.class, null, null);

    ClusterMetricsInfo routerResponse = responses.get(0);
    ClusterMetricsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getActiveNodes(),
        routerResponse.getActiveNodes());
  }

  /*
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getSchedulerInfo()} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testSchedulerInfoXML() throws Exception {

    List<SchedulerTypeInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + SCHEDULER, SchedulerTypeInfo.class, null, null);

    SchedulerTypeInfo routerResponse = responses.get(0);
    SchedulerTypeInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getSchedulerInfo().getSchedulerType(),
        routerResponse.getSchedulerInfo().getSchedulerType());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNodes(String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testNodesEmptyXML() throws Exception {

    List<NodesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + NODES, NodesInfo.class, null, null);

    NodesInfo routerResponse = responses.get(0);
    NodesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getNodes().size(),
        routerResponse.getNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNodes(String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testNodesXML() throws Exception {

    List<NodesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + NODES, NodesInfo.class, STATES, "LOST");

    NodesInfo routerResponse = responses.get(0);
    NodesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getNodes().size(),
        routerResponse.getNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNode(String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testNodeXML() throws Exception {

    List<NodeInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, getNodeId()),
        NodeInfo.class, null, null);

    NodeInfo routerResponse = responses.get(0);
    NodeInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getVersion(),
        routerResponse.getVersion());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateNodeResource} inside Router.
   */
  @Test
  public void testUpdateNodeResource() throws Exception {

    // wait until a node shows up and check the resources
    GenericTestUtils.waitFor(() -> getNodeId() != null, 100, 5 * 1000);
    String nodeId = getNodeId();

    // assert memory and default vcores
    List<NodeInfo> responses0 = performGetCalls(
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, getNodeId()),
        NodeInfo.class, null, null);
    NodeInfo nodeInfo0 = responses0.get(0);
    assertEquals(8192, nodeInfo0.getTotalResource().getMemorySize());
    assertEquals(8, nodeInfo0.getTotalResource().getvCores());

    // update memory to 4096MB and 5 cores
    Resource resource = Resource.newInstance(4096, 5);
    ResourceOptionInfo resourceOption = new ResourceOptionInfo(
        ResourceOption.newInstance(resource, 1000));
    Response routerResponse = performCall(
        RM_WEB_SERVICE_PATH + format(NODE_RESOURCE, nodeId),
        null, null, resourceOption, POST);
    assertResponseStatusCode(Response.Status.OK, routerResponse.getStatusInfo());
    ResourceInfo totalResource = routerResponse.readEntity(ResourceInfo.class);
    assertEquals(resource.getMemorySize(), totalResource.getMemorySize());
    assertEquals(resource.getVirtualCores(), totalResource.getvCores());

    // assert updated memory and cores
    List<NodeInfo> responses1 = performGetCalls(
        RM_WEB_SERVICE_PATH + format(NODES_NODEID, getNodeId()),
        NodeInfo.class, null, null);
    NodeInfo nodeInfo1 = responses1.get(0);
    assertEquals(4096, nodeInfo1.getTotalResource().getMemorySize());
    assertEquals(5, nodeInfo1.getTotalResource().getvCores());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getActivities(HttpServletRequest, String, String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testActiviesXML() throws Exception {

    List<ActivitiesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + SCHEDULER_ACTIVITIES,
        ActivitiesInfo.class, null, null);

    ActivitiesInfo routerResponse = responses.get(0);
    ActivitiesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppActivities} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppActivitiesXML() throws Exception {

    String appId = submitApplication();

    List<AppActivitiesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + SCHEDULER_APP_ACTIVITIES,
        AppActivitiesInfo.class, APP_ID, appId);

    AppActivitiesInfo routerResponse = responses.get(0);
    AppActivitiesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppStatistics} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppStatisticsXML() throws Exception {

    submitApplication();

    List<ApplicationStatisticsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + APP_STATISTICS,
        ApplicationStatisticsInfo.class, STATES, "RUNNING");

    ApplicationStatisticsInfo routerResponse = responses.get(0);
    ApplicationStatisticsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getStatItems().size(),
        routerResponse.getStatItems().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#dumpSchedulerLogs} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testDumpSchedulerLogsXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse =
        performCall(RM_WEB_SERVICE_PATH + SCHEDULER_LOGS,
            null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    Response response = performCall(
        RM_WEB_SERVICE_PATH + SCHEDULER_LOGS, TIME, "1", null, POST);

    assertEquals(SC_INTERNAL_SERVER_ERROR, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#createNewApplication} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testNewApplicationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + APPS_NEW_APPLICATION, null,
        null, new String(""), PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    Response response = performCall(
        RM_WEB_SERVICE_PATH + APPS_NEW_APPLICATION, null,
        null, null, POST);

    assertEquals(SC_OK, response.getStatus());
    NewApplication ci = response.readEntity(NewApplication.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#submitApplication} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testSubmitApplicationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + APPS, null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    context.setApplicationId(getNewApplicationId().getApplicationId());

    Response response = performCall(
        RM_WEB_SERVICE_PATH + APPS, null, null, context, POST);

    assertEquals(SC_ACCEPTED, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getApps} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppsXML() throws Exception {

    submitApplication();

    List<AppsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + APPS, AppsInfo.class, null, null);

    AppsInfo routerResponse = responses.get(0);
    AppsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getApps().size(),
        routerResponse.getApps().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getApp} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppXML() throws Exception {

    String appId = submitApplication();

    List<AppInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_APPID, appId),
        AppInfo.class, null, null);

    AppInfo routerResponse = responses.get(0);
    AppInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getAMHostHttpAddress(),
        routerResponse.getAMHostHttpAddress());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppAttempts} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppAttemptXML() throws Exception {

    String appId = submitApplication();

    List<AppAttemptsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_APPATTEMPTS, appId),
        AppAttemptsInfo.class, null, null);

    AppAttemptsInfo routerResponse = responses.get(0);
    AppAttemptsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getAttempts().size(),
        routerResponse.getAttempts().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppState} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppStateXML() throws Exception {

    String appId = submitApplication();

    List<AppState> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_STATE, appId),
        AppState.class, null, null);

    AppState routerResponse = responses.get(0);
    AppState rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getState(),
        routerResponse.getState());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateAppState} inside Router.
   */
  @Test
  @Timeout(value = 20000)
  public void testUpdateAppStateXML() throws Exception {

    String appId = submitApplication();
    String pathApp =
        RM_WEB_SERVICE_PATH + format(APPS_APPID_STATE, appId);

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        pathApp, null, null, null, POST);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    AppState appState = new AppState("KILLED");

    Response response = performCall(
        pathApp, null, null, appState, PUT);

    assertEquals(SC_ACCEPTED, response.getStatus());
    AppState ci = response.readEntity(AppState.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppPriority} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppPriorityXML() throws Exception {

    String appId = submitApplication();

    List<AppPriority> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_PRIORITY, appId),
        AppPriority.class, null, null);

    AppPriority routerResponse = responses.get(0);
    AppPriority rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(rmResponse.getPriority(), routerResponse.getPriority());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateApplicationPriority(
   *     AppPriority, HttpServletRequest, String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testUpdateAppPriorityXML() throws Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_PRIORITY, appId),
        null, null, null, POST);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    AppPriority appPriority = new AppPriority(1);

    Response response = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_PRIORITY, appId),
        null, null, appPriority, PUT);

    assertEquals(SC_OK, response.getStatus());
    AppPriority ci = response.readEntity(AppPriority.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppQueue(HttpServletRequest, String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppQueueXML() throws Exception {

    String appId = submitApplication();

    List<AppQueue> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_QUEUE, appId),
        AppQueue.class, null, null);

    AppQueue routerResponse = responses.get(0);
    AppQueue rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(rmResponse.getQueue(), routerResponse.getQueue());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateAppQueue(AppQueue, HttpServletRequest, String)}
   * inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testUpdateAppQueueXML() throws Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_QUEUE, appId),
        null, null, null, POST);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    AppQueue appQueue = new AppQueue("default");

    Response response = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_APPID_QUEUE, appId),
        null, null, appQueue, PUT);

    assertEquals(SC_OK, response.getStatus());
    AppQueue ci = response.readEntity(AppQueue.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppTimeouts} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppTimeoutsXML() throws Exception {

    String appId = submitApplication();

    List<AppTimeoutsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(APPS_TIMEOUTS, appId),
        AppTimeoutsInfo.class, null, null);

    AppTimeoutsInfo routerResponse = responses.get(0);
    AppTimeoutsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getAppTimeouts().size(),
        routerResponse.getAppTimeouts().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getAppTimeout} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAppTimeoutXML() throws Exception {

    String appId = submitApplication();
    String pathApp = RM_WEB_SERVICE_PATH + format(APPS_TIMEOUTS, appId);
    List<AppTimeoutInfo> responses = performGetCalls(
        pathApp + "/" + "LIFETIME", AppTimeoutInfo.class, null, null);

    AppTimeoutInfo routerResponse = responses.get(0);
    AppTimeoutInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getExpireTime(),
        routerResponse.getExpireTime());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateApplicationTimeout}
   * inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testUpdateAppTimeoutsXML() throws Exception {

    String appId = submitApplication();

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_TIMEOUT, appId),
        null, null, null, POST);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with a bad request
    AppTimeoutInfo appTimeoutInfo = new AppTimeoutInfo();

    Response response = performCall(
        RM_WEB_SERVICE_PATH + format(APPS_TIMEOUT, appId),
        null, null, appTimeoutInfo, PUT);

    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#createNewReservation(HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testNewReservationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_NEW,
        null, null, new String(""), PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    Response response = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_NEW,
        null, null, null, POST);

    assertEquals(SC_OK, response.getStatus());
    NewReservation ci = response.readEntity(NewReservation.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#submitReservation(
   *     ReservationSubmissionRequestInfo, HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testSubmitReservationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_SUBMIT, null,
        null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    ReservationSubmissionRequestInfo context =
        new ReservationSubmissionRequestInfo();
    context.setReservationId(getNewReservationId().getReservationId());
    // ReservationDefinition is null

    Response response = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_SUBMIT, null, null, context, POST);

    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#updateReservation(
   *     ReservationUpdateRequestInfo, HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testUpdateReservationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_UPDATE, null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    String reservationId = getNewReservationId().getReservationId();
    ReservationUpdateRequestInfo context = new ReservationUpdateRequestInfo();
    context.setReservationId(reservationId);

    Response response = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_UPDATE, null, null, context, POST);

    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#deleteReservation(
   *     ReservationDeleteRequestInfo, HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testDeleteReservationXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_DELETE, null, null, new String(""), PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    String reservationId = getNewReservationId().getReservationId();
    ReservationDeleteRequestInfo context = new ReservationDeleteRequestInfo();
    context.setReservationId(reservationId);

    Response response = performCall(
        RM_WEB_SERVICE_PATH + RESERVATION_DELETE, null, null, context, POST);

    assertEquals(SC_BAD_REQUEST, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getNodeToLabels(HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetNodeToLabelsXML() throws Exception {

    List<NodeToLabelsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + GET_NODE_TO_LABELS,
        NodeToLabelsInfo.class, null, null);

    NodeToLabelsInfo routerResponse = responses.get(0);
    NodeToLabelsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getNodeToLabels().size(),
        routerResponse.getNodeToLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getClusterNodeLabels(HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetClusterNodeLabelsXML() throws Exception {

    List<NodeLabelsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + GET_NODE_LABELS,
        NodeLabelsInfo.class, null, null);

    NodeLabelsInfo routerResponse = responses.get(0);
    NodeLabelsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getNodeLabels().size(),
        routerResponse.getNodeLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getLabelsOnNode(HttpServletRequest, String)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetLabelsOnNodeXML() throws Exception {

    List<NodeLabelsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + format(NODES_NODEID_GETLABELS, getNodeId()),
        NodeLabelsInfo.class, null, null);

    NodeLabelsInfo routerResponse = responses.get(0);
    NodeLabelsInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getNodeLabels().size(),
        routerResponse.getNodeLabels().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getLabelsToNodes(Set<String>)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetLabelsMappingEmptyXML() throws Exception {

    List<LabelsToNodesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + LABEL_MAPPINGS,
        LabelsToNodesInfo.class, null, null);

    LabelsToNodesInfo routerResponse = responses.get(0);
    LabelsToNodesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getLabelsToNodes().size(),
        routerResponse.getLabelsToNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#getLabelsToNodes(Set<String>)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetLabelsMappingXML() throws Exception {

    List<LabelsToNodesInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + LABEL_MAPPINGS,
        LabelsToNodesInfo.class, LABELS, "label1");

    LabelsToNodesInfo routerResponse = responses.get(0);
    LabelsToNodesInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getLabelsToNodes().size(),
        routerResponse.getLabelsToNodes().size());
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#addToClusterNodeLabels(
   *     NodeLabelsInfo, HttpServletRequest)} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testAddToClusterNodeLabelsXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + ADD_NODE_LABELS,
        null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method

    List<NodeLabel> nodeLabels = new ArrayList<>();
    nodeLabels.add(NodeLabel.newInstance("default"));
    NodeLabelsInfo context = new NodeLabelsInfo(nodeLabels);

    Response response = performCall(
        RM_WEB_SERVICE_PATH + ADD_NODE_LABELS, null, null, context, POST);

    assertEquals(SC_OK, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#removeFromClusterNodeLabels} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testRemoveFromClusterNodeLabelsXML()
      throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + REMOVE_NODE_LABELS, null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    addNodeLabel();

    Response response = performCall(
        RM_WEB_SERVICE_PATH + REMOVE_NODE_LABELS,
        LABELS, "default", null, POST);

    assertEquals(SC_OK, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#replaceLabelsOnNodes} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testReplaceLabelsOnNodesXML() throws Exception {

    // Test with a wrong HTTP method
    Response badResponse = performCall(
        RM_WEB_SERVICE_PATH + REPLACE_NODE_TO_LABELS, null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    addNodeLabel();

    NodeToLabelsEntryList context = new NodeToLabelsEntryList();

    Response response = performCall(
        RM_WEB_SERVICE_PATH + REPLACE_NODE_TO_LABELS,
        null, null, context, POST);

    assertEquals(SC_OK, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of
   * {@link RMWebServiceProtocol#replaceLabelsOnNode} inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testReplaceLabelsOnNodeXML() throws Exception {

    // Test with a wrong HTTP method
    String pathNode = RM_WEB_SERVICE_PATH +
        format(NODES_NODEID_REPLACE_LABELS, getNodeId());
    Response badResponse = performCall(
        pathNode, null, null, "", PUT);

    assertEquals(SC_METHOD_NOT_ALLOWED, badResponse.getStatus());

    // Test with the correct HTTP method
    addNodeLabel();

    Response response = performCall(
        pathNode, LABELS, "default", null, POST);

    assertEquals(SC_OK, response.getStatus());
    String ci = response.readEntity(String.class);
    assertNotNull(ci);
  }

  /**
   * This test validates the correctness of {@link WebServices#getAppAttempt}
   * inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetAppAttemptXML() throws Exception {

    String appId = submitApplication();
    String pathAttempts = RM_WEB_SERVICE_PATH + format(
        APPS_APPID_APPATTEMPTS_APPATTEMPTID, appId, getAppAttempt(appId));
    List<AppAttemptInfo> responses = performGetCalls(
        pathAttempts, AppAttemptInfo.class, null, null);

    AppAttemptInfo routerResponse = responses.get(0);
    AppAttemptInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getAppAttemptId(),
        routerResponse.getAppAttemptId());
  }

  /**
   * This test validates the correctness of {@link WebServices#getContainers}
   * inside Router.
   */
  @Test
  @Timeout(value = 2)
  public void testGetContainersXML() throws Exception {

    String appId = submitApplication();
    String pathAttempts = RM_WEB_SERVICE_PATH + format(
        APPS_APPID_APPATTEMPTS_APPATTEMPTID_CONTAINERS,
        appId, getAppAttempt(appId));
    List<ContainersInfo> responses = performGetCalls(
        pathAttempts, ContainersInfo.class, null, null);

    ContainersInfo routerResponse = responses.get(0);
    ContainersInfo rmResponse = responses.get(1);

    assertNotNull(routerResponse);
    assertNotNull(rmResponse);

    assertEquals(
        rmResponse.getContainers().size(),
        routerResponse.getContainers().size());
  }

  @Test
  @Timeout(value = 60)
  public void testGetAppsMultiThread() throws Exception {
    final int iniNumApps = getNumApps();

    // This submits an application
    testGetContainersXML();
    // This submits an application
    testAppsXML();

    // Wait at most 10 seconds until we see all the applications
    GenericTestUtils.waitFor(() -> {
      try {
        // Check if we have the 2 apps we submitted
        return getNumApps() == iniNumApps + 2;
      } catch (Exception e) {
        fail();
      }
      return false;
    }, 100, 10 * 1000);

    // Multithreaded getApps()
    ExecutorService threadpool = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("REST Tester #%d")
            .build());
    CompletionService<Void> svc = new ExecutorCompletionService<>(threadpool);
    try {
      // Submit a bunch of operations concurrently
      for (int i = 0; i < NUM_THREADS_TESTS; i++) {
        svc.submit(() -> {
          assertEquals(iniNumApps + 2, getNumApps());
          return null;
        });
      }
    } finally {
      threadpool.shutdown();
    }

    assertEquals(iniNumApps + 2, getNumApps());
  }

  /**
   * Get the number of applications in the system.
   * @return Number of applications in the system
   * @throws Exception If we cannot get the applications.
   */
  private int getNumApps() throws Exception {
    List<AppsInfo> responses = performGetCalls(
        RM_WEB_SERVICE_PATH + APPS, AppsInfo.class, null, null);
    AppsInfo routerResponse = responses.get(0);
    AppsInfo rmResponse = responses.get(1);
    assertEquals(rmResponse.getApps().size(), routerResponse.getApps().size());
    return rmResponse.getApps().size();
  }

  private String getNodeId() {
    Client clientToRM = ClientBuilder.newClient();
    WebTarget toRM = clientToRM.target(rmAddress)
        .path(RM_WEB_SERVICE_PATH + NODES);
    Response response =
        toRM.request(APPLICATION_XML).get(Response.class);
    NodesInfo ci = response.readEntity(NodesInfo.class);
    List<NodeInfo> nodes = ci.getNodes();
    if (nodes.isEmpty()) {
      return null;
    }
    return nodes.get(0).getNodeId();
  }

  private NewApplication getNewApplicationId() {
    Client clientToRM = ClientBuilder.newClient();
    WebTarget toRM = clientToRM.target(rmAddress)
        .path(RM_WEB_SERVICE_PATH + APPS_NEW_APPLICATION);
    Response response = toRM.request(APPLICATION_XML).post(null);
    return response.readEntity(NewApplication.class);
  }

  private String submitApplication() {
    ApplicationSubmissionContextInfo context =
        new ApplicationSubmissionContextInfo();
    String appId = getNewApplicationId().getApplicationId();
    context.setApplicationId(appId);

    Client clientToRouter = ClientBuilder.newClient();
    WebTarget toRM = clientToRouter.target(rmAddress)
        .path(RM_WEB_SERVICE_PATH + APPS);

    toRM.request(APPLICATION_XML).post(Entity.entity(context, APPLICATION_XML), Response.class);

    return appId;
  }

  private NewReservation getNewReservationId() {
    Client clientToRM = ClientBuilder.newClient();
    WebTarget toRM = clientToRM.target(rmAddress)
        .path(RM_WEB_SERVICE_PATH + RESERVATION_NEW);
    Response response = toRM.
        request(APPLICATION_XML)
        .post(null, Response.class);
    return response.readEntity(NewReservation.class);
  }

  private String addNodeLabel() {
    Client clientToRM = ClientBuilder.newClient();
    WebTarget toRM = clientToRM.target(rmAddress)
        .path(RM_WEB_SERVICE_PATH + ADD_NODE_LABELS);
    List<NodeLabel> nodeLabels = new ArrayList<>();
    nodeLabels.add(NodeLabel.newInstance("default"));
    NodeLabelsInfo context = new NodeLabelsInfo(nodeLabels);
    Response response = toRM
        .request(APPLICATION_XML)
        .post(Entity.entity(context, APPLICATION_XML), Response.class);
    return response.readEntity(String.class);
  }

  private String getAppAttempt(String appId) {
    Client clientToRM = ClientBuilder.newClient();
    String pathAppAttempt = RM_WEB_SERVICE_PATH + format(APPS_APPID_APPATTEMPTS, appId);
    WebTarget toRM = clientToRM.
        target(rmAddress).
        path(pathAppAttempt);
    Response response = toRM.
        request(APPLICATION_XML).
        get(Response.class);
    AppAttemptsInfo ci = response.readEntity(AppAttemptsInfo.class);
    return ci.getAttempts().get(0).getAppAttemptId();
  }

  /**
   * Convert format using {name} (HTTP base) into %s (Java based).
   * @param format Initial format using {}.
   * @param args Arguments for the format.
   * @return New format using %s.
   */
  private static String format(String format, Object... args) {
    Pattern p = Pattern.compile("\\{.*?}");
    Matcher m = p.matcher(format);
    String newFormat = m.replaceAll("%s");
    return String.format(newFormat, args);
  }
}
