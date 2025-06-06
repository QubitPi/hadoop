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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.test.TestName;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests to verify fairshare and minshare preemption, using parameterization.
 */
public class TestFairSchedulerPreemption extends FairSchedulerTestBase {
  private static final File ALLOC_FILE = new File(TEST_DIR, "test-queues");
  private static final int GB = 1024;
  private static final String TC_DISABLE_AM_PREEMPTION_GLOBALLY =
      "testDisableAMPreemptionGlobally";

  // Scheduler clock
  private final ControlledClock clock = new ControlledClock();

  // Node Capacity = NODE_CAPACITY_MULTIPLE * (1 GB or 1 vcore)
  private static final int NODE_CAPACITY_MULTIPLE = 4;

  private boolean fairsharePreemption;
  private boolean drf;

  // App that takes up the entire cluster
  private FSAppAttempt greedyApp;

  // Starving app that is expected to instigate preemption
  private FSAppAttempt starvingApp;

  @RegisterExtension
  private TestName testName = new TestName();

  public static Collection<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
        {"MinSharePreemption", 0},
        {"MinSharePreemptionWithDRF", 1},
        {"FairSharePreemption", 2},
        {"FairSharePreemptionWithDRF", 3}
    });
  }

  private void initTestFairSchedulerPreemption(String name, int mode)
      throws IOException {
    fairsharePreemption = (mode > 1); // 2 and 3
    drf = (mode % 2 == 1); // 1 and 3
    writeAllocFile();
    setup();
  }

  public void setup() throws IOException {
    createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        ALLOC_FILE.getAbsolutePath());
    conf.setBoolean(FairSchedulerConfiguration.PREEMPTION, true);
    conf.setFloat(FairSchedulerConfiguration.PREEMPTION_THRESHOLD, 0f);
    conf.setInt(FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL, 0);
    conf.setLong(FairSchedulerConfiguration.UPDATE_INTERVAL_MS, 60_000L);
    String testMethod = testName.getMethodName();
    if (testMethod.startsWith(TC_DISABLE_AM_PREEMPTION_GLOBALLY)) {
      conf.setBoolean(FairSchedulerConfiguration.AM_PREEMPTION, false);
    }
    setupCluster();
  }

  @AfterEach
  public void teardown() {
    ALLOC_FILE.delete();
    conf = null;
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  private void writeAllocFile() {
    /*
     * Queue hierarchy:
     * root
     * |--- preemptable
     *      |--- child-1
     *      |--- child-2
     * |--- preemptable-sibling
     * |--- nonpreemptable
     *      |--- child-1
     *      |--- child-2
     */
    AllocationFileWriter allocationFileWriter;
    if (fairsharePreemption) {
      allocationFileWriter = AllocationFileWriter.create()
          .addQueue(new AllocationFileQueue.Builder("root")
              .subQueue(new AllocationFileQueue.Builder("preemptable")
                  .fairSharePreemptionThreshold(1)
                  .fairSharePreemptionTimeout(0)
                  .subQueue(new AllocationFileQueue.Builder("child-1")
                      .build())
                  .subQueue(new AllocationFileQueue.Builder("child-2")
                      .build())
                  .build())
              .subQueue(new AllocationFileQueue.Builder("preemptable-sibling")
                  .fairSharePreemptionThreshold(1)
                  .fairSharePreemptionTimeout(0)
                  .build())
              .subQueue(new AllocationFileQueue.Builder("nonpreemptable")
                  .allowPreemptionFrom(false)
                  .fairSharePreemptionThreshold(1)
                  .fairSharePreemptionTimeout(0)
                  .subQueue(new AllocationFileQueue.Builder("child-1")
                      .build())
                  .subQueue(new AllocationFileQueue.Builder("child-2")
                      .build())
                  .build())
              .build());
    } else {
      allocationFileWriter = AllocationFileWriter.create()
          .addQueue(new AllocationFileQueue.Builder("root")
              .subQueue(new AllocationFileQueue.Builder("preemptable")
                  .minSharePreemptionTimeout(0)
                  .subQueue(new AllocationFileQueue.Builder("child-1")
                      .minResources("4096mb,4vcores")
                      .build())
                  .subQueue(new AllocationFileQueue.Builder("child-2")
                      .minResources("4096mb,4vcores")
                      .build())
                  .build())
              .subQueue(new AllocationFileQueue.Builder("preemptable-sibling")
                  .minSharePreemptionTimeout(0)
                  .build())
              .subQueue(new AllocationFileQueue.Builder("nonpreemptable")
                  .allowPreemptionFrom(false)
                  .minSharePreemptionTimeout(0)
                  .subQueue(new AllocationFileQueue.Builder("child-1")
                      .minResources("4096mb,4vcores")
                      .build())
                  .subQueue(new AllocationFileQueue.Builder("child-2")
                      .minResources("4096mb,4vcores")
                      .build())
                  .build())
              .build());
    }

    if (drf) {
      allocationFileWriter.drfDefaultQueueSchedulingPolicy();
    }
    allocationFileWriter.writeToFile(ALLOC_FILE.getAbsolutePath());

    assertTrue(ALLOC_FILE.exists(),
        "Allocation file does not exist, not running the test");
  }

  private void setupCluster() throws IOException {
    resourceManager = new MockRM(conf);
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    // YARN-6249, FSLeafQueue#lastTimeAtMinShare is initialized to the time in
    // the real world, so we should keep the clock up with it.
    clock.setTime(SystemClock.getInstance().getTime());
    scheduler.setClock(clock);
    resourceManager.start();

    // Create and add two nodes to the cluster, with capacities
    // disproportional to the container requests.
    addNode(NODE_CAPACITY_MULTIPLE * GB, 3 * NODE_CAPACITY_MULTIPLE);
    addNode(NODE_CAPACITY_MULTIPLE * GB, 3 * NODE_CAPACITY_MULTIPLE);

    // Reinitialize the scheduler so DRF policy picks up cluster capacity
    // TODO (YARN-6194): One shouldn't need to call this
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Verify if child-1 and child-2 are preemptable
    FSQueue child1 =
        scheduler.getQueueManager().getQueue("nonpreemptable.child-1");
    assertFalse(child1.isPreemptable());
    FSQueue child2 =
        scheduler.getQueueManager().getQueue("nonpreemptable.child-2");
    assertFalse(child2.isPreemptable());
  }

  private void sendEnoughNodeUpdatesToAssignFully() {
    for (RMNode node : rmNodes) {
      NodeUpdateSchedulerEvent nodeUpdateSchedulerEvent =
          new NodeUpdateSchedulerEvent(node);
      for (int i = 0; i < NODE_CAPACITY_MULTIPLE; i++) {
        scheduler.handle(nodeUpdateSchedulerEvent);
      }
    }
  }

  /**
   * Submit an application to a given queue and take over the entire cluster.
   *
   * @param queueName queue name
   */
  private void takeAllResources(String queueName) {
    // Create an app that takes up all the resources on the cluster
    ApplicationAttemptId appAttemptId
        = createSchedulingRequest(GB, 1, queueName, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size());
    greedyApp = scheduler.getSchedulerApp(appAttemptId);
    scheduler.update();
    sendEnoughNodeUpdatesToAssignFully();
    assertEquals(8, greedyApp.getLiveContainers().size());
    // Verify preemptable for queue and app attempt
    assertTrue(
        scheduler.getQueueManager().getQueue(queueName).isPreemptable()
            == greedyApp.isPreemptable());
  }

  /**
   * Submit an application to a given queue and preempt half resources of the
   * cluster.
   *
   * @param queueName queue name
   * @throws InterruptedException
   *         if any thread has interrupted the current thread.
   */
  private void preemptHalfResources(String queueName)
      throws InterruptedException {
    ApplicationAttemptId appAttemptId
        = createSchedulingRequest(2 * GB, 2, queueName, "default",
        NODE_CAPACITY_MULTIPLE * rmNodes.size() / 2);
    starvingApp = scheduler.getSchedulerApp(appAttemptId);

    // Move clock enough to identify starvation
    clock.tickSec(1);
    scheduler.update();
  }

  /**
   * Submit application to {@code queue1} and take over the entire cluster.
   * Submit application with larger containers to {@code queue2} that
   * requires preemption from the first application.
   *
   * @param queue1 first queue
   * @param queue2 second queue
   * @throws InterruptedException if interrupted while waiting
   */
  private void submitApps(String queue1, String queue2)
      throws InterruptedException {
    takeAllResources(queue1);
    preemptHalfResources(queue2);
  }

  private void verifyPreemption(int numStarvedAppContainers,
                                int numGreedyAppContainers)
      throws InterruptedException {
    // Sleep long enough for four containers to be preempted.
    for (int i = 0; i < 1000; i++) {
      if (greedyApp.getLiveContainers().size() == numGreedyAppContainers) {
        break;
      }
      Thread.sleep(10);
    }

    // Post preemption, verify the greedyApp has the correct # of containers.
    assertEquals(numGreedyAppContainers, greedyApp.getLiveContainers().size(),
        "Incorrect # of containers on the greedy app");

    // Verify the queue metrics are set appropriately. The greedyApp started
    // with 8 1GB, 1vcore containers.
    assertEquals(8 - numGreedyAppContainers,
        greedyApp.getQueue().getMetrics().getAggregatePreemptedContainers(),
        "Incorrect # of preempted containers in QueueMetrics");

    // Verify the node is reserved for the starvingApp
    for (RMNode rmNode : rmNodes) {
      FSSchedulerNode node = (FSSchedulerNode)
          scheduler.getNodeTracker().getNode(rmNode.getNodeID());
      if (node.getContainersForPreemption().size() > 0) {
        assertTrue(node.getPreemptionList().keySet().contains(starvingApp),
            "node should be reserved for the starvingApp");
      }
    }

    sendEnoughNodeUpdatesToAssignFully();

    // Verify the preempted containers are assigned to starvingApp
    assertEquals(numStarvedAppContainers, starvingApp.getLiveContainers().size(),
        "Starved app is not assigned the right # of containers");

    // Verify the node is not reserved for the starvingApp anymore
    for (RMNode rmNode : rmNodes) {
      FSSchedulerNode node = (FSSchedulerNode)
          scheduler.getNodeTracker().getNode(rmNode.getNodeID());
      if (node.getContainersForPreemption().size() > 0) {
        assertFalse(node.getPreemptionList().keySet().contains(starvingApp));
      }
    }
  }

  private void verifyNoPreemption() throws InterruptedException {
    // Sleep long enough to ensure not even one container is preempted.
    for (int i = 0; i < 100; i++) {
      if (greedyApp.getLiveContainers().size() != 8) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals(8, greedyApp.getLiveContainers().size());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testPreemptionWithinSameLeafQueue(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    String queue = "root.preemptable.child-1";
    submitApps(queue, queue);
    if (fairsharePreemption) {
      verifyPreemption(2, 4);
    } else {
      verifyNoPreemption();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testPreemptionBetweenTwoSiblingLeafQueues(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    submitApps("root.preemptable.child-1", "root.preemptable.child-2");
    verifyPreemption(2, 4);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testPreemptionBetweenNonSiblingQueues(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    submitApps("root.preemptable.child-1", "root.nonpreemptable.child-1");
    verifyPreemption(2, 4);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testNoPreemptionFromDisallowedQueue(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    submitApps("root.nonpreemptable.child-1", "root.preemptable.child-1");
    verifyNoPreemption();
  }

  /**
   * Set the number of AM containers for each node.
   *
   * @param numAMContainersPerNode number of AM containers per node
   */
  private void setNumAMContainersPerNode(int numAMContainersPerNode) {
    List<FSSchedulerNode> potentialNodes =
        scheduler.getNodeTracker().getNodesByResourceName("*");
    for (FSSchedulerNode node: potentialNodes) {
      List<RMContainer> containers=
          node.getCopiedListOfRunningContainers();
      // Change the first numAMContainersPerNode out of 4 containers to
      // AM containers
      for (int i = 0; i < numAMContainersPerNode; i++) {
        ((RMContainerImpl) containers.get(i)).setAMContainer(true);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testPreemptionSelectNonAMContainer(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    takeAllResources("root.preemptable.child-1");
    setNumAMContainersPerNode(2);
    preemptHalfResources("root.preemptable.child-2");

    verifyPreemption(2, 4);

    ArrayList<RMContainer> containers =
        (ArrayList<RMContainer>) starvingApp.getLiveContainers();
    String host0 = containers.get(0).getNodeId().getHost();
    String host1 = containers.get(1).getNodeId().getHost();
    // Each node provides two and only two non-AM containers to be preempted, so
    // the preemption happens on both nodes.
    assertTrue(!host0.equals(host1), "Preempted containers should come from two different "
        + "nodes.");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testAppNotPreemptedBelowFairShare(String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    takeAllResources("root.preemptable.child-1");
    tryPreemptMoreThanFairShare("root.preemptable.child-2");
  }

  private void tryPreemptMoreThanFairShare(String queueName)
          throws InterruptedException {
    ApplicationAttemptId appAttemptId
            = createSchedulingRequest(3 * GB, 3, queueName, "default",
            NODE_CAPACITY_MULTIPLE * rmNodes.size() / 2);
    starvingApp = scheduler.getSchedulerApp(appAttemptId);

    verifyPreemption(1, 5);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDisableAMPreemption(String name, int mode) throws IOException {
    initTestFairSchedulerPreemption(name, mode);
    testDisableAMPreemption(false);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testDisableAMPreemptionGlobally(String name, int mode)
      throws IOException {
    initTestFairSchedulerPreemption(name, mode);
    testDisableAMPreemption(true);
  }

  private void testDisableAMPreemption(boolean global) {
    takeAllResources("root.preemptable.child-1");
    setNumAMContainersPerNode(2);
    RMContainer container = greedyApp.getLiveContainers().stream()
            .filter(rmContainer -> rmContainer.isAMContainer())
            .findFirst()
            .get();
    if (!global) {
      greedyApp.setEnableAMPreemption(false);
    }
    assertFalse(greedyApp.canContainerBePreempted(container, null));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testPreemptionBetweenSiblingQueuesWithParentAtFairShare(
      String name, int mode) throws InterruptedException, IOException {
    initTestFairSchedulerPreemption(name, mode);
    // Run this test only for fairshare preemption
    if (!fairsharePreemption) {
      return;
    }

    // Let one of the child queues take over the entire cluster
    takeAllResources("root.preemptable.child-1");

    // Submit a job so half the resources go to parent's sibling
    preemptHalfResources("root.preemptable-sibling");
    verifyPreemption(2, 4);

    // Submit a job to the child's sibling to force preemption from the child
    preemptHalfResources("root.preemptable.child-2");
    verifyPreemption(1, 2);
  }

  /* It tests the case that there is less-AM-container solution in the
   * remaining nodes.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testRelaxLocalityPreemptionWithLessAMInRemainingNodes(
      String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    takeAllResources("root.preemptable.child-1");
    RMNode node1 = rmNodes.get(0);
    setAllAMContainersOnNode(node1.getNodeID());
    ApplicationAttemptId greedyAppAttemptId =
        getGreedyAppAttemptIdOnNode(node1.getNodeID());
    updateRelaxLocalityRequestSchedule(node1, GB, 4);
    verifyRelaxLocalityPreemption(node1.getNodeID(), greedyAppAttemptId, 4);
  }

  /* It tests the case that there is no less-AM-container solution in the
   * remaining nodes.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("getParameters")
  public void testRelaxLocalityPreemptionWithNoLessAMInRemainingNodes(
      String name, int mode) throws Exception {
    initTestFairSchedulerPreemption(name, mode);
    takeAllResources("root.preemptable.child-1");
    RMNode node1 = rmNodes.get(0);
    setNumAMContainersOnNode(3, node1.getNodeID());
    RMNode node2 = rmNodes.get(1);
    setAllAMContainersOnNode(node2.getNodeID());
    ApplicationAttemptId greedyAppAttemptId =
        getGreedyAppAttemptIdOnNode(node2.getNodeID());
    updateRelaxLocalityRequestSchedule(node1, GB * 2, 1);
    verifyRelaxLocalityPreemption(node2.getNodeID(), greedyAppAttemptId, 6);
  }

  private void setAllAMContainersOnNode(NodeId nodeId) {
    setNumAMContainersOnNode(Integer.MAX_VALUE, nodeId);
  }

  private void setNumAMContainersOnNode(int num, NodeId nodeId) {
    int count = 0;
    SchedulerNode node = scheduler.getNodeTracker().getNode(nodeId);
    for (RMContainer container: node.getCopiedListOfRunningContainers()) {
      count++;
      if (count <= num) {
        ((RMContainerImpl) container).setAMContainer(true);
      } else {
        break;
      }
    }
  }

  private ApplicationAttemptId getGreedyAppAttemptIdOnNode(NodeId nodeId) {
    SchedulerNode node = scheduler.getNodeTracker().getNode(nodeId);
    return node.getCopiedListOfRunningContainers().get(0)
        .getApplicationAttemptId();
  }

  /*
   * Send the resource requests allowed relax locality to scheduler. The
   * params node/nodeMemory/numNodeContainers used for NODE_LOCAL request.
   */
  private void updateRelaxLocalityRequestSchedule(RMNode node, int nodeMemory,
      int numNodeContainers) {
    // Make the RACK_LOCAL and OFF_SWITCH requests big enough that they can't be
    // satisfied. This forces the RR that we consider for preemption to be the
    // NODE_LOCAL one.
    ResourceRequest nodeRequest = createResourceRequest(nodeMemory,
        node.getHostName(), 1, numNodeContainers, true);
    ResourceRequest rackRequest =
        createResourceRequest(GB * 10, node.getRackName(), 1, 1, true);
    ResourceRequest anyRequest =
        createResourceRequest(GB * 10, ResourceRequest.ANY, 1, 1, true);

    List<ResourceRequest> resourceRequests =
        Arrays.asList(nodeRequest, rackRequest, anyRequest);

    ApplicationAttemptId starvedAppAttemptId = createSchedulingRequest(
        "root.preemptable.child-2", "default", resourceRequests);
    starvingApp = scheduler.getSchedulerApp(starvedAppAttemptId);

    // Move clock enough to identify starvation
    clock.tickSec(1);
    scheduler.update();
  }

  private void verifyRelaxLocalityPreemption(NodeId notBePreemptedNodeId,
      ApplicationAttemptId greedyAttemptId, int numGreedyAppContainers)
      throws Exception {
    // Make sure 4 containers were preempted from the greedy app, but also that
    // none were preempted on our all-AM node, even though the NODE_LOCAL RR
    // asked for resources on it.

    // TODO (YARN-7655) The starved app should be allocated 4 containers.
    // It should be possible to modify the RRs such that this is true
    // after YARN-7903.
    verifyPreemption(0, numGreedyAppContainers);
    SchedulerNode node = scheduler.getNodeTracker()
        .getNode(notBePreemptedNodeId);
    for (RMContainer container : node.getCopiedListOfRunningContainers()) {
      assert(container.isAMContainer());
      assert(container.getApplicationAttemptId().equals(greedyAttemptId));
    }
  }

}
