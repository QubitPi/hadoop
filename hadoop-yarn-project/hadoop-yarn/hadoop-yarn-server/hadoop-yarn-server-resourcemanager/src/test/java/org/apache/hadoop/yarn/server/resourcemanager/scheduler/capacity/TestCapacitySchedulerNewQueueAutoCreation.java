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

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.HashSet;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestCapacitySchedulerNewQueueAutoCreation
    extends TestCapacitySchedulerAutoCreatedQueueBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.yarn.server.resourcemanager
          .scheduler.capacity.TestCapacitySchedulerAutoCreatedQueueBase.class);
  private static final QueuePath EMPTY_AUTO_PARENT = new QueuePath("root.empty-auto-parent");
  private static final QueuePath A_A2_AUTO = new QueuePath("root.a.a2-auto");
  private static final QueuePath E_AUTO = new QueuePath("root.e-auto");
  private static final QueuePath E_E1 = new QueuePath("root.e.e1");
  private static final QueuePath A_A_AUTO_A2 = new QueuePath("root.a.a-auto.a2");
  private static final QueuePath A_A1_AUTO_A2_AUTO = new QueuePath("root.a.a1-auto.a2-auto");
  public static final int GB = 1024;
  public static final int MAX_MEMORY = 1200;
  private MockRM mockRM = null;
  private CapacityScheduler cs;
  private CapacitySchedulerConfiguration csConf;
  private CapacitySchedulerQueueManager autoQueueHandler;
  private AutoCreatedQueueDeletionPolicy policy = new
      AutoCreatedQueueDeletionPolicy();

  public CapacityScheduler getCs() {
    return cs;
  }

  public AutoCreatedQueueDeletionPolicy getPolicy() {
    return policy;
  }

  /*
  Create the following structure:
           root
        /   |   \
      a     b    e
    /
  a1
   */
  @BeforeEach
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // By default, set 3 queues, a/b, and a.a1
    csConf.setQueues(ROOT, new String[]{"a", "b"});
    csConf.setNonLabeledQueueWeight(ROOT, 1f);
    csConf.setNonLabeledQueueWeight(A, 1f);
    csConf.setNonLabeledQueueWeight(B, 1f);
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setNonLabeledQueueWeight(A1, 1f);
    csConf.setAutoQueueCreationV2Enabled(ROOT, true);
    csConf.setAutoQueueCreationV2Enabled(A, true);
    csConf.setAutoQueueCreationV2Enabled(E, true);
    csConf.setAutoQueueCreationV2Enabled(new QueuePath(PARENT_QUEUE), true);
    // Test for auto deletion when expired
    csConf.setAutoExpiredDeletionTime(1);
  }

  @AfterEach
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  protected void startScheduler() throws Exception {
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    // Policy for new auto created queue's auto deletion when expired
    policy.init(cs.getConfiguration(), cs.getRMContext(), cs);
    mockRM.start();
    cs.start();
    autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    mockRM.registerNode("h1:1234", MAX_MEMORY * GB); // label = x
  }

  /*
  Create and validate the following structure:

                          root
     ┌─────┬────────┬─────┴─────┬─────────┐
     a     b      c-auto     e-auto     d-auto
     |                        |
    a1                      e1-auto
   */
  private void createBasicQueueStructureAndValidate() throws Exception {
    // queue's weights are 1
    // root
    // - a (w=1)
    // - b (w=1)
    // - c-auto (w=1)
    // - d-auto (w=1)
    // - e-auto (w=1)
    //   - e1-auto (w=1)
    MockNM nm1 = mockRM.registerNode("h1:1234", 1200 * GB); // label = x

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue c = cs.getQueue("root.c-auto");
    assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    assertEquals(((LeafQueue)c).getUserLimitFactor(), -1, 1e-6);
    assertEquals(((LeafQueue)c).getMaxAMResourcePerQueuePercent(), 1, 1e-6);

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    assertEquals(1 / 4f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(300 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/4 abs resource, a2/a1 has the same weight, so a2 has 1/8 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    assertEquals(1 / 8f, a2.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, a2.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(150 * GB,
        a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // try, create leaf + parent, will success
    createQueue("root.e-auto.e1-auto");

    // Now check capacity of e and e1 (under root we have 5 queues, so e1 get
    // 1/5 capacity
    CSQueue e = cs.getQueue("root.e-auto");
    assertEquals(1 / 5f, e.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, e.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Under e, there's only one queue, so e1/e have same capacity
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");
    assertEquals(1 / 5f, e1.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, e1.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(240 * GB,
        e1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  /*
  Create and validate the structure:
                         root
     ┌─────┬────────┬─────┴───────┐
     a     b      c-auto       d-auto
     |
     a1
   */
  @Test
  public void testAutoCreateQueueWithSiblingsUnderRoot() throws Exception {
    startScheduler();

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue c = cs.getQueue("root.c-auto");
    assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue d = cs.getQueue("root.d-auto");
    assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Now we check queue c again, it should also become 1/4 capacity
    assertEquals(1 / 4f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(300 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  /*
  Create and validate the structure:
          root
     ┌─────┴─────┐
     b           a
               /  \
              a1  a2-auto
   */
  @Test
  public void testAutoCreateQueueStaticParentOneLevel() throws Exception {
    startScheduler();
    // Now we add a two-level queue, create leaf only
    // Now add another queue a2-auto, under root.a
    createQueue("root.a.a2-auto");

    // root.a has 1/2 abs resource, a2/a1 has the same weight, so a2 has 1/4 abs
    // capacity
    CSQueue a2 = cs.getQueue("root.a.a2-auto");
    assertEquals(1 / 4f, a2.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, a2.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(MAX_MEMORY * (1 / 4f) * GB,
        a2.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);

  }

  /*
  Create and validate the structure:
          root
     ┌─────┴─────┐
     b            a
               |    \
             a1    a2-auto
                   |     \
               a3-auto   a4-auto
   */
  @Test
  public void testAutoCreateQueueAutoParentTwoLevelsWithSiblings()
      throws Exception {
    startScheduler();
    csConf.setAutoQueueCreationV2Enabled(A_A2_AUTO, true);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto is alone with weight 1/4
    createQueue("root.a.a2-auto.a3-auto");
    CSQueue a3 = cs.getQueue("root.a.a2-auto.a3-auto");
    assertEquals(1 / 4f, a3.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, a3.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(MAX_MEMORY * (1 / 4f) * GB,
        a3.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);

    // root.a has 1/2 abs resource -> a1 and a2-auto same weight 1/4
    // -> a3-auto and a4-auto same weight 1/8
    createQueue("root.a.a2-auto.a4-auto");
    CSQueue a4 = cs.getQueue("root.a.a2-auto.a4-auto");
    assertEquals(1 / 8f, a3.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, a3.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(MAX_MEMORY * (1 / 8f) * GB,
        a4.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize(),
        1e-6);
  }

  @Test
  public void testAutoCreateQueueShouldFailWhenNonParentQueue()
      throws Exception {
    assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      createQueue("root.a.a1.a2-auto");
    });
  }

  @Test
  public void testAutoCreateQueueWhenSiblingsNotInWeightMode()
      throws Exception {
    assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      // If the new queue mode is used it's allowed to
      // create a new dynamic queue when the sibling is
      // not in weight mode
      assumeTrue(csConf.isLegacyQueueMode() == true);
      csConf.setCapacity(A, 50f);
      csConf.setCapacity(B, 50f);
      csConf.setCapacity(A1, 100f);
      cs.reinitialize(csConf, mockRM.getRMContext());
      createQueue("root.a.a2-auto");
    });
  }

  @Test()
  public void testAutoCreateMaximumQueueDepth()
      throws Exception {
    startScheduler();
    // By default, max depth is 2, therefore this is an invalid scenario
    assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a3-auto.a4-auto.a5-auto"));

    // Set depth 3 for root.a, making it a valid scenario
    csConf.setMaximumAutoCreatedQueueDepth(A, 3);
    cs.reinitialize(csConf, mockRM.getRMContext());
    try {
      createQueue("root.a.a3-auto.a4-auto.a5-auto");
    } catch (SchedulerDynamicEditException sde) {
      LOG.error("%s", sde);
      fail("Depth is set for root.a, exception should not be thrown");
    }

    // Set global depth to 3
    csConf.setMaximumAutoCreatedQueueDepth(3);
    csConf.unset(QueuePrefixes.getQueuePrefix(A)
        + CapacitySchedulerConfiguration.MAXIMUM_QUEUE_DEPTH);
    cs.reinitialize(csConf, mockRM.getRMContext());
    try {
      createQueue("root.a.a6-auto.a7-auto.a8-auto");
    } catch (SchedulerDynamicEditException sde) {
      LOG.error("%s", sde);
      fail("Depth is set globally, exception should not be thrown");
    }

    // Set depth on a dynamic queue, which has no effect on auto queue creation validation
    csConf.setMaximumAutoCreatedQueueDepth(new QueuePath("root.a.a6-auto.a7-auto.a8-auto"), 10);
    assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("root.a.a6-auto.a7-auto.a8-auto.a9-auto.a10-auto.a11-auto"));
  }

  @Test
  public void testAutoCreateQueueShouldFailIfNotEnabledForParent()
      throws Exception {
    assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();
      csConf.setAutoQueueCreationV2Enabled(ROOT, false);
      cs.reinitialize(csConf, mockRM.getRMContext());
      createQueue("root.c-auto");
    });
  }

  @Test
  public void testAutoCreateQueueRefresh() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Refresh the queue to make sure all queues are still exist.
    // (Basically, dynamic queues should not disappear after refresh).
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    assertEquals(5, cs.getQueue("root").getChildQueues().size());
    assertNotNull(cs.getQueue("root.c-auto"));
  }

  @Test
  public void testConvertDynamicToStaticQueue() throws Exception {
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Now, update root.a's weight to 6
    csConf.setNonLabeledQueueWeight(new QueuePath("root.a"), 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Double confirm, after refresh, we should still see root queue has 5
    // children.
    assertEquals(5, cs.getQueue("root").getChildQueues().size());

    // Get queue a
    CSQueue a = cs.getQueue("root.a");

    // a's abs resource should be 6/10, (since a.weight=6, all other 4 peers
    // have weight=1).
    assertEquals(6 / 10f, a.getAbsoluteCapacity(), 1e-6);
    assertEquals(720 * GB,
        a.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    assertEquals(6f, a.getQueueCapacities().getWeight(), 1e-6);

    // Set queue c-auto's weight to 6, and mark c-auto to be static queue
    csConf.setQueues(ROOT, new String[]{"a", "b", "c-auto"});
    csConf.setNonLabeledQueueWeight(new QueuePath("root.c-auto"), 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Get queue c
    CSQueue c = cs.getQueue("root.c-auto");

    // c's abs resource should be 6/15, (since a/c.weight=6, all other 3 peers
    // have weight=1).
    assertEquals(6 / 15f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(480 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    assertEquals(6f, c.getQueueCapacities().getWeight(), 1e-6);

    // First, create e2-auto queue
    createQueue("root.e-auto.e2-auto");

    // Do change 2nd level queue from dynamic to static
    csConf.setQueues(ROOT, new String[]{"a", "b", "c-auto", "e-auto"});
    csConf.setNonLabeledQueueWeight(new QueuePath("root.e-auto"), 6f);
    csConf.setQueues(E_AUTO, new String[]{"e1-auto"});
    csConf.setNonLabeledQueueWeight(new QueuePath("root.e-auto.e1-auto"), 6f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Get queue e1
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");

    // e's abs resource should be 6/20 * (6/7),
    // (since a/c/e.weight=6, all other 2 peers
    // have weight=1, and e1's weight is 6, e2's weight is 1).
    float e1NormalizedWeight = (6 / 20f) * (6 / 7f);
    assertEquals(e1NormalizedWeight, e1.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(e1, MAX_MEMORY * e1NormalizedWeight);
    assertEquals(6f, e1.getQueueCapacities().getWeight(), 1e-6);
  }

  /*
  Create the structure and convert d-auto to static and leave d1-auto as dynamic
                        root
     ┌─────┬─────────────┴──────┐
     a     b                 d-auto
     |                         |
     a1                     d1-auto
   */
  @Test
  public void testConvertDynamicParentToStaticParent() throws Exception {
    startScheduler();
    createQueue("root.d-auto.d1-auto");
    csConf.setQueues(ROOT, new String[]{"a", "b", "d-auto"});
    csConf.setNonLabeledQueueWeight(new QueuePath("root.a"), 6f);
    csConf.setNonLabeledQueueWeight(new QueuePath("root.d-auto"), 1f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CSQueue d = cs.getQueue("root.d-auto");

    assertEquals(1 / 8f, d.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(d, MAX_MEMORY * (1 / 8f));
    assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);

    CSQueue d1 = cs.getQueue("root.d-auto.d1-auto");
    assertEquals(1 / 8f, d1.getAbsoluteCapacity(), 1e-6);
    assertQueueMinResource(d1, MAX_MEMORY * (1 / 8f));
    assertEquals(1f, d1.getQueueCapacities().getWeight(), 1e-6);
  }

  @Test
  public void testAutoQueueCreationOnAppSubmission() throws Exception {
    startScheduler();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    assertNotNull(e);
    assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    assertNotNull(user0);
    assertTrue(user0.isDynamicQueue());
  }

  @Test
  public void testChildlessParentQueueWhenAutoQueueCreationEnabled()
      throws Exception {
    startScheduler();
    csConf.setQueues(ROOT, new String[]{"a", "b", "empty-auto-parent"});
    csConf.setNonLabeledQueueWeight(ROOT, 1f);
    csConf.setNonLabeledQueueWeight(A, 1f);
    csConf.setNonLabeledQueueWeight(B, 1f);
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setNonLabeledQueueWeight(A1, 1f);
    csConf.setAutoQueueCreationV2Enabled(ROOT, true);
    csConf.setAutoQueueCreationV2Enabled(A, true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    CSQueue empty = cs.getQueue("root.empty-auto-parent");
    assertTrue(empty instanceof LeafQueue,
        "empty-auto-parent is not a LeafQueue");
    empty.stopQueue();

    csConf.setQueues(ROOT, new String[]{"a", "b", "empty-auto-parent"});
    csConf.setNonLabeledQueueWeight(ROOT, 1f);
    csConf.setNonLabeledQueueWeight(A, 1f);
    csConf.setNonLabeledQueueWeight(B, 1f);
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setNonLabeledQueueWeight(A1, 1f);
    csConf.setAutoQueueCreationV2Enabled(ROOT, true);
    csConf.setAutoQueueCreationV2Enabled(A, true);
    csConf.setAutoQueueCreationV2Enabled(EMPTY_AUTO_PARENT, true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    empty = cs.getQueue("root.empty-auto-parent");
    assertTrue(empty instanceof AbstractParentQueue,
        "empty-auto-parent is not a ParentQueue");
    assertEquals(0, empty.getChildQueues().size(),
        "empty-auto-parent has children");
    assertTrue(((AbstractParentQueue)empty).isEligibleForAutoQueueCreation(),
        "empty-auto-parent is not eligible for auto queue creation");
  }

  @Test
  public void testAutoQueueCreationWithDisabledMappingRules() throws Exception {
    startScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    // Set ApplicationPlacementContext to null in the submitted application
    // in order to imitate a submission with mapping rules turned off
    SchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId,
        "root.a.a1-auto.a2-auto", USER0, null);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    SchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent(
        appAttemptId, false);
    cs.handle(addAppEvent);
    cs.handle(addAttemptEvent);

    CSQueue a2Auto = cs.getQueue("root.a.a1-auto.a2-auto");
    assertNotNull(a2Auto);
  }

  @Test
  public void testAutoCreateQueueUserLimitDisabled() throws Exception {
    startScheduler();
    createBasicQueueStructureAndValidate();

    submitApp(cs, USER0, USER0, "root.e-auto");

    AbstractCSQueue e = (AbstractCSQueue) cs.getQueue("root.e-auto");
    assertNotNull(e);
    assertTrue(e.isDynamicQueue());

    AbstractCSQueue user0 = (AbstractCSQueue) cs.getQueue(
        "root.e-auto." + USER0);
    assertNotNull(user0);
    assertTrue(user0.isDynamicQueue());
    assertTrue(user0 instanceof LeafQueue);

    LeafQueue user0LeafQueue = (LeafQueue) user0;

    // Assert user limit factor is -1
    assertTrue(user0LeafQueue.getUserLimitFactor() == -1);

    // Assert user max applications not limited
    assertEquals(user0LeafQueue.getMaxApplicationsPerUser(),
        user0LeafQueue.getMaxApplications());

    // Assert AM Resource
    assertEquals(user0LeafQueue.getAMResourceLimit().getMemorySize(),
        user0LeafQueue.
            getMaxAMResourcePerQueuePercent() * MAX_MEMORY * GB, 1e-6);

    // Assert user limit (no limit) when limit factor is -1
    assertEquals(MAX_MEMORY * GB,
        user0LeafQueue.getEffectiveMaxCapacityDown("",
            user0LeafQueue.getMinimumAllocation()).getMemorySize(), 1e-6);
  }

  @Test
  public void testAutoQueueCreationMaxAppUpdate() throws Exception {
    startScheduler();

    // When no conf for max apps
    LeafQueue a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    assertNotNull(a1);
    assertEquals(csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity(), a1.getMaxApplications(), 1);

    LeafQueue b = (LeafQueue)cs.
        getQueue("root.b");
    assertNotNull(b);
    assertEquals(csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity(), b.getMaxApplications(), 1);

    createQueue("root.e");

    // Make sure other children queues
    // max app correct.
    LeafQueue e = (LeafQueue)cs.
        getQueue("root.e");
    assertNotNull(e);
    assertEquals(csConf.getMaximumSystemApplications()
            * e.getAbsoluteCapacity(), e.getMaxApplications(), 1);

    a1 =  (LeafQueue)cs.
        getQueue("root.a.a1");
    assertNotNull(a1);
    assertEquals(csConf.getMaximumSystemApplications()
            * a1.getAbsoluteCapacity(), a1.getMaxApplications(), 1);

    b = (LeafQueue)cs.
        getQueue("root.b");
    assertNotNull(b);
    assertEquals(csConf.getMaximumSystemApplications()
            * b.getAbsoluteCapacity(), b.getMaxApplications(), 1);

    // When update global max app per queue
    csConf.setGlobalMaximumApplicationsPerQueue(1000);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(1000, b.getMaxApplications());
    assertEquals(1000, a1.getMaxApplications());
    assertEquals(1000, e.getMaxApplications());

    // when set some queue for max apps
    csConf.setMaximumApplicationsPerQueue(new QueuePath("root.e1"), 50);
    createQueue("root.e1");
    LeafQueue e1 = (LeafQueue)cs.
        getQueue("root.e1");
    assertNotNull(e1);

    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(50, e1.getMaxApplications());
  }

  @Test
  public void testAutoCreateQueueWithAmbiguousNonFullPathParentName()
      throws Exception {
    assertThrows(SchedulerDynamicEditException.class, () -> {
      startScheduler();

      createQueue("root.a.a");
      createQueue("a.a");
    });
  }

  @Test
  public void testAutoCreateQueueIfFirstExistingParentQueueIsNotStatic()
      throws Exception {
    startScheduler();

    // create a dynamic ParentQueue
    createQueue("root.a.a-parent-auto.a1-leaf-auto");
    assertNotNull(cs.getQueue("root.a.a-parent-auto"));

    // create a new dynamic LeafQueue under the existing ParentQueue
    createQueue("root.a.a-parent-auto.a2-leaf-auto");

    CSQueue a2Leaf = cs.getQueue("a2-leaf-auto");

    // Make sure a2-leaf-auto is under a-parent-auto
    assertEquals("root.a.a-parent-auto",
        a2Leaf.getParent().getQueuePath());
  }

  @Test
  public void testAutoCreateQueueIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.getQueue("root.b");
    assertFalse(b.isDynamicQueue());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.getQueue("root.a.b");
    assertTrue(bAutoParent.isDynamicQueue());
    assertTrue(bAutoParent.hasChildQueues());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    assertTrue(bAutoLeafQueue.isDynamicQueue());
    assertFalse(bAutoLeafQueue.hasChildQueues());
  }

  @Test
  public void testAutoCreateQueueMaxQueuesLimit() throws Exception {
    startScheduler();

    csConf.setAutoCreatedQueuesV2MaxChildQueuesLimit(E, 5);
    cs.reinitialize(csConf, mockRM.getRMContext());

    for (int i = 0; i < 5; ++i) {
      createQueue("root.e.q_" + i);
    }

    // Check if max queue limit can't be exceeded
    try {
      createQueue("root.e.q_6");
      fail("Can't exceed max queue limit.");
    } catch (Exception ex) {
      assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
  }

  @Test
  public void testAutoCreatedQueueTemplateConfig() throws Exception {
    startScheduler();

    QueuePath childQueuesOfA = new QueuePath("root.a.*");
    QueuePath aQueuePath = new QueuePath("root.a");
    QueuePath cQueuePath = new QueuePath("root.c");

    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        childQueuesOfA) + "capacity", "6w");
    cs.reinitialize(csConf, mockRM.getRMContext());

    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    assertEquals(6f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not set by template");
    assertEquals(-1f, a2.getUserLimitFactor(), 1e-6,
        "user limit factor should be disabled with dynamic queues");
    assertEquals(1f, a2.getMaxAMResourcePerQueuePercent(), 1e-6,
        "maximum AM resource percent should be 1 with dynamic queues");

    // Set the user-limit-factor and maximum-am-resource-percent via templates to ensure their
    // modified defaults are indeed overridden
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        childQueuesOfA) + "user-limit-factor", "10");
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        childQueuesOfA) + "maximum-am-resource-percent", "0.8");

    cs.reinitialize(csConf, mockRM.getRMContext());
    a2 = (LeafQueue) cs.getQueue("root.a.a-auto.a2");
    assertEquals(6f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is overridden");
    assertEquals(10f, a2.getUserLimitFactor(), 1e-6,
        "user limit factor should be modified by templates");
    assertEquals(0.8f, a2.getMaxAMResourcePerQueuePercent(), 1e-6,
        "maximum AM resource percent should be modified by templates");


    csConf.setNonLabeledQueueWeight(new QueuePath("root.a.a-auto.a2"), 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(4f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not explicitly set");

    csConf.setBoolean(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        aQueuePath) + CapacitySchedulerConfiguration
        .AUTO_CREATE_CHILD_QUEUE_AUTO_REMOVAL_ENABLE, false);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue a3 = createQueue("root.a.a3");
    assertFalse(a3.isEligibleForAutoDeletion(),
        "auto queue deletion should be turned off on a3");

    // Set the capacity of label TEST
    csConf.set(AutoCreatedQueueTemplate.getAutoQueueTemplatePrefix(
        cQueuePath) + "accessible-node-labels.TEST.capacity", "6w");
    csConf.setQueues(ROOT, new String[]{"a", "b", "c"});
    csConf.setAutoQueueCreationV2Enabled(C, true);
    cs.reinitialize(csConf, mockRM.getRMContext());
    AbstractLeafQueue c1 = createQueue("root.c.c1");
    assertEquals(6f, c1.getQueueCapacities().getWeight("TEST"), 1e-6,
        "weight is not set for label TEST");
    cs.reinitialize(csConf, mockRM.getRMContext());
    c1 = (AbstractLeafQueue) cs.getQueue("root.c.c1");
    assertEquals(6f, c1.getQueueCapacities().getWeight("TEST"), 1e-6,
        "weight is not set for label TEST");
  }

  @Test
  public void testAutoCreatedQueueConfigChange() throws Exception {
    startScheduler();
    AbstractLeafQueue a2 = createQueue("root.a.a-auto.a2");
    csConf.setNonLabeledQueueWeight(a2.getQueuePathObject(), 4f);
    cs.reinitialize(csConf, mockRM.getRMContext());

    assertEquals(4f, a2.getQueueCapacities().getWeight(), 1e-6,
        "weight is not explicitly set");

    a2 = (AbstractLeafQueue) cs.getQueue("root.a.a-auto.a2");
    csConf.setState(A_A_AUTO_A2, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(QueueState.STOPPED, a2.getState(),
        "root.a.a-auto.a2 has not been stopped");

    csConf.setState(A_A_AUTO_A2, QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(QueueState.RUNNING, a2.getState(),
        "root.a.a-auto.a2 is not running");
  }

  @Test
  public void testAutoCreateQueueState() throws Exception {
    startScheduler();

    createQueue("root.e.e1");
    csConf.setState(E, QueueState.STOPPED);
    csConf.setState(E_E1, QueueState.STOPPED);
    csConf.setState(A, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Make sure the static queue is stopped
    assertEquals(cs.getQueue("root.a").getState(),
        QueueState.STOPPED);
    // If not set, default is the queue state of parent
    assertEquals(cs.getQueue("root.a.a1").getState(),
        QueueState.STOPPED);

    assertEquals(cs.getQueue("root.e").getState(),
        QueueState.STOPPED);
    assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e state to RUNNING
    csConf.setState(E, QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.STOPPED);

    // Make root.e.e1 state to RUNNING
    csConf.setState(E_E1, QueueState.RUNNING);
    cs.reinitialize(csConf, mockRM.getRMContext());
    assertEquals(cs.getQueue("root.e.e1").getState(),
        QueueState.RUNNING);
  }

  @Test
  public void testAutoQueueCreationDepthLimitFromStaticParent()
      throws Exception {
    startScheduler();

    // a is the first existing queue here and it is static, therefore
    // the distance is 2
    createQueue("root.a.a-auto.a1-auto");
    assertNotNull(cs.getQueue("root.a.a-auto.a1-auto"));

    try {
      createQueue("root.a.a-auto.a2-auto.a3-auto");
      fail("Queue creation should not succeed because the distance " +
          "from the first static parent is above limit");
    } catch (SchedulerDynamicEditException ignored) {

    }

  }

  @Test
  public void testCapacitySchedulerAutoQueueDeletion() throws Exception {
    startScheduler();
    csConf.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());
    csConf.setAutoExpiredDeletionTime(1);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Set<String> policies = new HashSet<>();
    policies.add(
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());

    assertTrue(cs.getSchedulingMonitorManager().
        isSameConfiguredPolicies(policies), "No AutoCreatedQueueDeletionPolicy " +
        "is present in running monitors");

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNotNull(a1, "a1 is not present");
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    assertNotNull(a2, "a2 is not present");
    assertTrue(a2.isDynamicQueue(), "a2 is not a dynamic queue");

    // Now there are still 1 app in a2 queue.
    assertEquals(1, a2.getNumApplications());

    // Wait the time expired.
    long l1 = a2.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > csConf.getAutoExpiredDeletionTime();
    }, 100, 2000);

    // Make sure the queue will not be deleted
    // when expired with remaining apps.
    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    assertNotNull(a2, "a2 is not present");

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    assertEquals(0, a2.getNumApplications());

    // Wait the a2 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a2Tmp = (AbstractCSQueue) cs.getQueue(
            "root.a.a1-auto.a2-auto");
      return a2Tmp == null;
    }, 100, 3000);

    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    assertNull(a2, "a2 is not deleted");

    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNotNull(a1, "a1 is not present");

    // Now the parent queue without child
    // will be deleted for expired.
    // Wait a1 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a1Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto");
      return a1Tmp == null;
    }, 100, 3000);
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNull(a1, "a1 is not deleted");
  }

  @Test
  public void testCapacitySchedulerAutoQueueDeletionDisabled()
      throws Exception {
    startScheduler();
    // Test for disabled auto deletion
    csConf.setAutoExpiredDeletionEnabled(
        A_A1_AUTO_A2_AUTO, false);
    csConf.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    csConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());
    csConf.setAutoExpiredDeletionTime(1);
    cs.reinitialize(csConf, mockRM.getRMContext());

    Set<String> policies = new HashSet<>();
    policies.add(
        AutoCreatedQueueDeletionPolicy.class.getCanonicalName());

    assertTrue(cs.getSchedulingMonitorManager().isSameConfiguredPolicies(policies),
        "No AutoCreatedQueueDeletionPolicy is present in running monitors");

    ApplicationAttemptId a2App = submitApp(cs, USER0,
        "a2-auto", "root.a.a1-auto");

    // Wait a2 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.a.a1-auto.a2-auto") != null,
        100, 2000);

    AbstractCSQueue a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNotNull(a1, "a1 is not present");
    AbstractCSQueue a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    assertNotNull(a2, "a2 is not present");
    assertTrue(a2.isDynamicQueue(), "a2 is not a dynamic queue");

    // Make app finished.
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(a2App,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        a2App.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // Now there are no apps in a2 queue.
    assertEquals(0, a2.getNumApplications());

    // Wait the time expired.
    long l1 = a2.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > csConf.getAutoExpiredDeletionTime();
    }, 100, 2000);

    // The auto deletion is no enabled for a2-auto
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNotNull(a1, "a1 is not present");
    a2 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto.a2-auto");
    assertNotNull(a2, "a2 is not present");
    assertTrue(a2.isDynamicQueue(), "a2 is not a dynamic queue");

    // Enabled now
    // The auto deletion will work.
    csConf.setAutoExpiredDeletionEnabled(
        A_A1_AUTO_A2_AUTO, true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    // Wait the a2 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a2Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto.a2-auto");
      return a2Tmp == null;
    }, 100, 3000);

    a2 = (AbstractCSQueue) cs.
        getQueue("root.a.a1-auto.a2-auto");
    assertNull(a2, "a2 is not deleted");
    // The parent will not be deleted with child queues
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNotNull(a1, "a1 is not present");

    // Now the parent queue without child
    // will be deleted for expired.
    // Wait a1 deleted.
    GenericTestUtils.waitFor(() -> {
      AbstractCSQueue a1Tmp = (AbstractCSQueue) cs.getQueue(
          "root.a.a1-auto");
      return a1Tmp == null;
    }, 100, 3000);
    a1 = (AbstractCSQueue) cs.getQueue(
        "root.a.a1-auto");
    assertNull(a1, "a1 is not deleted");
  }

  @Test
  public void testAutoCreateQueueAfterRemoval() throws Exception {
    // queue's weights are 1
    // root
    // - a (w=1)
    // - b (w=1)
    // - c-auto (w=1)
    // - d-auto (w=1)
    // - e-auto (w=1)
    //   - e1-auto (w=1)
    startScheduler();

    createBasicQueueStructureAndValidate();

    // Under e, there's only one queue, so e1/e have same capacity
    CSQueue e1 = cs.getQueue("root.e-auto.e1-auto");
    assertEquals(1 / 5f, e1.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, e1.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(240 * GB,
        e1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e1.
    cs.removeQueue(e1);
    CSQueue e = cs.getQueue("root.e-auto");
    assertEquals(1 / 5f, e.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, e.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(240 * GB,
        e.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal e.
    cs.removeQueue(e);
    CSQueue d = cs.getQueue("root.d-auto");
    assertEquals(1 / 4f, d.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, d.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(300 * GB,
        d.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal d.
    cs.removeQueue(d);
    CSQueue c = cs.getQueue("root.c-auto");
    assertEquals(1 / 3f, c.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, c.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(400 * GB,
        c.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check after removal c.
    cs.removeQueue(c);
    CSQueue b = cs.getQueue("root.b");
    assertEquals(1 / 2f, b.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, b.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());

    // Check can't remove static queue b.
    try {
      cs.removeQueue(b);
      fail("Can't remove static queue b!");
    } catch (Exception ex) {
      assertTrue(ex
          instanceof SchedulerDynamicEditException);
    }
    // Check a.
    CSQueue a = cs.getQueue("root.a");
    assertEquals(1 / 2f, a.getAbsoluteCapacity(), 1e-6);
    assertEquals(1f, a.getQueueCapacities().getWeight(), 1e-6);
    assertEquals(600 * GB,
        b.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
  }

  @Test
  public void testQueueInfoIfAmbiguousQueueNames() throws Exception {
    startScheduler();

    AbstractCSQueue b = (AbstractCSQueue) cs.
        getQueue("root.b");
    assertFalse(b.isDynamicQueue());
    assertEquals("root.b",
        b.getQueueInfo().getQueuePath());

    createQueue("root.a.b.b");

    AbstractCSQueue bAutoParent = (AbstractCSQueue) cs.
        getQueue("root.a.b");
    assertTrue(bAutoParent.isDynamicQueue());
    assertTrue(bAutoParent.hasChildQueues());
    assertEquals("root.a.b",
        bAutoParent.getQueueInfo().getQueuePath());

    AbstractCSQueue bAutoLeafQueue =
        (AbstractCSQueue) cs.getQueue("root.a.b.b");
    assertTrue(bAutoLeafQueue.isDynamicQueue());
    assertFalse(bAutoLeafQueue.hasChildQueues());
    assertEquals("root.a.b.b",
        bAutoLeafQueue.getQueueInfo().getQueuePath());

    // Make sure all queue name are ambiguous
    assertEquals("b",
        b.getQueueInfo().getQueueName());
    assertEquals("b",
        bAutoParent.getQueueInfo().getQueueName());
    assertEquals("b",
        bAutoLeafQueue.getQueueInfo().getQueueName());
  }

  @Test
  public void testRemoveDanglingAutoCreatedQueuesOnReinit() throws Exception {
    startScheduler();

    // Validate static parent deletion
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    assertTrue(aAuto.isDynamicQueue());

    csConf.setState(A, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    assertEquals(QueueState.STOPPED, aAuto.getState(), "root.a.a-auto is not in STOPPED state");
    csConf.setQueues(ROOT, new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    CSQueue aAutoNew = cs.getQueue("root.a.a-auto");
    assertNull(aAutoNew);

    submitApp(cs, USER0, "a-auto", "root.a");
    aAutoNew = cs.getQueue("root.a.a-auto");
    assertNotNull(aAutoNew);

    // Validate static grandparent deletion
    csConf.setQueues(ROOT, new String[]{"a", "b"});
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setAutoQueueCreationV2Enabled(A1, true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.a.a1.a1-auto");
    CSQueue a1Auto = cs.getQueue("root.a.a1.a1-auto");
    assertNotNull(a1Auto, "a1-auto should exist");

    csConf.setQueues(ROOT, new String[]{"b"});
    cs.reinitialize(csConf, mockRM.getRMContext());
    a1Auto = cs.getQueue("root.a.a1.a1-auto");
    assertNull(a1Auto, "a1-auto has no parent and should not exist");

    // Validate dynamic parent deletion
    csConf.setState(B, QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());
    csConf.setAutoQueueCreationV2Enabled(B, true);
    cs.reinitialize(csConf, mockRM.getRMContext());

    createQueue("root.b.b-auto-parent.b-auto-leaf");
    CSQueue bAutoParent = cs.getQueue("root.b.b-auto-parent");
    assertNotNull(bAutoParent, "b-auto-parent should exist");
    ParentQueue b = (ParentQueue) cs.getQueue("root.b");
    b.removeChildQueue(bAutoParent);

    cs.reinitialize(csConf, mockRM.getRMContext());

    bAutoParent = cs.getQueue("root.b.b-auto-parent");
    assertNull(bAutoParent, "b-auto-parent should not exist ");
    CSQueue bAutoLeaf = cs.getQueue("root.b.b-auto-parent.b-auto-leaf");
    assertNull(bAutoLeaf, "b-auto-leaf should not exist " +
        "when its dynamic parent is removed");
  }

  @Test
  public void testParentQueueDynamicChildRemoval() throws Exception {
    startScheduler();

    createQueue("root.a.a-auto");
    createQueue("root.a.a-auto");
    AbstractCSQueue aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    assertTrue(aAuto.isDynamicQueue());
    ParentQueue a = (ParentQueue) cs.
        getQueue("root.a");
    createQueue("root.e.e1-auto");
    AbstractCSQueue eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");
    assertTrue(eAuto.isDynamicQueue());
    ParentQueue e = (ParentQueue) cs.
        getQueue("root.e");

    // Try to remove a static child queue
    try {
      a.removeChildQueue(cs.getQueue("root.a.a1"));
      fail("root.a.a1 is a static queue and should not be removed at " +
          "runtime");
    } catch (SchedulerDynamicEditException ignored) {
    }

    // Try to remove a dynamic queue with a different parent
    try {
      a.removeChildQueue(eAuto);
      fail("root.a should not be able to remove root.e.e1-auto");
    } catch (SchedulerDynamicEditException ignored) {
    }

    a.removeChildQueue(aAuto);
    e.removeChildQueue(eAuto);

    aAuto = (AbstractCSQueue) cs.
        getQueue("root.a.a-auto");
    eAuto = (AbstractCSQueue) cs.
        getQueue("root.e.e1-auto");

    assertNull(aAuto, "root.a.a-auto should have been removed");
    assertNull(eAuto, "root.e.e1-auto should have been removed");
  }

  @Test()
  public void testAutoCreateInvalidParent() throws Exception {
    startScheduler();
    assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalid.queue"));
    assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalid.queue.longer"));
    assertThrows(SchedulerDynamicEditException.class,
        () -> createQueue("invalidQueue"));
  }

  protected AbstractLeafQueue createQueue(String queuePath) throws YarnException,
      IOException {
    return autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private void assertQueueMinResource(CSQueue queue, float expected) {
    assertEquals(Math.round(expected * GB),
        queue.getQueueResourceQuotas().getEffectiveMinResource()
            .getMemorySize(), 1e-6);
  }
}
