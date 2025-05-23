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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTags;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.LongBinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test behaviors of single constraint app placement allocator.
 */
public class TestSingleConstraintAppPlacementAllocator {
  private AppSchedulingInfo appSchedulingInfo;
  private AllocationTagsManager spyAllocationTagsManager;
  private RMContext rmContext;
  private SchedulerRequestKey schedulerRequestKey;
  private SingleConstraintAppPlacementAllocator allocator;

  @BeforeEach
  public void setup() throws Exception {
    // stub app scheduling info.
    appSchedulingInfo = mock(AppSchedulingInfo.class);
    when(appSchedulingInfo.getApplicationId()).thenReturn(
        TestUtils.getMockApplicationId(1));
    when(appSchedulingInfo.getApplicationAttemptId()).thenReturn(
        TestUtils.getMockApplicationAttemptId(1, 1));
    when(appSchedulingInfo.getDefaultNodeLabelExpression()).thenReturn("y");
    // stub RMContext
    rmContext = TestUtils.getMockRMContext();

    // Create allocation tags manager
    AllocationTagsManager allocationTagsManager = new AllocationTagsManager(
        rmContext);
    PlacementConstraintManager placementConstraintManager =
        new MemoryPlacementConstraintManager();
    spyAllocationTagsManager = spy(allocationTagsManager);
    schedulerRequestKey = new SchedulerRequestKey(Priority.newInstance(1), 2L,
        TestUtils.getMockContainerId(1, 1));
    rmContext.setAllocationTagsManager(spyAllocationTagsManager);
    rmContext.setPlacementConstraintManager(placementConstraintManager);

    // Create allocator
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
  }

  private void assertValidSchedulingRequest(
      SchedulingRequest schedulingRequest) {
    // Create allocator to avoid fields polluted by previous runs
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
  }

  private void assertInvalidSchedulingRequest(
      SchedulingRequest schedulingRequest, boolean recreateAllocator) {
    try {
      // Create allocator
      if (recreateAllocator) {
        allocator = new SingleConstraintAppPlacementAllocator();
        allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
      }
      allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    } catch (SchedulerInvalidResourceRequestException e) {
      // Expected
      return;
    }
    fail("Expect failure for schedulingRequest=" + schedulingRequest.toString());
  }

  @Test
  public void testSchedulingRequestValidation() {
    // Valid
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    assertEquals("", allocator.getTargetNodePartition());

    // Valid (with partition)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition("x"))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    assertEquals("x", allocator.getTargetNodePartition());

    // Valid (without specifying node partition)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer")).build())
        .resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    // Node partition is unspecified, use the default node label expression y
    assertEquals("y", allocator.getTargetNodePartition());

    // Valid (with application Id target)
    assertValidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer")).build())
        .resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build());
    // Allocation tags should not include application Id
    assertEquals("y", allocator.getTargetNodePartition());

    // Invalid (without sizing)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer")).build())
        .build(), true);

    // Invalid (without target tags)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE).build())
        .build(), true);

    // Invalid (not GUARANTEED)
    assertInvalidSchedulingRequest(SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build(), true);
  }

  @Test
  public void testSchedulingRequestUpdate() {
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNotIn(PlacementConstraints.NODE,
                    PlacementConstraints.PlacementTargets
                        .allocationTag("mapper", "reducer"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with exactly same scheduling request, should succeeded.
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with scheduling request different at #allocations,
    // should succeeded.
    schedulingRequest.getResourceSizing().setNumAllocations(10);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);

    // Update allocator with a newly constructed scheduling request different at
    // #allocations, should succeeded.
    SchedulingRequest newSchedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNotIn(PlacementConstraints.NODE,
                    PlacementConstraints.PlacementTargets.nodePartition(""),
                    PlacementConstraints.PlacementTargets
                        .allocationTag("mapper", "reducer"))
                .build()).resourceSizing(
            ResourceSizing.newInstance(11, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, newSchedulingRequest, false);

    // Update allocator with scheduling request different at resource,
    // should failed.
    schedulingRequest.getResourceSizing().setResources(
        Resource.newInstance(2048, 1));
    assertInvalidSchedulingRequest(schedulingRequest, false);

    // Update allocator with a different placement target (allocator tag),
    // should failed
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetCardinality(PlacementConstraints.NODE, 0, 1,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    assertInvalidSchedulingRequest(schedulingRequest, false);

    // Update allocator with recover == true
    int existingNumAllocations =
        allocator.getSchedulingRequest().getResourceSizing()
            .getNumAllocations();
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition(""))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, true);
    assertEquals(existingNumAllocations + 1,
        allocator.getSchedulingRequest().getResourceSizing()
            .getNumAllocations());
  }

  @Test
  public void testFunctionality() throws InvalidAllocationTagsQueryException {
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNotIn(PlacementConstraints.NODE,
                    PlacementConstraints.PlacementTargets
                        .allocationTag("mapper", "reducer"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNode("host1", "/rack1", 123, 1024));
    verify(spyAllocationTagsManager, times(1)).getNodeCardinalityByOp(
        eq(NodeId.fromString("host1:123")), any(AllocationTags.class),
        any(LongBinaryOperator.class));

    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    // Valid (with partition)
    schedulingRequest = SchedulingRequest.newBuilder().executionType(
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
        .allocationRequestId(10L).priority(Priority.newInstance(1))
        .placementConstraintExpression(PlacementConstraints
            .targetNotIn(PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets
                    .allocationTag("mapper", "reducer"),
                PlacementConstraints.PlacementTargets.nodePartition("x"))
            .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
        .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNode("host1", "/rack1", 123, 1024));
    verify(spyAllocationTagsManager, atLeast(1)).getNodeCardinalityByOp(
        eq(NodeId.fromString("host1:123")), any(AllocationTags.class),
        any(LongBinaryOperator.class));

    SchedulerNode node1 = mock(SchedulerNode.class);
    when(node1.getPartition()).thenReturn("x");
    when(node1.getNodeID()).thenReturn(NodeId.fromString("host1:123"));

    assertTrue(allocator
        .precheckNode(node1, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));

    SchedulerNode node2 = mock(SchedulerNode.class);
    when(node1.getPartition()).thenReturn("");
    when(node1.getNodeID()).thenReturn(NodeId.fromString("host2:123"));
    assertFalse(allocator
        .precheckNode(node2, SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY));
  }

  @Test
  public void testNodeAttributesFunctionality() {
    // 1. Simple java=1.8 validation
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNodeAttribute(PlacementConstraints.NODE,
                    NodeAttributeOpCode.EQ,
                    PlacementConstraints.PlacementTargets
                        .nodeAttribute("java", "1.8"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest, false);
    Set<NodeAttribute> attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("java", NodeAttributeType.STRING, "1.8"));
    boolean result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertTrue(result, "Allocation should be success for java=1.8");

    // 2. verify python!=3 validation
    SchedulingRequest schedulingRequest2 =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(PlacementConstraints
                .targetNodeAttribute(PlacementConstraints.NODE,
                    NodeAttributeOpCode.NE,
                    PlacementConstraints.PlacementTargets
                        .nodeAttribute("python", "3"),
                    PlacementConstraints.PlacementTargets.nodePartition(""))
                .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    // Create allocator
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest2, false);
    attributes = new HashSet<>();
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertTrue(result, "Allocation should be success as python doesn't exist");

    // 3. verify python!=3 validation when node has python=2
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest2, false);
    attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "2"));
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertTrue(result, "Allocation should be success as python=3 doesn't exist in node");

    // 4. verify python!=3 validation when node has python=3
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest2, false);
    attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "3"));
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertFalse(result, "Allocation should fail as python=3 exist in node");
  }

  @Test
  public void testConjunctionNodeAttributesFunctionality() {
    // 1. verify and(python!=3:java=1.8) validation when node has python=3
    SchedulingRequest schedulingRequest1 =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(
                PlacementConstraints.and(
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.NE,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("python", "3")),
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.EQ,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("java", "1.8")))
                    .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest1, false);
    Set<NodeAttribute> attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "3"));
    attributes.add(
        NodeAttribute.newInstance("java", NodeAttributeType.STRING, "1.8"));
    boolean result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertFalse(result, "Allocation should fail as python=3 exists in node");

    // 2. verify and(python!=3:java=1.8) validation when node has python=2
    // and java=1.8
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest1, false);
    attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "2"));
    attributes.add(
        NodeAttribute.newInstance("java", NodeAttributeType.STRING, "1.8"));
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertTrue(result, "Allocation should be success as python=2 exists in node");

    // 3. verify or(python!=3:java=1.8) validation when node has python=3
    SchedulingRequest schedulingRequest2 =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(
                PlacementConstraints.or(
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.NE,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("python", "3")),
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.EQ,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("java", "1.8")))
                    .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest2, false);
    attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "3"));
    attributes.add(
        NodeAttribute.newInstance("java", NodeAttributeType.STRING, "1.8"));
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertTrue(result, "Allocation should be success as java=1.8 exists in node");

    // 4. verify or(python!=3:java=1.8) validation when node has python=3
    // and java=1.7.
    allocator = new SingleConstraintAppPlacementAllocator();
    allocator.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
    allocator.updatePendingAsk(schedulerRequestKey, schedulingRequest2, false);
    attributes = new HashSet<>();
    attributes.add(
        NodeAttribute.newInstance("python", NodeAttributeType.STRING, "3"));
    attributes.add(
        NodeAttribute.newInstance("java", NodeAttributeType.STRING, "1.7"));
    result = allocator.canAllocate(NodeType.NODE_LOCAL,
        TestUtils.getMockNodeWithAttributes("host1", "/rack1", 123, 1024,
            attributes));
    assertFalse(result,
        "Allocation should fail as java=1.8 doesnt exist in node");
  }
}
