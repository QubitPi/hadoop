/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService
        .RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests to verify that the {@link ContainerScheduler} is able to
 * recover active containers based on RecoveredContainerStatus and
 * ExecutionType.
 */
public class TestContainerSchedulerRecovery {
  private static final Resource CONTAINER_SIZE =
      Resource.newInstance(1024, 4);
  private static final ResourceUtilization ZERO =
      ResourceUtilization.newInstance(0, 0, 0.0f);

  @Mock private NMContext context;

  @Mock private NodeManagerMetrics metrics;

  @Mock private AsyncDispatcher dispatcher;

  @Mock private ContainerTokenIdentifier token;

  @Mock private ContainerImpl container;

  @Mock private ApplicationId appId;

  @Mock private ApplicationAttemptId appAttemptId;

  @Mock private ContainerId containerId;

  private ContainerScheduler spy;


  private RecoveredContainerState createRecoveredContainerState(
      RecoveredContainerStatus status) {
    RecoveredContainerState mockState = mock(RecoveredContainerState.class);
    when(mockState.getStatus()).thenReturn(status);
    return mockState;
  }

  /**
   * Set up the {@link ContainersMonitor} dependency of
   * {@link ResourceUtilizationTracker} so that we can
   * verify the resource utilization.
   */
  private void setupContainerMonitor() {
    ContainersMonitor containersMonitor = mock(ContainersMonitor.class);
    when(containersMonitor.getVCoresAllocatedForContainers()).thenReturn(10L);
    when(containersMonitor.getPmemAllocatedForContainers()).thenReturn(10240L);
    when(containersMonitor.getVmemRatio()).thenReturn(1.0f);
    when(containersMonitor.getVmemAllocatedForContainers()).thenReturn(10240L);

    ContainerManager cm = mock(ContainerManager.class);
    when(cm.getContainersMonitor()).thenReturn(containersMonitor);
    when(context.getContainerManager()).thenReturn(cm);
    spy = new ContainerScheduler(context, dispatcher, metrics, 0);
  }

  @BeforeEach public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    setupContainerMonitor();
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getResource()).thenReturn(CONTAINER_SIZE);
    when(containerId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(containerId.getApplicationAttemptId().getApplicationId())
        .thenReturn(appId);
    when(containerId.getContainerId()).thenReturn(123L);
  }

  @AfterEach public void tearDown() {
  }

  /*Test if a container is recovered as QUEUED, GUARANTEED,
  * it should be added to queuedGuaranteedContainers map.
  * */
  @Test public void testRecoverContainerQueuedGuaranteed()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.QUEUED);
    when(token.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(1, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as QUEUED, OPPORTUNISTIC,
  * it should be added to queuedOpportunisticContainers map.
  * */
  @Test public void testRecoverContainerQueuedOpportunistic()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.QUEUED);
    when(token.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(1, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as PAUSED, GUARANTEED,
  * it should be added to queuedGuaranteedContainers map.
  * */
  @Test public void testRecoverContainerPausedGuaranteed()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
        createRecoveredContainerState(RecoveredContainerStatus.PAUSED);
    when(token.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(1, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as PAUSED, OPPORTUNISTIC,
  * it should be added to queuedOpportunisticContainers map.
  * */
  @Test public void testRecoverContainerPausedOpportunistic()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.PAUSED);
    when(token.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(1, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as LAUNCHED, GUARANTEED,
  * it should be added to runningContainers map.
  * */
  @Test public void testRecoverContainerLaunchedGuaranteed()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.LAUNCHED);
    when(token.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(1, spy.getNumRunningContainers());
    assertEquals(
        ResourceUtilization.newInstance(1024, 1024, 4.0f),
        spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as LAUNCHED, OPPORTUNISTIC,
  * it should be added to runningContainers map.
  * */
  @Test public void testRecoverContainerLaunchedOpportunistic()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.LAUNCHED);
    when(token.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(1, spy.getNumRunningContainers());
    assertEquals(
        ResourceUtilization.newInstance(1024, 1024, 4.0f),
        spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as REQUESTED, GUARANTEED,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testRecoverContainerRequestedGuaranteed()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.REQUESTED);
    when(token.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as REQUESTED, OPPORTUNISTIC,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testRecoverContainerRequestedOpportunistic()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.REQUESTED);
    when(token.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as COMPLETED, GUARANTEED,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testRecoverContainerCompletedGuaranteed()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.COMPLETED);
    when(token.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as COMPLETED, OPPORTUNISTIC,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testRecoverContainerCompletedOpportunistic()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.COMPLETED);
    when(token.getExecutionType()).thenReturn(ExecutionType.OPPORTUNISTIC);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as GUARANTEED but no executionType set,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testContainerQueuedNoExecType()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.QUEUED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }

  /*Test if a container is recovered as PAUSED but no executionType set,
  * it should not be added to any map mentioned below.
  * */
  @Test public void testContainerPausedNoExecType()
      throws IllegalArgumentException, IllegalAccessException {
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    RecoveredContainerState rcs =
            createRecoveredContainerState(RecoveredContainerStatus.PAUSED);
    when(container.getContainerTokenIdentifier()).thenReturn(token);
    spy.recoverActiveContainer(container, rcs);
    assertEquals(0, spy.getNumQueuedGuaranteedContainers());
    assertEquals(0, spy.getNumQueuedOpportunisticContainers());
    assertEquals(0, spy.getNumRunningContainers());
    assertEquals(ZERO, spy.getCurrentUtilization());
  }
}