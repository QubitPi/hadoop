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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests to verify all the Container's Launcher Events in
 * {@link ContainersLauncher} are handled as expected.
 */
public class TestContainersLauncher {

  @Mock
  private ApplicationImpl app1;

  @Mock
  private ContainerImpl container;

  @Mock
  private ApplicationId appId;

  @Mock
  private ApplicationAttemptId appAttemptId;

  @Mock
  private ContainerId containerId;

  @Mock
  private ContainersLauncherEvent event;

  @Mock
  private NodeManager.NMContext context;

  @Mock
  private AsyncDispatcher dispatcher;

  @Mock
  private ContainerExecutor exec;

  @Mock
  private LocalDirsHandlerService dirsHandler;

  @Mock
  private ContainerManagerImpl containerManager;

  @Mock
  private ExecutorService containerLauncher;

  @Mock
  private Configuration conf;

  @Mock
  private ContainerLaunch containerLaunch;

  private ContainersLauncher spy;

  @BeforeEach
  public void setup() throws IllegalArgumentException, IllegalAccessException {
    MockitoAnnotations.initMocks(this);
    ContainersLauncher tempContainersLauncher = new ContainersLauncher(
        context, dispatcher, exec, dirsHandler, containerManager);
    ConcurrentMap<ApplicationId, Application> applications =
        new ConcurrentHashMap<>();
    applications.put(appId, app1);
    spy = spy(tempContainersLauncher);
    spy.containerLauncher = containerLauncher;
    conf = doReturn(conf).when(spy).getConfig();
    when(event.getContainer()).thenReturn(container);
    when(container.getContainerId()).thenReturn(containerId);
    when(containerId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(containerId.getApplicationAttemptId().getApplicationId())
        .thenReturn(appId);
    when(context.getApplications()).thenReturn(applications);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLaunchContainerEvent()
      throws IllegalArgumentException {
    Map<ContainerId, ContainerLaunch> dummyMap = spy.running;
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.LAUNCH_CONTAINER);
    assertEquals(0, dummyMap.size());
    spy.handle(event);
    assertEquals(1, dummyMap.size());
    Mockito.verify(containerLauncher, Mockito.times(1))
        .submit(Mockito.any(ContainerLaunch.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRelaunchContainerEvent()
      throws IllegalArgumentException {
    Map<ContainerId, ContainerLaunch> dummyMap = spy.running;
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.RELAUNCH_CONTAINER);
    assertEquals(0, dummyMap.size());
    spy.handle(event);
    assertEquals(1, dummyMap.size());
    Mockito.verify(containerLauncher, Mockito.times(1))
        .submit(Mockito.any(ContainerRelaunch.class));
    for (ContainerId cid : dummyMap.keySet()) {
      Object o = dummyMap.get(cid);
      assertEquals(true, (o instanceof ContainerRelaunch));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRecoverContainerEvent()
      throws IllegalArgumentException {
    Map<ContainerId, ContainerLaunch> dummyMap = spy.running;
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.RECOVER_CONTAINER);
    assertEquals(0, dummyMap.size());
    spy.handle(event);
    assertEquals(1, dummyMap.size());
    Mockito.verify(containerLauncher, Mockito.times(1))
        .submit(Mockito.any(RecoveredContainerLaunch.class));
    for (ContainerId cid : dummyMap.keySet()) {
      Object o = dummyMap.get(cid);
      assertEquals(true, (o instanceof RecoveredContainerLaunch));
    }
  }

  @Test
  public void testRecoverPausedContainerEvent()
      throws IllegalArgumentException {
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.RECOVER_PAUSED_CONTAINER);
    spy.handle(event);
    Mockito.verify(containerLauncher, Mockito.times(1))
        .submit(Mockito.any(RecoverPausedContainerLaunch.class));
  }

  @Test
  public void testCleanupContainerEvent()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    spy.running.clear();
    spy.running.put(containerId, containerLaunch);

    when(event.getType())
        .thenReturn(ContainersLauncherEventType.CLEANUP_CONTAINER);
    assertEquals(1, spy.running.size());
    spy.handle(event);
    assertEquals(0, spy.running.size());
    Mockito.verify(containerLauncher, Mockito.times(1))
        .submit(Mockito.any(ContainerCleanup.class));
  }

  @Test
  public void testCleanupContainerForReINITEvent()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    spy.running.clear();
    spy.running.put(containerId, containerLaunch);

    when(event.getType())
        .thenReturn(ContainersLauncherEventType.CLEANUP_CONTAINER_FOR_REINIT);
    final List<ContainerId> cleanedContainers = new ArrayList<>();
    doAnswer(invocation -> {
      cleanedContainers.add((ContainerId)invocation.getArguments()[1]);
      return null;
    }).when(spy).cleanup(any(), any(), anyBoolean());
    spy.handle(event);
    assertEquals(containerId, cleanedContainers.get(0), "container not cleaned");
  }

  @Test
  public void testSignalContainerEvent()
      throws IllegalArgumentException, IllegalAccessException, IOException {

    SignalContainersLauncherEvent dummyEvent =
        mock(SignalContainersLauncherEvent.class);
    when(dummyEvent.getContainer()).thenReturn(container);
    when(container.getContainerId()).thenReturn(containerId);
    when(containerId.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(containerId.getApplicationAttemptId().getApplicationId())
        .thenReturn(appId);

    spy.running.clear();
    spy.running.put(containerId, containerLaunch);
    when(dummyEvent.getType())
        .thenReturn(ContainersLauncherEventType.SIGNAL_CONTAINER);
    when(dummyEvent.getCommand())
        .thenReturn(SignalContainerCommand.GRACEFUL_SHUTDOWN);
    doNothing().when(containerLaunch)
        .signalContainer(SignalContainerCommand.GRACEFUL_SHUTDOWN);
    spy.handle(dummyEvent);
    assertEquals(1, spy.running.size());
    Mockito.verify(containerLaunch, Mockito.times(1))
        .signalContainer(SignalContainerCommand.GRACEFUL_SHUTDOWN);
  }

  @Test
  public void testPauseContainerEvent()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    spy.running.clear();
    spy.running.put(containerId, containerLaunch);
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.PAUSE_CONTAINER);
    doNothing().when(containerLaunch).pauseContainer();
    spy.handle(event);
    assertEquals(1, spy.running.size());
    Mockito.verify(containerLaunch, Mockito.times(1)).pauseContainer();
  }

  @Test
  public void testResumeContainerEvent()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    spy.running.clear();
    spy.running.put(containerId, containerLaunch);
    when(event.getType())
        .thenReturn(ContainersLauncherEventType.RESUME_CONTAINER);
    doNothing().when(containerLaunch).resumeContainer();
    spy.handle(event);
    assertEquals(1, spy.running.size());
    Mockito.verify(containerLaunch, Mockito.times(1)).resumeContainer();
  }
}
