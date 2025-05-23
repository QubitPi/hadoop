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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.slf4j.event.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatcher;

public class TestNodesListManager {
  private boolean isRMAppEvent;
  private boolean isNodesListEvent;

  @Test
  @Timeout(value = 300)
  public void testNodeUsableEvent() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    final Dispatcher dispatcher = getDispatcher();
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 28000);
    Resource clusterResource = Resource.newInstance(28000, 8);
    RMNode rmnode = MockNodes.newNodeInfo(1, clusterResource);

    // Create killing APP
    RMApp killRmApp = MockRMAppSubmitter.submitWithMemory(200, rm);
    rm.killApp(killRmApp.getApplicationId());
    rm.waitForState(killRmApp.getApplicationId(), RMAppState.KILLED);

    // Create finish APP
    RMApp finshRmApp = MockRMAppSubmitter.submitWithMemory(2000, rm);
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt = finshRmApp.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    // Fire Event for NODE_USABLE
    // Should not have RMAppNodeUpdateEvent to AsyncDispatcher.
    dispatcher.getEventHandler().handle(new NodesListManagerEvent(
        NodesListManagerEventType.NODE_USABLE, rmnode));
    assertFalse(getIsRMAppEvent(), "Got unexpected RM app event");
    assertTrue(getIsNodesListEvent(), "Received no NodesListManagerEvent");
  }

  @Test
  public void testCachedResolver() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    ControlledClock clock = new ControlledClock();
    clock.setTime(0);
    final int CACHE_EXPIRY_INTERVAL_SECS = 30;
    NodesListManager.CachedResolver resolver =
        new NodesListManager.CachedResolver(clock, CACHE_EXPIRY_INTERVAL_SECS);
    resolver.init(new YarnConfiguration());
    resolver.start();
    resolver.addToCache("testCachedResolverHost1", "1.1.1.1");
    assertEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));

    resolver.addToCache("testCachedResolverHost2", "1.1.1.2");
    assertEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));

    // test removeFromCache
    resolver.removeFromCache("testCachedResolverHost1");
    assertNotEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));

    // test expiry
    clock.tickMsec(CACHE_EXPIRY_INTERVAL_SECS * 1000 + 1);
    resolver.getExpireChecker().run();
    assertNotEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertNotEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));
  }

  @Test
  public void testDefaultResolver() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);

    YarnConfiguration conf = new YarnConfiguration();

    MockRM rm = new MockRM(conf);
    rm.init(conf);
    NodesListManager nodesListManager = rm.getNodesListManager();

    NodesListManager.Resolver resolver = nodesListManager.getResolver();
    assertTrue(resolver instanceof NodesListManager.DirectResolver,
        "default resolver should be DirectResolver");
  }

  @Test
  public void testCachedResolverWithEvent() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS, 30);

    MockRM rm = new MockRM(conf);
    rm.init(conf);
    NodesListManager nodesListManager = rm.getNodesListManager();
    nodesListManager.init(conf);
    nodesListManager.start();

    NodesListManager.CachedResolver resolver =
        (NodesListManager.CachedResolver)nodesListManager.getResolver();

    resolver.addToCache("testCachedResolverHost1", "1.1.1.1");
    resolver.addToCache("testCachedResolverHost2", "1.1.1.2");
    assertEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));

    RMNode rmnode1 = MockNodes.newNodeInfo(1, Resource.newInstance(28000, 8),
        1, "testCachedResolverHost1", 1234);
    RMNode rmnode2 = MockNodes.newNodeInfo(1, Resource.newInstance(28000, 8),
        1, "testCachedResolverHost2", 1234);

    nodesListManager.handle(
        new NodesListManagerEvent(NodesListManagerEventType.NODE_USABLE,
            rmnode1));
    assertNotEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));

    nodesListManager.handle(
        new NodesListManagerEvent(NodesListManagerEventType.NODE_USABLE,
            rmnode2));
    assertNotEquals("1.1.1.1",
        resolver.resolve("testCachedResolverHost1"));
    assertNotEquals("1.1.1.2",
        resolver.resolve("testCachedResolverHost2"));

  }

  /*
   * Create dispatcher object
   */
  private Dispatcher getDispatcher() {
    return new DrainDispatcher() {
      @SuppressWarnings("unchecked")
      @Override
      public EventHandler<Event> getEventHandler() {

        class EventArgMatcher implements ArgumentMatcher<AbstractEvent> {
          @Override
          public boolean matches(AbstractEvent argument) {
            if (argument instanceof RMAppNodeUpdateEvent) {
              isRMAppEvent = true;
            }
            if (argument instanceof NodesListManagerEvent) {
              isNodesListEvent = true;
            }
            return false;
          }
        }

        EventHandler handler = spy(super.getEventHandler());
        doNothing().when(handler).handle(argThat(new EventArgMatcher()));
        return handler;
      }
    };
  }

  public boolean getIsNodesListEvent() {
    return isNodesListEvent;
  }

  public boolean getIsRMAppEvent() {
    return isRMAppEvent;
  }
}
