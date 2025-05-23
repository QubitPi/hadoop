/******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ReservationSystemTestUtil {

  private static Random rand = new Random();

  public final static String RESERVATION_Q_SHORT = "dedicated";
  public final static String reservationQ = "root." + RESERVATION_Q_SHORT;
  public final static String DEDICATED_PATH = CapacitySchedulerConfiguration.ROOT
          + CapacitySchedulerConfiguration.DOT + RESERVATION_Q_SHORT;
  private final static String DEFAULT_PATH = CapacitySchedulerConfiguration.ROOT + ".default";
  private final static String A_PATH = CapacitySchedulerConfiguration.ROOT + ".a";
  private final static String A1_PATH = A_PATH + ".a1";
  private final static String A2_PATH = A_PATH + ".a2";
  private final static QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  private final static QueuePath DEDICATED = new QueuePath(DEDICATED_PATH);
  private final static QueuePath DEFAULT = new QueuePath(DEFAULT_PATH);
  private final static QueuePath A = new QueuePath(A_PATH);
  private final static QueuePath A1 = new QueuePath(A1_PATH);
  private final static QueuePath A2 = new QueuePath(A2_PATH);

  public static ReservationId getNewReservationId() {
    return ReservationId.newInstance(rand.nextLong(), rand.nextLong());
  }

  public static ReservationSchedulerConfiguration createConf(
      String reservationQ, long timeWindow, float instConstraint,
      float avgConstraint) {

    ReservationSchedulerConfiguration realConf =
        new CapacitySchedulerConfiguration();
    ReservationSchedulerConfiguration conf = spy(realConf);
    QueuePath reservationQueuePath = new QueuePath(reservationQ);
    when(conf.getReservationWindow(reservationQueuePath)).thenReturn(timeWindow);
    when(conf.getInstantaneousMaxCapacity(reservationQueuePath))
        .thenReturn(instConstraint);
    when(conf.getAverageCapacity(reservationQueuePath)).thenReturn(avgConstraint);

    return conf;
  }

  public static void validateReservationQueue(
      AbstractReservationSystem reservationSystem, String planQName) {
    Plan plan = reservationSystem.getPlan(planQName);
    assertNotNull(plan);
    assertTrue(plan instanceof InMemoryPlan);
    assertEquals(planQName, plan.getQueueName());
    assertEquals(8192, plan.getTotalCapacity().getMemorySize());
    assertTrue(
        plan.getReservationAgent() instanceof AlignedPlannerWithGreedy);
    assertTrue(plan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }

  public static void setupFSAllocationFile(String allocationFile) {
    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("default")
            .weight(1).build())
        .addQueue(new AllocationFileQueue.Builder("a")
            .weight(1)
            .subQueue(new AllocationFileQueue.Builder("a1")
                .weight(3).build())
            .subQueue(new AllocationFileQueue.Builder("a2")
                .weight(7).build())
            .build())
        .addQueue(new AllocationFileQueue.Builder("dedicated")
            .weight(8)
            .reservation()
            .build())
        .writeToFile(allocationFile);
  }

  public static void updateFSAllocationFile(String allocationFile) {
    AllocationFileWriter.create()
        .drfDefaultQueueSchedulingPolicy()
        .addQueue(new AllocationFileQueue.Builder("default")
            .weight(5).build())
        .addQueue(new AllocationFileQueue.Builder("a")
            .weight(5)
            .subQueue(new AllocationFileQueue.Builder("a1")
                .weight(3).build())
            .subQueue(new AllocationFileQueue.Builder("a2")
                .weight(7).build())
            .build())
        .addQueue(new AllocationFileQueue.Builder("dedicated")
            .weight(10)
            .reservation()
            .build())
        .addQueue(new AllocationFileQueue.Builder("reservation")
            .weight(80)
            .reservation()
            .build())
        .writeToFile(allocationFile);
  }

  public static FairScheduler setupFairScheduler(RMContext rmContext,
      Configuration conf, int numContainers) throws IOException {
    FairScheduler scheduler = new FairScheduler();
    scheduler.setRMContext(rmContext);

    when(rmContext.getScheduler()).thenReturn(scheduler);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rmContext);

    Resource resource =
        ReservationSystemTestUtil.calculateClusterResource(numContainers);
    RMNode node1 = MockNodes.newNodeInfo(1, resource, 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    return scheduler;
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration) {
    return createSimpleReservationDefinition(arrival, deadline, duration, 1);
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration, int parallelism) {
    return createSimpleReservationDefinition(arrival, deadline, duration,
        parallelism, null);
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration, int parallelism,
      String recurrenceExpression) {
    // create a request with a single atomic ask
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), parallelism, parallelism, duration);
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    if (recurrenceExpression != null) {
      rDef.setRecurrenceExpression(recurrenceExpression);
    }
    return rDef;
  }

  public static ReservationSubmissionRequest createSimpleReservationRequest(
      ReservationId reservationId, int numContainers, long arrival,
      long deadline, long duration) {
    return createSimpleReservationRequest(reservationId, numContainers, arrival,
        deadline, duration, Priority.UNDEFINED);
  }

  public static ReservationSubmissionRequest createSimpleReservationRequest(
      ReservationId reservationId, int numContainers, long arrival,
      long deadline, long duration, Priority priority) {
    // create a request with a single atomic ask
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), numContainers, 1, duration);
    ReservationRequests reqs = ReservationRequests.newInstance(
        Collections.singletonList(r), ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef = ReservationDefinition.newInstance(arrival,
        deadline, reqs, "testClientRMService#reservation", "0", priority);
    ReservationSubmissionRequest request = ReservationSubmissionRequest
        .newInstance(rDef, reservationQ, reservationId);
    return request;
  }

  public static RMContext createMockRMContext() {
    RMContext context = mock(RMContext.class);
    when(context.getStateStore()).thenReturn(new MemoryRMStateStore());
    return context;
  }

  public CapacityScheduler mockCapacityScheduler(int numContainers)
      throws IOException {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);

    CapacityScheduler cs = spy(new CapacityScheduler());
    cs.setConf(new YarnConfiguration());

    RMContext mockRmContext = createRMContext(conf);

    cs.setRMContext(mockRmContext);
    try {
      cs.serviceInit(conf);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    initializeRMContext(numContainers, cs, mockRmContext);
    return cs;
  }

  @SuppressWarnings("rawtypes")
  public static void initializeRMContext(int numContainers,
      AbstractYarnScheduler scheduler, RMContext mockRMContext) {

    when(mockRMContext.getScheduler()).thenReturn(scheduler);
    Resource r = calculateClusterResource(numContainers);
    doReturn(r).when(scheduler).getClusterResource();
  }

  public static RMContext createRMContext(Configuration conf) {
    RMContext mockRmContext = spy(new RMContextImpl(null, null, null,
        null, null, null, new RMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInRM(conf),
        new ClientToAMTokenSecretManagerInRM(), null));

    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(nlm.getQueueResource(any(String.class), anySet(),
        any(Resource.class))).thenAnswer(new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[2];
          }
        });

    when(nlm.getResourceByLabel(any(), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
        });

    mockRmContext.setNodeLabelManager(nlm);
    mockRmContext
        .setMultiNodeSortingManager(mock(MultiNodeSortingManager.class));
    return mockRmContext;
  }

  public static void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    // Define default queue
    final String defQPath = CapacitySchedulerConfiguration.ROOT + ".default";
    final QueuePath defQ = new QueuePath(defQPath);
    conf.setCapacity(defQ, 10);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] {"default", "a", RESERVATION_Q_SHORT});
    conf.setCapacity(A, 10);
    conf.setCapacity(DEDICATED, 80);
    // Set as reservation queue
    conf.setReservable(DEDICATED, true);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setCapacity(A2, 70);
  }

  public static void setupDynamicQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] {RESERVATION_Q_SHORT});
    conf.setCapacity(DEDICATED, 100);
    // Set as reservation queue
    conf.setReservable(DEDICATED, true);
  }

  public static String getFullReservationQueueName() {
    return CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT + RESERVATION_Q_SHORT;
  }

  public static QueuePath getFullReservationQueuePath() {
    return new QueuePath(getFullReservationQueueName());
  }

  public static String getReservationQueueName() {
    return reservationQ;
  }

  public static void updateQueueConfiguration(
      CapacitySchedulerConfiguration conf, String newQ) {
    // Define default queue
    final String prefix = CapacitySchedulerConfiguration.ROOT
            + CapacitySchedulerConfiguration.DOT;
    conf.setCapacity(DEFAULT, 5);

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[] {"default", "a", RESERVATION_Q_SHORT, newQ});
    conf.setCapacity(A, 5);
    conf.setCapacity(DEDICATED, 10);
    // Set as reservation queue
    conf.setReservable(DEDICATED, true);

    final QueuePath newQueue = new QueuePath(prefix + newQ);
    conf.setCapacity(newQueue, 80);
    // Set as reservation queue
    conf.setReservable(newQueue, true);

    // Define 2nd-level queues
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, 30);
    conf.setCapacity(A2, 70);
  }

  public static ReservationDefinition generateRandomRR(Random rand, long i) {
    rand.setSeed(i);
    long now = System.currentTimeMillis();

    // start time at random in the next 12 hours
    long arrival = rand.nextInt(12 * 3600 * 1000);
    // deadline at random in the next day
    long deadline = arrival + rand.nextInt(24 * 3600 * 1000);

    // create a request with a single atomic ask
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(now + arrival);
    rr.setDeadline(now + deadline);

    int gang = 1 + rand.nextInt(9);
    int par = (rand.nextInt(1000) + 1) * gang;
    long dur = rand.nextInt(2 * 3600 * 1000); // random duration within 2h
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), par, gang, dur);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rand.nextInt(3);
    ReservationRequestInterpreter[] type =
        ReservationRequestInterpreter.values();
    reqs.setInterpreter(type[rand.nextInt(type.length)]);
    rr.setReservationRequests(reqs);

    return rr;

  }

  public static Map<ReservationInterval, Resource> generateAllocation(
      long startTime, long step, int[] alloc) {
    return generateAllocation(startTime, step, alloc, null);
  }

  public static Map<ReservationInterval, Resource> generateAllocation(
      long startTime, long step, int[] alloc, String recurrenceExpression) {
    Map<ReservationInterval, Resource> req = new TreeMap<>();

    long period = 0;
    if (recurrenceExpression != null) {
      period = Long.parseLong(recurrenceExpression);
    }

    long rStart;
    long rEnd;
    for (int j = 0; j < 86400000; j += period) {
      for (int i = 0; i < alloc.length; i++) {
        rStart = (startTime + i * step) + j * period;
        rEnd = (startTime + (i + 1) * step) + j * period;
        if (period > 0) {
          rStart = rStart % period + j * period;
          rEnd = rEnd % period + j * period;
          if (rStart > rEnd) {
            // skip wrap-around entry
            continue;
          }
        }

        req.put(new ReservationInterval(rStart, rEnd),
            ReservationSystemUtil.toResource(ReservationRequest
                .newInstance(Resource.newInstance(1024, 1), alloc[i])));

      }
      // execute only once if non-periodic
      if (period == 0) {
        break;
      }
    }
    return req;
  }

  public static RLESparseResourceAllocation generateRLESparseResourceAllocation(
      int[] alloc, long[] timeSteps) {
    TreeMap<Long, Resource> allocationsMap = new TreeMap<>();
    for (int i = 0; i < alloc.length; i++) {
      allocationsMap.put(timeSteps[i],
          Resource.newInstance(alloc[i], alloc[i]));
    }
    RLESparseResourceAllocation rleVector = new RLESparseResourceAllocation(
        allocationsMap, new DefaultResourceCalculator());
    return rleVector;
  }

  public static Resource calculateClusterResource(int numContainers) {
    return Resource.newInstance(numContainers * 1024, numContainers);
  }


  public static Map<ReservationInterval, Resource> toAllocation(
      RLESparseResourceAllocation rle, long start, long end) {
    Map<ReservationInterval, Resource> resAlloc = new TreeMap<>();

    for (Map.Entry<Long, Resource> e : rle.getCumulative().entrySet()) {
      Long nextKey = rle.getCumulative().higherKey(e.getKey());
      if (nextKey == null) {
        break;
      } else {
        if (e.getKey() >= start && e.getKey() <= end && nextKey >= start
            && nextKey <= end) {
          resAlloc.put(new ReservationInterval(e.getKey(), nextKey),
              e.getValue());
        }
      }
    }

    return resAlloc;
  }



}
