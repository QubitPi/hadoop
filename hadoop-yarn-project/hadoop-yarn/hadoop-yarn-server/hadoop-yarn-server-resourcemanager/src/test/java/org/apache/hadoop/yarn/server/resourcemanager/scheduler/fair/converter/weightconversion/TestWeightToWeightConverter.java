/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestWeightToWeightConverter extends WeightConverterTestBase {
  private WeightToWeightConverter converter;
  private CapacitySchedulerConfiguration csConfig;

  public static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  public static final QueuePath ROOT_A = new QueuePath("root", "a");
  public static final QueuePath ROOT_B = new QueuePath("root", "b");
  public static final QueuePath ROOT_C = new QueuePath("root", "c");

  @BeforeEach
  public void setup() {
    converter = new WeightToWeightConverter();
    csConfig = new CapacitySchedulerConfiguration(
        new Configuration(false));
  }

  @Test
  public void testNoChildQueueConversion() {
    FSQueue root = createFSQueues();
    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ROOT), 0.0f, "root weight");
    assertEquals(22, csConfig.getPropsWithPrefix(PREFIX).size(), "Converted items");
  }

  @Test
  public void testSingleWeightConversion() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ROOT), 0.0f, "root weight");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ROOT_A), 0.0f, "root.a weight");
    assertEquals(23, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
  }

  @Test
  public void testMultiWeightConversion() {
    FSQueue root = createFSQueues(1, 2, 3);

    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(25, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ROOT), 0.0f, "root weight");
    assertEquals(1.0f, csConfig.getNonLabeledQueueWeight(ROOT_A), 0.0f, "root.a weight");
    assertEquals(2.0f, csConfig.getNonLabeledQueueWeight(ROOT_B), 0.0f, "root.b weight");
    assertEquals(3.0f, csConfig.getNonLabeledQueueWeight(ROOT_C), 0.0f, "root.c weight");
  }

  @Test
  public void testAutoCreateV2FlagOnParent() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, csConfig);

    assertTrue(csConfig.isAutoQueueCreationV2Enabled(ROOT), "root autocreate v2 enabled");
  }

  @Test
  public void testAutoCreateV2FlagOnParentWithoutChildren() {
    FSQueue root = createParent(new ArrayList<>());
    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(22, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
    assertTrue(csConfig.isAutoQueueCreationV2Enabled(ROOT), "root autocreate v2 enabled");
  }
}
