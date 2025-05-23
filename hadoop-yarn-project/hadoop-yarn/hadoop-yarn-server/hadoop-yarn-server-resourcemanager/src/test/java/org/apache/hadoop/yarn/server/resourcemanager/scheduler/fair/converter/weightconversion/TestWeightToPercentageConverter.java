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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestWeightToPercentageConverter
    extends WeightConverterTestBase {
  private WeightToPercentConverter converter;
  private CapacitySchedulerConfiguration csConfig;
  public static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  public static final QueuePath ROOT_A = new QueuePath("root", "a");
  public static final QueuePath ROOT_B = new QueuePath("root", "b");
  public static final QueuePath ROOT_C = new QueuePath("root", "c");

  @BeforeEach
  public void setup() {
    converter = new WeightToPercentConverter();
    csConfig = new CapacitySchedulerConfiguration(
        new Configuration(false));
  }

  @Test
  public void testSingleWeightConversion() {
    FSQueue root = createFSQueues(1);
    converter.convertWeightsForChildQueues(root, csConfig);

    assertFalse(csConfig.getAllowZeroCapacitySum(ROOT),
        "Capacity zerosum allowed");
    assertEquals(100.000f, csConfig.getNonLabeledQueueCapacity(new QueuePath("root.a")), 0.0f,
        "root.a capacity");
  }

  @Test
  public void testNoChildQueueConversion() {
    FSQueue root = createFSQueues();
    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(20, csConfig.getPropsWithPrefix(PREFIX).size(), "Converted items");
  }

  @Test
  public void testMultiWeightConversion() {
    FSQueue root = createFSQueues(1, 2, 3);

    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(23, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
    // this is no fixing - it's the result of BigDecimal rounding
    assertEquals(16.667f,
        csConfig.getNonLabeledQueueCapacity(ROOT_A), 0.0f, "root.a capacity");
    assertEquals(33.333f,
        csConfig.getNonLabeledQueueCapacity(ROOT_B), 0.0f, "root.b capacity");
    assertEquals(50.000f,
        csConfig.getNonLabeledQueueCapacity(ROOT_C), 0.0f, "root.c capacity");
  }

  @Test
  public void testMultiWeightConversionWhenOfThemIsZero() {
    FSQueue root = createFSQueues(0, 1, 1);

    converter.convertWeightsForChildQueues(root, csConfig);

    assertFalse(csConfig.getAllowZeroCapacitySum(ROOT), "Capacity zerosum allowed");
    assertEquals(23, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(ROOT_A), 0.0f,
        "root.a capacity");
    assertEquals(50.000f, csConfig.getNonLabeledQueueCapacity(ROOT_B), 0.0f,
        "root.b capacity");
    assertEquals(50.000f, csConfig.getNonLabeledQueueCapacity(ROOT_C), 0.0f,
        "root.c capacity");
  }

  @Test
  public void testMultiWeightConversionWhenAllOfThemAreZero() {
    FSQueue root = createFSQueues(0, 0, 0);

    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(24, csConfig.getPropsWithPrefix(PREFIX).size(), "Number of properties");
    assertTrue(csConfig.getAllowZeroCapacitySum(ROOT), "Capacity zerosum allowed");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(ROOT_A), 0.0f,
        "root.a capacity");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(ROOT_B), 0.0f,
        "root.b capacity");
    assertEquals(0.000f, csConfig.getNonLabeledQueueCapacity(ROOT_C), 0.0f,
        "root.c capacity");
  }

  @Test
  public void testCapacityFixingWithThreeQueues() {
    FSQueue root = createFSQueues(1, 1, 1);

    converter.convertWeightsForChildQueues(root, csConfig);

    assertEquals(23, csConfig.getPropsWithPrefix(PREFIX).size(),
        "Number of properties");
    assertEquals(33.334f, csConfig.getNonLabeledQueueCapacity(ROOT_A), 0.0f,
        "root.a capacity");
    assertEquals(33.333f, csConfig.getNonLabeledQueueCapacity(ROOT_B), 0.0f,
        "root.b capacity");
    assertEquals(33.333f, csConfig.getNonLabeledQueueCapacity(ROOT_C), 0.0f,
        "root.c capacity");
  }

  @Test
  public void testCapacityFixingWhenTotalCapacityIsGreaterThanHundred() {
    Map<String, BigDecimal> capacities = new HashMap<>();
    capacities.put("root.a", new BigDecimal("50.001"));
    capacities.put("root.b", new BigDecimal("25.500"));
    capacities.put("root.c", new BigDecimal("25.500"));

    testCapacityFixing(capacities, new BigDecimal("100.001"));
  }

  @Test
  public void testCapacityFixWhenTotalCapacityIsLessThanHundred() {
    Map<String, BigDecimal> capacities = new HashMap<>();
    capacities.put("root.a", new BigDecimal("49.999"));
    capacities.put("root.b", new BigDecimal("25.500"));
    capacities.put("root.c", new BigDecimal("25.500"));

    testCapacityFixing(capacities, new BigDecimal("99.999"));
  }

  private void testCapacityFixing(Map<String, BigDecimal> capacities,
      BigDecimal total) {
    // Note: we call fixCapacities() directly because it makes
    // testing easier
    boolean needCapacityValidationRelax =
        converter.fixCapacities(capacities,
            total);

    assertFalse(needCapacityValidationRelax, "Capacity zerosum allowed");
    assertEquals(new BigDecimal("50.000"), capacities.get("root.a"), "root.a capacity");
    assertEquals(new BigDecimal("25.500"), capacities.get("root.b"), "root.b capacity");
    assertEquals(new BigDecimal("25.500"), capacities.get("root.c"), "root.c capacity");
  }
}
