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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQueueCapacityVector {
  private static final String CUSTOM_RESOURCE = "custom";
  public static final String MIXED_CAPACITY_VECTOR_STRING =
      "[custom=3.0,memory-mb=10.0w,vcores=6.0%]";

  private final YarnConfiguration conf = new YarnConfiguration();

  @BeforeEach
  public void setUp() {
    conf.set(YarnConfiguration.RESOURCE_TYPES, CUSTOM_RESOURCE);
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void getResourceNamesByCapacityType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, ResourceUnitCapacityType.PERCENTAGE);
    capacityVector.setResource(VCORES_URI, 6, ResourceUnitCapacityType.PERCENTAGE);

    // custom is not set, defaults to 0
    assertEquals(1, capacityVector.getResourceNamesByCapacityType(
        ResourceUnitCapacityType.ABSOLUTE).size());
    assertTrue(capacityVector.getResourceNamesByCapacityType(
        ResourceUnitCapacityType.ABSOLUTE).contains(CUSTOM_RESOURCE));

    assertEquals(2, capacityVector.getResourceNamesByCapacityType(
        ResourceUnitCapacityType.PERCENTAGE).size());
    assertTrue(capacityVector.getResourceNamesByCapacityType(
        ResourceUnitCapacityType.PERCENTAGE).contains(VCORES_URI));
    assertTrue(capacityVector.getResourceNamesByCapacityType(
        ResourceUnitCapacityType.PERCENTAGE).contains(MEMORY_URI));
    assertEquals(10, capacityVector.getResource(MEMORY_URI).getResourceValue(), EPSILON);
    assertEquals(6, capacityVector.getResource(VCORES_URI).getResourceValue(), EPSILON);
  }

  @Test
  public void isResourceOfType() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, ResourceUnitCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, ResourceUnitCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, ResourceUnitCapacityType.ABSOLUTE);

    assertTrue(capacityVector.isResourceOfType(MEMORY_URI, ResourceUnitCapacityType.WEIGHT));
    assertTrue(capacityVector.isResourceOfType(VCORES_URI,
        ResourceUnitCapacityType.PERCENTAGE));
    assertTrue(capacityVector.isResourceOfType(CUSTOM_RESOURCE,
        ResourceUnitCapacityType.ABSOLUTE));
  }

  @Test
  public void testIterator() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();
    List<QueueCapacityVectorEntry> entries = Lists.newArrayList(capacityVector);

    assertEquals(3, entries.size());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    List<QueueCapacityVectorEntry> emptyEntries = Lists.newArrayList(emptyCapacityVector);

    assertEquals(0, emptyEntries.size());
  }

  @Test
  public void testToString() {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    capacityVector.setResource(MEMORY_URI, 10, ResourceUnitCapacityType.WEIGHT);
    capacityVector.setResource(VCORES_URI, 6, ResourceUnitCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, ResourceUnitCapacityType.ABSOLUTE);

    assertEquals(MIXED_CAPACITY_VECTOR_STRING, capacityVector.toString());

    QueueCapacityVector emptyCapacityVector = new QueueCapacityVector();
    assertEquals("[]", emptyCapacityVector.toString());
  }

  @Test
  public void testIsMixedType() {
    // Starting from ABSOLUTE mode
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();
    assertFalse(capacityVector.isMixedCapacityVector());

    capacityVector.setResource(VCORES_URI, 6, ResourceUnitCapacityType.PERCENTAGE);
    capacityVector.setResource(MEMORY_URI, 10, ResourceUnitCapacityType.PERCENTAGE);
    capacityVector.setResource(CUSTOM_RESOURCE, 3, ResourceUnitCapacityType.PERCENTAGE);
    assertFalse(capacityVector.isMixedCapacityVector());

    capacityVector.setResource(VCORES_URI, 6, ResourceUnitCapacityType.WEIGHT);
    assertTrue(capacityVector.isMixedCapacityVector());
  }
}