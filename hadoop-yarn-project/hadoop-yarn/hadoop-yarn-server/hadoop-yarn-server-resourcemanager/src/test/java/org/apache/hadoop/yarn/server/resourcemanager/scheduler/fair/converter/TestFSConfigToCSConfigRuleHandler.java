/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.DYNAMIC_MAX_ASSIGN;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CAPACITY_PERCENTAGE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MIN_RESOURCES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CHILD_CAPACITY;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.MAX_CHILD_QUEUE_LIMIT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.QUEUE_AUTO_CREATE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.RESERVATION_SYSTEM;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigToCSConfigRuleHandler.FAIR_AS_DRF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FSConfigToCSConfigRuleHandler.
 *
 */
public class TestFSConfigToCSConfigRuleHandler {
  private static final String ABORT = "abort";
  private static final String WARNING = "warning";

  private FSConfigToCSConfigRuleHandler ruleHandler;
  private DryRunResultHolder dryRunResultHolder;

  @BeforeEach
  public void setup() {
    dryRunResultHolder = new DryRunResultHolder();
  }


  private ConversionOptions createDryRunConversionOptions() {
    return new ConversionOptions(dryRunResultHolder, true);
  }

  private ConversionOptions createDefaultConversionOptions() {
    return new ConversionOptions(dryRunResultHolder, false);
  }

  @Test
  public void testInitPropertyActionsToWarning() throws IOException {
    ruleHandler = new FSConfigToCSConfigRuleHandler(new Properties(),
        createDefaultConversionOptions());

    ruleHandler.handleChildQueueCount("test", 1);
    ruleHandler.handleDynamicMaxAssign();
    ruleHandler.handleMaxCapacityPercentage("test");
    ruleHandler.handleMaxChildCapacity();
    ruleHandler.handleMinResources();
    ruleHandler.handleMaxResources();
    ruleHandler.handleReservationSystem();
  }

  @Test
  public void testAllRulesWarning() throws IOException {
    Properties rules = new Properties();
    rules.put(DYNAMIC_MAX_ASSIGN, WARNING);
    rules.put(MAX_CAPACITY_PERCENTAGE, WARNING);
    rules.put(MAX_RESOURCES, WARNING);
    rules.put(MIN_RESOURCES, WARNING);
    rules.put(MAX_CHILD_CAPACITY, WARNING);
    rules.put(QUEUE_AUTO_CREATE, WARNING);
    rules.put(RESERVATION_SYSTEM, WARNING);
    rules.put(FAIR_AS_DRF, WARNING);

    ruleHandler = new FSConfigToCSConfigRuleHandler(rules,
        createDefaultConversionOptions());

    ruleHandler.handleDynamicMaxAssign();
    ruleHandler.handleMaxCapacityPercentage("test");
    ruleHandler.handleMaxChildCapacity();
    ruleHandler.handleMinResources();
    ruleHandler.handleMaxResources();
    ruleHandler.handleReservationSystem();
  }

  @Test
  public void testAllRulesAbort() throws IOException {
    Properties rules = new Properties();
    rules.put(DYNAMIC_MAX_ASSIGN, ABORT);
    rules.put(MAX_CAPACITY_PERCENTAGE, ABORT);
    rules.put(MAX_CHILD_CAPACITY, ABORT);
    rules.put(MAX_RESOURCES, ABORT);
    rules.put(MIN_RESOURCES, ABORT);
    rules.put(QUEUE_AUTO_CREATE, ABORT);
    rules.put(RESERVATION_SYSTEM, ABORT);
    rules.put(FAIR_AS_DRF, ABORT);
    rules.put(MAX_CHILD_QUEUE_LIMIT, "1");

    ruleHandler = new FSConfigToCSConfigRuleHandler(rules,
        createDefaultConversionOptions());

    expectAbort(() -> ruleHandler.handleChildQueueCount("test", 2),
        ConversionException.class);
    expectAbort(() -> ruleHandler.handleDynamicMaxAssign());
    expectAbort(() -> ruleHandler.handleMaxCapacityPercentage("test"));
    expectAbort(() -> ruleHandler.handleMaxChildCapacity());
    expectAbort(() -> ruleHandler.handleMaxResources());
    expectAbort(() -> ruleHandler.handleMinResources());
    expectAbort(() -> ruleHandler.handleReservationSystem());
    expectAbort(() -> ruleHandler.handleFairAsDrf("test"));
  }

  @Test
  public void testMaxChildQueueCountNotInteger() throws IOException {
    assertThrows(ConversionException.class, () -> {
      Properties rules = new Properties();
      rules.put(MAX_CHILD_QUEUE_LIMIT, "abc");

      ruleHandler = new FSConfigToCSConfigRuleHandler(rules,
          createDefaultConversionOptions());

      ruleHandler.handleChildQueueCount("test", 1);
    });
  }

  @Test
  public void testDryRunWarning() {
    Properties rules = new Properties();

    ruleHandler = new FSConfigToCSConfigRuleHandler(rules,
        createDryRunConversionOptions());

    ruleHandler.handleDynamicMaxAssign();
    ruleHandler.handleMaxChildCapacity();

    assertEquals(2, dryRunResultHolder.getWarnings().size(),
        "Number of warnings");
    assertEquals(0, dryRunResultHolder.getErrors().size(),
        "Number of errors");
  }

  @Test
  public void testDryRunError() {
    Properties rules = new Properties();
    rules.put(DYNAMIC_MAX_ASSIGN, ABORT);
    rules.put(MAX_CHILD_CAPACITY, ABORT);

    ruleHandler = new FSConfigToCSConfigRuleHandler(rules,
        createDryRunConversionOptions());

    ruleHandler.handleDynamicMaxAssign();
    ruleHandler.handleMaxChildCapacity();

    assertEquals(0, dryRunResultHolder.getWarnings().size(),
        "Number of warnings");
    assertEquals(2, dryRunResultHolder.getErrors().size(),
        "Number of errors");
  }

  private void expectAbort(VoidCall call) {
    expectAbort(call, UnsupportedPropertyException.class);
  }

  private void expectAbort(VoidCall call, Class<?> exceptionClass) {
    boolean exceptionThrown = false;
    Throwable thrown = null;

    try {
      call.apply();
    } catch (Throwable t) {
      thrown = t;
      exceptionThrown = true;
    }

    assertTrue(exceptionThrown, "Exception was not thrown");
    assertEquals(exceptionClass, thrown.getClass(), "Unexpected exception");
  }

  @FunctionalInterface
  private interface VoidCall {
    void apply();
  }
}
