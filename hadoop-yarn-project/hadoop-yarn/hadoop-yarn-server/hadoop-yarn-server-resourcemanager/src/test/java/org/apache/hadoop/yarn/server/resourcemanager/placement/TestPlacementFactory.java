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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for the {@link PlacementFactory}.
 */
public class TestPlacementFactory {

  /**
   * Check that non existing class throws exception.
   *
   * @throws ClassNotFoundException
   */
  @Test
  public void testGetNonExistRuleText() throws ClassNotFoundException {
    assertThrows(ClassNotFoundException.class, ()->{
      final String nonExist = "my.placement.Rule";
      PlacementFactory.getPlacementRule(nonExist, null);
    });
  }

  /**
   * Check existing class using the class name.
   * Relies on the {@link DefaultPlacementRule} of the FS.
   */
  @Test
  public void testGetExistRuleText() {
    final String exists = DefaultPlacementRule.class.getCanonicalName();
    PlacementRule rule = null;
    try {
      rule = PlacementFactory.getPlacementRule(exists, null);
    } catch (ClassNotFoundException cnfe) {
      fail("Class should have been found");
    }
    assertNotNull(rule, "Rule object is null");
    assertEquals(rule.getName(), exists, "Names not equal");
  }

  /**
   * Existing class using the class reference.
   * Relies on the {@link DefaultPlacementRule} of the FS.
   */
  @Test
  public void testGetRuleClass() {
    PlacementRule rule = PlacementFactory.getPlacementRule(
        DefaultPlacementRule.class, null);
    assertNotNull(rule, "Rule object is null");
    // Should take anything as the second object: ignores unknown types in the
    // default implementation.
    rule = PlacementFactory.getPlacementRule(
        DefaultPlacementRule.class, "");
    assertNotNull(rule, "Rule object is null");
  }
}
