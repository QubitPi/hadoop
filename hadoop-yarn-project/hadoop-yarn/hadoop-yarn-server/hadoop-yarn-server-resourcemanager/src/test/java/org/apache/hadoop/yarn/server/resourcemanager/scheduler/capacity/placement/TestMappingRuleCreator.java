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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleResult;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleResultType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMappingRuleCreator {
  private static final String MATCH_ALL = "*";

  private static final String DEFAULT_QUEUE = "root.default";
  private static final String SECONDARY_GROUP = "users";
  private static final String PRIMARY_GROUP = "superuser";
  private static final String APPLICATION_NAME = "testapplication";
  private static final String SPECIFIED_QUEUE = "root.users.hadoop";
  private static final String USER_NAME = "testuser";

  private MappingRuleCreator ruleCreator;

  private VariableContext variableContext;
  private MappingRulesDescription description;
  private Rule rule;

  @BeforeEach
  public void setup() {
    ruleCreator = new MappingRuleCreator();
    prepareMappingRuleDescription();
    variableContext = new VariableContext();

    variableContext.put("%user", USER_NAME);
    variableContext.put("%specified", SPECIFIED_QUEUE);
    variableContext.put("%application", APPLICATION_NAME);
    variableContext.put("%primary_group", PRIMARY_GROUP);
    variableContext.put("%secondary_group", SECONDARY_GROUP);
    variableContext.put("%default", DEFAULT_QUEUE);
    variableContext.putExtraDataset("groups",
        Sets.newLinkedHashSet(PRIMARY_GROUP, SECONDARY_GROUP));
  }

  @Test
  public void testAllUserMatcher() {
    variableContext.put("%user", USER_NAME);
    verifyPlacementSucceeds(USER_NAME);

    variableContext.put("%user", "dummyuser");
    verifyPlacementSucceeds("dummyuser");
  }

  @Test
  public void testSpecificUserMatcherPasses() {
    rule.setMatches(USER_NAME);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  public void testSpecificUserMatcherFails() {
    rule.setMatches(USER_NAME);
    variableContext.put("%user", "dummyuser");

    verifyNoPlacementOccurs();
  }

  @Test
  public void testSpecificGroupMatcher() {
    rule.setMatches(PRIMARY_GROUP);
    rule.setType(Type.GROUP);

    verifyPlacementSucceeds();
  }

  @Test
  public void testAllGroupMatcherFailsDueToMatchString() {


    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, () -> {
          rule.setType(Type.GROUP);
          // fails because "*" is not applicable to group type
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().
        contains("Cannot match '*' for groups"));

  }

  @Test
  public void testApplicationNameMatcherPasses() {
    rule.setType(Type.APPLICATION);
    rule.setMatches(APPLICATION_NAME);

    verifyPlacementSucceeds();
  }

  @Test
  public void testApplicationNameMatcherFails() {
    rule.setType(Type.APPLICATION);
    rule.setMatches("dummyApplication");

    verifyNoPlacementOccurs();
  }

  @Test
  public void testDefaultRule() {
    rule.setPolicy(Policy.DEFAULT_QUEUE);

    verifyPlacementSucceeds(DEFAULT_QUEUE, false);
  }

  @Test
  public void testSpecifiedRule() {
    rule.setPolicy(Policy.SPECIFIED);

    verifyPlacementSucceeds(SPECIFIED_QUEUE);
  }

  @Test
  public void testSpecifiedRuleWithNoCreate() {
    rule.setPolicy(Policy.SPECIFIED);
    rule.setCreate(false);

    verifyPlacementSucceeds(SPECIFIED_QUEUE, false);
  }

  @Test
  public void testRejectRule() {
    rule.setPolicy(Policy.REJECT);

    verifyPlacementRejected();
  }

  @Test
  public void testSetDefaultRule() {
    rule.setPolicy(Policy.SET_DEFAULT_QUEUE);
    rule.setValue("root.users.default");

    verifyNoPlacementOccurs();
    assertEquals("root.users.default",
        variableContext.get("%default"), "Default queue");
  }

  @Test
  public void testSetDefaultRuleWithMissingQueue() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, () -> {
          rule.setPolicy(Policy.SET_DEFAULT_QUEUE);
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("default queue is undefined"));
  }

  @Test
  public void testPrimaryGroupRule() {
    rule.setPolicy(Policy.PRIMARY_GROUP);

    verifyPlacementSucceeds(PRIMARY_GROUP);
  }

  @Test
  public void testPrimaryGroupRuleWithNoCreate() {
    rule.setPolicy(Policy.PRIMARY_GROUP);
    rule.setCreate(false);

    verifyPlacementSucceeds(PRIMARY_GROUP, false);
  }

  @Test
  public void testPrimaryGroupRuleWithParent() {
    rule.setPolicy(Policy.PRIMARY_GROUP);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root." + PRIMARY_GROUP);
  }

  @Test
  public void testSecondaryGroupRule() {
    rule.setPolicy(Policy.SECONDARY_GROUP);

    verifyPlacementSucceeds(SECONDARY_GROUP);
  }

  @Test
  public void testSecondaryGroupRuleWithNoCreate() {
    rule.setPolicy(Policy.SECONDARY_GROUP);
    rule.setCreate(false);

    verifyPlacementSucceeds(SECONDARY_GROUP, false);
  }

  @Test
  public void testSecondaryGroupRuleWithParent() {
    rule.setPolicy(Policy.SECONDARY_GROUP);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root." + SECONDARY_GROUP);
  }

  @Test
  public void testUserRule() {
    rule.setPolicy(Policy.USER);

    verifyPlacementSucceeds(USER_NAME);
  }

  @Test
  public void testUserRuleWithParent() {
    rule.setPolicy(Policy.USER);
    rule.setParentQueue("root.users");

    verifyPlacementSucceeds("root.users." + USER_NAME);
  }

  @Test
  public void testCustomRule() {
    rule.setPolicy(Policy.CUSTOM);
    rule.setCustomPlacement("root.%primary_group.%secondary_group");

    verifyPlacementSucceeds(
        String.format("root.%s.%s", PRIMARY_GROUP, SECONDARY_GROUP));
  }

  @Test
  public void testCustomRuleWithNoCreate() {
    rule.setPolicy(Policy.CUSTOM);
    rule.setCustomPlacement("root.%primary_group.%secondary_group");
    rule.setCreate(false);

    verifyPlacementSucceeds(
        String.format("root.%s.%s", PRIMARY_GROUP, SECONDARY_GROUP), false);
  }

  @Test
  public void testCustomRuleWithMissingQueue() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, () -> {
          rule.setPolicy(Policy.CUSTOM);
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("custom queue is undefined"));

  }

  @Test
  public void testPrimaryGroupUserRule() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);

    verifyPlacementSucceeds("superuser.testuser");
  }

  @Test
  public void testPrimaryGroupUserRuleWithNoCreate() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);
    rule.setCreate(false);

    verifyPlacementSucceeds("superuser.testuser", false);
  }

  @Test
  public void testPrimaryGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.PRIMARY_GROUP_USER);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root.superuser.testuser");
  }

  @Test
  public void testSecondaryGroupNestedRule() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);

    verifyPlacementSucceeds("users.testuser");
  }

  @Test
  public void testSecondaryGroupNestedRuleWithNoCreate() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);
    rule.setCreate(false);

    verifyPlacementSucceeds("users.testuser", false);
  }

  @Test
  public void testSecondaryGroupNestedRuleWithParent() {
    rule.setPolicy(Policy.SECONDARY_GROUP_USER);
    rule.setParentQueue("root");

    verifyPlacementSucceeds("root.users.testuser");
  }

  @Test
  public void testApplicationNamePlacement() {
    rule.setPolicy(Policy.APPLICATION_NAME);

    verifyPlacementSucceeds(APPLICATION_NAME);
  }

  @Test
  public void testApplicationNamePlacementWithParent() {
    rule.setPolicy(Policy.APPLICATION_NAME);
    rule.setParentQueue("root.applications");

    verifyPlacementSucceeds("root.applications." + APPLICATION_NAME);
  }

  @Test
  public void testDefaultQueueFallback() {
    rule.setFallbackResult(FallbackResult.PLACE_DEFAULT);

    testFallback(MappingRuleResultType.PLACE_TO_DEFAULT);
  }

  @Test
  public void testRejectFallback() {
    rule.setFallbackResult(FallbackResult.REJECT);

    testFallback(MappingRuleResultType.REJECT);
  }

  @Test
  public void testSkipFallback() {
    rule.setFallbackResult(FallbackResult.SKIP);

    testFallback(MappingRuleResultType.SKIP);
  }

  private void testFallback(MappingRuleResultType expectedType) {
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);

    assertEquals(expectedType, mpr.getFallback().getResult(),
        "Fallback result");
  }

  @Test
  public void testFallbackResultUnset() {
    rule.setFallbackResult(null);
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    MappingRule mpr = rules.get(0);
    assertEquals(MappingRuleResultType.SKIP,
        mpr.getFallback().getResult(), "Fallback result");
  }

  @Test
  public void testTypeUnset() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, () -> {
          rule.setType(null);
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("Rule type is undefined"));
  }

  @Test
  public void testMatchesUnset() {

    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, ()->{
          rule.setMatches(null);
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("Match string is undefined"));

  }

  @Test
  public void testMatchesEmpty() {

    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, ()->{
          rule.setMatches("");
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("Match string is empty"));
  }

  @Test
  public void testPolicyUnset() {
    rule.setPolicy(null);

    IllegalArgumentException illegalArgumentException =
        assertThrows(IllegalArgumentException.class, ()->{
          rule.setPolicy(null);
          ruleCreator.getMappingRules(description);
        });

    assertTrue(illegalArgumentException.getMessage().contains("Rule policy is undefined"));
  }

  private void prepareMappingRuleDescription() {
    description = new MappingRulesDescription();

    rule = new Rule();
    rule.setType(Type.USER);
    rule.setFallbackResult(FallbackResult.SKIP);
    rule.setPolicy(Policy.USER);
    rule.setMatches(MATCH_ALL);

    List<Rule> rules = new ArrayList<>();
    rules.add(rule);

    description.setRules(rules);
  }

  private void verifyPlacementSucceeds() {
    verifyPlacement(MappingRuleResultType.PLACE, null, true);
  }

  private void verifyPlacementSucceeds(String expectedQueue) {
    verifyPlacement(MappingRuleResultType.PLACE, expectedQueue, true);
  }

  private void verifyPlacementSucceeds(String expectedQueue,
      boolean allowCreate) {
    verifyPlacement(MappingRuleResultType.PLACE, expectedQueue,
        allowCreate);
  }

  private void verifyPlacementRejected() {
    verifyPlacement(MappingRuleResultType.REJECT, null, true);
  }

  private void verifyNoPlacementOccurs() {
    verifyPlacement(null, null, true);
  }

  private void verifyPlacement(MappingRuleResultType expectedResultType,
      String expectedQueue, boolean allowCreate) {
    List<MappingRule> rules = ruleCreator.getMappingRules(description);
    assertEquals(1, rules.size(), "Number of rules");
    MappingRule mpr = rules.get(0);
    MappingRuleResult result = mpr.evaluate(variableContext);

    assertEquals(allowCreate, result.isCreateAllowed(), "Create flag");

    if (expectedResultType != null) {
      assertEquals(expectedResultType, result.getResult(),
          "Mapping rule result");
    } else {
      assertEquals(MappingRuleResultType.SKIP, result.getResult(),
          "Mapping rule result");
    }

    if (expectedQueue != null) {
      assertEquals(expectedQueue, result.getQueue(), "Evaluated queue");
    }
  }
}
