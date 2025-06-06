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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test helper class is primarily used by
 * {@link TestRMWebServicesFairSchedulerCustomResourceTypes}.
 */
public class FairSchedulerJsonVerifications {

  private static final Set<String> RESOURCE_FIELDS =
      Sets.newHashSet("minResources", "amUsedResources", "amMaxResources",
          "fairResources", "clusterResources", "reservedResources",
              "maxResources", "usedResources", "steadyFairResources",
              "demandResources");
  private final Set<String> customResourceTypes;

  FairSchedulerJsonVerifications(List<String> customResourceTypes) {
    this.customResourceTypes = Sets.newHashSet(customResourceTypes);
  }

  public void verify(JSONObject jsonObject) {
    try {
      verifyResourcesContainDefaultResourceTypes(jsonObject, RESOURCE_FIELDS);
      verifyResourcesContainCustomResourceTypes(jsonObject, RESOURCE_FIELDS);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyResourcesContainDefaultResourceTypes(JSONObject queue,
      Set<String> resourceCategories) throws JSONException {
    for (String resourceCategory : resourceCategories) {
      boolean hasResourceCategory = queue.has(resourceCategory);
      assertTrue(hasResourceCategory, "Queue " + queue + " does not have resource category key: "
          + resourceCategory);
      verifyResourceContainsDefaultResourceTypes(
          queue.getJSONObject(resourceCategory));
    }
  }

  private void verifyResourceContainsDefaultResourceTypes(
      JSONObject jsonObject) {
    Object memory = jsonObject.opt("memory");
    Object vCores = jsonObject.opt("vCores");

    assertNotNull(memory, "Key 'memory' not found in: " + jsonObject);
    assertNotNull(vCores, "Key 'vCores' not found in: " + jsonObject);
  }

  private void verifyResourcesContainCustomResourceTypes(JSONObject queue,
      Set<String> resourceCategories) throws JSONException {
    for (String resourceCategory : resourceCategories) {
      assertTrue(queue.has(resourceCategory),
          "Queue " + queue + " does not have resource category key: "
          + resourceCategory);
      verifyResourceContainsAllCustomResourceTypes(
          queue.getJSONObject(resourceCategory));
    }
  }

  private void verifyResourceContainsAllCustomResourceTypes(
      JSONObject resourceCategory) throws JSONException {
    assertTrue(resourceCategory.has("resourceInformations"),
        "resourceCategory does not have resourceInformations: "
        + resourceCategory);

    JSONObject resourceInformations =
        resourceCategory.getJSONObject("resourceInformations");
    assertTrue(resourceInformations.has("resourceInformation"),
        "resourceInformations does not have resourceInformation object: "
        + resourceInformations);
    JSONArray customResources =
        resourceInformations.getJSONArray("resourceInformation");

    // customResources will include vcores / memory as well
    assertEquals(customResourceTypes.size(), customResources.length() - 2,
       "Different number of custom resource types found than expected");

    for (int i = 0; i < customResources.length(); i++) {
      JSONObject customResource = customResources.getJSONObject(i);
      assertTrue(customResource.has("name"),
          "Resource type does not have name field: " + customResource);
      assertTrue(customResource.has("resourceType"),
          "Resource type does not have name resourceType field: "
          + customResource);
      assertTrue(customResource.has("units"),
          "Resource type does not have name units field: " + customResource);
      assertTrue(customResource.has("value"),
          "Resource type does not have name value field: " + customResource);

      String name = customResource.getString("name");
      String unit = customResource.getString("units");
      String resourceType = customResource.getString("resourceType");
      Long value = customResource.getLong("value");

      if (ResourceInformation.MEMORY_URI.equals(name)
          || ResourceInformation.VCORES_URI.equals(name)) {
        continue;
      }

      assertTrue(customResourceTypes.contains(name),
          "Custom resource type " + name + " not found");
      assertEquals("k", unit);
      assertEquals(ResourceTypes.COUNTABLE,
          ResourceTypes.valueOf(resourceType));
      assertNotNull(value, "Custom resource value " + value + " is null!");
    }
  }
}
