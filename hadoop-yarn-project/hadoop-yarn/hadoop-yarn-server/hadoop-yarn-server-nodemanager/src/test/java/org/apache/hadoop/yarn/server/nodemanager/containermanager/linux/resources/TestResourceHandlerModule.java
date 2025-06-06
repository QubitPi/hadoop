/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.mockito.Mockito.mock;

public class TestResourceHandlerModule {
  private static final Logger LOG =
       LoggerFactory.getLogger(TestResourceHandlerModule.class);
  private Configuration emptyConf;
  private Configuration networkEnabledConf;

  @BeforeEach
  public void setup() throws Exception {
    emptyConf = new YarnConfiguration();
    networkEnabledConf = new YarnConfiguration();

    networkEnabledConf.setBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        true);
    ResourceHandlerModule.nullifyResourceHandlerChain();
  }

  @Test
  public void testOutboundBandwidthHandler() {
    try {
      //This resourceHandler should be non-null only if network as a resource
      //is explicitly enabled
      OutboundBandwidthResourceHandler resourceHandler = ResourceHandlerModule
          .initOutboundBandwidthResourceHandler(emptyConf);
      assertNull(resourceHandler);

      //When network as a resource is enabled this should be non-null
      resourceHandler = ResourceHandlerModule
          .initOutboundBandwidthResourceHandler(networkEnabledConf);
      assertNotNull(resourceHandler);

      //Ensure that outbound bandwidth resource handler is present in the chain
      ResourceHandlerChain resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(networkEnabledConf,
              mock(Context.class));
      if (resourceHandlerChain != null) {
        List<ResourceHandler> resourceHandlers = resourceHandlerChain
            .getResourceHandlerList();
        //Exactly one resource handler in chain
        assertThat(resourceHandlers).hasSize(1);
        //Same instance is expected to be in the chain.
        assertTrue(resourceHandlers.get(0) == resourceHandler);
      } else {
        fail("Null returned");
      }
    } catch (ResourceHandlerException e) {
      fail("Unexpected ResourceHandlerException: " + e);
    }
  }

  @Test
  public void testDiskResourceHandler() throws Exception {

    DiskResourceHandler handler =
        ResourceHandlerModule.initDiskResourceHandler(emptyConf);
    assertNull(handler);

    Configuration diskConf = new YarnConfiguration();
    diskConf.setBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED, true);

    handler = ResourceHandlerModule.initDiskResourceHandler(diskConf);
    assertNotNull(handler);

    ResourceHandlerChain resourceHandlerChain =
        ResourceHandlerModule.getConfiguredResourceHandlerChain(diskConf,
            mock(Context.class));
    if (resourceHandlerChain != null) {
      List<ResourceHandler> resourceHandlers =
          resourceHandlerChain.getResourceHandlerList();
      // Exactly one resource handler in chain
      assertThat(resourceHandlers).hasSize(1);
      // Same instance is expected to be in the chain.
      assertTrue(resourceHandlers.get(0) == handler);
    } else {
      fail("Null returned");
    }
  }
}