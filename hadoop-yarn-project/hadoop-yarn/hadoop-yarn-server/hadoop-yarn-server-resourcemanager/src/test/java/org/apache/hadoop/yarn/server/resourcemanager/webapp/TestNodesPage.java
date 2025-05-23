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
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.NodesPage.NodesBlock;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * This tests the NodesPage block table that it should contain the table body
 * data for all the columns in the table as specified in the header.
 */
public class TestNodesPage {
  
  final int numberOfRacks = 2;
  final int numberOfNodesPerRack = 8;
  // The following is because of the way TestRMWebApp.mockRMContext creates
  // nodes.
  final int numberOfLostNodesPerRack = 1;

  // Number of Actual Table Headers for NodesPage.NodesBlock might change in
  // future. In that case this value should be adjusted to the new value.
  private final int numberOfThInMetricsTable = 25;
  private final int numberOfActualTableHeaders = 18;
  private final int numberOfThForOpportunisticContainers = 4;

  private Injector injector;
  
  @BeforeEach
  public void setUp() throws Exception {
    setUpInternal(false);
  }

  private void setUpInternal(final boolean useDRC) throws Exception {
    final RMContext mockRMContext =
        TestRMWebApp.mockRMContext(3, numberOfRacks, numberOfNodesPerRack,
          8 * TestRMWebApp.GiB);
    injector =
        WebAppTests.createMockInjector(RMContext.class, mockRMContext,
          new Module() {
            @Override
            public void configure(Binder binder) {
              try {
                binder.bind(ResourceManager.class).toInstance(
                    TestRMWebApp.mockRm(mockRMContext, useDRC));
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            }
          });
  }

  @Test
  public void testNodesBlockRender() throws Exception {
    injector.getInstance(NodesBlock.class).render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer, times(numberOfActualTableHeaders + numberOfThInMetricsTable)).print("<th");
    verify(writer, times(numberOfThInMetricsTable)).print("<td");
  }
  
  @Test
  public void testNodesBlockRenderForLostNodes() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.state", "lost");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer,
        times(numberOfActualTableHeaders + numberOfThInMetricsTable))
        .print("<th");
    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
  }

  @Test
  public void testNodesBlockRenderForLostNodesWithGPUResources()
      throws Exception {
    Map<String, ResourceInformation> oldRtMap =
        ResourceUtils.getResourceTypes();
    CustomResourceTypesConfigurationProvider.
        initResourceTypes(ResourceInformation.GPU_URI);
    this.setUpInternal(true);
    try {
      // Test gpu as a custom resource.
      //<th class="yarn io/gpu">
      //  yarn.io/gpu Used
      //</th>
      //<th class="yarn io/gpu">
      //   yarn.io/gpu Avail
      //</th>
      this.testNodesBlockRenderForLostNodesWithGPU();
    } finally {
      ResourceUtils.initializeResourcesFromResourceInformationMap(oldRtMap);
    }
  }

  public void testNodesBlockRenderForLostNodesWithGPU() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.state", "lost");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer,
        times(numberOfActualTableHeaders
            + numberOfThInMetricsTable + 2))
        .print("<th");
    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
  }

  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithNonEmptyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "x");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);
    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
    verify(writer, times(1)).print("<script");
  }
  
  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithEmptyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
  }
  
  @Test
  public void testNodesBlockRenderForNodeLabelFilterWithAnyLabel() {
    NodesBlock nodesBlock = injector.getInstance(NodesBlock.class);
    nodesBlock.set("node.label", "*");
    nodesBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
  }

  @Test
  public void testNodesBlockRenderForOpportunisticContainers() {
    final RMContext mockRMContext =
        TestRMWebApp.mockRMContext(3, numberOfRacks, numberOfNodesPerRack,
            8 * TestRMWebApp.GiB);
    mockRMContext.getYarnConfiguration().setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    injector =
        WebAppTests.createMockInjector(RMContext.class, mockRMContext,
            new Module() {
              @Override
              public void configure(Binder binder) {
                try {
                  binder.bind(ResourceManager.class).toInstance(
                      TestRMWebApp.mockRm(mockRMContext));
                } catch (IOException e) {
                  throw new IllegalStateException(e);
                }
              }
            });
    injector.getInstance(NodesBlock.class).render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    verify(writer, times(
        numberOfActualTableHeaders + numberOfThInMetricsTable +
            numberOfThForOpportunisticContainers)).print("<th");
    verify(writer, times(numberOfThInMetricsTable))
        .print("<td");
  }
}
