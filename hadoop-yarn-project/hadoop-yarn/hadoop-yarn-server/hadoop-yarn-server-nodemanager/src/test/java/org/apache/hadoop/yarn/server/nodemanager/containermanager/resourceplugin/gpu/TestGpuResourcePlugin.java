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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuUtilizations;
import org.junit.jupiter.api.Test;
import java.util.List;

public class TestGpuResourcePlugin {

  private GpuDiscoverer createMockDiscoverer() throws YarnException {
    GpuDiscoverer gpuDiscoverer = mock(GpuDiscoverer.class);
    when(gpuDiscoverer.isAutoDiscoveryEnabled()).thenReturn(true);

    PerGpuDeviceInformation gpu =
        new PerGpuDeviceInformation();
    gpu.setProductName("testGpu");
    List<PerGpuDeviceInformation> gpus = Lists.newArrayList();
    gpus.add(gpu);

    GpuDeviceInformation gpuDeviceInfo = new GpuDeviceInformation();
    gpuDeviceInfo.setGpus(gpus);
    when(gpuDiscoverer.getGpuDeviceInformation()).thenReturn(gpuDeviceInfo);
    return gpuDiscoverer;
  }

  @Test
  public void testResourceHandlerNotInitialized() throws YarnException {
    assertThrows(YarnException.class, () -> {
      GpuDiscoverer gpuDiscoverer = createMockDiscoverer();
      GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler =
          mock(GpuNodeResourceUpdateHandler.class);

      GpuResourcePlugin target =
          new GpuResourcePlugin(gpuNodeResourceUpdateHandler, gpuDiscoverer);

      target.getNMResourceInfo();
    });
  }

  @Test
  public void testResourceHandlerIsInitialized() throws YarnException {
    GpuDiscoverer gpuDiscoverer = createMockDiscoverer();
    GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler =
        mock(GpuNodeResourceUpdateHandler.class);

    GpuResourcePlugin target =
        new GpuResourcePlugin(gpuNodeResourceUpdateHandler, gpuDiscoverer);

    target.createResourceHandler(null, null, null);

    //Not throwing any exception
    target.getNMResourceInfo();
  }

  @Test
  public void testGetNMResourceInfoAutoDiscoveryEnabled()
      throws YarnException {
    GpuDiscoverer gpuDiscoverer = createMockDiscoverer();

    GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler =
        mock(GpuNodeResourceUpdateHandler.class);

    GpuResourcePlugin target =
        new GpuResourcePlugin(gpuNodeResourceUpdateHandler, gpuDiscoverer);

    target.createResourceHandler(null, null, null);

    NMGpuResourceInfo resourceInfo =
        (NMGpuResourceInfo) target.getNMResourceInfo();
    assertNotNull(
        resourceInfo.getGpuDeviceInformation(), "GpuDeviceInformation should not be null");

    List<PerGpuDeviceInformation> gpus =
        resourceInfo.getGpuDeviceInformation().getGpus();
    assertNotNull(gpus, "List of PerGpuDeviceInformation should not be null");

    assertEquals(1, gpus.size(), "List of PerGpuDeviceInformation should have a " +
        "size of 1");
    assertEquals("testGpu", gpus.get(0).getProductName(),
        "Product name of GPU does not match");
  }

  @Test
  public void testGetNMResourceInfoAutoDiscoveryDisabled()
      throws YarnException {
    GpuDiscoverer gpuDiscoverer = createMockDiscoverer();
    when(gpuDiscoverer.isAutoDiscoveryEnabled()).thenReturn(false);

    GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler =
        mock(GpuNodeResourceUpdateHandler.class);

    GpuResourcePlugin target =
        new GpuResourcePlugin(gpuNodeResourceUpdateHandler, gpuDiscoverer);

    target.createResourceHandler(null, null, null);

    NMGpuResourceInfo resourceInfo =
        (NMGpuResourceInfo) target.getNMResourceInfo();
    assertNull(resourceInfo.getGpuDeviceInformation());
  }

  @Test
  public void testAvgNodeGpuUtilization()
      throws Exception {
    GpuDiscoverer gpuDiscoverer = createNodeGPUUtilizationDiscoverer();

    GpuNodeResourceUpdateHandler gpuNodeResourceUpdateHandler =
        new GpuNodeResourceUpdateHandler(gpuDiscoverer, new Configuration());

    assertEquals(0.5F,
        gpuNodeResourceUpdateHandler.getAvgNodeGpuUtilization(), 1e-6);
  }

  private GpuDiscoverer createNodeGPUUtilizationDiscoverer()
      throws YarnException {
    GpuDiscoverer gpuDiscoverer = mock(GpuDiscoverer.class);

    PerGpuDeviceInformation gpu1 =
        new PerGpuDeviceInformation();
    PerGpuUtilizations perGpuUtilizations1 =
        new PerGpuUtilizations();
    perGpuUtilizations1.setOverallGpuUtilization(0.4F);

    gpu1.setGpuUtilizations(perGpuUtilizations1);

    PerGpuDeviceInformation gpu2 =
        new PerGpuDeviceInformation();
    PerGpuUtilizations perGpuUtilizations2 =
        new PerGpuUtilizations();
    perGpuUtilizations2.setOverallGpuUtilization(0.6F);
    gpu2.setGpuUtilizations(perGpuUtilizations2);

    List<PerGpuDeviceInformation> gpus = Lists.newArrayList();
    gpus.add(gpu1);
    gpus.add(gpu2);

    GpuDeviceInformation gpuDeviceInfo = new GpuDeviceInformation();
    gpuDeviceInfo.setGpus(gpus);
    when(gpuDiscoverer.getGpuDeviceInformation()).thenReturn(gpuDeviceInfo);
    return gpuDiscoverer;
  }
}
