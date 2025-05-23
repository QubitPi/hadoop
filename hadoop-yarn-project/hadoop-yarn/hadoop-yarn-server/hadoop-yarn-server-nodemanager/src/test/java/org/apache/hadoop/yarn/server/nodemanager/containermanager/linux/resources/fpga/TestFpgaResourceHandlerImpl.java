/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.thirdparty.com.google.common.io.FileWriteMode;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;

public class TestFpgaResourceHandlerImpl {

  private static final String HASHABLE_STRING = "abcdef";
  private static final String EXPECTED_HASH =
      "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721";

  private Context mockContext;
  private FpgaResourceHandlerImpl fpgaResourceHandler;
  private Configuration configuration;
  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;
  private NMStateStoreService mockNMStateStore;
  private ConcurrentHashMap<ContainerId, Container> runningContainersMap;
  private IntelFpgaOpenclPlugin mockVendorPlugin;
  private List<FpgaDevice> deviceList;
  private FpgaDiscoverer fpgaDiscoverer;
  private static final String vendorType = "IntelOpenCL";
  private File dummyAocx;

  private String getTestParentFolder() {
    File f = new File("target/temp/" +
        TestFpgaResourceHandlerImpl.class.getName());
    return f.getAbsolutePath();
  }

  @BeforeEach
  public void setup() throws IOException, YarnException {
    CustomResourceTypesConfigurationProvider.
        initResourceTypes(ResourceInformation.FPGA_URI);
    configuration = new YarnConfiguration();

    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
    mockNMStateStore = mock(NMStateStoreService.class);
    mockContext = mock(Context.class);
    // Assumed devices parsed from output
    deviceList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      deviceList.add(new FpgaDevice(vendorType, 247, i, "acl" + i));
    }
    String aocxPath = getTestParentFolder() + "/test.aocx";
    mockVendorPlugin = mockPlugin(vendorType, deviceList, aocxPath);
    fpgaDiscoverer = new FpgaDiscoverer();
    fpgaDiscoverer.setResourceHanderPlugin(mockVendorPlugin);
    fpgaDiscoverer.initialize(configuration);
    when(mockContext.getNMStateStore()).thenReturn(mockNMStateStore);
    runningContainersMap = new ConcurrentHashMap<>();
    when(mockContext.getContainers()).thenReturn(runningContainersMap);

    fpgaResourceHandler = new FpgaResourceHandlerImpl(mockContext,
        mockCGroupsHandler, mockPrivilegedExecutor, mockVendorPlugin,
        fpgaDiscoverer);

    dummyAocx = new File(aocxPath);
    Files.createParentDirs(dummyAocx);
    Files.touch(dummyAocx);
    Files.asCharSink(dummyAocx, StandardCharsets.UTF_8, FileWriteMode.APPEND)
        .write(HASHABLE_STRING);
  }

  @AfterEach
  public void teardown() {
    if (dummyAocx != null) {
      dummyAocx.delete();
    }
  }

  @Test
  public void testBootstrap() throws ResourceHandlerException {
    // Case 1. auto
    String allowed = "auto";
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, allowed);
    fpgaResourceHandler.bootstrap(configuration);
    // initPlugin() was also called in setup()
    verify(mockVendorPlugin, times(2)).initPlugin(configuration);
    verify(mockCGroupsHandler, times(1)).initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    assertEquals(5, fpgaResourceHandler.getFpgaAllocator()
        .getAvailableFpgaCount());
    assertEquals(5, fpgaResourceHandler.getFpgaAllocator()
        .getAllowedFpga().size());
    // Case 2. subset of devices
    fpgaResourceHandler = new FpgaResourceHandlerImpl(mockContext,
        mockCGroupsHandler, mockPrivilegedExecutor, mockVendorPlugin,
        fpgaDiscoverer);
    allowed = "0,1,2";
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, allowed);
    fpgaResourceHandler.bootstrap(configuration);
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAllowedFpga().size());
    List<FpgaDevice> allowedDevices =
        fpgaResourceHandler.getFpgaAllocator().getAllowedFpga();
    for (String s : allowed.split(",")) {
      boolean check = false;
      for (FpgaDevice device : allowedDevices) {
        if (String.valueOf(device.getMinor()).equals(s)) {
          check = true;
        }
      }
      assertTrue(check, "Minor:" + s +" found");
    }
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    // Case 3. User configuration contains invalid minor device number
    fpgaResourceHandler = new FpgaResourceHandlerImpl(mockContext,
        mockCGroupsHandler, mockPrivilegedExecutor, mockVendorPlugin,
        fpgaDiscoverer);
    allowed = "0,1,7";
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, allowed);
    fpgaResourceHandler.bootstrap(configuration);
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAllowedFpga().size());
  }

  @Test
  public void testBootstrapWithInvalidUserConfiguration()
      throws ResourceHandlerException {
    // User configuration contains invalid minor device number
    String allowed = "0,1,7";
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, allowed);
    fpgaResourceHandler.bootstrap(configuration);
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAllowedFpga().size());
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    String[] invalidAllowedStrings = {"a,1,2,", "a,1,2", "0,1,2,#", "a", "1,"};
    for (String s : invalidAllowedStrings) {
      boolean invalidConfiguration = false;
      configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, s);
      try {
        fpgaResourceHandler.bootstrap(configuration);
      } catch (ResourceHandlerException e) {
        invalidConfiguration = true;
      }
      assertTrue(invalidConfiguration);
    }

    String[] allowedStrings = {"1,2", "1"};
    for (String s : allowedStrings) {
      boolean invalidConfiguration = false;
      configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, s);
      try {
        fpgaResourceHandler.bootstrap(configuration);
      } catch (ResourceHandlerException e) {
        invalidConfiguration = true;
      }
      assertFalse(invalidConfiguration);
    }
  }

  @Test
  public void testBootStrapWithEmptyUserConfiguration()
      throws ResourceHandlerException {
    // User configuration contains invalid minor device number
    String allowed = "";
    boolean invalidConfiguration = false;
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, allowed);
    try {
      fpgaResourceHandler.bootstrap(configuration);
    } catch (ResourceHandlerException e) {
      invalidConfiguration = true;
    }
    assertTrue(invalidConfiguration);
  }

  @Test
  public void testAllocationWithPreference()
      throws ResourceHandlerException, PrivilegedOperationException {
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0,1,2");
    fpgaResourceHandler.bootstrap(configuration);
    // Case 1. The id-0 container request 1 FPGA of IntelOpenCL type and GEMM IP
    fpgaResourceHandler.preStart(mockContainer(0, 1, "GEMM"));
    assertEquals(1, fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    verifyDeniedDevices(getContainerId(0), Arrays.asList(1, 2));
    List<FpgaDevice> list = fpgaResourceHandler.getFpgaAllocator()
        .getUsedFpga().get(getContainerId(0).toString());
    for (FpgaDevice device : list) {
      assertEquals("GEMM", device.getIPID(), "IP should be updated to GEMM");
    }
    // Case 2. The id-1 container request 3 FPGA of IntelOpenCL and GEMM IP. this should fail
    boolean flag = false;
    try {
      fpgaResourceHandler.preStart(mockContainer(1, 3, "GZIP"));
    } catch (ResourceHandlerException e) {
      flag = true;
    }
    assertTrue(flag);
    // Case 3. Release the id-0 container
    fpgaResourceHandler.postComplete(getContainerId(0));
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    // Now we have enough devices, re-allocate for the id-1 container
    fpgaResourceHandler.preStart(mockContainer(1, 3, "GEMM"));
    // Id-1 container should have 0 denied devices
    verifyDeniedDevices(getContainerId(1), new ArrayList<>());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    // Release container id-1
    fpgaResourceHandler.postComplete(getContainerId(1));
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    // Case 4. Now all 3 devices should have IPID GEMM
    // Try container id-2 and id-3
    fpgaResourceHandler.preStart(mockContainer(2, 1, "GZIP"));
    fpgaResourceHandler.postComplete(getContainerId(2));
    fpgaResourceHandler.preStart(mockContainer(3, 2, "GEMM"));

    // IPID should be GEMM for id-3 container
    list = fpgaResourceHandler.getFpgaAllocator()
        .getUsedFpga().get(getContainerId(3).toString());
    for (FpgaDevice device : list) {
      assertEquals("GEMM", device.getIPID(), "IPID should be GEMM");
    }
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(1,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    fpgaResourceHandler.postComplete(getContainerId(3));
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    // Case 5. id-4 request 0 FPGA device
    fpgaResourceHandler.preStart(mockContainer(4, 0, ""));
    // Deny all devices for id-4
    verifyDeniedDevices(getContainerId(4), Arrays.asList(0, 1, 2));
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    // Case 6. id-5 with invalid FPGA device
    try {
      fpgaResourceHandler.preStart(mockContainer(5, -2, ""));
    } catch (ResourceHandlerException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testsAllocationWithExistingIPIDDevices()
      throws ResourceHandlerException, PrivilegedOperationException,
      IOException {
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0,1,2");
    fpgaResourceHandler.bootstrap(configuration);
    // The id-0 container request 3 FPGA of IntelOpenCL type and GEMM IP
    fpgaResourceHandler.preStart(mockContainer(0, 3, "GEMM"));
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    List<FpgaDevice> list =
        fpgaResourceHandler
            .getFpgaAllocator()
            .getUsedFpga()
            .get(getContainerId(0).toString());
    fpgaResourceHandler.postComplete(getContainerId(0));
    for (FpgaDevice device : list) {
      assertEquals("GEMM", device.getIPID(),
          "IP should be updated to GEMM");
    }

    // Case 1. id-1 container request preStart, with no plugin.configureIP called
    fpgaResourceHandler.preStart(mockContainer(1, 1, "GEMM"));
    fpgaResourceHandler.preStart(mockContainer(2, 1, "GEMM"));
    // we should have 3 times due to id-1 skip 1 invocation
    verify(mockVendorPlugin, times(3)).configureIP(anyString(),
        any(FpgaDevice.class));
    fpgaResourceHandler.postComplete(getContainerId(1));
    fpgaResourceHandler.postComplete(getContainerId(2));

    // Case 2. id-2 container request preStart, with 1 plugin.configureIP called
    // Add some characters to the dummy file to have its hash changed
    Files.asCharSink(dummyAocx, StandardCharsets.UTF_8, FileWriteMode.APPEND)
        .write("12345");
    fpgaResourceHandler.preStart(mockContainer(1, 1, "GZIP"));
    // we should have 4 times invocation
    verify(mockVendorPlugin, times(4)).configureIP(anyString(),
        any(FpgaDevice.class));
  }

  @Test
  public void testAllocationWithZeroDevices()
      throws ResourceHandlerException, PrivilegedOperationException {
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0,1,2");
    fpgaResourceHandler.bootstrap(configuration);
    // The id-0 container request 0 FPGA
    fpgaResourceHandler.preStart(mockContainer(0, 0, null));
    verifyDeniedDevices(getContainerId(0), Arrays.asList(0, 1, 2));
    verify(mockVendorPlugin, times(0)).retrieveIPfilePath(anyString(),
        anyString(), anyMap());
    verify(mockVendorPlugin, times(0)).configureIP(anyString(),
        any(FpgaDevice.class));
  }

  @Test
  public void testStateStore()
      throws ResourceHandlerException, IOException {
    // Case 1. store 3 devices
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0,1,2");
    fpgaResourceHandler.bootstrap(configuration);
    Container container0 = mockContainer(0, 3, "GEMM");
    fpgaResourceHandler.preStart(container0);
    List<FpgaDevice> assigned =
        fpgaResourceHandler
            .getFpgaAllocator()
            .getUsedFpga()
            .get(getContainerId(0).toString());
    verify(mockNMStateStore).storeAssignedResources(container0,
        ResourceInformation.FPGA_URI,
        new ArrayList<>(assigned));
    fpgaResourceHandler.postComplete(getContainerId(0));
    // Case 2. ask 0, no store api called
    Container container1 = mockContainer(1, 0, "");
    fpgaResourceHandler.preStart(container1);
    verify(mockNMStateStore, never()).storeAssignedResources(
        eq(container1), eq(ResourceInformation.FPGA_URI), anyList());
  }

  @Test
  public void testReacquireContainer() throws ResourceHandlerException {
    Container c0 = mockContainer(0, 2, "GEMM");
    List<FpgaDevice> assigned = new ArrayList<>();
    assigned.add(new FpgaDevice(
        vendorType, 247, 0, "acl0"));
    assigned.add(new FpgaDevice(
        vendorType, 247, 1, "acl1"));
    // Mock we've stored the c0 states
    mockStateStoreForContainer(c0, assigned);
    // NM start
    configuration.set(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES, "0,1,2");
    fpgaResourceHandler.bootstrap(configuration);
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    // Case 1. try recover state for id-0 container
    fpgaResourceHandler.reacquireContainer(getContainerId(0));
    // minor number matches
    List<FpgaDevice> used =
        fpgaResourceHandler.getFpgaAllocator().
        getUsedFpga().get(getContainerId(0).toString());
    int count = 0;
    for (FpgaDevice device : used) {
      if (device.getMinor() == 0){
        count++;
      }
      if (device.getMinor() == 1) {
        count++;
      }
    }
    assertEquals(2, count, "Unexpected used minor number in allocator");
    List<FpgaDevice> available =
        fpgaResourceHandler
            .getFpgaAllocator()
            .getAvailableFpga()
            .get(vendorType);
    count = 0;
    for (FpgaDevice device : available) {
      if (device.getMinor() == 2) {
        count++;
      }
    }
    assertEquals(1, count, "Unexpected available minor number in allocator");


    // Case 2. Recover a not allowed device with minor number 5
    Container c1 = mockContainer(1, 1, "GEMM");
    assigned = new ArrayList<>();
    assigned.add(new FpgaDevice(
        vendorType, 247, 5, "acl0"));
    // Mock we've stored the c1 states
    mockStateStoreForContainer(c1, assigned);
    boolean flag = false;
    try {
      fpgaResourceHandler.reacquireContainer(getContainerId(1));
    } catch (ResourceHandlerException e) {
      flag = true;
    }
    assertTrue(flag);
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(1,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    // Case 3. recover a already used device by other container
    Container c2 = mockContainer(2, 1, "GEMM");
    assigned = new ArrayList<>();
    assigned.add(new FpgaDevice(
        vendorType, 247, 1, "acl0"));
    // Mock we've stored the c2 states
    mockStateStoreForContainer(c2, assigned);
    flag = false;
    try {
      fpgaResourceHandler.reacquireContainer(getContainerId(2));
    } catch (ResourceHandlerException e) {
      flag = true;
    }
    assertTrue(flag);
    assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(1,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

    // Case 4. recover a normal container c3 with remaining minor device number 2
    Container c3 = mockContainer(3, 1, "GEMM");
    assigned = new ArrayList<>();
    assigned.add(new FpgaDevice(
        vendorType, 247, 2, "acl2"));
    // Mock we've stored the c2 states
    mockStateStoreForContainer(c3, assigned);
    fpgaResourceHandler.reacquireContainer(getContainerId(3));
    assertEquals(3,
        fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    assertEquals(0,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
  }

  @Test
  public void testSha256CalculationFails() throws ResourceHandlerException {
    ResourceHandlerException exception =
        assertThrows(ResourceHandlerException.class, () -> {
          dummyAocx.delete();
          fpgaResourceHandler.preStart(mockContainer(0, 1, "GEMM"));
        });
    assertEquals("Could not calculate SHA-256", exception.getMessage());
  }

  @Test
  public void testSha256CalculationSucceeds()
      throws IOException, ResourceHandlerException {
    mockVendorPlugin =
        mockPlugin(vendorType, deviceList, dummyAocx.getAbsolutePath());
    fpgaResourceHandler = new FpgaResourceHandlerImpl(mockContext,
        mockCGroupsHandler, mockPrivilegedExecutor, mockVendorPlugin,
        fpgaDiscoverer);

    fpgaResourceHandler.bootstrap(configuration);
    fpgaResourceHandler.preStart(mockContainer(0, 1, "GEMM"));

    // IP file is assigned to the first device
    List<FpgaDevice> devices =
        fpgaResourceHandler.getFpgaAllocator().getAllowedFpga();
    FpgaDevice device = devices.get(0);
    assertEquals(EXPECTED_HASH, device.getAocxHash(), "Hash value");
  }

  private void verifyDeniedDevices(ContainerId containerId,
      List<Integer> deniedDevices)
      throws ResourceHandlerException, PrivilegedOperationException {
    verify(mockCGroupsHandler, atLeastOnce()).createCGroup(
        CGroupsHandler.CGroupController.DEVICES, containerId.toString());

    if (null != deniedDevices && !deniedDevices.isEmpty()) {
      verify(mockPrivilegedExecutor, times(1)).executePrivilegedOperation(
          new PrivilegedOperation(PrivilegedOperation.OperationType.FPGA, Arrays
              .asList(FpgaResourceHandlerImpl.CONTAINER_ID_CLI_OPTION,
                  containerId.toString(),
                  FpgaResourceHandlerImpl.EXCLUDED_FPGAS_CLI_OPTION,
                  StringUtils.join(",", deniedDevices))), true);
    } else if (deniedDevices.isEmpty()) {
      verify(mockPrivilegedExecutor, times(1)).executePrivilegedOperation(
          new PrivilegedOperation(PrivilegedOperation.OperationType.FPGA, Arrays
              .asList(FpgaResourceHandlerImpl.CONTAINER_ID_CLI_OPTION,
                  containerId.toString())), true);
    }
  }

  private static IntelFpgaOpenclPlugin mockPlugin(String type,
      List<FpgaDevice> list, String aocxPath) {
    IntelFpgaOpenclPlugin plugin = mock(IntelFpgaOpenclPlugin.class);
    when(plugin.initPlugin(any())).thenReturn(true);
    when(plugin.getFpgaType()).thenReturn(type);
    when(plugin.retrieveIPfilePath(anyString(),
        anyString(), anyMap())).thenReturn(aocxPath);
    when(plugin.configureIP(anyString(), any()))
        .thenReturn(true);
    when(plugin.discover(anyInt())).thenReturn(list);
    return plugin;
  }

  private static Container mockContainer(int id, int numFpga, String IPID) {
    Container c = mock(Container.class);

    Resource res = Resource.newInstance(1024, 1);
    ResourceMappings resMapping = new ResourceMappings();
    res.setResourceValue(ResourceInformation.FPGA_URI, numFpga);
    when(c.getResource()).thenReturn(res);
    when(c.getResourceMappings()).thenReturn(resMapping);

    when(c.getContainerId()).thenReturn(getContainerId(id));

    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    Map<String, String> envs = new HashMap<>();
    if (numFpga > 0) {
      envs.put("REQUESTED_FPGA_IP_ID", IPID);
    }
    when(c.getLaunchContext()).thenReturn(clc);
    when(clc.getEnvironment()).thenReturn(envs);
    when(c.getWorkDir()).thenReturn("/tmp");
    ResourceSet resourceSet = new ResourceSet();
    when(c.getResourceSet()).thenReturn(resourceSet);

    return c;
  }

  private void mockStateStoreForContainer(Container container,
      List<FpgaDevice> assigned) {
    ResourceMappings rmap = new ResourceMappings();
    ResourceMappings.AssignedResources ar =
        new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(new ArrayList<>(assigned));
    rmap.addAssignedResources(ResourceInformation.FPGA_URI, ar);
    when(container.getResourceMappings()).thenReturn(rmap);
    runningContainersMap.put(container.getContainerId(), container);
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }
}
