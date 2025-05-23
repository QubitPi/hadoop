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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides mechanisms to get various resource handlers - cpu, memory, network,
 * disk etc., - based on configuration.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerModule {
  static final Logger LOG =
       LoggerFactory.getLogger(ResourceHandlerModule.class);
  private static boolean cgroupsV2Enabled;
  private static volatile ResourceHandlerChain resourceHandlerChain;

  /**
   * This specific implementation might provide resource management as well
   * as resource metrics functionality. We need to ensure that the same
   * instance is used for both.
   */
  private static volatile CGroupsHandler cGroupV1Handler;
  private static volatile CGroupsHandler cGroupV2Handler;
  private static volatile TrafficControlBandwidthHandlerImpl
      trafficControlBandwidthHandler;
  private static volatile NetworkPacketTaggingHandlerImpl
      networkPacketTaggingHandlerImpl;
  private static volatile CGroupsBlkioResourceHandlerImpl
      cGroupsBlkioResourceHandler;
  private static volatile MemoryResourceHandler
      cGroupsMemoryResourceHandler;
  private static volatile CpuResourceHandler
      cGroupsCpuResourceHandler;

  private static void initializeCGroupHandlers(Configuration conf,
                                               CGroupsHandler.CGroupController controller)
      throws ResourceHandlerException {
    if (cgroupsV2Enabled) {
      initializeCGroupV2Handler(conf);
      if (!isMountedInCGroupsV2(controller)) {
        LOG.info("Cgroup v2 is enabled but {} is not mounted in cgroups v2, falling back to v1",
            controller);
        initializeCGroupV1Handler(conf);
      }
    } else {
      initializeCGroupV1Handler(conf);
    }
  }

  private static void initializeCGroupV1Handler(Configuration conf)
      throws ResourceHandlerException {
    if (cGroupV1Handler == null) {
      synchronized (CGroupsHandler.class) {
        if (cGroupV1Handler == null) {
          cGroupV1Handler = new CGroupsHandlerImpl(
              conf, PrivilegedOperationExecutor.getInstance(conf));
          LOG.debug("Value of CGroupsV1Handler is: {}", cGroupV1Handler);
        }
      }
    }
  }

  private static void initializeCGroupV2Handler(Configuration conf)
      throws ResourceHandlerException {
    if (cGroupV2Handler == null) {
      synchronized (CGroupsHandler.class) {
        if (cGroupV2Handler == null) {
          cGroupV2Handler = new CGroupsV2HandlerImpl(
              conf, PrivilegedOperationExecutor.getInstance(conf));
          LOG.debug("Value of CGroupsV2Handler is: {}", cGroupV2Handler);
        }
      }
    }
  }

  private static boolean isMountedInCGroupsV1(CGroupsHandler.CGroupController controller) {
    return (cGroupV1Handler != null && cGroupV1Handler.getControllerPath(controller) != null);
  }

  private static boolean isMountedInCGroupsV2(CGroupsHandler.CGroupController controller) {
    return (cGroupV2Handler != null && cGroupV2Handler.getControllerPath(controller) != null);
  }

  /**
   * Returns a (possibly null) reference to a cGroupsHandler. This handler is
   * non-null only if one or more of the known cgroups-based resource
   * handlers are in use and have been initialized.
   */

  public static CGroupsHandler getCGroupsHandler() {
    return cGroupV1Handler;
  }

  /**
   * Returns relative root for cgroups.  Returns null if cGroupsHandler is
   * not initialized, or if the path is empty.
   */
  public static String getCgroupsRelativeRoot() {
    if (getCGroupsHandler() == null) {
      return null;
    }
    String cGroupPath = getCGroupsHandler().getRelativePathForCGroup("");
    if (cGroupPath == null || cGroupPath.isEmpty()) {
      return null;
    }
    return cGroupPath.replaceAll("/$", "");
  }

  public static NetworkPacketTaggingHandlerImpl
      getNetworkResourceHandler() {
    return networkPacketTaggingHandlerImpl;
  }

  public static DiskResourceHandler
      getDiskResourceHandler() {
    return cGroupsBlkioResourceHandler;
  }

  public static MemoryResourceHandler
      getMemoryResourceHandler() {
    return cGroupsMemoryResourceHandler;
  }

  public static CpuResourceHandler
      getCpuResourceHandler() {
    return cGroupsCpuResourceHandler;
  }

  private static CpuResourceHandler initCGroupsCpuResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    boolean cgroupsCpuEnabled =
        conf.getBoolean(YarnConfiguration.NM_CPU_RESOURCE_ENABLED,
            YarnConfiguration.DEFAULT_NM_CPU_RESOURCE_ENABLED);
    boolean cgroupsLCEResourcesHandlerEnabled =
        conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
            DefaultLCEResourcesHandler.class)
            .equals(CgroupsLCEResourcesHandler.class);
    if (cgroupsCpuEnabled || cgroupsLCEResourcesHandlerEnabled) {
      if (cGroupsCpuResourceHandler == null) {
        synchronized (CpuResourceHandler.class) {
          if (cGroupsCpuResourceHandler == null) {
            LOG.debug("Creating new cgroups cpu handler");

            initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.CPU);
            if (isMountedInCGroupsV2(CGroupsHandler.CGroupController.CPU)) {
              cGroupsCpuResourceHandler = new CGroupsV2CpuResourceHandlerImpl(cGroupV2Handler);
            } else {
              cGroupsCpuResourceHandler = new CGroupsCpuResourceHandlerImpl(cGroupV1Handler);
            }
            return cGroupsCpuResourceHandler;
          }
        }
      }
    }
    return null;
  }

  private static TrafficControlBandwidthHandlerImpl
      getTrafficControlBandwidthHandler(Configuration conf)
        throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_ENABLED)) {
      if (trafficControlBandwidthHandler == null) {
        synchronized (OutboundBandwidthResourceHandler.class) {
          if (trafficControlBandwidthHandler == null) {
            LOG.info("Creating new traffic control bandwidth handler.");

            initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.NET_CLS);
            trafficControlBandwidthHandler = new
                TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
                .getInstance(conf), cGroupV1Handler,
                new TrafficController(conf, PrivilegedOperationExecutor
                    .getInstance(conf)));
          }
        }
      }

      return trafficControlBandwidthHandler;
    } else {
      return null;
    }
  }

  public static ResourceHandler initNetworkResourceHandler(Configuration conf)
        throws ResourceHandlerException {
    boolean useNetworkTagHandler = conf.getBoolean(
        YarnConfiguration.NM_NETWORK_TAG_HANDLER_ENABLED,
        YarnConfiguration.DEFAULT_NM_NETWORK_TAG_HANDLER_ENABLED);
    if (useNetworkTagHandler) {
      LOG.info("Using network-tagging-handler.");
      return getNetworkTaggingHandler(conf);
    } else {
      LOG.info("Using traffic control bandwidth handler");
      return getTrafficControlBandwidthHandler(conf);
    }
  }

  public static ResourceHandler getNetworkTaggingHandler(Configuration conf)
      throws ResourceHandlerException {
    if (networkPacketTaggingHandlerImpl == null) {
      synchronized (OutboundBandwidthResourceHandler.class) {
        if (networkPacketTaggingHandlerImpl == null) {
          LOG.info("Creating new network-tagging-handler.");

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.NET_CLS);
          networkPacketTaggingHandlerImpl =
              new NetworkPacketTaggingHandlerImpl(
                  PrivilegedOperationExecutor.getInstance(conf), cGroupV1Handler);
        }
      }
    }
    return networkPacketTaggingHandlerImpl;
  }

  public static OutboundBandwidthResourceHandler
      initOutboundBandwidthResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    return getTrafficControlBandwidthHandler(conf);
  }

  public static DiskResourceHandler initDiskResourceHandler(Configuration conf)
      throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_DISK_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISK_RESOURCE_ENABLED)) {
      return getCgroupsBlkioResourceHandler(conf);
    }
    return null;
  }

  private static CGroupsBlkioResourceHandlerImpl getCgroupsBlkioResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (cGroupsBlkioResourceHandler == null) {
      synchronized (DiskResourceHandler.class) {
        if (cGroupsBlkioResourceHandler == null) {
          LOG.debug("Creating new cgroups blkio handler");

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.BLKIO);
          cGroupsBlkioResourceHandler =
              new CGroupsBlkioResourceHandlerImpl(cGroupV1Handler);
        }
      }
    }
    return cGroupsBlkioResourceHandler;
  }

  public static MemoryResourceHandler initMemoryResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (conf.getBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENABLED)) {
      return getCgroupsMemoryResourceHandler(conf);
    }
    return null;
  }

  private static MemoryResourceHandler
      getCgroupsMemoryResourceHandler(
      Configuration conf) throws ResourceHandlerException {
    if (cGroupsMemoryResourceHandler == null) {
      synchronized (MemoryResourceHandler.class) {
        if (cGroupsMemoryResourceHandler == null) {

          initializeCGroupHandlers(conf, CGroupsHandler.CGroupController.MEMORY);
          if (isMountedInCGroupsV2(CGroupsHandler.CGroupController.MEMORY)) {
            cGroupsMemoryResourceHandler = new CGroupsV2MemoryResourceHandlerImpl(cGroupV2Handler);
          } else {
            cGroupsMemoryResourceHandler = new CGroupsMemoryResourceHandlerImpl(cGroupV1Handler);
          }
        }
      }
    }
    return cGroupsMemoryResourceHandler;
  }

  private static ResourceHandler getNumaResourceHandler(Configuration conf,
      Context nmContext) {
    if (YarnConfiguration.numaAwarenessEnabled(conf)) {
      return new NumaResourceHandlerImpl(conf, nmContext);
    }
    return null;
  }

  private static void addHandlerIfNotNull(List<ResourceHandler> handlerList,
      ResourceHandler handler) {
    if (handler != null) {
      handlerList.add(handler);
    }
  }

  private static void initializeConfiguredResourceHandlerChain(
      Configuration conf, Context nmContext)
      throws ResourceHandlerException {
    ArrayList<ResourceHandler> handlerList = new ArrayList<>();

    addHandlerIfNotNull(handlerList,
        initNetworkResourceHandler(conf));
    addHandlerIfNotNull(handlerList,
        initDiskResourceHandler(conf));
    addHandlerIfNotNull(handlerList,
        initMemoryResourceHandler(conf));
    addHandlerIfNotNull(handlerList,
        initCGroupsCpuResourceHandler(conf));
    addHandlerIfNotNull(handlerList, getNumaResourceHandler(conf, nmContext));
    addHandlersFromConfiguredResourcePlugins(handlerList, conf, nmContext);
    resourceHandlerChain = new ResourceHandlerChain(handlerList);
  }

  private static void addHandlersFromConfiguredResourcePlugins(
      List<ResourceHandler> handlerList, Configuration conf,
      Context nmContext) throws ResourceHandlerException {
    ResourcePluginManager pluginManager = nmContext.getResourcePluginManager();

    if (pluginManager == null) {
      LOG.warn("Plugin manager was null while trying to add " +
          "ResourceHandlers from configuration!");
      return;
    }

    Map<String, ResourcePlugin> pluginMap = pluginManager.getNameToPlugins();
    if (pluginMap == null) {
      LOG.debug("List of plugins of ResourcePluginManager was empty " +
          "while trying to add ResourceHandlers from configuration!");
      return;
    } else {
      LOG.debug("List of plugins of ResourcePluginManager: {}",
          pluginManager.getNameToPlugins());
    }

    for (ResourcePlugin plugin : pluginMap.values()) {
      initializeCGroupV1Handler(conf);
      addHandlerIfNotNull(handlerList,
          plugin.createResourceHandler(nmContext,
              cGroupV1Handler,
              PrivilegedOperationExecutor.getInstance(conf)));
    }
  }

  public static ResourceHandlerChain getConfiguredResourceHandlerChain(
      Configuration conf, Context nmContext) throws ResourceHandlerException {
    cgroupsV2Enabled =
        conf.getBoolean(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_V2_ENABLED,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_V2_ENABLED);

    if (resourceHandlerChain == null) {
      synchronized (ResourceHandlerModule.class) {
        if (resourceHandlerChain == null) {
          initializeConfiguredResourceHandlerChain(conf, nmContext);
        }
      }
    }

    if (resourceHandlerChain.getResourceHandlerList().size() != 0) {
      return resourceHandlerChain;
    } else {
      return null;
    }
  }

  @VisibleForTesting
  static void nullifyResourceHandlerChain() throws ResourceHandlerException {
    resourceHandlerChain = null;
  }

  /**
   * If a cgroup mount directory is specified, it returns cgroup directories
   * with valid names.
   * The requirement is that each hierarchy has to be named with the comma
   * separated names of subsystems supported.
   * For example: /sys/fs/cgroup/cpu,cpuacct
   * @param cgroupMountPath Root cgroup mount path (/sys/fs/cgroup in the
   *                        example above)
   * @return A path to cgroup subsystem set mapping in the same format as
   *         {@link CGroupsHandlerImpl#parseMtab(String)}
   * @throws IOException if the specified directory cannot be listed
   */
  public static Map<String, Set<String>> parseConfiguredCGroupPath(
      String cgroupMountPath) throws IOException {
    File cgroupDir = new File(cgroupMountPath);
    File[] list = cgroupDir.listFiles();
    if (list == null) {
      throw new IOException("Empty cgroup mount directory specified: " +
          cgroupMountPath);
    }

    Map<String, Set<String>> pathSubsystemMappings = new HashMap<>();
    Set<String> validCGroups =
        CGroupsHandler.CGroupController.getValidV1CGroups();
    for (File candidate: list) {
      Set<String> cgroupList =
          new HashSet<>(Arrays.asList(candidate.getName().split(",")));
      // Collect the valid subsystem names
      cgroupList.retainAll(validCGroups);
      if (!cgroupList.isEmpty()) {
        if (candidate.isDirectory()) {
          pathSubsystemMappings.put(candidate.getAbsolutePath(), cgroupList);
        } else {
          LOG.warn("The following cgroup is not a directory " +
              candidate.getAbsolutePath());
        }
      }
    }
    return pathSubsystemMappings;
  }
}
