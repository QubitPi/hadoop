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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestDataStorage {
  private final static String DEFAULT_BPID = "bp-0";
  private final static String CLUSTER_ID = "cluster0";
  private final static String CLUSTER_ID2 = "cluster1";
  private final static String BUILD_VERSION = "2.0";
  private final static String SOFTWARE_VERSION = "2.0";
  private final static long CTIME = 1;
  private final static File TEST_DIR = GenericTestUtils.getTestDir("dstest");
  private final static StartupOption START_OPT = StartupOption.REGULAR;

  private DataNode mockDN = Mockito.mock(DataNode.class);
  private NamespaceInfo nsInfo;
  private DataStorage storage;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    storage = new DataStorage();
    nsInfo = new NamespaceInfo(0, CLUSTER_ID, DEFAULT_BPID, CTIME,
        BUILD_VERSION, SOFTWARE_VERSION);
    FileUtil.fullyDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs(), "Failed to make test dir.");
    Mockito.when(mockDN.getConf()).thenReturn(conf);
  }

  @AfterEach
  public void tearDown() throws IOException {
    storage.unlockAll();
    FileUtil.fullyDelete(TEST_DIR);
  }

  private static List<StorageLocation> createStorageLocations(int numLocs)
      throws IOException {
    return createStorageLocations(numLocs, false);
  }

  /**
   * Create a list of StorageLocations.
   * If asFile sets to true, create StorageLocation as regular files, otherwise
   * create directories for each location.
   * @param numLocs the total number of StorageLocations to be created.
   * @param asFile set to true to create as file.
   * @return a list of StorageLocations.
   */
  private static List<StorageLocation> createStorageLocations(
      int numLocs, boolean asFile) throws IOException {
    List<StorageLocation> locations = new ArrayList<StorageLocation>();
    for (int i = 0; i < numLocs; i++) {
      String uri = TEST_DIR + "/data" + i;
      File file = new File(uri);
      if (asFile) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      } else {
        file.mkdirs();
      }
      StorageLocation loc = StorageLocation.parse(uri);
      locations.add(loc);
    }
    return locations;
  }

  private static List<NamespaceInfo> createNamespaceInfos(int num) {
    List<NamespaceInfo> nsInfos = new ArrayList<NamespaceInfo>();
    for (int i = 0; i < num; i++) {
      String bpid = "bp-" + i;
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, CTIME, BUILD_VERSION,
          SOFTWARE_VERSION));
    }
    return nsInfos;
  }

  /** Check whether the path is a valid DataNode data directory. */
  private static void checkDir(File dataDir) {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(dataDir);
    assertTrue(sd.getRoot().isDirectory());
    assertTrue(sd.getCurrentDir().isDirectory());
    assertTrue(sd.getVersionFile().isFile());
  }

  /** Check whether the root is a valid BlockPoolSlice storage. */
  private static void checkDir(File root, String bpid) {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
    File bpRoot = new File(sd.getCurrentDir(), bpid);
    Storage.StorageDirectory bpSd = new Storage.StorageDirectory(bpRoot);
    assertTrue(bpSd.getRoot().isDirectory());
    assertTrue(bpSd.getCurrentDir().isDirectory());
    assertTrue(bpSd.getVersionFile().isFile());
  }

  @Test
  public void testAddStorageDirectories() throws IOException,
      URISyntaxException {
    final int numLocations = 3;
    final int numNamespace = 3;
    List<StorageLocation> locations = createStorageLocations(numLocations);

    // Add volumes for multiple namespaces.
    List<NamespaceInfo> namespaceInfos = createNamespaceInfos(numNamespace);
    for (NamespaceInfo ni : namespaceInfos) {
      storage.addStorageLocations(mockDN, ni, locations, START_OPT);
      for (StorageLocation sl : locations) {
        checkDir(new File(sl.getUri()));
        checkDir(new File(sl.getUri()), ni.getBlockPoolID());
      }
    }

    assertEquals(numLocations, storage.getNumStorageDirs());

    locations = createStorageLocations(numLocations);
    List<StorageDirectory> addedLocation =
        storage.addStorageLocations(mockDN, namespaceInfos.get(0),
            locations, START_OPT);
    assertTrue(addedLocation.isEmpty());

    // The number of active storage dirs has not changed, since it tries to
    // add the storage dirs that are under service.
    assertEquals(numLocations, storage.getNumStorageDirs());

    // Add more directories.
    locations = createStorageLocations(6);
    storage.addStorageLocations(mockDN, nsInfo, locations, START_OPT);
    assertEquals(6, storage.getNumStorageDirs());
  }

  @Test
  public void testAddStorageDirectoriesFailure() throws IOException {
    final int numLocations = 1;
    List<StorageLocation> locations = createStorageLocations(numLocations);
    assertEquals(numLocations, locations.size());

    NamespaceInfo namespaceInfo = new NamespaceInfo(0, CLUSTER_ID,
        DEFAULT_BPID, CTIME, BUILD_VERSION, SOFTWARE_VERSION);
    List<StorageDirectory> successLocations = storage.addStorageLocations(
        mockDN, namespaceInfo, locations, START_OPT);
    assertEquals(1, successLocations.size());

    // After the DataNode restarts, the value of the clusterId is different
    // from the value before the restart.
    storage.unlockAll();
    DataNode newMockDN = Mockito.mock(DataNode.class);
    Mockito.when(newMockDN.getConf()).thenReturn(new HdfsConfiguration());
    DataStorage newStorage = new DataStorage();
    NamespaceInfo newNamespaceInfo = new NamespaceInfo(0, CLUSTER_ID2,
        DEFAULT_BPID, CTIME, BUILD_VERSION, SOFTWARE_VERSION);
    successLocations = newStorage.addStorageLocations(
            newMockDN, newNamespaceInfo, locations, START_OPT);
    assertEquals(0, successLocations.size());
    newStorage.unlockAll();
    newMockDN.shutdown();
  }

  @Test
  public void testMissingVersion() throws IOException,
      URISyntaxException {
    final int numLocations = 1;
    final int numNamespace = 1;
    List<StorageLocation> locations = createStorageLocations(numLocations);

    StorageLocation firstStorage = locations.get(0);
    Storage.StorageDirectory sd = new Storage.StorageDirectory(firstStorage);
    // the directory is not initialized so VERSION does not exist
    // create a fake directory under current/
    File currentDir = new File(sd.getCurrentDir(),
        "BP-787466439-172.26.24.43-1462305406642");
    assertTrue(currentDir.mkdirs(), "unable to mkdir " + currentDir.getName());

    // Add volumes for multiple namespaces.
    List<NamespaceInfo> namespaceInfos = createNamespaceInfos(numNamespace);
    for (NamespaceInfo ni : namespaceInfos) {
      storage.addStorageLocations(mockDN, ni, locations, START_OPT);
    }

    // It should not format the directory because VERSION is missing.
    assertTrue(currentDir.exists(), "Storage directory was formatted");
  }

  @Test
  public void testRecoverTransitionReadFailure() throws IOException {
    final int numLocations = 3;
    List<StorageLocation> locations =
        createStorageLocations(numLocations, true);
    try {
      storage.recoverTransitionRead(mockDN, nsInfo, locations, START_OPT);
      fail("An IOException should throw: all StorageLocations are NON_EXISTENT");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "All specified directories have failed to load.", e);
    }
    assertEquals(0, storage.getNumStorageDirs());
  }

  /**
   * This test enforces the behavior that if there is an exception from
   * doTransition() during DN starts up, the storage directories that have
   * already been processed are still visible, i.e., in
   * DataStorage.storageDirs().
   */
  @Test
  public void testRecoverTransitionReadDoTransitionFailure()
      throws IOException {
    final int numLocations = 3;
    List<StorageLocation> locations = createStorageLocations(numLocations);
    // Prepare volumes
    storage.recoverTransitionRead(mockDN, nsInfo, locations, START_OPT);
    assertEquals(numLocations, storage.getNumStorageDirs());

    // Reset DataStorage
    storage.unlockAll();
    storage = new DataStorage();
    // Trigger an exception from doTransition().
    nsInfo.clusterID = "cluster1";
    try {
      storage.recoverTransitionRead(mockDN, nsInfo, locations, START_OPT);
      fail("Expect to throw an exception from doTransition()");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("All specified directories", e);
    }
    assertEquals(0, storage.getNumStorageDirs());
  }
}
