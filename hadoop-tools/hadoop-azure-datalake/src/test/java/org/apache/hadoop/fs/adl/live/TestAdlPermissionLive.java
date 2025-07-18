/*
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
 *
 */


package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test ACL permission on file/folder on Adl file system.
 */
public class TestAdlPermissionLive {

  private static Path testRoot = new Path("/test");
  private FsPermission permission;
  private Path path;
  private FileSystem adlStore;

  public void initTestAdlPermissionLive(FsPermission pTestPermission) throws Exception {
    permission = pTestPermission;
    setUp();
  }

  public static Collection adlCreateNonRecursiveTestData()
      throws UnsupportedEncodingException {
    /*
      Test Data
      File/Folder name, User permission, Group permission, Other Permission,
      Parent already exist
      shouldCreateSucceed, expectedExceptionIfFileCreateFails
    */
    final Collection<Object[]> datas = new ArrayList<>();
    for (FsAction g : FsAction.values()) {
      for (FsAction o : FsAction.values()) {
        datas.add(new Object[] {new FsPermission(FsAction.ALL, g, o)});
      }
    }
    return datas;
  }

  @AfterAll
  public static void cleanUp() throws IOException, URISyntaxException {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      assertTrue(AdlStorageConfiguration.createStorageConnector()
          .delete(testRoot, true));
    }
  }

  public void setUp() throws Exception {
    assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
    adlStore = AdlStorageConfiguration.createStorageConnector();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("adlCreateNonRecursiveTestData")
  public void testFilePermission(FsPermission pTestPermission) throws Exception {
    initTestAdlPermissionLive(pTestPermission);
    path = new Path(testRoot, UUID.randomUUID().toString());
    adlStore.getConf()
        .set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");

    adlStore.mkdirs(path.getParent(),
        new FsPermission(FsAction.ALL, FsAction.WRITE, FsAction.NONE));
    adlStore.removeDefaultAcl(path.getParent());

    adlStore.create(path, permission, true, 1024, (short) 1, 1023, null);
    FileStatus status = adlStore.getFileStatus(path);
    assertEquals(permission, status.getPermission());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("adlCreateNonRecursiveTestData")
  public void testFolderPermission(FsPermission pTestPermission) throws Exception {
    initTestAdlPermissionLive(pTestPermission);
    path = new Path(testRoot, UUID.randomUUID().toString());
    adlStore.getConf()
        .set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");
    adlStore.mkdirs(path.getParent(),
        new FsPermission(FsAction.ALL, FsAction.WRITE, FsAction.NONE));
    adlStore.removeDefaultAcl(path.getParent());

    adlStore.mkdirs(path, permission);
    FileStatus status = adlStore.getFileStatus(path);
    assertEquals(permission, status.getPermission());
  }
}
