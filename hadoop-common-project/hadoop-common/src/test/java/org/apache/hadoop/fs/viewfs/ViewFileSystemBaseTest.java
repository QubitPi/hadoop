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
package org.apache.hadoop.fs.viewfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.TestFileUtil;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.fs.FileSystemTestHelper.*;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE;
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <p>
 * A collection of tests for the {@link ViewFileSystem}.
 * This test should be used for testing ViewFileSystem that has mount links to 
 * a target file system such  localFs or Hdfs etc.

 * </p>
 * <p>
 * To test a given target file system create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fsTarget</code> 
 * to point to the file system to which you want the mount targets
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */

abstract public class ViewFileSystemBaseTest {
  FileSystem fsView;  // the view file system - the mounts are here
  FileSystem fsTarget;  // the target file system - the mount will point here
  Path targetTestRoot;
  Configuration conf;
  final FileSystemTestHelper fileSystemTestHelper;
  private static final Logger LOG =
      LoggerFactory.getLogger(ViewFileSystemBaseTest.class);

  public ViewFileSystemBaseTest() {
      this.fileSystemTestHelper = createFileSystemHelper();
  }

  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper();
  }

  @TempDir
  public File temporaryFolder;

  @BeforeEach
  public void setUp() throws Exception {
    initializeTargetTestRoot();
    
    // Make  user and data dirs - we creates links to them in the mount table
    fsTarget.mkdirs(new Path(targetTestRoot,"user"));
    fsTarget.mkdirs(new Path(targetTestRoot,"data"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir2"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir3"));
    FileSystemTestHelper.createFile(fsTarget, new Path(targetTestRoot,"aFile"));
    
    
    // Now we use the mount fs to set links to user and dir
    // in the test root
    
    // Set up the defaultMT in the config with our mount point links
    conf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
  }
  
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    fsTarget.delete(targetTestRoot, true);
    
    fsTarget.mkdirs(targetTestRoot);
  }
  
  void setupMountPoints() {
    ConfigUtil.addLink(conf, "/targetRoot", targetTestRoot.toUri());
    ConfigUtil.addLink(conf, "/user", new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/user2", new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/data", new Path(targetTestRoot,"data").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path(targetTestRoot,"dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3",
        new Path(targetTestRoot,"dir3").toUri());
    ConfigUtil.addLink(conf, "/danglingLink",
        new Path(targetTestRoot, "missingTarget").toUri());
    ConfigUtil.addLink(conf, "/linkToAFile",
        new Path(targetTestRoot, "aFile").toUri());
  }
  
  @Test
  public void testGetMountPoints() {
    ViewFileSystem viewfs = (ViewFileSystem) fsView;
    MountPoint[] mountPoints = viewfs.getMountPoints();
    for (MountPoint mountPoint : mountPoints) {
      LOG.info("MountPoint: " + mountPoint.getMountedOnPath() + " => "
          + mountPoint.getTargetFileSystemURIs()[0]);
    }
    assertEquals(mountPoints.length, getExpectedMountPoints());
  }
  
  int getExpectedMountPoints() {
    return 8;
  }
  
  /**
   * This default implementation is when viewfs has mount points
   * into file systems, such as LocalFs that do no have delegation tokens.
   * It should be overridden for when mount points into hdfs.
   */
  @Test
  public void testGetDelegationTokens() throws IOException {
    Token<?>[] delTokens = 
        fsView.addDelegationTokens("sanjay", new Credentials());
    assertEquals(delTokens.length, getExpectedDelegationTokenCount());
  }
  
  int getExpectedDelegationTokenCount() {
    return 0;
  }

  @Test
  public void testGetDelegationTokensWithCredentials() throws IOException {
    Credentials credentials = new Credentials();
    List<Token<?>> delTokens =
        Arrays.asList(fsView.addDelegationTokens("sanjay", credentials));

    int expectedTokenCount = getExpectedDelegationTokenCountWithCredentials();

    assertEquals(delTokens.size(), expectedTokenCount);
    Credentials newCredentials = new Credentials();
    for (int i = 0; i < expectedTokenCount / 2; i++) {
      Token<?> token = delTokens.get(i);
      newCredentials.addToken(token.getService(), token);
    }

    List<Token<?>> delTokens2 =
        Arrays.asList(fsView.addDelegationTokens("sanjay", newCredentials));
    assertEquals(delTokens2.size(), (expectedTokenCount + 1) / 2);
  }

  int getExpectedDelegationTokenCountWithCredentials() {
    return 0;
  }

  @Test
  public void testBasicPaths() {
    assertEquals(fsView.getUri(), FsConstants.VIEWFS_URI);
    assertEquals(fsView.getWorkingDirectory(),
        fsView.makeQualified(
            new Path("/user/" + System.getProperty("user.name"))));
    assertEquals(
        fsView.getHomeDirectory(),
        fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))));
    assertEquals(
        fsView.makeQualified(new Path("/foo/bar")),
        new Path("/foo/bar").makeQualified(FsConstants.VIEWFS_URI, null));
  }

  @Test
  public void testLocatedOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(true);
  }

  @Test
  public void testOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(false);
  }

  /**
   * Test modify operations (create, mkdir, delete, etc)
   * on the mount file system where the pathname references through
   * the mount points.  Hence these operation will modify the target
   * file system.
   *
   * Verify the operation via mountfs (ie fSys) and *also* via the
   *  target file system (ie fSysLocal) that the mount link points-to.
   */
  private void testOperationsThroughMountLinksInternal(boolean located)
      throws IOException {
    // Create file
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    assertTrue(fsView.isFile(new Path("/user/foo")),
        "Created file should be type file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "user/foo")),
        "Target of created file should be type file");
    
    // Delete the created file
    assertTrue(fsView.delete(new Path("/user/foo"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/user/foo")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "user/foo")),
        "Target File should not exist after delete");
    
    // Create file with a 2 component dirs
    fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/foo");
    assertTrue(fsView.isFile(new Path("/internalDir/linkToDir2/foo")),
        "Created file should be type file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "dir2/foo")),
        "Target of created file should be type file");
    
    // Delete the created file
    assertTrue(fsView.delete(new Path("/internalDir/linkToDir2/foo"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/internalDir/linkToDir2/foo")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "dir2/foo")),
        "Target File should not exist after delete");
    
    
    // Create file with a 3 component dirs
    fileSystemTestHelper.createFile(fsView, "/internalDir/internalDir2/linkToDir3/foo");
    assertTrue(fsView.isFile(new Path("/internalDir/internalDir2/linkToDir3/foo")),
        "Created file should be type file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "dir3/foo")),
        "Target of created file should be type file");
    
    // Recursive Create file with missing dirs
    fileSystemTestHelper.createFile(fsView,
        "/internalDir/linkToDir2/missingDir/miss2/foo");
    assertTrue(fsView.isFile(new Path("/internalDir/linkToDir2/missingDir/miss2/foo")),
        "Created file should be type file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "dir2/missingDir/miss2/foo")),
        "Target of created file should be type file");

    
    // Delete the created file
    assertTrue(fsView.delete(
            new Path("/internalDir/internalDir2/linkToDir3/foo"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/internalDir/internalDir2/linkToDir3/foo")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "dir3/foo")),
        "Target File should not exist after delete");
    
      
    // mkdir
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    assertTrue(fsView.isDirectory(new Path("/user/dirX")),
        "New dir should be type dir");
    assertTrue(fsTarget.isDirectory(new Path(targetTestRoot, "user/dirX")),
        "Target of new dir should be of type dir");
    
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
    assertTrue(fsView.isDirectory(new Path("/user/dirX/dirY")),
        "New dir should be type dir");
    assertTrue(fsTarget.isDirectory(new Path(targetTestRoot, "user/dirX/dirY")),
        "Target of new dir should be of type dir");
    

    // Delete the created dir
    assertTrue(fsView.delete(new Path("/user/dirX/dirY"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/user/dirX/dirY")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "user/dirX/dirY")),
        "Target File should not exist after delete");
    
    assertTrue(fsView.delete(new Path("/user/dirX"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/user/dirX")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "user/dirX")));
    
    // Rename a file 
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/user/fooBar"));
    assertFalse(fsView.exists(new Path("/user/foo")),
        "Renamed src should not exist");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "user/foo")),
        "Renamed src should not exist in target");
    assertTrue(fsView.isFile(fileSystemTestHelper.getTestRootPath(fsView, "/user/fooBar")),
        "Renamed dest should  exist as file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "user/fooBar")),
        "Renamed dest should  exist as file in target");
    
    fsView.mkdirs(new Path("/user/dirFoo"));
    fsView.rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
    assertFalse(fsView.exists(new Path("/user/dirFoo")),
        "Renamed src should not exist");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "user/dirFoo")),
        "Renamed src should not exist in target");
    assertTrue(fsView.isDirectory(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirFooBar")),
        "Renamed dest should  exist as dir");
    assertTrue(fsTarget.isDirectory(new Path(targetTestRoot, "user/dirFooBar")),
        "Renamed dest should  exist as dir in target");
    
    // Make a directory under a directory that's mounted from the root of another FS
    fsView.mkdirs(new Path("/targetRoot/dirFoo"));
    assertTrue(fsView.exists(new Path("/targetRoot/dirFoo")));
    boolean dirFooPresent = false;
    for (FileStatus fileStatus :
        listStatusInternal(located, new Path("/targetRoot/"))) {
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    assertTrue(dirFooPresent);
  }
  
  // rename across mount points that point to same target also fail 
  @Test
  public void testRenameAcrossMounts1() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      fsView.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }
  
  
  // rename across mount points fail if the mount link targets are different
  // even if the targets are part of the same target FS

  @Test
  public void testRenameAcrossMounts2() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      fsView.rename(new Path("/user/foo"), new Path("/data/fooBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }

  // RenameStrategy SAME_TARGET_URI_ACROSS_MOUNTPOINT enabled
  // to rename across mount points that point to same target URI
  @Test
  public void testRenameAcrossMounts3() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/user/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/user2/fooBarBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBarBar"));
  }

  // RenameStrategy SAME_FILESYSTEM_ACROSS_MOUNTPOINT enabled
  // to rename across mount points where the mount link targets are different
  // but are part of the same target FS
  @Test
  public void testRenameAcrossMounts4() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/data/fooBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/user/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/data/fooBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "data/fooBar"));
  }


  // rename across nested mount points that point to same target also fail
  @Test
  public void testRenameAcrossNestedMountPointSameTarget() throws IOException {
    setUpNestedMountPoint();
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      // Nested mount points point to the same target should fail
      // /user -> /user
      // /user/userA -> /user
      // Rename strategy: SAME_MOUNTPOINT
      fsView.rename(new Path("/user/foo"), new Path("/user/userA/foo"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }


  // rename across nested mount points fail if the mount link targets are different
  // even if the targets are part of the same target FS
  @Test
  public void testRenameAcrossMountPointDifferentTarget() throws IOException {
    setUpNestedMountPoint();
    fileSystemTestHelper.createFile(fsView, "/data/foo");
    // /data -> /data
    // /data/dataA -> /dataA
    // Rename strategy: SAME_MOUNTPOINT
    try {
      fsView.rename(new Path("/data/foo"), new Path("/data/dataA/fooBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }

  // RenameStrategy SAME_TARGET_URI_ACROSS_MOUNTPOINT enabled
  // to rename across nested mount points that point to same target URI
  @Test
  public void testRenameAcrossNestedMountPointSameTargetUriAcrossMountPoint() throws IOException {
    setUpNestedMountPoint();
    //  /user/foo -> /user
    // /user/userA/fooBarBar -> /user
    // Rename strategy: SAME_TARGET_URI_ACROSS_MOUNTPOINT
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/user/userA/fooBarBar"));
    ContractTestUtils.assertPathDoesNotExist(fsView2, "src should not exist after rename",
        new Path("/user/foo"));
    ContractTestUtils.assertPathDoesNotExist(fsTarget, "src should not exist after rename",
        new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2, fileSystemTestHelper.getTestRootPath(fsView2, "/user/userA/fooBarBar"));
    ContractTestUtils.assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBarBar"));
  }

  // RenameStrategy SAME_FILESYSTEM_ACROSS_MOUNTPOINT enabled
  // to rename across mount points where the mount link targets are different
  // but are part of the same target FS
  @Test
  public void testRenameAcrossNestedMountPointSameFileSystemAcrossMountPoint() throws IOException {
    setUpNestedMountPoint();
    // /data/foo -> /data
    // /data/dataA/fooBar -> /dataA
    // Rename strategy: SAME_FILESYSTEM_ACROSS_MOUNTPOINT
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/data/foo");
    fsView2.rename(new Path("/data/foo"), new Path("/data/dataB/fooBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/data/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "data/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/user/fooBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBar"));
  }

  @Test
  public void testOperationsThroughNestedMountPointsInternal()
      throws IOException {
    setUpNestedMountPoint();
    // Create file with nested mount point
    fileSystemTestHelper.createFile(fsView, "/user/userB/foo");
    assertTrue(fsView.getFileStatus(new Path("/user/userB/foo")).isFile(),
        "Created file should be type file");
    assertTrue(fsTarget.getFileStatus(new Path(targetTestRoot, "userB/foo")).isFile(),
        "Target of created file should be type file");

    // Delete the created file with nested mount point
    assertTrue(fsView.delete(new Path("/user/userB/foo"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/user/userB/foo")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "userB/foo")),
        "Target File should not exist after delete");

    // Create file with a 2 component dirs with nested mount point
    fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/linkToDir2/foo");
    assertTrue(fsView.getFileStatus(new Path("/internalDir/linkToDir2/linkToDir2/foo")).isFile(),
        "Created file should be type file");
    assertTrue(fsTarget.getFileStatus(new Path(targetTestRoot, "linkToDir2/foo")).isFile(),
        "Target of created file should be type file");

    // Delete the created file with nested mount point
    assertTrue(fsView.delete(new Path("/internalDir/linkToDir2/linkToDir2/foo"), false),
        "Delete should succeed");
    assertFalse(fsView.exists(new Path("/internalDir/linkToDir2/linkToDir2/foo")),
        "File should not exist after delete");
    assertFalse(fsTarget.exists(new Path(targetTestRoot, "linkToDir2/foo")),
        "Target File should not exist after delete");
  }

  private void setUpNestedMountPoint() throws IOException {
    // Enable nested mount point, ViewFilesystem should support both non-nested and nested mount points
    ConfigUtil.setIsNestedMountPointSupported(conf, true);
    ConfigUtil.addLink(conf, "/user/userA",
        new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/user/userB",
        new Path(targetTestRoot, "userB").toUri());
    ConfigUtil.addLink(conf, "/data/dataA",
        new Path(targetTestRoot, "dataA").toUri());
    ConfigUtil.addLink(conf, "/data/dataB",
        new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2/linkToDir2",
        new Path(targetTestRoot,"linkToDir2").toUri());
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }

  static protected boolean SupportsBlocks = false; //  local fs use 1 block
                                                   // override for HDFS
  @Test
  public void testGetBlockLocations() throws IOException {
    Path targetFilePath = new Path(targetTestRoot,"data/largeFile");
    FileSystemTestHelper.createFile(fsTarget, 
        targetFilePath, 10, 1024);
    Path viewFilePath = new Path("/data/largeFile");
    assertTrue(fsView.isFile(viewFilePath),
        "Created File should be type File");
    BlockLocation[] viewBL = fsView.getFileBlockLocations(fsView.getFileStatus(viewFilePath), 0, 10240+100);
    assertEquals(SupportsBlocks ? 10 : 1, viewBL.length);
    BlockLocation[] targetBL = fsTarget.getFileBlockLocations(fsTarget.getFileStatus(targetFilePath), 0, 10240+100);
    compareBLs(viewBL, targetBL);
    
    
    // Same test but now get it via the FileStatus Parameter
    fsView.getFileBlockLocations(
        fsView.getFileStatus(viewFilePath), 0, 10240+100);
    targetBL = fsTarget.getFileBlockLocations(
        fsTarget.getFileStatus(targetFilePath), 0, 10240+100);
    compareBLs(viewBL, targetBL);  
  }
  
  void compareBLs(BlockLocation[] viewBL, BlockLocation[] targetBL) {
    assertEquals(viewBL.length, targetBL.length);
    int i = 0;
    for (BlockLocation vbl : viewBL) {
      assertEquals(vbl.toString(), targetBL[i].toString());
      assertEquals(vbl.getOffset(), targetBL[i].getOffset());
      assertEquals(vbl.getLength(), targetBL[i].getLength());
      i++;
    }
  }

  @Test
  public void testLocatedListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(true);
  }


  /**
   * Test "readOps" (e.g. list, listStatus) 
   * on internal dirs of mount table
   * These operations should succeed.
   */
  
  // test list on internal dirs of mount table 
  @Test
  public void testListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(false);
  }

  private void testListOnInternalDirsOfMountTableInternal(boolean located)
      throws IOException {
    
    // list on Slash

    FileStatus[] dirPaths = listStatusInternal(located, new Path("/"));
    FileStatus fs;
    verifyRootChildren(dirPaths);

    // list on internal dir
    dirPaths = listStatusInternal(located, new Path("/internalDir"));
    assertEquals(dirPaths.length, 2);

    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/internalDir2", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory(), "A mount should appear as symlink");
    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/linkToDir2",
        dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
  }

  private void verifyRootChildren(FileStatus[] dirPaths) throws IOException {
    FileStatus fs;
    assertEquals(dirPaths.length, getExpectedDirPaths());
    fs = fileSystemTestHelper.containsPath(fsView, "/user", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileSystemTestHelper.containsPath(fsView, "/data", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory(), "A mount should appear as symlink");
    fs = fileSystemTestHelper.containsPath(fsView, "/danglingLink", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileSystemTestHelper.containsPath(fsView, "/linkToAFile", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
  }

  int getExpectedDirPaths() {
    return 7;
  }
  
  @Test
  public void testListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(false);
  }

  @Test
  public void testLocatedListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(true);
  }

  private void testListOnMountTargetDirsInternal(boolean located)
      throws IOException {
    final Path dataPath = new Path("/data");

    FileStatus[] dirPaths = listStatusInternal(located, dataPath);

    FileStatus fs;
    assertEquals(dirPaths.length, 0);
    
    // add a file
    long len = fileSystemTestHelper.createFile(fsView, "/data/foo");
    dirPaths = listStatusInternal(located, dataPath);
    assertEquals(dirPaths.length, 1);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isFile(), "Created file shoudl appear as a file");
    assertEquals(fs.getLen(), len);
    
    // add a dir
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/data/dirX"));
    dirPaths = listStatusInternal(located, dataPath);
    assertEquals(dirPaths.length, 2);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isFile(), "Created file shoudl appear as a file");
    fs = fileSystemTestHelper.containsPath(fsView, "/data/dirX", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory(), "Created dir should appear as a dir");
  }

  private FileStatus[] listStatusInternal(boolean located, Path dataPath) throws IOException {
    FileStatus[] dirPaths = new FileStatus[0];
    if (located) {
      RemoteIterator<LocatedFileStatus> statIter =
          fsView.listLocatedStatus(dataPath);
      ArrayList<LocatedFileStatus> tmp = new ArrayList<LocatedFileStatus>(10);
      while (statIter.hasNext()) {
        tmp.add(statIter.next());
      }
      dirPaths = tmp.toArray(dirPaths);
    } else {
      dirPaths = fsView.listStatus(dataPath);
    }
    return dirPaths;
  }

  @Test
  public void testFileStatusOnMountLink() throws IOException {
    assertTrue(fsView.getFileStatus(new Path("/")).isDirectory());
    checkFileStatus(fsView, "/", fileType.isDir);
    checkFileStatus(fsView, "/user", fileType.isDir); // link followed => dir
    checkFileStatus(fsView, "/data", fileType.isDir);
    checkFileStatus(fsView, "/internalDir", fileType.isDir);
    checkFileStatus(fsView, "/internalDir/linkToDir2", fileType.isDir);
    checkFileStatus(fsView, "/internalDir/internalDir2/linkToDir3",
        fileType.isDir);
    checkFileStatus(fsView, "/linkToAFile", fileType.isFile);
  }
  
  @Test
  public void testgetFSonDanglingLink() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.getFileStatus(new Path("/danglingLink")));
  }
  
  @Test
  public void testgetFSonNonExistingInternalDir() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.getFileStatus(new Path("/internalDir/nonExisting")));
  }
  
  /*
   * Test resolvePath(p) 
   */
  
  @Test
  public void testResolvePathInternalPaths() throws IOException {
    assertEquals(fsView.resolvePath(new Path("/")), new Path("/"));
    assertEquals(fsView.resolvePath(new Path("/internalDir")),
        new Path("/internalDir"));
  }
  @Test
  public void testResolvePathMountPoints() throws IOException {
    assertEquals(fsView.resolvePath(new Path("/user")),
        new Path(targetTestRoot,"user"));
    assertEquals(fsView.resolvePath(new Path("/data")),
        new Path(targetTestRoot,"data"));
    assertEquals(fsView.resolvePath(new Path("/internalDir/linkToDir2")),
        new Path(targetTestRoot,"dir2"));
    assertEquals(fsView.resolvePath(new Path("/internalDir/internalDir2/linkToDir3")),
        new Path(targetTestRoot,"dir3"));

  }
  
  @Test
  public void testResolvePathThroughMountPoints() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    assertEquals(fsView.resolvePath(new Path("/user/foo")),
        new Path(targetTestRoot,"user/foo"));
    
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    assertEquals(fsView.resolvePath(new Path("/user/dirX")),
        new Path(targetTestRoot,"user/dirX"));


    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
    assertEquals(fsView.resolvePath(new Path("/user/dirX/dirY")),
        new Path(targetTestRoot,"user/dirX/dirY"));
  }

  @Test
  public void testResolvePathDanglingLink() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.resolvePath(new Path("/danglingLink")));
  }
  
  @Test
  public void testResolvePathMissingThroughMountPoints() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.resolvePath(new Path("/user/nonExisting")));
  }
  

  @Test
  public void testResolvePathMissingThroughMountPoints2() throws IOException {
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    assertThrows(FileNotFoundException.class,
        () -> fsView.resolvePath(new Path("/user/dirX/nonExisting")));
  }
  
  /**
   * Test modify operations (create, mkdir, rename, etc) 
   * on internal dirs of mount table
   * These operations should fail since the mount table is read-only or
   * because the internal dir that it is trying to create already
   * exits.
   */
 
 
  // Mkdir on existing internal mount table succeed except for /
  @Test
  public void testInternalMkdirSlash() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/")));
  }
  
  public void testInternalMkdirExisting1() throws IOException {
    assertTrue(fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView,
        "/internalDir")), "mkdir of existing dir should succeed");
  }

  public void testInternalMkdirExisting2() throws IOException {
    assertTrue(fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView,
        "/internalDir/linkToDir2")), "mkdir of existing dir should succeed");
  }
  
  // Mkdir for new internal mount table should fail
  @Test
  public void testInternalMkdirNew() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/dirNew")));
  }
  @Test
  public void testInternalMkdirNew2() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/internalDir/dirNew")));
  }
  
  // Create File on internal mount table should fail
  
  @Test
  public void testInternalCreate1() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fileSystemTestHelper.createFile(fsView, "/foo")); // 1 component
  }
  
  @Test
  public void testInternalCreate2() throws IOException {  // 2 component
    assertThrows(AccessControlException.class,
        () -> fileSystemTestHelper.createFile(fsView, "/internalDir/foo"));
  }
  
  @Test
  public void testInternalCreateMissingDir() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fileSystemTestHelper.createFile(fsView, "/missingDir/foo"));
  }
  
  @Test
  public void testInternalCreateMissingDir2() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fileSystemTestHelper.createFile(fsView, "/missingDir/miss2/foo"));
  }
  
  
  @Test
  public void testInternalCreateMissingDir3() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fileSystemTestHelper.createFile(fsView, "/internalDir/miss2/foo"));
  }
  
  // Delete on internal mount table should fail
  
  @Test
  public void testInternalDeleteNonExisting() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.delete(new Path("/NonExisting"), false));
  }
  @Test
  public void testInternalDeleteNonExisting2() throws IOException {
    assertThrows(FileNotFoundException.class,
        () -> fsView.delete(new Path("/internalDir/NonExisting"), false));
  }
  @Test
  public void testInternalDeleteExisting() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.delete(new Path("/internalDir"), false));
  }
  @Test
  public void testInternalDeleteExisting2() throws IOException {
    fsView.getFileStatus(
            new Path("/internalDir/linkToDir2")).isDirectory();
    assertThrows(AccessControlException.class,
        () -> fsView.delete(new Path("/internalDir/linkToDir2"), false));
  } 
  
  @Test
  public void testMkdirOfMountLink() throws IOException {
    // data exists - mkdirs returns true even though no permission in internal
    // mount table
    assertTrue(fsView.mkdirs(new Path("/data")),
        "mkdir of existing mount link should succeed");
  }
  
  
  // Rename on internal mount table should fail
  
  @Test
  public void testInternalRename1() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.rename(new Path("/internalDir"), new Path("/newDir")));
  }
  @Test
  public void testInternalRename2() throws IOException {
    fsView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory();
    assertThrows(AccessControlException.class,
        () -> fsView.rename(new Path("/internalDir/linkToDir2"),
        new Path("/internalDir/dir1")));
  }
  @Test
  public void testInternalRename3() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.rename(new Path("/user"), new Path("/internalDir/linkToDir2")));
  }
  @Test
  public void testInternalRenameToSlash() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.rename(new Path("/internalDir/linkToDir2/foo"), new Path("/")));
  }
  @Test
  public void testInternalRenameFromSlash() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.rename(new Path("/"), new Path("/bar")));
  }
  
  @Test
  public void testInternalSetOwner() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.setOwner(new Path("/internalDir"), "foo", "bar"));
  }
  
  @Test
  public void testCreateNonRecursive() throws IOException {
    Path path = fileSystemTestHelper.getTestRootPath(fsView, "/user/foo");
    fsView.createNonRecursive(path, false, 1024, (short)1, 1024L, null);
    FileStatus status = fsView.getFileStatus(new Path("/user/foo"));
    assertTrue(fsView.isFile(new Path("/user/foo")),
        "Created file should be type file");
    assertTrue(fsTarget.isFile(new Path(targetTestRoot, "user/foo")),
        "Target of created file should be type file");
  }

  @Test
  public void testRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(false);
  }

  @Test
  public void testLocatedRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(true);
  }

  private void testRootReadableExecutableInternal(boolean located)
      throws IOException {
    // verify executable permission on root: cd /
    //
    assertFalse(fsView.getWorkingDirectory().isRoot(),
        "In root before cd");
    fsView.setWorkingDirectory(new Path("/"));
    assertTrue(fsView.getWorkingDirectory().isRoot(),
        "Not in root dir after cd");

    // verify readable
    //
    verifyRootChildren(listStatusInternal(located,
        fsView.getWorkingDirectory()));

    // verify permissions
    //
    final FileStatus rootStatus =
        fsView.getFileStatus(fsView.getWorkingDirectory());
    final FsPermission perms = rootStatus.getPermission();

    assertTrue(perms.getUserAction().implies(FsAction.EXECUTE),
        "User-executable permission not set!");
    assertTrue(perms.getUserAction().implies(FsAction.READ),
        "User-readable permission not set!");
    assertTrue(perms.getGroupAction().implies(FsAction.EXECUTE),
        "Group-executable permission not set!");
    assertTrue(perms.getGroupAction().implies(FsAction.READ),
        "Group-readable permission not set!");
    assertTrue(perms.getOtherAction().implies(FsAction.EXECUTE),
        "Other-executable permission not set!");
    assertTrue(perms.getOtherAction().implies(FsAction.READ),
        "Other-readable permission not set!");
  }

  /**
   * Verify the behavior of ACL operations on paths above the root of
   * any mount table entry.
   */

  @Test
  public void testInternalModifyAclEntries() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.modifyAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>()));
  }

  @Test
  public void testInternalRemoveAclEntries() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.removeAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>()));
  }

  @Test
  public void testInternalRemoveDefaultAcl() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.removeDefaultAcl(new Path("/internalDir")));
  }

  @Test
  public void testInternalRemoveAcl() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.removeAcl(new Path("/internalDir")));
  }

  @Test
  public void testInternalSetAcl() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.setAcl(new Path("/internalDir"), new ArrayList<AclEntry>()));
  }

  @Test
  public void testInternalGetAclStatus() throws IOException {
    final UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
    AclStatus aclStatus = fsView.getAclStatus(new Path("/internalDir"));
    assertEquals(currentUser.getUserName(), aclStatus.getOwner());
    assertEquals(currentUser.getGroupNames()[0], aclStatus.getGroup());
    assertEquals(AclUtil.getMinimalAcl(PERMISSION_555), aclStatus.getEntries());
    assertFalse(aclStatus.isStickyBit());
  }

  @Test
  public void testInternalSetXAttr() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.setXAttr(new Path("/internalDir"), "xattrName", null));
  }

  @Test
  public void testInternalGetXAttr() throws IOException {
    assertThrows(NotInMountpointException.class,
        () -> fsView.getXAttr(new Path("/internalDir"), "xattrName"));
  }

  @Test
  public void testInternalGetXAttrs() throws IOException {
    assertThrows(NotInMountpointException.class,
        () -> fsView.getXAttrs(new Path("/internalDir")));
  }

  @Test
  public void testInternalGetXAttrsWithNames() throws IOException {
    assertThrows(NotInMountpointException.class,
        () -> fsView.getXAttrs(new Path("/internalDir"), new ArrayList<String>()));
  }

  @Test
  public void testInternalListXAttr() throws IOException {
    assertThrows(NotInMountpointException.class,
        () -> fsView.listXAttrs(new Path("/internalDir")));
  }

  @Test
  public void testInternalRemoveXAttr() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.removeXAttr(new Path("/internalDir"), "xattrName"));
  }

  @Test
  public void testInternalCreateSnapshot1() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.createSnapshot(new Path("/internalDir")));
  }

  @Test
  public void testInternalCreateSnapshot2() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.createSnapshot(new Path("/internalDir"), "snap1"));
  }

  @Test
  public void testInternalRenameSnapshot() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.renameSnapshot(new Path("/internalDir"), "snapOldName",
        "snapNewName"));
  }

  @Test
  public void testInternalDeleteSnapshot() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.deleteSnapshot(new Path("/internalDir"), "snap1"));
  }

  @Test
  public void testInternalSetStoragePolicy() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.setStoragePolicy(new Path("/internalDir"), "HOT"));
  }

  @Test
  public void testInternalUnsetStoragePolicy() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.unsetStoragePolicy(new Path("/internalDir")));
  }

  @Test
  public void testInternalSatisfyStoragePolicy() throws IOException {
    assertThrows(AccessControlException.class,
        () -> fsView.satisfyStoragePolicy(new Path("/internalDir")));
  }

  @Test
  public void testInternalgetStoragePolicy() throws IOException {
    assertThrows(NotInMountpointException.class,
        () -> fsView.getStoragePolicy(new Path("/internalDir")));
  }

  @Test
  public void testInternalGetAllStoragePolicies() throws IOException {
    Collection<? extends BlockStoragePolicySpi> policies =
        fsView.getAllStoragePolicies();
    for (FileSystem fs : fsView.getChildFileSystems()) {
      try {
        for (BlockStoragePolicySpi s : fs.getAllStoragePolicies()) {
          assertTrue(policies.contains(s), "Missing policy: " + s);
        }
      } catch (UnsupportedOperationException e) {
        // ignore
      }
    }
  }

  @Test
  public void testConfLinkSlash() throws Exception {
    String clusterName = "ClusterX";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);

    Configuration newConf = new Configuration();
    ConfigUtil.addLink(newConf, clusterName, "/",
        new Path(targetTestRoot, "/").toUri());

    String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + ".";
    try {
      FileSystem.get(viewFsUri, newConf);
      fail("ViewFileSystem should error out on mount table entry: "
          + mtPrefix + Constants.CONFIG_VIEWFS_LINK + "." + "/");
    } catch (Exception e) {
      if (e instanceof UnsupportedFileSystemException) {
        String msg = " Use " + Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH +
            " instead";
        GenericTestUtils.assertExceptionContains(msg, e);
      } else {
        fail("Unexpected exception: " + e.getMessage());
      }
    }
  }

  @Test
  public void testTrashRoot() throws IOException {

    Path mountDataRootPath = new Path("/data");
    Path fsTargetFilePath = new Path("debug.log");
    Path mountDataFilePath = new Path(mountDataRootPath, fsTargetFilePath);
    Path mountDataNonExistingFilePath = new Path(mountDataRootPath, "no.log");
    fileSystemTestHelper.createFile(fsTarget, fsTargetFilePath);

    // Get Trash roots for paths via ViewFileSystem handle
    Path mountDataRootTrashPath = fsView.getTrashRoot(mountDataRootPath);
    Path mountDataFileTrashPath = fsView.getTrashRoot(mountDataFilePath);

    // Get Trash roots for the same set of paths via the mounted filesystem
    Path fsTargetRootTrashRoot = fsTarget.getTrashRoot(mountDataRootPath);
    Path fsTargetFileTrashPath = fsTarget.getTrashRoot(mountDataFilePath);

    // Verify if Trash roots from ViewFileSystem matches that of the ones
    // from the target mounted FileSystem.
    assertEquals(fsTargetRootTrashRoot.toUri().getPath(),
        mountDataRootTrashPath.toUri().getPath());
    assertEquals(fsTargetFileTrashPath.toUri().getPath(),
        mountDataFileTrashPath.toUri().getPath());
    assertEquals(mountDataFileTrashPath.toUri().getPath(),
        mountDataRootTrashPath.toUri().getPath());


    // Verify trash root for an non-existing file but on a valid mountpoint.
    Path trashRoot = fsView.getTrashRoot(mountDataNonExistingFilePath);
    assertEquals(trashRoot.toUri().getPath(),
        mountDataRootTrashPath.toUri().getPath());

    // Verify trash root for invalid mounts.
    Path invalidMountRootPath = new Path("/invalid_mount");
    Path invalidMountFilePath = new Path(invalidMountRootPath, "debug.log");
    try {
      fsView.getTrashRoot(invalidMountRootPath);
      fail("ViewFileSystem getTashRoot should fail for non-mountpoint paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }
    try {
      fsView.getTrashRoot(invalidMountFilePath);
      fail("ViewFileSystem getTashRoot should fail for non-mountpoint paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }
    try {
      fsView.getTrashRoot(null);
      fail("ViewFileSystem getTashRoot should fail for empty paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }

    // Move the file to trash
    FileStatus fileStatus = fsTarget.getFileStatus(fsTargetFilePath);
    Configuration newConf = new Configuration(conf);
    newConf.setLong("fs.trash.interval", 1000);
    Trash lTrash = new Trash(fsTarget, newConf);
    boolean trashed = lTrash.moveToTrash(fsTargetFilePath);
    assertTrue(trashed, "File " + fileStatus + " move to " +
        "trash failed.");

    // Verify ViewFileSystem trash roots shows the ones from
    // target mounted FileSystem.
    assertTrue(fsView.getTrashRoots(true).size() > 0, "");
  }

  // Default implementation of getTrashRoot for a fallback FS mounted at root:
  // e.g., fallbackFS.uri.getPath = '/'
  Path getTrashRootInFallBackFS() throws IOException {
    return new Path(fsTarget.getHomeDirectory().toUri().getPath(),
        TRASH_PREFIX);
  }

  /**
   * Test TRASH_FORCE_INSIDE_MOUNT_POINT feature for getTrashRoot.
   */
  @Test
  public void testTrashRootForceInsideMountPoint() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Configuration conf2 = new Configuration(conf);
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, true);
    ConfigUtil.addLinkFallback(conf2, targetTestRoot.toUri());
    FileSystem fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);

    // Case 1: path p in the /data mount point.
    // Return a trash root within the /data mount point.
    Path dataTestPath = new Path("/data/dir/file");
    Path dataTrashRoot = fsView2.makeQualified(
        new Path("/data/" + TRASH_PREFIX + "/" + ugi.getShortUserName()));
    assertEquals(fsView2.getTrashRoot(dataTestPath), dataTrashRoot);

    // Case 2: path p not found in mount table.
    // Return a trash root in fallback FS.
    Path nonExistentPath = new Path("/nonExistentDir/nonExistentFile");
    Path expectedTrash =
        fsView2.makeQualified(getTrashRootInFallBackFS());
    assertEquals(fsView2.getTrashRoot(nonExistentPath), expectedTrash);

    // Case 3: turn off the CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT flag.
    // Return a trash root in user home dir.
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, false);
    fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);
    Path targetFSUserHomeTrashRoot = fsTarget.makeQualified(
        new Path(fsTarget.getHomeDirectory(), TRASH_PREFIX));
    assertEquals(fsView2.getTrashRoot(dataTestPath),
        targetFSUserHomeTrashRoot);

    // Case 4: viewFS without fallback. Expect exception for a nonExistent path
    conf2 = new Configuration(conf);
    fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);
    try {
      fsView2.getTrashRoot(nonExistentPath);
    } catch (NotInMountpointException ignored) {
    }
  }

  /**
   * A mocked FileSystem which returns a deep trash dir.
   */
  static class DeepTrashRootMockFS extends MockFileSystem {
    public static final Path TRASH =
        new Path("/vol/very/deep/deep/trash/dir/.Trash");

    @Override
    public Path getTrashRoot(Path path) {
      return TRASH;
    }
  }

  /**
   * Test getTrashRoot that is very deep inside a mount point.
   */
  @Test
  public void testTrashRootDeepTrashDir() throws IOException {

    Configuration conf2 = ViewFileSystemTestSetup.createConfig();
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, true);
    conf2.setClass("fs.mocktrashfs.impl", DeepTrashRootMockFS.class,
        FileSystem.class);
    ConfigUtil.addLink(conf2, "/mnt/datavol1",
        URI.create("mocktrashfs://localhost/vol"));
    Path testPath = new Path("/mnt/datavol1/projs/proj");
    FileSystem fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);
    Path expectedTrash = fsView2.makeQualified(
        new Path("/mnt/datavol1/very/deep/deep/trash/dir/.Trash"));
    assertEquals(fsView2.getTrashRoot(testPath), expectedTrash);
  }

  /**
   * Test getTrashRoots() for all users.
   */
  @Test
  public void testTrashRootsAllUsers() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, true);
    FileSystem fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);

    // Case 1: verify correct trash roots from fsView and fsView2
    int beforeTrashRootNum = fsView.getTrashRoots(true).size();
    int beforeTrashRootNum2 = fsView2.getTrashRoots(true).size();
    assertEquals(beforeTrashRootNum2, beforeTrashRootNum);

    fsView.mkdirs(new Path("/data/" + TRASH_PREFIX + "/user1"));
    fsView.mkdirs(new Path("/data/" + TRASH_PREFIX + "/user2"));
    fsView.mkdirs(new Path("/user/" + TRASH_PREFIX + "/user3"));
    fsView.mkdirs(new Path("/user/" + TRASH_PREFIX + "/user4"));
    fsView.mkdirs(new Path("/user2/" + TRASH_PREFIX + "/user5"));
    int afterTrashRootsNum = fsView.getTrashRoots(true).size();
    int afterTrashRootsNum2 = fsView2.getTrashRoots(true).size();
    assertEquals(afterTrashRootsNum, beforeTrashRootNum);
    assertEquals(afterTrashRootsNum2, beforeTrashRootNum2 + 5);

    // Case 2: per-user mount point
    fsTarget.mkdirs(new Path(targetTestRoot, "Users/userA/.Trash/userA"));
    Configuration conf3 = new Configuration(conf2);
    ConfigUtil.addLink(conf3, "/Users/userA",
        new Path(targetTestRoot, "Users/userA").toUri());
    FileSystem fsView3 = FileSystem.get(FsConstants.VIEWFS_URI, conf3);
    int trashRootsNum3 = fsView3.getTrashRoots(true).size();
    assertEquals(trashRootsNum3, afterTrashRootsNum2 + 1);

    // Case 3: single /Users mount point for all users
    fsTarget.mkdirs(new Path(targetTestRoot, "Users/.Trash/user1"));
    fsTarget.mkdirs(new Path(targetTestRoot, "Users/.Trash/user2"));
    Configuration conf4 = new Configuration(conf2);
    ConfigUtil.addLink(conf4, "/Users",
        new Path(targetTestRoot, "Users").toUri());
    FileSystem fsView4 = FileSystem.get(FsConstants.VIEWFS_URI, conf4);
    int trashRootsNum4 = fsView4.getTrashRoots(true).size();
    assertEquals(trashRootsNum4, afterTrashRootsNum2 + 2);

    // Case 4: test trash roots in fallback FS
    fsTarget.mkdirs(new Path(targetTestRoot, ".Trash/user10"));
    fsTarget.mkdirs(new Path(targetTestRoot, ".Trash/user11"));
    fsTarget.mkdirs(new Path(targetTestRoot, ".Trash/user12"));
    Configuration conf5 = new Configuration(conf2);
    ConfigUtil.addLinkFallback(conf5, targetTestRoot.toUri());
    FileSystem fsView5 = FileSystem.get(FsConstants.VIEWFS_URI, conf5);
    int trashRootsNum5 = fsView5.getTrashRoots(true).size();
    assertEquals(trashRootsNum5, afterTrashRootsNum2 + 3);
  }

  /**
   * Test getTrashRoots() for current user.
   */
  @Test
  public void testTrashRootsCurrentUser() throws IOException {
    String currentUser =
        UserGroupInformation.getCurrentUser().getShortUserName();
    Configuration conf2 = new Configuration(conf);
    conf2.setBoolean(CONFIG_VIEWFS_TRASH_FORCE_INSIDE_MOUNT_POINT, true);
    FileSystem fsView2 = FileSystem.get(FsConstants.VIEWFS_URI, conf2);

    int beforeTrashRootNum = fsView.getTrashRoots(false).size();
    int beforeTrashRootNum2 = fsView2.getTrashRoots(false).size();
    assertEquals(beforeTrashRootNum2, beforeTrashRootNum);

    fsView.mkdirs(new Path("/data/" + TRASH_PREFIX + "/" + currentUser));
    fsView.mkdirs(new Path("/data/" + TRASH_PREFIX + "/user2"));
    fsView.mkdirs(new Path("/user/" + TRASH_PREFIX + "/" + currentUser));
    fsView.mkdirs(new Path("/user/" + TRASH_PREFIX + "/user4"));
    fsView.mkdirs(new Path("/user2/" + TRASH_PREFIX + "/user5"));
    int afterTrashRootsNum = fsView.getTrashRoots(false).size();
    int afterTrashRootsNum2 = fsView2.getTrashRoots(false).size();
    assertEquals(afterTrashRootsNum, beforeTrashRootNum);
    assertEquals(afterTrashRootsNum2, beforeTrashRootNum2 + 2);

    // Test trash roots in fallback FS
    Configuration conf3 = new Configuration(conf2);
    fsTarget.mkdirs(new Path(targetTestRoot, TRASH_PREFIX + "/" + currentUser));
    ConfigUtil.addLinkFallback(conf3, targetTestRoot.toUri());
    FileSystem fsView3 = FileSystem.get(FsConstants.VIEWFS_URI, conf3);
    int trashRootsNum3 = fsView3.getTrashRoots(false).size();
    assertEquals(trashRootsNum3, afterTrashRootsNum2 + 1);
  }

  @Test
  public void testViewFileSystemUtil() throws Exception {
    Configuration newConf = new Configuration(conf);

    FileSystem fileSystem = FileSystem.get(FsConstants.LOCAL_FS_URI,
        newConf);
    assertFalse(ViewFileSystemUtil.isViewFileSystem(fileSystem),
        "Unexpected FileSystem: " + fileSystem);

    fileSystem = FileSystem.get(FsConstants.VIEWFS_URI,
        newConf);
    assertTrue(ViewFileSystemUtil.isViewFileSystem(fileSystem),
        "Unexpected FileSystem: " + fileSystem);

    // Case 1: Verify FsStatus of root path returns all MountPoints status.
    Map<MountPoint, FsStatus> mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, InodeTree.SlashPath);
    assertEquals(mountPointFsStatusMap.size(), getExpectedMountPoints());

    // Case 2: Verify FsStatus of an internal dir returns all
    // MountPoints status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/internalDir"));
    assertEquals(mountPointFsStatusMap.size(), getExpectedMountPoints());

    // Case 3: Verify FsStatus of a matching MountPoint returns exactly
    // the corresponding MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/user"));
    assertEquals(mountPointFsStatusMap.size(), 1);
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      assertEquals("/user", entry.getKey().getMountedOnPath().toString());
    }

    // Case 4: Verify FsStatus of a path over a MountPoint returns the
    // corresponding MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/user/cloud"));
    assertEquals(mountPointFsStatusMap.size(), 1);
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      assertEquals("/user", entry.getKey().getMountedOnPath().toString());
    }

    // Case 5: Verify FsStatus of any level of an internal dir
    // returns all MountPoints status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem,
            new Path("/internalDir/internalDir2"));
    assertEquals(mountPointFsStatusMap.size(), getExpectedMountPoints());

    // Case 6: Verify FsStatus of a MountPoint URI returns
    // the MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("viewfs:/user/"));
    assertEquals(mountPointFsStatusMap.size(), 1);
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      assertEquals("/user", entry.getKey().getMountedOnPath().toString());
    }

    // Case 7: Verify FsStatus of a non MountPoint path throws exception
    final FileSystem fsCopy = fileSystem;
    assertThrows(NotInMountpointException.class,
        () -> ViewFileSystemUtil.getStatus(fsCopy, new Path("/non-existing")));
  }

  @Test
  public void testCheckOwnerWithFileStatus()
      throws IOException, InterruptedException {
    final UserGroupInformation userUgi = UserGroupInformation
        .createUserForTesting("user@HADOOP.COM", new String[]{"hadoop"});
    userUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String doAsUserName = ugi.getUserName();
        assertEquals("user@HADOOP.COM", doAsUserName);
        FileSystem vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
        FileStatus stat = vfs.getFileStatus(new Path("/internalDir"));
        assertEquals(stat.getOwner(), userUgi.getShortUserName());
        return null;
      }
    });
  }

  @Test
  public void testUsed() throws IOException {
    try {
      fsView.getUsed();
      fail("ViewFileSystem getUsed() should fail for slash root path when the" +
          " slash root mount point is not configured.");
    } catch (NotInMountpointException e) {
      // expected exception.
    }
    long usedSpaceByPathViaViewFs = fsView.getUsed(new Path("/user"));
    long usedSpaceByPathViaTargetFs =
        fsTarget.getUsed(new Path(targetTestRoot, "user"));
    assertEquals(usedSpaceByPathViaTargetFs, usedSpaceByPathViaViewFs,
        "Space used not matching between ViewFileSystem and " +
            "the mounted FileSystem!");

    Path mountDataRootPath = new Path("/data");
    String fsTargetFileName = "debug.log";
    Path fsTargetFilePath = new Path(targetTestRoot, "data/debug.log");
    Path mountDataFilePath = new Path(mountDataRootPath, fsTargetFileName);
    fileSystemTestHelper.createFile(fsTarget, fsTargetFilePath);

    usedSpaceByPathViaViewFs = fsView.getUsed(mountDataFilePath);
    usedSpaceByPathViaTargetFs = fsTarget.getUsed(fsTargetFilePath);
    assertEquals(usedSpaceByPathViaTargetFs, usedSpaceByPathViaViewFs,
        "Space used not matching between ViewFileSystem and " +
            "the mounted FileSystem!");
  }

  @Test
  public void testLinkTarget() throws Exception {

    assumeTrue(fsTarget.supportsSymlinks() &&
        fsTarget.areSymlinksEnabled());

    // Symbolic link
    final String targetFileName = "debug.log";
    final String linkFileName = "debug.link";
    final Path targetFile = new Path(targetTestRoot, targetFileName);
    final Path symLink = new Path(targetTestRoot, linkFileName);

    FileSystemTestHelper.createFile(fsTarget, targetFile);
    fsTarget.createSymlink(targetFile, symLink, false);

    final Path mountTargetRootPath = new Path("/targetRoot");
    final Path mountTargetSymLinkPath = new Path(mountTargetRootPath,
        linkFileName);
    final Path expectedMountLinkTarget = fsTarget.makeQualified(
        new Path(targetTestRoot, targetFileName));
    final Path actualMountLinkTarget = fsView.getLinkTarget(
        mountTargetSymLinkPath);

    assertEquals(expectedMountLinkTarget, actualMountLinkTarget,
        "Resolved link target path not matching!");

    // Relative symbolic link
    final String relativeFileName = "dir2/../" + targetFileName;
    final String link2FileName = "dir2/rel.link";
    final Path relTargetFile = new Path(targetTestRoot, relativeFileName);
    final Path relativeSymLink = new Path(targetTestRoot, link2FileName);
    fsTarget.createSymlink(relTargetFile, relativeSymLink, true);

    final Path mountTargetRelativeSymLinkPath = new Path(mountTargetRootPath,
        link2FileName);
    final Path expectedMountRelLinkTarget = fsTarget.makeQualified(
        new Path(targetTestRoot, relativeFileName));
    final Path actualMountRelLinkTarget = fsView.getLinkTarget(
        mountTargetRelativeSymLinkPath);

    assertEquals(expectedMountRelLinkTarget, actualMountRelLinkTarget,
        "Resolved relative link target path not matching!");

    try {
      fsView.getLinkTarget(new Path("/linkToAFile"));
      fail("Resolving link target for a ViewFs mount link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      GenericTestUtils.assertExceptionContains("not a symbolic link", e);
    }

    try {
      fsView.getLinkTarget(fsView.makeQualified(
          new Path(mountTargetRootPath, targetFileName)));
      fail("Resolving link target for a non sym link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      GenericTestUtils.assertExceptionContains("not a symbolic link", e);
    }

    try {
      fsView.getLinkTarget(new Path("/targetRoot/non-existing-file"));
      fail("Resolving link target for a non existing link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      GenericTestUtils.assertExceptionContains("File does not exist:", e);
    }
  }

  @Test
  public void testViewFileSystemInnerCache() throws Exception {
    ViewFileSystem.InnerCache cache =
        new ViewFileSystem.InnerCache(new FsGetter());
    FileSystem fs = cache.get(fsTarget.getUri(), conf);

    // InnerCache caches filesystem.
    assertSame(cache.get(fsTarget.getUri(), conf), fs);

    // InnerCache and FileSystem.CACHE are independent.
    assertNotSame(FileSystem.get(fsTarget.getUri(), conf), fs);

    // close InnerCache.
    cache.closeAll();
    try {
      fs.exists(new Path("/"));
      if (!(fs instanceof LocalFileSystem)) {
        // Ignore LocalFileSystem because it can still be used after close.
        fail("Expect Filesystem closed exception");
      }
    } catch (IOException e) {
      assertExceptionContains("Filesystem closed", e);
    }
  }

  @Test
  public void testCloseChildrenFileSystem() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());
    config.setBoolean("fs.viewfs.impl.disable.cache", false);
    URI uri = new URI("viewfs://" + clusterName + "/");

    ViewFileSystem viewFs = (ViewFileSystem) FileSystem.get(uri, config);
    assertTrue(viewFs.getChildFileSystems().length > 0,
        "viewfs should have at least one child fs.");
    // viewFs is cached in FileSystem.CACHE
    assertSame(FileSystem.get(uri, config), viewFs);

    // child fs is not cached in FileSystem.CACHE
    FileSystem child = viewFs.getChildFileSystems()[0];
    assertNotSame(FileSystem.get(child.getUri(), config), child);

    viewFs.close();
    for (FileSystem childfs : viewFs.getChildFileSystems()) {
      try {
        childfs.exists(new Path("/"));
        if (!(childfs instanceof LocalFileSystem)) {
          // Ignore LocalFileSystem because it can still be used after close.
          fail("Expect Filesystem closed exception");
        }
      } catch (IOException e) {
        assertExceptionContains("Filesystem closed", e);
      }
    }
  }

  @Test
  public void testChildrenFileSystemLeak() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());

    final int cacheSize = TestFileUtil.getCacheSize();
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem
        .get(new URI("viewfs://" + clusterName + "/"), config);
    viewFs.resolvePath(
        new Path(String.format("viewfs://%s/%s", clusterName, "/user")));
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 1);
    viewFs.close();
    assertEquals(TestFileUtil.getCacheSize(), cacheSize);
  }

  @Test
  public void testDeleteOnExit() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());

    Path testDir = new Path("/user/testDeleteOnExit");
    Path realTestPath = new Path(targetTestRoot, "user/testDeleteOnExit");
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem
        .get(new URI("viewfs://" + clusterName + "/"), config);
    viewFs.mkdirs(testDir);
    assertTrue(viewFs.exists(testDir));
    assertTrue(fsTarget.exists(realTestPath));

    viewFs.deleteOnExit(testDir);
    viewFs.close();
    assertFalse(fsTarget.exists(realTestPath));
  }

  @Test
  public void testGetContentSummary() throws IOException {
    ContentSummary summaryBefore =
        fsView.getContentSummary(new Path("/internalDir"));
    String expected = "GET CONTENT SUMMARY";
    Path filePath =
        new Path("/internalDir/internalDir2/linkToDir3", "foo");

    try (FSDataOutputStream outputStream = fsView.create(filePath)) {
      outputStream.write(expected.getBytes());
    }

    Path newDirPath = new Path("/internalDir/linkToDir2", "bar");
    fsView.mkdirs(newDirPath);

    ContentSummary summaryAfter =
        fsView.getContentSummary(new Path("/internalDir"));
    assertEquals(summaryBefore.getFileCount() + 1,
        summaryAfter.getFileCount(),
        "The file count didn't match");
    assertEquals(summaryBefore.getLength() + expected.length(),
        summaryAfter.getLength(),
        "The size didn't match");
    assertEquals(summaryBefore.getDirectoryCount() + 1,
        summaryAfter.getDirectoryCount(),
        "The directory count didn't match");
  }

  @Test
  public void testGetContentSummaryWithFileInLocalFS() throws Exception {
    ContentSummary summaryBefore =
        fsView.getContentSummary(new Path("/internalDir"));
    String expected = "GET CONTENT SUMMARY";
    File localFile = temporaryFolder.toPath().resolve("localFile").toFile();
    try (FileOutputStream fos = new FileOutputStream(localFile)) {
      fos.write(expected.getBytes());
    }
    ConfigUtil.addLink(conf,
        "/internalDir/internalDir2/linkToLocalFile", localFile.toURI());

    try (FileSystem fs = FileSystem.get(FsConstants.VIEWFS_URI, conf)) {
      ContentSummary summaryAfter =
          fs.getContentSummary(new Path("/internalDir"));
      assertEquals(summaryAfter.getFileCount(),
          summaryBefore.getFileCount() + 1,
          "The file count didn't match");
      assertEquals(summaryAfter.getLength(),
          summaryBefore.getLength() + expected.length(),
          "The directory count didn't match");
    }
  }

  @Test
  public void testTargetFileSystemLazyInitialization() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    config.setBoolean(CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    config.setClass("fs.mockfs.impl",
        TestChRootedFileSystem.MockFileSystem.class, FileSystem.class);
    ConfigUtil.addLink(config, clusterName, "/user",
        URI.create("mockfs://mockauth1/mockpath"));
    ConfigUtil.addLink(config, clusterName,
        "/mock", URI.create("mockfs://mockauth/mockpath"));

    final int cacheSize = TestFileUtil.getCacheSize();
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem
        .get(new URI("viewfs://" + clusterName + "/"), config);

    // As no inner file system instance has been initialized,
    // cache size will remain the same
    // cache is disabled for viewfs scheme, so the viewfs:// instance won't
    // go in the cache even after the initialization
    assertEquals(TestFileUtil.getCacheSize(), cacheSize);

    // This resolve path will initialize the file system corresponding
    // to the mount table entry of the path "/user"
    viewFs.resolvePath(
        new Path(String.format("viewfs://%s/%s", clusterName, "/user")));

    // Cache size will increase by 1.
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 1);
    // This resolve path will initialize the file system corresponding
    // to the mount table entry of the path "/mock"
    viewFs.resolvePath(new Path(String.format("viewfs://%s/%s", clusterName,
        "/mock")));
    // One more file system instance will get initialized.
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 2);
    viewFs.close();
    // Initialized FileSystem instances will not be removed from cache as
    // viewfs inner cache is disabled
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 2);
  }

  @Test
  public void testTargetFileSystemLazyInitializationForChecksumMethods()
      throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    config.setBoolean(CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    config.setClass("fs.othermockfs.impl",
        TestChRootedFileSystem.MockFileSystem.class, FileSystem.class);
    ConfigUtil.addLink(config, clusterName, "/user",
        URI.create("othermockfs://mockauth1/mockpath"));
    ConfigUtil.addLink(config, clusterName,
        "/mock", URI.create("othermockfs://mockauth/mockpath"));

    final int cacheSize = TestFileUtil.getCacheSize();
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem.get(
        new URI("viewfs://" + clusterName + "/"), config);

    // As no inner file system instance has been initialized,
    // cache size will remain the same
    // cache is disabled for viewfs scheme, so the viewfs:// instance won't
    // go in the cache even after the initialization
    assertEquals(TestFileUtil.getCacheSize(), cacheSize);

    // This is not going to initialize any filesystem instance
    viewFs.setVerifyChecksum(true);

    // Cache size will remain the same
    assertEquals(TestFileUtil.getCacheSize(), cacheSize);

    // This resolve path will initialize the file system corresponding
    // to the mount table entry of the path "/user"
    viewFs.getFileChecksum(
        new Path(String.format("viewfs://%s/%s", clusterName, "/user")));

    // Cache size will increase by 1.
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 1);

    viewFs.close();
    // Initialized FileSystem instances will not be removed from cache as
    // viewfs inner cache is disabled
    assertEquals(TestFileUtil.getCacheSize(), cacheSize + 1);
  }

  @Test
  public void testInvalidMountPoints() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    config.set(ConfigUtil.getConfigViewFsPrefix(clusterName) + "." +
        Constants.CONFIG_VIEWFS_LINK + "." + "/invalidPath",
        "othermockfs:|mockauth/mockpath");

    try {
      FileSystem viewFs = FileSystem.get(
          new URI("viewfs://" + clusterName + "/"), config);
      fail("FileSystem should not initialize. Should fail with IOException");
    } catch (IOException ex) {
      assertTrue(ex.getMessage().startsWith("URISyntax exception"),
          "Should get URISyntax Exception");
    }
  }
}
