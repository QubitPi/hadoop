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

package org.apache.hadoop.fs;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * <p>
 * A collection of tests for the {@link FileContext} to test path names passed
 * as URIs. This test should be used for testing an instance of FileContext that
 * has been initialized to a specific default FileSystem such a LocalFileSystem,
 * HDFS,S3, etc, and where path names are passed that are URIs in a different
 * FileSystem.
 * </p>
 * 
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc1</code> and
 * <code>fc2</code>
 * 
 * The tests will do operations on fc1 that use a URI in fc2
 * 
 * {@link FileContext} instance variable.
 * </p>
 */
public abstract class FileContextURIBase {
  private static final String basePath =
      GenericTestUtils.getTempPath("testContextURI");
  private static final Path BASE = new Path(basePath);

  // Matches anything containing <, >, :, ", |, ?, *, or anything that ends with
  // space or dot.
  private static final Pattern WIN_INVALID_FILE_NAME_PATTERN = Pattern.compile(
    "^(.*?[<>\\:\"\\|\\?\\*].*?)|(.*?[ \\.])$");

  protected FileContext fc1;
  protected FileContext fc2;

  //Helper method to make path qualified
  protected Path qualifiedPath(String path, FileContext fc) {
    return fc.makeQualified(new Path(BASE, path));
  }

  @BeforeEach
  public void setUp() throws Exception { }

  @AfterEach
  public void tearDown() throws Exception {
    // Clean up after test completion
    // No need to clean fc1 as fc1 and fc2 points same location
    if (fc2 != null) {
      fc2.delete(BASE, true);
    }
  }

  @Test
  public void testCreateFile() throws IOException {
    String fileNames[] = { 
        "testFile", "test File",
        "test*File", "test#File",
        "test1234", "1234Test",
        "test)File", "test_File", 
        "()&^%$#@!~_+}{><?",
        "  ", "^ " };

    for (String f : fileNames) {
      if (!isTestableFileNameOnPlatform(f)) {
        continue;
      }

      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      assertFalse(exists(fc2, testPath));

      // Now create file
      createFile(fc1, testPath);
      // Ensure fc2 has the created file
      assertTrue(exists(fc2, testPath));
    }
  }

  @Test
  public void testCreateFileWithNullName() throws IOException {
    String fileName = null;

    try {

      Path testPath = qualifiedPath(fileName, fc2);
      // Ensure file does not exist
      assertFalse(exists(fc2, testPath));

      // Create a file on fc2's file system using fc1
      createFile(fc1, testPath);
      fail("Create file with null name should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }

  }

  @Test
  public void testCreateExistingFile() throws Exception {
    String fileName = "testCreateExistingFile";
    Path testPath = qualifiedPath(fileName, fc2);

    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));

    // Create a file on fc2's file system using fc1
    createFile(fc1, testPath);

    // Create same file with fc1
    LambdaTestUtils.intercept(IOException.class, () ->
        createFile(fc2, testPath));

    // Ensure fc2 has the created file
    fc2.getFileStatus(testPath);
  }

  @Test
  public void testCreateFileInNonExistingDirectory() throws IOException {
    String fileName = "testCreateFileInNonExistingDirectory/testFile";

    Path testPath = qualifiedPath(fileName, fc2);

    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));

    // Create a file on fc2's file system using fc1
    createFile(fc1, testPath);

    // Ensure using fc2 that file is created
    assertTrue(isDir(fc2, testPath.getParent()));
    assertEquals("testCreateFileInNonExistingDirectory",
        testPath.getParent().getName());
    fc2.getFileStatus(testPath);

  }

  @Test
  public void testCreateDirectory() throws IOException {

    Path path = qualifiedPath("test/hadoop", fc2);
    Path falsePath = qualifiedPath("path/doesnot.exist", fc2);
    Path subDirPath = qualifiedPath("dir0", fc2);

    // Ensure that testPath does not exist in fc1
    assertFalse(exists(fc1, path));
    assertFalse(isFile(fc1, path));
    assertFalse(isDir(fc1, path));

    // Create a directory on fc2's file system using fc1
   fc1.mkdir(path, FsPermission.getDefault(), true);

    // Ensure fc2 has directory
    assertTrue(isDir(fc2, path));
    assertTrue(exists(fc2, path));
    assertFalse(isFile(fc2, path));

    // Test to create same dir twice, (HDFS mkdir is similar to mkdir -p )
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);
    // This should not throw exception
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);

    // Create Sub Dirs
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);

    // Check parent dir
    Path parentDir = path.getParent();
    assertTrue(exists(fc2, parentDir));
    assertFalse(isFile(fc2, parentDir));

    // Check parent parent dir
    Path grandparentDir = parentDir.getParent();
    assertTrue(exists(fc2, grandparentDir));
    assertFalse(isFile(fc2, grandparentDir));

    // Negative test cases
    assertFalse(exists(fc2, falsePath));
    assertFalse(isDir(fc2, falsePath));

    // TestCase - Create multiple directories
    String dirNames[] = { 
        "createTest/testDir", "createTest/test Dir",
        "createTest/test*Dir", "createTest/test#Dir",
        "createTest/test1234", "createTest/test_DIr",
        "createTest/1234Test", "createTest/test)Dir",
        "createTest/()&^%$#@!~_+}{><?",
        "createTest/  ", "createTest/^ " };

    for (String f : dirNames) {
      if (!isTestableFileNameOnPlatform(f)) {
        continue;
      }

      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      assertFalse(exists(fc2, testPath));

      // Now create directory
     fc1.mkdir(testPath, FsPermission.getDefault(), true);
      // Ensure fc2 has the created directory
      assertTrue(exists(fc2, testPath));
      assertTrue(isDir(fc2, testPath));
    }
    // delete the parent directory and verify that the dir no longer exists
    final Path parent = qualifiedPath("createTest", fc2);
    fc2.delete(parent, true);
    assertFalse(exists(fc2, parent));

  }

  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = qualifiedPath("test/hadoop", fc2);
    assertFalse(exists(fc2, testDir));
    fc2.mkdir(testDir, FsPermission.getDefault(), true);
    assertTrue(exists(fc2, testDir));

    // Create file on fc1 using fc2 context
    createFile(fc1, qualifiedPath("test/hadoop/file", fc2));

    Path testSubDir = qualifiedPath("test/hadoop/file/subdir", fc2);
    try {
      fc1.mkdir(testSubDir, FsPermission.getDefault(), true);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(exists(fc1, testSubDir));

    Path testDeepSubDir = qualifiedPath("test/hadoop/file/deep/sub/dir", fc1);
    try {
      fc2.mkdir(testDeepSubDir, FsPermission.getDefault(), true);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(exists(fc1, testDeepSubDir));

  }

  @Test
  public void testIsDirectory() throws IOException {
    String dirName = "dirTest";
    String invalidDir = "nonExistantDir";
    String rootDir = "/";

    Path existingPath = qualifiedPath(dirName, fc2);
    Path nonExistingPath = qualifiedPath(invalidDir, fc2);
    Path pathToRootDir = qualifiedPath(rootDir, fc2);

    // Create a directory on fc2's file system using fc1
    fc1.mkdir(existingPath, FsPermission.getDefault(), true);

    // Ensure fc2 has directory
    assertTrue(isDir(fc2, existingPath));
    assertTrue(isDir(fc2, pathToRootDir));

    // Negative test case
    assertFalse(isDir(fc2, nonExistingPath));

  }

  @Test
  public void testDeleteFile() throws IOException {
    Path testPath = qualifiedPath("testDeleteFile", fc2);

    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));

    // First create a file on file system using fc1
    createFile(fc1, testPath);

    // Ensure file exist
    assertTrue(exists(fc2, testPath));

    // Delete file using fc2
    fc2.delete(testPath, false);

    // Ensure fc2 does not have deleted file
    assertFalse(exists(fc2, testPath));

  }

  @Test
  public void testDeleteNonExistingFile() throws IOException {
    String testFileName = "testDeleteNonExistingFile";
    Path testPath = qualifiedPath(testFileName, fc2);

    // TestCase1 : Test delete on file never existed
    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));

    // Delete on non existing file should return false
    assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create , Delete , Delete file
    // Create a file on fc2's file system using fc1
    createFile(fc1, testPath);
    // Ensure file exist
    assertTrue(exists(fc2, testPath));

    // Delete test file, deleting existing file should return true
    assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));
    // Delete on non existing file should return false
    assertFalse(fc2.delete(testPath, false));

  }

  @Test
  public void testDeleteNonExistingFileInDir() throws IOException {
    String testFileInDir = "testDeleteNonExistingFileInDir/testDir/TestFile";
    Path testPath = qualifiedPath(testFileInDir, fc2);

    // TestCase1 : Test delete on file never existed
    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));

    // Delete on non existing file should return false
    assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create , Delete , Delete file
    // Create a file on fc2's file system using fc1
    createFile(fc1, testPath);
    // Ensure file exist
    assertTrue(exists(fc2, testPath));

    // Delete test file, deleting existing file should return true
    assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));
    // Delete on non existing file should return false
    assertFalse(fc2.delete(testPath, false));

  }

  @Test
  public void testDeleteDirectory() throws IOException {
    String dirName = "dirTest";
    Path testDirPath = qualifiedPath(dirName, fc2);
    // Ensure directory does not exist
    assertFalse(exists(fc2, testDirPath));

    // Create a directory on fc2's file system using fc1
   fc1.mkdir(testDirPath, FsPermission.getDefault(), true);

    // Ensure dir is created
    assertTrue(exists(fc2, testDirPath));
    assertTrue(isDir(fc2, testDirPath));

    fc2.delete(testDirPath, true);

    // Ensure that directory is deleted
    assertFalse(isDir(fc2, testDirPath));

    // TestCase - Create and delete multiple directories
    String dirNames[] = { 
        "deleteTest/testDir", "deleteTest/test Dir",
        "deleteTest/test*Dir", "deleteTest/test#Dir", 
        "deleteTest/test1234", "deleteTest/1234Test", 
        "deleteTest/test)Dir", "deleteTest/test_DIr",
        "deleteTest/()&^%$#@!~_+}{><?",
        "deleteTest/  ",
        "deleteTest/^ " };

    for (String f : dirNames) {
      if (!isTestableFileNameOnPlatform(f)) {
        continue;
      }

      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      assertFalse(exists(fc2, testPath));

      // Now create directory
     fc1.mkdir(testPath, FsPermission.getDefault(), true);
      // Ensure fc2 has the created directory
      assertTrue(exists(fc2, testPath));
      assertTrue(isDir(fc2, testPath));
      // Delete dir
      assertTrue(fc2.delete(testPath, true));
      // verify if directory is deleted
      assertFalse(exists(fc2, testPath));
      assertFalse(isDir(fc2, testPath));
    }
  }

  @Test
  public void testDeleteNonExistingDirectory() throws IOException {
    String testDirName = "testDeleteNonExistingDirectory";
    Path testPath = qualifiedPath(testDirName, fc2);

    // TestCase1 : Test delete on directory never existed
    // Ensure directory does not exist
    assertFalse(exists(fc2, testPath));

    // Delete on non existing directory should return false
    assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create dir, Delete dir, Delete dir
    // Create a file on fc2's file system using fc1

    fc1.mkdir(testPath, FsPermission.getDefault(), true);
    // Ensure dir exist
    assertTrue(exists(fc2, testPath));

    // Delete test file, deleting existing file should return true
    assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    assertFalse(exists(fc2, testPath));
    // Delete on non existing file should return false
    assertFalse(fc2.delete(testPath, false));
  }

  @Test
  public void testModificationTime() throws IOException {
    String testFile = "testModificationTime";
    long fc2ModificationTime, fc1ModificationTime;

    Path testPath = qualifiedPath(testFile, fc2);

    // Create a file on fc2's file system using fc1
    createFile(fc1, testPath);
    // Get modification time using fc2 and fc1
    fc1ModificationTime = fc1.getFileStatus(testPath).getModificationTime();
    fc2ModificationTime = fc2.getFileStatus(testPath).getModificationTime();
    // Ensure fc1 and fc2 reports same modification time
    assertEquals(fc1ModificationTime, fc2ModificationTime);
  }

  @Test
  public void testFileStatus() throws IOException {
    String fileName = "testModificationTime";
    Path path2 = fc2.makeQualified(new Path(BASE, fileName));

    // Create a file on fc2's file system using fc1
    createFile(fc1, path2);
    FsStatus fc2Status = fc2.getFsStatus(path2);

    // FsStatus , used, free and capacity are non-negative longs
    assertNotNull(fc2Status);
    assertTrue(fc2Status.getCapacity() > 0);
    assertTrue(fc2Status.getRemaining() > 0);
    assertTrue(fc2Status.getUsed() > 0);

  }

  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    String testFile = "test/hadoop/fileDoesNotExist";
    Path testPath = qualifiedPath(testFile, fc2);
    try {
      fc1.getFileStatus(testPath);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    String testFile = "test/hadoop/file";
    Path testPath = qualifiedPath(testFile, fc2);
    try {
      fc1.listStatus(testPath);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

    
  @Test
  public void testListStatus() throws Exception {
    final String hPrefix = "test/hadoop";
    final String[] dirs = { 
        hPrefix + "/a", 
        hPrefix + "/b", 
        hPrefix + "/c",
        hPrefix + "/1", 
        hPrefix + "/#@#@",
        hPrefix + "/&*#$#$@234"};
    ArrayList<Path> testDirs = new ArrayList<Path>();

    for (String d : dirs) {
      if (!isTestableFileNameOnPlatform(d)) {
        continue;
      }

      testDirs.add(qualifiedPath(d, fc2));
    }
    assertFalse(exists(fc1, testDirs.get(0)));

    for (Path path : testDirs) {
     fc1.mkdir(path, FsPermission.getDefault(), true);
    }

    // test listStatus that returns an array of FileStatus
    FileStatus[] paths = fc1.util().listStatus(qualifiedPath("test", fc1));
    assertEquals(1, paths.length);
    assertEquals(qualifiedPath(hPrefix, fc1), paths[0].getPath());

    paths = fc1.util().listStatus(qualifiedPath(hPrefix, fc1));
    assertEquals(testDirs.size(), paths.length);
    for (int i = 0; i < testDirs.size(); i++) {
      boolean found = false;
      for (int j = 0; j < paths.length; j++) {
        if (qualifiedPath(testDirs.get(i).toString(), fc1).equals(
            paths[j].getPath())) {

          found = true;
        }
      }
      assertTrue(found, testDirs.get(i) + " not found");
    }

    paths = fc1.util().listStatus(qualifiedPath(dirs[0], fc1));
    assertEquals(0, paths.length);
    
    // test listStatus that returns an iterator of FileStatus
    RemoteIterator<FileStatus> pathsItor = 
      fc1.listStatus(qualifiedPath("test", fc1));
    assertEquals(qualifiedPath(hPrefix, fc1), pathsItor.next().getPath());
    assertFalse(pathsItor.hasNext());

    pathsItor = fc1.listStatus(qualifiedPath(hPrefix, fc1));
    int dirLen = 0;
    for (; pathsItor.hasNext(); dirLen++) {
      boolean found = false;
      FileStatus stat = pathsItor.next();
      for (int j = 0; j < dirs.length; j++) {
        if (qualifiedPath(dirs[j],fc1).equals(stat.getPath())) {
          found = true;
          break;
        }
      }
      assertTrue(found, stat.getPath() + " not found");
    }
    assertEquals(testDirs.size(), dirLen);

    pathsItor = fc1.listStatus(qualifiedPath(dirs[0], fc1));
    assertFalse(pathsItor.hasNext());
  }

  /**
   * Returns true if the argument is a file name that is testable on the platform
   * currently running the test.  This is intended for use by tests so that they
   * can skip checking file names that aren't supported by the underlying
   * platform.  The current implementation specifically checks for patterns that
   * are not valid file names on Windows when the tests are running on Windows.
   * 
   * @param fileName String file name to check
   * @return boolean true if the argument is valid as a file name
   */
  private static boolean isTestableFileNameOnPlatform(String fileName) {
    boolean valid = true;

    if (Shell.WINDOWS) {
      // Disallow reserved characters: <, >, :, ", |, ?, *.
      // Disallow trailing space or period.
      // See http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
      valid = !WIN_INVALID_FILE_NAME_PATTERN.matcher(fileName).matches();
    }

    return valid;
  }
}
