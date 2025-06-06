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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSymlinkLocalFSFileSystem extends TestSymlinkLocalFS {

  @BeforeAll
  public static void testSetup() throws Exception {
    FileSystem filesystem = FileSystem.getLocal(new Configuration());
    wrapper = new FileSystemTestWrapper(filesystem);
  }

  @Disabled("RawLocalFileSystem#mkdir does not treat existence of directory" +
      " as an error")
  @Override
  @Test
  @Timeout(value = 10)
  public void testMkdirExistingLink() throws IOException {}

  @Disabled("FileSystem#create defaults to creating parents," +
      " throwing an IOException instead of FileNotFoundException")
  @Override
  @Test
  @Timeout(value = 10)
  public void testCreateFileViaDanglingLinkParent() throws IOException {}

  @Disabled("RawLocalFileSystem does not throw an exception if the path" +
      " already exists")
  @Override
  @Test
  @Timeout(value = 10)
  public void testCreateFileDirExistingLink() throws IOException {}
  
  @Disabled("ChecksumFileSystem does not support append")
  @Override
  @Test
  @Timeout(value = 10)
  public void testAccessFileViaInterSymlinkAbsTarget() throws IOException {}

  @Override
  public void testRenameFileWithDestParentSymlink() throws IOException {
    assumeNotWindows();
    super.testRenameFileWithDestParentSymlink();
  }

  @Override
  @Test
  @Timeout(value = 10)
  /** Rename a symlink to itself */
  public void testRenameSymlinkToItself() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    createAndWriteFile(file);

    Path link = new Path(testBaseDir1(), "linkToFile1");
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.rename(link, link);
      fail("Failed to get expected IOException");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails with overwrite as well
    try {
      wrapper.rename(link, link, Rename.OVERWRITE);
      fail("Failed to get expected IOException");
    } catch (IOException e) {
      // Todo: Fix this test when HADOOP-9819 is fixed.
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException ||
                 unwrapException(e) instanceof FileNotFoundException);
    }
  }
}
