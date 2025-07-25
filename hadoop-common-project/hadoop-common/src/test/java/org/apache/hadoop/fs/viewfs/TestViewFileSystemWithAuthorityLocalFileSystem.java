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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 
 * Test the ViewFsBaseTest using a viewfs with authority: 
 *    viewfs://mountTableName/
 *    ie the authority is used to load a mount table.
 *    The authority name used is "default"
 *
 */
public class TestViewFileSystemWithAuthorityLocalFileSystem extends ViewFileSystemBaseTest {
  URI schemeWithAuthority;

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = FileSystem.getLocal(new Configuration());
    super.setUp(); // this sets up conf (and fcView which we replace)

    // Now create a viewfs using a mount table called "default"
    // hence viewfs://default/
    schemeWithAuthority = 
      new URI(FsConstants.VIEWFS_SCHEME, "default", "/", null, null);
    fsView = FileSystem.get(schemeWithAuthority, conf);
  }

  @Override
  @AfterEach
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    super.tearDown();
  }
 
  @Override
  Path getTrashRootInFallBackFS() throws IOException {
    return new Path(
        "/" + TRASH_PREFIX + "/" + UserGroupInformation.getCurrentUser()
            .getShortUserName());
  }

  @Override
  @Test
  public void testBasicPaths() {
    assertEquals(schemeWithAuthority,
        fsView.getUri());
    assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getWorkingDirectory());
    assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getHomeDirectory());
    assertEquals(
        new Path("/foo/bar").makeQualified(schemeWithAuthority, null),
        fsView.makeQualified(new Path("/foo/bar")));
  }
}
