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
package org.apache.hadoop.hdfs.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFSMainOperationsWebHdfs extends FSMainOperationsBaseTest {
  {
    GenericTestUtils.setLogLevel(ExceptionHandler.LOG, Level.TRACE);
  }

  private static MiniDFSCluster cluster = null;
  private static Path defaultWorkingDirectory;
  private static FileSystem fileSystem;
  
  public TestFSMainOperationsWebHdfs() {
    super("/tmp/TestFSMainOperationsWebHdfs");
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return fileSystem;
  }

  @BeforeAll
  public static void setupCluster() {
    final Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      //change root permission to 777
      cluster.getFileSystem().setPermission(
          new Path("/"), new FsPermission((short)0777));

      final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

      //get file system as a non-superuser
      final UserGroupInformation current = UserGroupInformation.getCurrentUser();
      final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          current.getShortUserName() + "x", new String[]{"user"});
      fileSystem = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(uri), conf);
        }
      });

      defaultWorkingDirectory = fileSystem.getWorkingDirectory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Test
  public void testConcat() throws Exception {
    Path[] paths = {new Path("/test/hadoop/file1"),
                    new Path("/test/hadoop/file2"),
                    new Path("/test/hadoop/file3")};

    DFSTestUtil.createFile(fSys, paths[0], 1024, (short) 3, 0);
    DFSTestUtil.createFile(fSys, paths[1], 1024, (short) 3, 0);
    DFSTestUtil.createFile(fSys, paths[2], 1024, (short) 3, 0);

    Path catPath = new Path("/test/hadoop/catFile");
    DFSTestUtil.createFile(fSys, catPath, 1024, (short) 3, 0);
    assertTrue(exists(fSys, catPath));

    fSys.concat(catPath, paths);

    assertFalse(exists(fSys, paths[0]));
    assertFalse(exists(fSys, paths[1]));
    assertFalse(exists(fSys, paths[2]));

    FileStatus fileStatus = fSys.getFileStatus(catPath);
    assertEquals(1024*4, fileStatus.getLen());
  }

  @Test
  public void testTruncate() throws Exception {
    final short repl = 3;
    final int blockSize = 1024;
    final int numOfBlocks = 2;
    Path dir = getTestRootPath(fSys, "test/hadoop");
    Path file = getTestRootPath(fSys, "test/hadoop/file");

    final byte[] data = getFileData(numOfBlocks, blockSize);
    createFile(fSys, file, data, blockSize, repl);

    final int newLength = blockSize;

    boolean isReady = fSys.truncate(file, newLength);

    assertTrue(isReady, "Recovery is not expected.");

    FileStatus fileStatus = fSys.getFileStatus(file);
    assertEquals(fileStatus.getLen(), newLength);
    AppendTestUtil.checkFullFile(fSys, file, newLength, data, file.toString());

    ContentSummary cs = fSys.getContentSummary(dir);
    assertEquals(cs.getSpaceConsumed(), newLength * repl, "Bad disk space usage");
    assertTrue(fSys.delete(dir, true), "Deleted");
  }

  // Test that WebHdfsFileSystem.jsonParse() closes the connection's input
  // stream.
  // Closing the inputstream in jsonParse will allow WebHDFS to reuse
  // connections to the namenode rather than needing to always open new ones.
  boolean closedInputStream = false;
  @Test
  public void testJsonParseClosesInputStream() throws Exception {
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fileSystem;
    Path file = getTestRootPath(fSys, "test/hadoop/file");
    createFile(file);
    final HttpOpParam.Op op = GetOpParam.Op.GETHOMEDIRECTORY;
    final URL url = webhdfs.toUrl(op, file);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(op.getType().toString());
    conn.connect();

    InputStream myIn = new InputStream(){
      private HttpURLConnection localConn = conn;
      @Override
      public void close() throws IOException {
        closedInputStream = true;
        localConn.getInputStream().close();
      }
      @Override
      public int read() throws IOException {
        return localConn.getInputStream().read();
      }
    };
    final HttpURLConnection spyConn = spy(conn);
    doReturn(myIn).when(spyConn).getInputStream();

    try {
      assertFalse(closedInputStream);
      WebHdfsFileSystem.jsonParse(spyConn, false);
      assertTrue(closedInputStream);
    } catch(IOException ioe) {
      fail();
    }
    conn.disconnect();
  }

  @Override
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    try {
      fSys.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(exists(fSys, testSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    try {
      fSys.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(exists(fSys, testDeepSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
  }

}
