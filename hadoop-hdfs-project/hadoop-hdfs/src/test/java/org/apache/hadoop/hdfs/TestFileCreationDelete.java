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
package org.apache.hadoop.hdfs;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

public class TestFileCreationDelete {
  {
    DFSTestUtil.setNameNodeLogLevel(Level.TRACE);
  }

  @Test
  public void testFileCreationDeleteParent() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // create file1.
      Path dir = new Path("/foo");
      Path file1 = new Path(dir, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      TestFileCreation.writeFile(stm1, 1000);
      stm1.hflush();

      // create file2.
      Path file2 = new Path("/file2");
      FSDataOutputStream stm2 = TestFileCreation.createFile(fs, file2, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);
      TestFileCreation.writeFile(stm2, 1000);
      stm2.hflush();

      // rm dir
      fs.delete(dir, true);

      // restart cluster.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(file2));
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
