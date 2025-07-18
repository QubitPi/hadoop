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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestSetrepIncreasing {
  static void setrep(int fromREP, int toREP, boolean simulatedStorage) throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    conf.set(DFSConfigKeys.DFS_REPLICATION_KEY, "" + fromREP);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000L);
    conf.set(DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, Integer.toString(2));
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(10).build();
    FileSystem fs = cluster.getFileSystem();
    assertTrue(fs instanceof DistributedFileSystem, "Not a HDFS: " + fs.getUri());

    try {
      Path root = TestDFSShell.mkdir(fs, 
          new Path("/test/setrep" + fromREP + "-" + toREP));
      Path f = TestDFSShell.writeFile(fs, new Path(root, "foo"));
      
      // Verify setrep for changing replication
      {
        String[] args = {"-setrep", "-w", "" + toREP, "" + f};
        FsShell shell = new FsShell();
        shell.setConf(conf);
        try {
          assertEquals(0, shell.run(args));
        } catch (Exception e) {
          assertTrue(false, "-setrep " + e);
        }
      }

      //get fs again since the old one may be closed
      fs = cluster.getFileSystem();
      FileStatus file = fs.getFileStatus(f);
      long len = file.getLen();
      for(BlockLocation locations : fs.getFileBlockLocations(file, 0, len)) {
        assertTrue(locations.getHosts().length == toREP);
      }
      TestDFSShell.show("done setrep waiting: " + root);
    } finally {
      try {fs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 120)
  public void testSetrepIncreasing() throws IOException {
    setrep(3, 7, false);
  }

  @Test
  @Timeout(value = 120)
  public void testSetrepIncreasingSimulatedStorage() throws IOException {
    setrep(3, 7, true);
  }

  @Test
  public void testSetRepWithStoragePolicyOnEmptyFile() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    DistributedFileSystem dfs = cluster.getFileSystem();
    try {
      Path d = new Path("/tmp");
      dfs.mkdirs(d);
      dfs.setStoragePolicy(d, "HOT");
      Path f = new Path(d, "foo");
      dfs.createNewFile(f);
      dfs.setReplication(f, (short) 4);
    } finally {
      dfs.close();
      cluster.shutdown();
    }
 }

  @Test
  public void testSetRepOnECFile() throws Exception {
    ClientProtocol client;
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
    client.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    client.setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());

    FileSystem dfs = cluster.getFileSystem();
    try {
      Path d = new Path("/tmp");
      dfs.mkdirs(d);
      Path f = new Path(d, "foo");
      dfs.createNewFile(f);
      FileStatus file = dfs.getFileStatus(f);
      assertTrue(file.isErasureCoded());

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));
      String[] args = {"-setrep", "2", "" + f};
      FsShell shell = new FsShell();
      shell.setConf(conf);
      assertEquals(0, shell.run(args));
      assertTrue(
          out.toString().contains("Did not set replication for: /tmp/foo"));

      // verify the replication factor of the EC file
      file = dfs.getFileStatus(f);
      assertEquals(1, file.getReplication());
    } finally {
      dfs.close();
      cluster.shutdown();
    }
  }
}
