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

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.function.Supplier;

/**
 * Tests to verify safe mode correctness.
 */
public class TestSafeMode {
  public static final Logger LOG = LoggerFactory.getLogger(TestSafeMode.class);
  private static final Path TEST_PATH = new Path("/test");
  private static final int BLOCK_SIZE = 1024;
  private static final String NEWLINE = System.getProperty("line.separator");
  Configuration conf; 
  MiniDFSCluster cluster;
  FileSystem fs;
  DistributedFileSystem dfs;
  private static final String NN_METRICS = "NameNodeActivity";

  @BeforeEach
  public void startUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();      
    fs = cluster.getFileSystem();
    dfs = (DistributedFileSystem)fs;
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * This test verifies that if SafeMode is manually entered, name-node does not
   * come out of safe mode even after the startup safe mode conditions are met.
   * <ol>
   * <li>Start cluster with 1 data-node.</li>
   * <li>Create 2 files with replication 1.</li>
   * <li>Re-start cluster with 0 data-nodes. 
   * Name-node should stay in automatic safe-mode.</li>
   * <li>Enter safe mode manually.</li>
   * <li>Start the data-node.</li>
   * <li>Wait longer than <code>dfs.namenode.safemode.extension</code> and
   * verify that the name-node is still in safe mode.</li>
   * </ol>
   *  
   * @throws IOException
   */
  @Test
  public void testManualSafeMode() throws IOException {      
    fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testManualSafeMode/file1");
    Path file2 = new Path("/tmp/testManualSafeMode/file2");
    
    // create two files with one block each.
    DFSTestUtil.createFile(fs, file1, 1000, (short)1, 0);
    DFSTestUtil.createFile(fs, file2, 1000, (short)1, 0);
    fs.close();
    cluster.shutdown();
    
    // now bring up just the NameNode.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    
    assertTrue(dfs.setSafeMode(SafeModeAction.GET),
        "No datanode is started. Should be in SafeMode");
    
    // manually set safemode.
    dfs.setSafeMode(SafeModeAction.ENTER);
    
    // now bring up the datanode and wait for it to be active.
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();
    
    // wait longer than dfs.namenode.safemode.extension
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignored) {}

    assertTrue(dfs.setSafeMode(SafeModeAction.GET), "should still be in SafeMode");
    assertFalse(dfs.setSafeMode(SafeModeAction.LEAVE), "should not be in SafeMode");
  }

  /**
   * Test that, if there are no blocks in the filesystem,
   * the NameNode doesn't enter the "safemode extension" period.
   */
  @Test
  @Timeout(value = 45)
  public void testNoExtensionIfNoBlocks() throws IOException {
    cluster.getConfiguration(0).setInt(
        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 60000);
    cluster.restartNameNode();
    // Even though we have safemode extension set high, we should immediately
    // exit safemode on startup because there are no blocks in the namespace.
    String status = cluster.getNameNode().getNamesystem().getSafemode();
    assertEquals("", status);
  }
  
  /**
   * Test that the NN initializes its under-replicated blocks queue
   * before it is ready to exit safemode (HDFS-1476)
   */
  @Test
  @Timeout(value = 45)
  public void testInitializeReplQueuesEarly() throws Exception {
    LOG.info("Starting testInitializeReplQueuesEarly");
    // Spray the blocks around the cluster when we add DNs instead of
    // concentrating all blocks on the first node.
    BlockManagerTestUtil.setWritingPrefersLocalNode(
        cluster.getNamesystem().getBlockManager(), false);
    
    cluster.startDataNodes(conf, 2, true, StartupOption.REGULAR, null);
    cluster.waitActive();

    LOG.info("Creating files");
    DFSTestUtil.createFile(fs, TEST_PATH, 15*BLOCK_SIZE, (short)1, 1L);
    
    LOG.info("Stopping all DataNodes");
    List<DataNodeProperties> dnprops = Lists.newLinkedList();
    dnprops.add(cluster.stopDataNode(0));
    dnprops.add(cluster.stopDataNode(0));
    dnprops.add(cluster.stopDataNode(0));
    
    cluster.getConfiguration(0).setFloat(
        DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY, 1f/15f);
    
    LOG.info("Restarting NameNode");
    cluster.restartNameNode();
    final NameNode nn = cluster.getNameNode();
    
    String status = nn.getNamesystem().getSafemode();
    assertEquals("Safe mode is ON. The reported blocks 0 needs additional "
            + "14 blocks to reach the threshold 0.9990 of total blocks 15." + NEWLINE
            + "The minimum number of live datanodes is not required. "
            + "Safe mode will be turned off automatically once the thresholds have "
            + "been reached.",
        status);
    assertFalse(NameNodeAdapter.safeModeInitializedReplQueues(nn),
        "Mis-replicated block queues should not be initialized " + "until threshold is crossed");
    
    LOG.info("Restarting one DataNode");
    cluster.restartDataNode(dnprops.remove(0));

    // Wait for block reports from all attached storages of
    // the restarted DN to come in.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return getLongCounter("StorageBlockReportNumOps",
            getMetrics(NN_METRICS)) == cluster.getStoragesPerDatanode();
      }
    }, 10, 10000);

    final long safe = NameNodeAdapter.getSafeModeSafeBlocks(nn);
    assertTrue(safe > 0, "Expected first block report to make some blocks safe.");
    assertTrue(safe < 15, "Did not expect first block report to make all blocks safe.");

    assertTrue(NameNodeAdapter.safeModeInitializedReplQueues(nn));

    // Ensure that UnderReplicatedBlocks goes up to 15 - safe. Misreplicated
    // blocks are processed asynchronously so this may take a few seconds.
    // Failure here will manifest as a test timeout.
    BlockManagerTestUtil.updateState(nn.getNamesystem().getBlockManager());
    long underReplicatedBlocks = nn.getNamesystem().getUnderReplicatedBlocks();
    while (underReplicatedBlocks != (15 - safe)) {
      LOG.info("UnderReplicatedBlocks expected=" + (15 - safe) +
               ", actual=" + underReplicatedBlocks);
      Thread.sleep(100);
      BlockManagerTestUtil.updateState(nn.getNamesystem().getBlockManager());
      underReplicatedBlocks = nn.getNamesystem().getUnderReplicatedBlocks();
    }
    
    cluster.restartDataNodes();
  }

  /**
   * Test that, when under-replicated blocks are processed at the end of
   * safe-mode, blocks currently under construction are not considered
   * under-construction or missing. Regression test for HDFS-2822.
   */
  @Test
  public void testRbwBlocksNotConsideredUnderReplicated() throws IOException {
    List<FSDataOutputStream> stms = Lists.newArrayList();
    try {
      // Create some junk blocks so that the NN doesn't just immediately
      // exit safemode on restart.
      DFSTestUtil.createFile(fs, new Path("/junk-blocks"),
          BLOCK_SIZE*4, (short)1, 1L);
      // Create several files which are left open. It's important to
      // create several here, because otherwise the first iteration of the
      // replication monitor will pull them off the replication queue and
      // hide this bug from the test!
      for (int i = 0; i < 10; i++) {
        FSDataOutputStream stm = fs.create(
            new Path("/append-" + i), true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
        stms.add(stm);
        stm.write(1);
        stm.hflush();
      }

      cluster.restartNameNode();
      FSNamesystem ns = cluster.getNameNode(0).getNamesystem();
      BlockManagerTestUtil.updateState(ns.getBlockManager());
      assertEquals(0, ns.getPendingReplicationBlocks());
      assertEquals(0, ns.getCorruptReplicaBlocks());
      assertEquals(0, ns.getMissingBlocksCount());

    } finally {
      for (FSDataOutputStream stm : stms) {
        IOUtils.closeStream(stm);
      }
      cluster.shutdown();
    }
  }

  public interface FSRun {
    public abstract void run(FileSystem fs) throws IOException;
  }

  /**
   * Assert that the given function fails to run due to a safe 
   * mode exception.
   */
  public void runFsFun(String msg, FSRun f) {
    try {
      f.run(fs);
      fail(msg);
    } catch (RemoteException re) {
      assertEquals(SafeModeException.class.getName(), re.getClassName());
      GenericTestUtils.assertExceptionContains("Name node is in safe mode", re);
    } catch (SafeModeException ignored) {
    } catch (IOException ioe) {
      fail(msg + " " + StringUtils.stringifyException(ioe));
    }
  }

  @Test
  public void testSafeModeExceptionText() throws Exception {
    final Path file1 = new Path("/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    assertTrue(dfs.setSafeMode(SafeModeAction.ENTER), "Could not enter SM");
    try {
      FSRun fsRun = new FSRun() {
        @Override
        public void run(FileSystem fileSystem) throws IOException {
          ((DistributedFileSystem)fileSystem).setQuota(file1, 1, 1);
        }
      };
      fsRun.run(fs);
      fail("Should not succeed with no exceptions!");
    } catch (RemoteException re) {
      assertEquals(SafeModeException.class.getName(), re.getClassName());
      GenericTestUtils.assertExceptionContains(
          NameNode.getServiceAddress(conf, true).getHostName(), re);
    } catch (IOException ioe) {
      fail("Encountered exception" + " " + StringUtils.stringifyException(ioe));
    }
  }

  /**
   * Run various fs operations while the NN is in safe mode,
   * assert that they are either allowed or fail as expected.
   */
  @Test
  public void testOperationsWhileInSafeMode() throws IOException,
      InterruptedException {
    final Path file1 = new Path("/file1");

    assertFalse(dfs.setSafeMode(SafeModeAction.GET));
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    assertTrue(dfs.setSafeMode(SafeModeAction.ENTER), "Could not enter SM");

    runFsFun("Set quota while in SM", new FSRun() { 
      @Override
      public void run(FileSystem fs) throws IOException {
        ((DistributedFileSystem)fs).setQuota(file1, 1, 1); 
      }});

    runFsFun("Set perm while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setPermission(file1, FsPermission.getDefault());
      }});

    runFsFun("Set owner while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setOwner(file1, "user", "group");
      }});

    runFsFun("Set repl while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setReplication(file1, (short)1);
      }});

    runFsFun("Append file while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        DFSTestUtil.appendFile(fs, file1, "new bytes");
      }});

    runFsFun("Truncate file while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.truncate(file1, 0);
      }});

    runFsFun("Delete file while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.delete(file1, false);
      }});

    runFsFun("Rename file while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.rename(file1, new Path("file2"));
      }});

    runFsFun("Set time while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setTimes(file1, 0, 0);
      }});

    runFsFun("modifyAclEntries while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.modifyAclEntries(file1, Lists.<AclEntry>newArrayList());
      }});

    runFsFun("removeAclEntries while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.removeAclEntries(file1, Lists.<AclEntry>newArrayList());
      }});

    runFsFun("removeDefaultAcl while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.removeDefaultAcl(file1);
      }});

    runFsFun("removeAcl while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.removeAcl(file1);
      }});

    runFsFun("setAcl while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setAcl(file1, Lists.<AclEntry>newArrayList());
      }});
    
    runFsFun("setXAttr while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.setXAttr(file1, "user.a1", null);
      }});
    
    runFsFun("removeXAttr while in SM", new FSRun() {
      @Override
      public void run(FileSystem fs) throws IOException {
        fs.removeXAttr(file1, "user.a1");
      }});
    
    try {
      DFSTestUtil.readFile(fs, file1);
    } catch (IOException ioe) {
      fail("Set times failed while in SM");
    }

    try {
      fs.getAclStatus(file1);
    } catch (IOException ioe) {
      fail("getAclStatus failed while in SM");
    }

    // Test access
    UserGroupInformation ugiX = UserGroupInformation.createRemoteUser("userX");
    FileSystem myfs = ugiX.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws IOException {
        return FileSystem.get(conf);
      }
    });
    myfs.access(file1, FsAction.READ);
    try {
      myfs.access(file1, FsAction.WRITE);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      // expected
    }

    ECSchema toAddSchema = new ECSchema("testcodec", 3, 2);
    ErasureCodingPolicy newPolicy =
        new ErasureCodingPolicy(toAddSchema, 128 * 1024);
    ErasureCodingPolicy[] policyArray =
        new ErasureCodingPolicy[]{newPolicy};
    try {
      dfs.addErasureCodingPolicies(policyArray);
      fail("AddErasureCodingPolicies should have failed.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot add erasure coding policy", ioe);
      // expected
    }

    try {
      dfs.removeErasureCodingPolicy("testECName");
      fail("RemoveErasureCodingPolicy should have failed.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot remove erasure coding policy", ioe);
      // expected
    }

    assertFalse(dfs.setSafeMode(SafeModeAction.LEAVE),
        "Could not leave SM");
  }

  /**
   * Verify that the NameNode stays in safemode when dfs.safemode.datanode.min
   * is set to a number greater than the number of live datanodes.
   */
  @Test
  public void testDatanodeThreshold() throws IOException {
    cluster.shutdown();
    Configuration conf = cluster.getConfiguration(0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);

    cluster.restartNameNode();
    fs = cluster.getFileSystem();

    String tipMsg = cluster.getNamesystem().getSafemode();
    assertTrue(tipMsg.contains("The number of live datanodes 0 needs an additional " +
            "1 live datanodes to reach the minimum number 1." +
            NEWLINE + "Safe mode will be turned off automatically"),
        "Safemode tip message doesn't look right: " + tipMsg);

    // Start a datanode
    cluster.startDataNodes(conf, 1, true, null, null);

    // Wait long enough for safemode check to refire
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {}

    // We now should be out of safe mode.
    assertEquals("", cluster.getNamesystem().getSafemode());
  }

  /*
   * Tests some utility methods that surround the SafeMode's state.
   * @throws IOException when there's an issue connecting to the test DFS.
   */
  @Test
  public void testSafeModeUtils() throws IOException {
    dfs = cluster.getFileSystem();

    // Enter safemode.
    dfs.setSafeMode(SafeModeAction.ENTER);
    assertTrue(dfs.isInSafeMode(), "State was expected to be in safemode.");

    // Exit safemode.
    dfs.setSafeMode(SafeModeAction.LEAVE);
    assertFalse(dfs.isInSafeMode(), "State was expected to be out of safemode.");
  }
  
  @Test
  public void testSafeModeWhenZeroBlockLocations() throws IOException {

    try {
      Path file1 = new Path("/tmp/testManualSafeMode/file1");
      Path file2 = new Path("/tmp/testManualSafeMode/file2");
      
      System.out.println("Created file1 and file2.");
      
      // create two files with one block each.
      DFSTestUtil.createFile(fs, file1, 1000, (short)1, 0);
      DFSTestUtil.createFile(fs, file2, 2000, (short)1, 0);
      checkGetBlockLocationsWorks(fs, file1);

      NameNode namenode = cluster.getNameNode();

      // manually set safemode.
      dfs.setSafeMode(SafeModeAction.ENTER);
      assertTrue(namenode.isInSafeMode(), "should still be in SafeMode");
      // getBlock locations should still work since block locations exists
      checkGetBlockLocationsWorks(fs, file1);
      dfs.setSafeMode(SafeModeAction.LEAVE);
      assertFalse(namenode.isInSafeMode(), "should not be in SafeMode");

      // Now 2nd part of the tests where there aren't block locations
      cluster.shutdownDataNodes();
      cluster.shutdownNameNode(0);
      
      // now bring up just the NameNode.
      cluster.restartNameNode();
      cluster.waitActive();
      
      System.out.println("Restarted cluster with just the NameNode");
      
      namenode = cluster.getNameNode();

      assertTrue(namenode.isInSafeMode(),
          "No datanode is started. Should be in SafeMode");
      FileStatus stat = fs.getFileStatus(file1);
      try {
        fs.getFileBlockLocations(stat, 0, 1000);
        assertTrue(false, "Should have got safemode exception");
      } catch (SafeModeException e) {
        // as expected 
      } catch (RemoteException re) {
        if (!re.getClassName().equals(SafeModeException.class.getName()))
            assertTrue(false, "Should have got safemode exception");
      }


      dfs.setSafeMode(SafeModeAction.LEAVE);
      assertFalse(namenode.isInSafeMode(), "Should not be in safemode");
      checkGetBlockLocationsWorks(fs, file1);

    } finally {
      if(fs != null) fs.close();
      if(cluster!= null) cluster.shutdown();
    }
  }

  void checkGetBlockLocationsWorks(FileSystem fs, Path fileName) throws IOException {
    FileStatus stat = fs.getFileStatus(fileName);
    try {  
      fs.getFileBlockLocations(stat, 0, 1000);
    } catch (SafeModeException e) {
      assertTrue(false, "Should have not got safemode exception");
    } catch (RemoteException re) {
      assertTrue(false, "Should have not got remote exception");
    }    
  }
}
