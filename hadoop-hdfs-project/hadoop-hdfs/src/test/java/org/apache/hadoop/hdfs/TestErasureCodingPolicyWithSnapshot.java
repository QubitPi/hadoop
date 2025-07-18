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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(120)
public class TestErasureCodingPolicyWithSnapshot {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;

  private final static int SUCCESS = 0;
  private ErasureCodingPolicy ecPolicy;
  private short groupSize;

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @BeforeEach
  public void setupCluster() throws IOException {
    ecPolicy = getEcPolicy();
    groupSize = (short) (ecPolicy.getNumDataUnits()
        + ecPolicy.getNumParityUnits());
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
  }

  @AfterEach
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test correctness of successive snapshot creation and deletion with erasure
   * coding policies. Create snapshot of ecDir's parent directory.
   */
  @Test
  public void testSnapshotsOnErasureCodingDirsParentDir() throws Exception {
    final int len = 1024;
    final Path ecDirParent = new Path("/parent");
    final Path ecDir = new Path(ecDirParent, "ecdir");
    final Path ecFile = new Path(ecDir, "ecfile");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDirParent);
    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);
    String contents = DFSTestUtil.readFile(fs, ecFile);
    final Path snap1 = fs.createSnapshot(ecDirParent, "snap1");
    final Path snap1ECDir = new Path(snap1, ecDir.getName());
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap1ECDir),
        "Got unexpected erasure coding policy");

    // Now delete the dir which has erasure coding policy. Re-create the dir again, and
    // take another snapshot
    fs.delete(ecDir, true);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    final Path snap2 = fs.createSnapshot(ecDirParent, "snap2");
    final Path snap2ECDir = new Path(snap2, ecDir.getName());
    assertNull(fs.getErasureCodingPolicy(snap2ECDir),
        "Expected null erasure coding policy");

    // Make dir again with system default ec policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap3 = fs.createSnapshot(ecDirParent, "snap3");
    final Path snap3ECDir = new Path(snap3, ecDir.getName());
    // Check that snap3's ECPolicy has the correct settings
    ErasureCodingPolicy ezSnap3 = fs.getErasureCodingPolicy(snap3ECDir);
    assertEquals(ecPolicy, ezSnap3, "Got unexpected erasure coding policy");

    // Check that older snapshots still have the old ECPolicy settings
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap1ECDir),
        "Got unexpected erasure coding policy");
    assertNull(fs.getErasureCodingPolicy(snap2ECDir),
        "Expected null erasure coding policy");

    // Verify contents of the snapshotted file
    final Path snapshottedECFile = new Path(snap1.toString() + "/"
        + ecDir.getName() + "/" + ecFile.getName());
    assertEquals(contents, DFSTestUtil.readFile(fs, snapshottedECFile),
        "Contents of snapshotted file have changed unexpectedly");

    // Now delete the snapshots out of order and verify the EC policy
    // correctness
    fs.deleteSnapshot(ecDirParent, snap2.getName());
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap1ECDir),
        "Got unexpected erasure coding policy");
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap3ECDir),
        "Got unexpected erasure coding policy");
    fs.deleteSnapshot(ecDirParent, snap1.getName());
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap3ECDir),
        "Got unexpected erasure coding policy");
  }

  /**
   * Test creation of snapshot on directory has erasure coding policy.
   */
  @Test
  public void testSnapshotsOnErasureCodingDir() throws Exception {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap1),
        "Got unexpected erasure coding policy");
  }

  /**
   * Test verify erasure coding policy is present after restarting the NameNode.
   */
  @Test
  public void testSnapshotsOnErasureCodingDirAfterNNRestart() throws Exception {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");
    ErasureCodingPolicy ecSnap = fs.getErasureCodingPolicy(snap1);
    assertEquals(ecPolicy, ecSnap, "Got unexpected erasure coding policy");

    // save namespace, restart namenode, and check ec policy correctness.
    fs.setSafeMode(SafeModeAction.ENTER);
    fs.saveNamespace();
    fs.setSafeMode(SafeModeAction.LEAVE);
    cluster.restartNameNode(true);

    ErasureCodingPolicy ecSnap1 = fs.getErasureCodingPolicy(snap1);
    assertEquals(ecPolicy, ecSnap1, "Got unexpected erasure coding policy");
    assertEquals(ecSnap.getSchema(), ecSnap1.getSchema(), "Got unexpected ecSchema");
  }

  /**
   * Test copy a snapshot will not preserve its erasure coding policy info.
   */
  @Test
  public void testCopySnapshotWillNotPreserveErasureCodingPolicy()
      throws Exception {
    final int len = 1024;
    final Path ecDir = new Path("/ecdir");
    final Path ecFile = new Path(ecDir, "ecFile");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");

    Path snap1Copy = new Path(ecDir.toString() + "-copy");
    final Path snap1CopyECDir = new Path("/ecdir-copy");
    String[] argv = new String[] { "-cp", "-px", snap1.toUri().toString(),
        snap1Copy.toUri().toString() };
    int ret = ToolRunner.run(new FsShell(conf), argv);
    assertEquals(SUCCESS, ret, "cp -px is not working on a snapshot");

    assertNull(fs.getErasureCodingPolicy(snap1CopyECDir),
        "Got unexpected erasure coding policy");
    assertEquals(ecPolicy, fs.getErasureCodingPolicy(snap1),
        "Got unexpected erasure coding policy");
  }

  @Test
  @Timeout(value = 300)
  public void testFileStatusAcrossNNRestart() throws IOException {
    final int len = 1024;
    final Path normalFile = new Path("/", "normalFile");
    DFSTestUtil.createFile(fs, normalFile, len, (short) 1, 0xFEED);

    final Path ecDir = new Path("/ecdir");
    final Path ecFile = new Path(ecDir, "ecFile");
    fs.mkdirs(ecDir);

    // Set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);

    // Verify FileStatus for normal and EC files
    ContractTestUtils.assertNotErasureCoded(fs, normalFile);
    ContractTestUtils.assertErasureCoded(fs, ecFile);

    cluster.restartNameNode(true);

    // Verify FileStatus for normal and EC files
    ContractTestUtils.assertNotErasureCoded(fs, normalFile);
    ContractTestUtils.assertErasureCoded(fs, ecFile);
  }

  @Test
  public void testErasureCodingPolicyOnDotSnapshotDir() throws IOException {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    // set erasure coding policy and create snapshot
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap = fs.createSnapshot(ecDir, "snap1");

    // verify the EC policy correctness
    ErasureCodingPolicy ecSnap = fs.getErasureCodingPolicy(snap);
    assertEquals(ecPolicy, ecSnap, "Got unexpected erasure coding policy");

    // verify the EC policy is null, not an exception
    final Path ecDotSnapshotDir = new Path(ecDir, ".snapshot");
    ErasureCodingPolicy ecSnap1 = fs.getErasureCodingPolicy(ecDotSnapshotDir);
    assertNull(ecSnap1, "Got unexpected erasure coding policy");
  }

  /**
   * Test creation of snapshot on directory which changes its
   * erasure coding policy.
   */
  @Test
  public void testSnapshotsOnErasureCodingDirAfterECPolicyChanges()
          throws Exception {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    final Path snap1 = fs.createSnapshot(ecDir, "snap1");
    assertNull(fs.getErasureCodingPolicy(snap1), "Expected null erasure coding policy");

    // Set erasure coding policy
    final ErasureCodingPolicy ec63Policy = SystemErasureCodingPolicies
        .getByID(SystemErasureCodingPolicies.RS_6_3_POLICY_ID);
    fs.setErasureCodingPolicy(ecDir, ec63Policy.getName());
    final Path snap2 = fs.createSnapshot(ecDir, "snap2");
    assertEquals(ec63Policy, fs.getErasureCodingPolicy(snap2),
        "Got unexpected erasure coding policy");

    // Verify the EC policy correctness after the unset operation
    fs.unsetErasureCodingPolicy(ecDir);
    final Path snap3 = fs.createSnapshot(ecDir, "snap3");
    assertNull(fs.getErasureCodingPolicy(snap3), "Expected null erasure coding policy");

    // Change the erasure coding policy and take another snapshot
    final ErasureCodingPolicy ec32Policy = SystemErasureCodingPolicies
        .getByID(SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
    fs.enableErasureCodingPolicy(ec32Policy.getName());
    fs.setErasureCodingPolicy(ecDir, ec32Policy.getName());
    final Path snap4 = fs.createSnapshot(ecDir, "snap4");
    assertEquals(ec32Policy, fs.getErasureCodingPolicy(snap4),
        "Got unexpected erasure coding policy");

    // Check that older snapshot still have the old ECPolicy settings
    assertNull(fs.getErasureCodingPolicy(snap1), "Expected null erasure coding policy");
    assertEquals(ec63Policy, fs.getErasureCodingPolicy(snap2),
        "Got unexpected erasure coding policy");
    assertNull(fs.getErasureCodingPolicy(snap3), "Expected null erasure coding policy");
  }
}
