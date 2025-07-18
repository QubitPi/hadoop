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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestFileAppend{
  private static final long RANDOM_TEST_RUNTIME = 10000;

  private static byte[] fileContents = null;

  static final DataChecksum DEFAULT_CHECKSUM =
      DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);
  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    stm.write(buffer);
  }

  //
  // verify that the data written to the full blocks are sane
  // 
  private void checkFile(DistributedFileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {;}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
          fileSys.getFileStatus(name), 0, AppendTestUtil.FILE_SIZE);
      if (locations.length < AppendTestUtil.NUM_BLOCKS) {
        System.out.println("Number of blocks found " + locations.length);
        done = false;
        continue;
      }
      for (int idx = 0; idx < AppendTestUtil.NUM_BLOCKS; idx++) {
        if (locations[idx].getHosts().length < repl) {
          System.out.println("Block index " + idx + " not yet replciated.");
          done = false;
          break;
        }
      }
    }
    byte[] expected = 
        new byte[AppendTestUtil.NUM_BLOCKS * AppendTestUtil.BLOCK_SIZE];
    System.arraycopy(fileContents, 0, expected, 0, expected.length);
    // do a sanity check. Read the file
    // do not check file status since the file is not yet closed.
    AppendTestUtil.checkFullFile(fileSys, name,
        AppendTestUtil.NUM_BLOCKS * AppendTestUtil.BLOCK_SIZE,
        expected, "Read 1", false);
  }

  @Test
  public void testBreakHardlinksIfNeeded() throws IOException {
    Configuration conf = new HdfsConfiguration();
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    FileSystem fs = cluster.getFileSystem();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    try {
      // create a new file, write to it and close it.
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = AppendTestUtil.createFile(fs, file1, 1);
      writeFile(stm);
      stm.close();

      // Get a handle to the datanode
      DataNode[] dn = cluster.listDataNodes();
      assertTrue(
                 dn.length == 1, "There should be only one datanode but found " + dn.length);

      LocatedBlocks locations = client.getNamenode().getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      List<LocatedBlock> blocks = locations.getLocatedBlocks();
      final FsDatasetSpi<?> fsd = dn[0].getFSDataset();

      //
      // Create hard links for a few of the blocks
      //
      for (int i = 0; i < blocks.size(); i = i + 2) {
        ExtendedBlock b = blocks.get(i).getBlock();
        final File f = FsDatasetTestUtil.getBlockFile(
            fsd, b.getBlockPoolId(), b.getLocalBlock());
        File link = new File(f.toString() + ".link");
        System.out.println("Creating hardlink for File " + f + " to " + link);
        HardLink.createHardLink(f, link);
      }

      // Detach all blocks. This should remove hardlinks (if any)
      for (int i = 0; i < blocks.size(); i++) {
        ExtendedBlock b = blocks.get(i).getBlock();
        System.out.println("breakHardlinksIfNeeded detaching block " + b);
        assertTrue(FsDatasetTestUtil.breakHardlinksIfNeeded(fsd, b),
            "breakHardlinksIfNeeded(" + b + ") should have returned true");
      }

      // Since the blocks were already detached earlier, these calls should
      // return false
      for (int i = 0; i < blocks.size(); i++) {
        ExtendedBlock b = blocks.get(i).getBlock();
        System.out.println("breakHardlinksIfNeeded re-attempting to " +
                "detach block " + b);
        assertTrue(FsDatasetTestUtil.breakHardlinksIfNeeded(fsd, b),
            "breakHardlinksIfNeeded(" + b + ") should have returned false");
      }
    } finally {
      client.close();
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test a simple flush on a simple HDFS file.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testSimpleFlush() throws IOException {
    Configuration conf = new HdfsConfiguration();
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/simpleFlush.dat");
      FSDataOutputStream stm = AppendTestUtil.createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      int mid = AppendTestUtil.FILE_SIZE /2;
      stm.write(fileContents, 0, mid);
      stm.hflush();
      System.out.println("Wrote and Flushed first part of file.");

      // write the remainder of the file
      stm.write(fileContents, mid, AppendTestUtil.FILE_SIZE - mid);
      System.out.println("Written second part of file");
      stm.hflush();
      stm.hflush();
      System.out.println("Wrote and Flushed second part of file.");

      // verify that full blocks are sane
      checkFile(fs, file1, 1);

      stm.close();
      System.out.println("Closed file.");

      // verify that entire file is good
      AppendTestUtil.checkFullFile(fs, file1, AppendTestUtil.FILE_SIZE,
          fileContents, "Read 2");

    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that file data can be flushed.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testComplexFlush() throws IOException {
    Configuration conf = new HdfsConfiguration();
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/complexFlush.dat");
      FSDataOutputStream stm = AppendTestUtil.createFile(fs, file1, 1);
      System.out.println("Created file complexFlush.dat");

      int start = 0;
      for (start = 0; (start + 29) < AppendTestUtil.FILE_SIZE; ) {
        stm.write(fileContents, start, 29);
        stm.hflush();
        start += 29;
      }
      stm.write(fileContents, start, AppendTestUtil.FILE_SIZE -start);
      // need to make sure we completely write out all full blocks before
      // the checkFile() call (see FSOutputSummer#flush)
      stm.flush();
      // verify that full blocks are sane
      checkFile(fs, file1, 1);
      stm.close();

      // verify that entire file is good
      AppendTestUtil.checkFullFile(fs, file1, AppendTestUtil.FILE_SIZE,
          fileContents, "Read 2");
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
 
  /**
   * FileNotFoundException is expected for appending to a non-exisiting file
   * 
   * @throws FileNotFoundException as the result
   */
  @Test
  public void testFileNotFound() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      Configuration conf = new HdfsConfiguration();
      File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
          .build();
      FileSystem fs = cluster.getFileSystem();
      try {
        Path file1 = new Path("/nonexistingfile.dat");
        fs.append(file1);
      } finally {
        fs.close();
        cluster.shutdown();
      }
    });
  }

  /** Test two consecutive appends on a file with a full block. */
  @Test
  public void testAppendTwice() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    final FileSystem fs1 = cluster.getFileSystem();
    final FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(conf);
    try {
  
      final Path p = new Path("/testAppendTwice/foo");
      final int len = 1 << 16;
      final byte[] fileContents = AppendTestUtil.initBuffer(len);

      {
        // create a new file with a full block.
        FSDataOutputStream out = fs2.create(p, true, 4096, (short)1, len);
        out.write(fileContents, 0, len);
        out.close();
      }
  
      //1st append does not add any data so that the last block remains full
      //and the last block in INodeFileUnderConstruction is a BlockInfo
      //but does not have a BlockUnderConstructionFeature.
      fs2.append(p);
      
      //2nd append should get AlreadyBeingCreatedException
      fs1.append(p);
      fail();
    } catch(RemoteException re) {
      AppendTestUtil.LOG.info("Got an exception:", re);
      assertEquals(AlreadyBeingCreatedException.class.getName(),
          re.getClassName());
    } finally {
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
  }

  /** Test two consecutive appends on a file with a full block. */
  @Test
  public void testAppend2Twice() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    final DistributedFileSystem fs1 = cluster.getFileSystem();
    final FileSystem fs2 = AppendTestUtil.createHdfsWithDifferentUsername(conf);
    try {
      final Path p = new Path("/testAppendTwice/foo");
      final int len = 1 << 16;
      final byte[] fileContents = AppendTestUtil.initBuffer(len);

      {
        // create a new file with a full block.
        FSDataOutputStream out = fs2.create(p, true, 4096, (short)1, len);
        out.write(fileContents, 0, len);
        out.close();
      }
  
      //1st append does not add any data so that the last block remains full
      //and the last block in INodeFileUnderConstruction is a BlockInfo
      //but does not have a BlockUnderConstructionFeature.
      ((DistributedFileSystem) fs2).append(p,
          EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);

      // 2nd append should get AlreadyBeingCreatedException
      fs1.append(p);
      fail();
    } catch(RemoteException re) {
      AppendTestUtil.LOG.info("Got an exception:", re);
      assertEquals(AlreadyBeingCreatedException.class.getName(),
          re.getClassName());
    } finally {
      fs2.close();
      fs1.close();
      cluster.shutdown();
    }
  }


  @Test
  public void testMultipleAppends() throws Exception {
    final long startTime = Time.monotonicNow();
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_KEY, 1);
    conf.setBoolean(
        HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
        false);

    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf,
        builderBaseDir).numDataNodes(4).build();
    final DistributedFileSystem fs = cluster.getFileSystem();
    try {
      final Path p = new Path("/testMultipleAppend/foo");
      final int blockSize = 1 << 16;
      final byte[] data = AppendTestUtil.initBuffer(blockSize);

      // create an empty file.
      fs.create(p, true, 4096, (short)3, blockSize).close();

      int fileLen = 0;
      for(int i = 0;
          i < 10 || Time.monotonicNow() - startTime < RANDOM_TEST_RUNTIME;
          i++) {
        int appendLen = ThreadLocalRandom.current().nextInt(100) + 1;
        if (fileLen + appendLen > data.length) {
          break;
        }

        AppendTestUtil.LOG.info(i + ") fileLen="  + fileLen
            + ", appendLen=" + appendLen);
        final FSDataOutputStream out = fs.append(p);
        out.write(data, fileLen, appendLen);
        out.close();
        fileLen += appendLen;
      }

      assertEquals(fileLen, fs.getFileStatus(p).getLen());
      final byte[] actual = new byte[fileLen];
      final FSDataInputStream in = fs.open(p);
      in.readFully(actual);
      in.close();
      for(int i = 0; i < fileLen; i++) {
        assertEquals(data[i], actual[i]);
      }
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /** Tests appending after soft-limit expires. */
  @Test
  public void testAppendAfterSoftLimit() 
      throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    //Set small soft-limit for lease
    final long softLimit = 1L;
    final long hardLimit = 9999999L;

    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(1).build();
    cluster.setLeasePeriod(softLimit, hardLimit);
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    FileSystem fs2 = new DistributedFileSystem();
    fs2.initialize(fs.getUri(), conf);

    final Path testPath = new Path("/testAppendAfterSoftLimit");
    final byte[] fileContents = AppendTestUtil.initBuffer(32);

    // create a new file without closing
    FSDataOutputStream out = fs.create(testPath);
    out.write(fileContents);

    //Wait for > soft-limit
    Thread.sleep(250);

    try {
      FSDataOutputStream appendStream2 = fs2.append(testPath);
      appendStream2.write(fileContents);
      appendStream2.close();
      assertEquals(fileContents.length, fs.getFileStatus(testPath).getLen());
    } finally {
      fs.close();
      fs2.close();
      cluster.shutdown();
    }
  }

  /** Tests appending after soft-limit expires. */
  @Test
  public void testAppend2AfterSoftLimit() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    //Set small soft-limit for lease
    final long softLimit = 1L;
    final long hardLimit = 9999999L;

    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(1).build();
    cluster.setLeasePeriod(softLimit, hardLimit);
    cluster.waitActive();

    DistributedFileSystem fs = cluster.getFileSystem();
    DistributedFileSystem fs2 = new DistributedFileSystem();
    fs2.initialize(fs.getUri(), conf);

    final Path testPath = new Path("/testAppendAfterSoftLimit");
    final byte[] fileContents = AppendTestUtil.initBuffer(32);

    // create a new file without closing
    FSDataOutputStream out = fs.create(testPath);
    out.write(fileContents);

    //Wait for > soft-limit
    Thread.sleep(250);

    try {
      FSDataOutputStream appendStream2 = fs2.append(testPath,
          EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
      appendStream2.write(fileContents);
      appendStream2.close();
      assertEquals(fileContents.length, fs.getFileStatus(testPath).getLen());
      // make sure we now have 1 block since the first writer was revoked
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(testPath.toString(),
          0L);
      assertEquals(1, blks.getLocatedBlocks().size());
      for (LocatedBlock blk : blks.getLocatedBlocks()) {
        assertEquals(fileContents.length, blk.getBlockSize());
      }
    } finally {
      fs.close();
      fs2.close();
      cluster.shutdown();
    }
  }

  /**
   * Old replica of the block should not be accepted as valid for append/read
   */
  @Test
  public void testFailedAppendBlockRejection() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
        "false");
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(3).build();
    DistributedFileSystem fs = null;
    try {
      fs = cluster.getFileSystem();
      Path path = new Path("/test");
      FSDataOutputStream out = fs.create(path);
      out.writeBytes("hello\n");
      out.close();

      // stop one datanode
      DataNodeProperties dnProp = cluster.stopDataNode(0);
      String dnAddress = dnProp.datanode.getXferAddress().toString();
      if (dnAddress.startsWith("/")) {
        dnAddress = dnAddress.substring(1);
      }

      // append again to bump genstamps
      for (int i = 0; i < 2; i++) {
        out = fs.append(path);
        out.writeBytes("helloagain\n");
        out.close();
      }

      // re-open and make the block state as underconstruction
      out = fs.append(path);
      cluster.restartDataNode(dnProp, true);
      // wait till the block report comes
      Thread.sleep(2000);
      // check the block locations, this should not contain restarted datanode
      BlockLocation[] locations = fs.getFileBlockLocations(path, 0,
          Long.MAX_VALUE);
      String[] names = locations[0].getNames();
      for (String node : names) {
        if (node.equals(dnAddress)) {
          fail("Failed append should not be present in latest block locations.");
        }
      }
      out.close();
    } finally {
      IOUtils.closeStream(fs);
      cluster.shutdown();
    }
  }

  /**
   * Old replica of the block should not be accepted as valid for append/read
   */
  @Test
  public void testMultiAppend2() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable",
        "false");
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(3).build();
    DistributedFileSystem fs = null;
    final String hello = "hello\n";
    try {
      fs = cluster.getFileSystem();
      Path path = new Path("/test");
      FSDataOutputStream out = fs.create(path);
      out.writeBytes(hello);
      out.close();

      // stop one datanode
      DataNodeProperties dnProp = cluster.stopDataNode(0);
      String dnAddress = dnProp.datanode.getXferAddress().toString();
      if (dnAddress.startsWith("/")) {
        dnAddress = dnAddress.substring(1);
      }

      // append again to bump genstamps
      for (int i = 0; i < 2; i++) {
        out = fs.append(path,
            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
        out.writeBytes(hello);
        out.close();
      }

      // re-open and make the block state as underconstruction
      out = fs.append(path, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK),
          4096, null);
      cluster.restartDataNode(dnProp, true);
      // wait till the block report comes
      Thread.sleep(2000);
      out.writeBytes(hello);
      out.close();
      // check the block locations
      LocatedBlocks blocks = fs.getClient().getLocatedBlocks(path.toString(), 0L);
      // since we append the file 3 time, we should be 4 blocks
      assertEquals(4, blocks.getLocatedBlocks().size());
      for (LocatedBlock block : blocks.getLocatedBlocks()) {
        assertEquals(hello.length(), block.getBlockSize());
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 4; i++) {
        sb.append(hello);
      }
      final byte[] content = sb.toString().getBytes();
      AppendTestUtil.checkFullFile(fs, path, content.length, content,
          "Read /test");

      // restart namenode to make sure the editlog can be properly applied
      cluster.restartNameNode(true);
      cluster.waitActive();
      AppendTestUtil.checkFullFile(fs, path, content.length, content,
          "Read /test");
      blocks = fs.getClient().getLocatedBlocks(path.toString(), 0L);
      // since we append the file 3 time, we should be 4 blocks
      assertEquals(4, blocks.getLocatedBlocks().size());
      for (LocatedBlock block : blocks.getLocatedBlocks()) {
        assertEquals(hello.length(), block.getBlockSize());
      }
    } finally {
      IOUtils.closeStream(fs);
      cluster.shutdown();
    }
  }
  
  @Test
  @Timeout(value = 10)
  public void testAppendCorruptedBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(1).build();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/appendCorruptBlock");
      DFSTestUtil.createFile(fs, fileName, 512, (short) 1, 0);
      DFSTestUtil.waitReplication(fs, fileName, (short) 1);
      assertTrue(fs.exists(fileName), "File not created");
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
      cluster.corruptBlockOnDataNodes(block);
      DFSTestUtil.appendFile(fs, fileName, "appendCorruptBlock");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 10)
  public void testConcurrentAppendRead()
      throws IOException, TimeoutException, InterruptedException {
    // Create a finalized replica and append to it
    // Read block data and checksum. Verify checksum.
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);

      // create a file with 1 byte of data.
      long initialFileLength = 1;
      DistributedFileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/appendCorruptBlock");
      DFSTestUtil.createFile(fs, fileName, initialFileLength, (short) 1, 0);
      DFSTestUtil.waitReplication(fs, fileName, (short) 1);
      assertTrue(fs.exists(fileName), "File not created");

      // Call FsDatasetImpl#append to append the block file,
      // which converts it to a rbw replica.
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);
      long newGS = block.getGenerationStamp() + 1;
      ReplicaHandler replicaHandler =
          dataSet.append(block, newGS, initialFileLength);

      // write data to block file
      ReplicaBeingWritten rbw =
          (ReplicaBeingWritten)replicaHandler.getReplica();
      ReplicaOutputStreams
          outputStreams = rbw.createStreams(false, DEFAULT_CHECKSUM);
      OutputStream dataOutput = outputStreams.getDataOut();

      byte[] appendBytes = new byte[1];
      dataOutput.write(appendBytes, 0, 1);
      dataOutput.flush();
      dataOutput.close();

      // update checksum file
      final int smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
      FsDatasetUtil.computeChecksum(rbw.getMetaFile(), rbw.getMetaFile(),
          rbw.getBlockFile(), smallBufferSize, conf);

      // read the block
      // the DataNode BlockSender should read from the rbw replica's in-memory
      // checksum, rather than on-disk checksum. Otherwise it will see a
      // checksum mismatch error.
      final byte[] readBlock = DFSTestUtil.readFileBuffer(fs, fileName);
      assertEquals(1, readBlock.length, "should have read only one byte!");
    } finally {
      cluster.shutdown();
    }
  }
}
