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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * this class tests the methods of the  SimulatedFSDataset.
 */
public class TestSimulatedFSDataset {
  Configuration conf = null;
  static final String bpid = "BP-TEST";
  static final int NUMBLOCKS = 20;
  static final int BLOCK_LENGTH_MULTIPLIER = 79;
  static final long FIRST_BLK_ID = 1;

  private int storageCount = 1;

  protected void pTestSimulatedFSDataset(int pStorageCount) {
    this.storageCount = pStorageCount;
  }

  @BeforeEach
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
  }
  
  static long blockIdToLen(long blkid) {
    return blkid * BLOCK_LENGTH_MULTIPLIER;
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset) throws IOException {
    return addSomeBlocks(fsdataset, false);
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset,
      boolean negativeBlkID) throws IOException {
    return addSomeBlocks(fsdataset, FIRST_BLK_ID, negativeBlkID);
  }

  static int addSomeBlocks(SimulatedFSDataset fsdataset, long startingBlockId,
      boolean negativeBlkID) throws IOException {
    int bytesAdded = 0;
    for (long i = startingBlockId; i < startingBlockId+NUMBLOCKS; ++i) {
      long blkID = negativeBlkID ? i * -1 : i;
      ExtendedBlock b = new ExtendedBlock(bpid, blkID, 0, 0);
      // we pass expected len as zero, - fsdataset should use the sizeof actual
      // data written
      ReplicaInPipeline bInfo = fsdataset.createRbw(
          StorageType.DEFAULT, null, b, false).getReplica();
      ReplicaOutputStreams out = bInfo.createStreams(true,
          DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512));
      try {
        OutputStream dataOut  = out.getDataOut();
        assertEquals(0, fsdataset.getLength(b));
        for (int j=1; j <= blockIdToLen(i); ++j) {
          dataOut.write(j);
          assertEquals(j, bInfo.getBytesOnDisk()); // correct length even as we write
          bytesAdded++;
        }
      } finally {
        out.close();
      }
      b.setNumBytes(blockIdToLen(i));
      fsdataset.finalizeBlock(b, false);
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
    }
    return bytesAdded;
  }

  static void readSomeBlocks(SimulatedFSDataset fsdataset,
      boolean negativeBlkID) throws IOException {
    for (long i = FIRST_BLK_ID; i <= NUMBLOCKS; ++i) {
      long blkID = negativeBlkID ? i * -1 : i;
      ExtendedBlock b = new ExtendedBlock(bpid, blkID, 0, 0);
      assertTrue(fsdataset.isValidBlock(b));
      assertEquals(blockIdToLen(i), fsdataset.getLength(b));
      checkBlockDataAndSize(fsdataset, b, blockIdToLen(i));
    }
  }
  
  @Test
  public void testFSDatasetFactory() {
    final Configuration conf = new Configuration();
    FsDatasetSpi.Factory<?> f = FsDatasetSpi.Factory.getFactory(conf);
    assertEquals(FsDatasetFactory.class, f.getClass());
    assertFalse(f.isSimulated());

    SimulatedFSDataset.setFactory(conf);
    FsDatasetSpi.Factory<?> s = FsDatasetSpi.Factory.getFactory(conf);
    assertEquals(SimulatedFSDataset.Factory.class, s.getClass());
    assertTrue(s.isSimulated());
  }

  @Test
  public void testGetMetaData() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, FIRST_BLK_ID, 5, 0);
    try {
      assertTrue(fsdataset.getMetaDataInputStream(b) == null);
      assertTrue(false, "Expected an IO exception");
    } catch (IOException e) {
      // ok - as expected
    }
    addSomeBlocks(fsdataset); // Only need to add one but ....
    b = new ExtendedBlock(bpid, FIRST_BLK_ID, 0, 0);
    InputStream metaInput = fsdataset.getMetaDataInputStream(b);
    DataInputStream metaDataInput = new DataInputStream(metaInput);
    short version = metaDataInput.readShort();
    assertEquals(BlockMetadataHeader.VERSION, version);
    DataChecksum checksum = DataChecksum.newDataChecksum(metaDataInput);
    assertEquals(DataChecksum.Type.NULL, checksum.getChecksumType());
    assertEquals(0, checksum.getChecksumSize());  
  }


  @Test
  public void testStorageUsage() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    assertEquals(fsdataset.getDfsUsed(), 0);
    assertEquals(fsdataset.getRemaining(), fsdataset.getCapacity());
    int bytesAdded = addSomeBlocks(fsdataset);
    assertEquals(bytesAdded, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded,  fsdataset.getRemaining());
  }



  static void checkBlockDataAndSize(SimulatedFSDataset fsdataset,
      ExtendedBlock b, long expectedLen) throws IOException {
    InputStream input = fsdataset.getBlockInputStream(b);
    long lengthRead = 0;
    int data;
    while ((data = input.read()) != -1) {
      assertEquals(SimulatedFSDataset.simulatedByte(b.getLocalBlock(),
          lengthRead), (byte) (data & SimulatedFSDataset.BYTE_MASK));
      lengthRead++;
    }
    assertEquals(expectedLen, lengthRead);
  }

  @Test
  public void testWriteRead() throws IOException {
    testWriteRead(false);
    testWriteRead(true);
  }

  private void testWriteRead(boolean negativeBlkID) throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    addSomeBlocks(fsdataset, negativeBlkID);
    readSomeBlocks(fsdataset, negativeBlkID);
  }

  @Test
  public void testGetBlockReport() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    assertBlockReportCountAndSize(fsdataset, 0);
    addSomeBlocks(fsdataset);
    assertBlockReportCountAndSize(fsdataset, NUMBLOCKS);
    assertBlockLengthInBlockReports(fsdataset);
  }
  
  @Test
  public void testInjectionEmpty() throws IOException {
    SimulatedFSDataset fsdataset = getSimulatedFSDataset(); 
    assertBlockReportCountAndSize(fsdataset, 0);
    int bytesAdded = addSomeBlocks(fsdataset);
    assertBlockReportCountAndSize(fsdataset, NUMBLOCKS);
    assertBlockLengthInBlockReports(fsdataset);
    
    // Inject blocks into an empty fsdataset
    //  - injecting the blocks we got above.
    SimulatedFSDataset sfsdataset = getSimulatedFSDataset();
    injectBlocksFromBlockReport(fsdataset, sfsdataset);
    assertBlockReportCountAndSize(fsdataset, NUMBLOCKS);
    assertBlockLengthInBlockReports(fsdataset, sfsdataset);

    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded, sfsdataset.getRemaining());
  }

  @Test
  public void testInjectionNonEmpty() throws IOException {
    SimulatedFSDataset fsdataset = getSimulatedFSDataset(); 
    assertBlockReportCountAndSize(fsdataset, 0);
    int bytesAdded = addSomeBlocks(fsdataset);
    assertBlockReportCountAndSize(fsdataset, NUMBLOCKS);
    assertBlockLengthInBlockReports(fsdataset);
    
    // Inject blocks into an non-empty fsdataset
    //  - injecting the blocks we got above.
    SimulatedFSDataset sfsdataset = getSimulatedFSDataset();
    // Add come blocks whose block ids do not conflict with
    // the ones we are going to inject.
    bytesAdded += addSomeBlocks(sfsdataset, NUMBLOCKS+1, false);
    assertBlockReportCountAndSize(sfsdataset, NUMBLOCKS);
    injectBlocksFromBlockReport(fsdataset, sfsdataset);
    assertBlockReportCountAndSize(sfsdataset, NUMBLOCKS * 2);
    assertBlockLengthInBlockReports(fsdataset, sfsdataset);
    assertEquals(bytesAdded, sfsdataset.getDfsUsed());
    assertEquals(sfsdataset.getCapacity()-bytesAdded,  sfsdataset.getRemaining());
    
    // Now test that the dataset cannot be created if it does not have sufficient cap
    conf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY, 10);
 
    try {
      sfsdataset = getSimulatedFSDataset();
      sfsdataset.addBlockPool(bpid, conf);
      injectBlocksFromBlockReport(fsdataset, sfsdataset);
      assertTrue(false, "Expected an IO exception");
    } catch (IOException e) {
      // ok - as expected
    }
  }

  public void checkInvalidBlock(ExtendedBlock b) {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    assertFalse(fsdataset.isValidBlock(b));
    try {
      fsdataset.getLength(b);
      assertTrue(false, "Expected an IO exception");
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.getBlockInputStream(b);
      assertTrue(false, "Expected an IO exception");
    } catch (IOException e) {
      // ok - as expected
    }
    
    try {
      fsdataset.finalizeBlock(b, false);
      assertTrue(false, "Expected an IO exception");
    } catch (IOException e) {
      // ok - as expected
    }
  }
  
  @Test
  public void testInValidBlocks() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    ExtendedBlock b = new ExtendedBlock(bpid, FIRST_BLK_ID, 5, 0);
    checkInvalidBlock(b);
    
    // Now check invlaid after adding some blocks
    addSomeBlocks(fsdataset);
    b = new ExtendedBlock(bpid, NUMBLOCKS + 99, 5, 0);
    checkInvalidBlock(b);
  }

  @Test
  public void testInvalidate() throws IOException {
    final SimulatedFSDataset fsdataset = getSimulatedFSDataset();
    int bytesAdded = addSomeBlocks(fsdataset);
    Block[] deleteBlocks = new Block[2];
    deleteBlocks[0] = new Block(1, 0, 0);
    deleteBlocks[1] = new Block(2, 0, 0);
    fsdataset.invalidate(bpid, deleteBlocks);
    checkInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[0]));
    checkInvalidBlock(new ExtendedBlock(bpid, deleteBlocks[1]));
    long sizeDeleted = blockIdToLen(1) + blockIdToLen(2);
    assertEquals(bytesAdded-sizeDeleted, fsdataset.getDfsUsed());
    assertEquals(fsdataset.getCapacity()-bytesAdded+sizeDeleted,  fsdataset.getRemaining());
    
    // Now make sure the rest of the blocks are valid
    for (int i=3; i <= NUMBLOCKS; ++i) {
      Block b = new Block(i, 0, 0);
      assertTrue(fsdataset.isValidBlock(new ExtendedBlock(bpid, b)));
    }
  }

  /**
   * Inject all of the blocks returned from sourceFSDataset's block reports
   * into destinationFSDataset.
   */
  private void injectBlocksFromBlockReport(SimulatedFSDataset sourceFSDataset,
      SimulatedFSDataset destinationFSDataset) throws IOException {
    for (Map.Entry<DatanodeStorage, BlockListAsLongs> ent :
        sourceFSDataset.getBlockReports(bpid).entrySet()) {
      destinationFSDataset.injectBlocks(bpid, ent.getValue());
    }
  }

  /**
   * Assert that the number of block reports returned from fsdataset matches
   * {@code storageCount}, and that the total number of blocks is equal to
   * expectedBlockCount.
   */
  private void assertBlockReportCountAndSize(SimulatedFSDataset fsdataset,
      int expectedBlockCount) {
    Map<DatanodeStorage, BlockListAsLongs> blockReportMap =
        fsdataset.getBlockReports(bpid);
    assertEquals(storageCount, blockReportMap.size());
    int totalCount = 0;
    for (Map.Entry<DatanodeStorage, BlockListAsLongs> ent :
        blockReportMap.entrySet()) {
      totalCount += ent.getValue().getNumberOfBlocks();
    }
    assertEquals(expectedBlockCount, totalCount);
  }

  /**
   * Convenience method to call {@link #assertBlockLengthInBlockReports(
   * SimulatedFSDataset,SimulatedFSDataset)} with a null second parameter.
   */
  private void assertBlockLengthInBlockReports(SimulatedFSDataset fsdataset)
      throws IOException {
    assertBlockLengthInBlockReports(fsdataset, null);
  }

  /**
   * Assert that, for all of the blocks in the block report(s) returned from
   * fsdataset, they are not null and their length matches the expectation.
   * If otherFSDataset is non-null, additionally confirm that its idea of the
   * length of the block matches as well.
   */
  private void assertBlockLengthInBlockReports(SimulatedFSDataset fsdataset,
      SimulatedFSDataset otherFSDataset) throws IOException {
    for (Map.Entry<DatanodeStorage, BlockListAsLongs> ent :
        fsdataset.getBlockReports(bpid).entrySet()) {
      for (Block b : ent.getValue()) {
        assertNotNull(b);
        assertEquals(blockIdToLen(b.getBlockId()), b.getNumBytes());
        if (otherFSDataset != null) {
          assertEquals(blockIdToLen(b.getBlockId()), otherFSDataset
              .getLength(new ExtendedBlock(bpid, b)));
        }
      }
    }
  }

  protected SimulatedFSDataset getSimulatedFSDataset() {
    SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
    fsdataset.addBlockPool(bpid, conf);
    return fsdataset;
  }

  @Test
  public void testConcurrentAddBlockPool() throws InterruptedException,
      IOException {
    final String[] bpids = {"BP-TEST1-", "BP-TEST2-"};
    final SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
    class AddBlockPoolThread extends Thread {
      private int id;
      private IOException ioe;
      public AddBlockPoolThread(int id) {
        super();
        this.id = id;
      }
      public void test() throws InterruptedException, IOException {
        this.join();
        if (ioe != null) {
          throw ioe;
        }
      }
      public void run() {
        for (int i=0; i < 10000; i++) {
          // add different block pools concurrently
          String newbpid = bpids[id] + i;
          fsdataset.addBlockPool(newbpid, conf);
          // and then add a block into the pool
          ExtendedBlock block = new ExtendedBlock(newbpid,1);
          try {
            // it will throw an exception if the block pool is not found
            fsdataset.createTemporary(StorageType.DEFAULT, null, block, false);
          } catch (IOException ioe) {
            // JUnit does not capture exception in non-main thread,
            // so cache it and then let main thread throw later.
            this.ioe = ioe;
          }
          assert(fsdataset.getReplicaString(newbpid,1) != "null");
        }
      }
    };
    AddBlockPoolThread t1 = new AddBlockPoolThread(0);
    AddBlockPoolThread t2 = new AddBlockPoolThread(1);
    t1.start();
    t2.start();
    t1.test();
    t2.test();
  }
}
